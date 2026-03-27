// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package main

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"unsafe"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// fieldLookupCache maps a proto message's full name to a map of
// field-name → FieldDescriptor. Built once at Init time and shared
// across all Flush calls to avoid per-row reflection lookups.
type fieldLookupCache map[protoreflect.FullName]map[string]protoreflect.FieldDescriptor

// buildFieldLookupCache walks the MessageDescriptor tree and builds
// an O(1) lookup table for every message (including nested STRUCTs).
func buildFieldLookupCache(md protoreflect.MessageDescriptor) fieldLookupCache {
	cache := make(fieldLookupCache)
	buildFieldCacheRecursive(md, cache)
	return cache
}

func buildFieldCacheRecursive(md protoreflect.MessageDescriptor, cache fieldLookupCache) {
	fullName := md.FullName()
	if _, exists := cache[fullName]; exists {
		return
	}
	fields := md.Fields()
	m := make(map[string]protoreflect.FieldDescriptor, fields.Len()*2)
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		m[string(f.Name())] = f
		if jn := f.JSONName(); jn != string(f.Name()) {
			m[jn] = f
		}
		if f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind {
			buildFieldCacheRecursive(f.Message(), cache)
		}
	}
	cache[fullName] = m
}

// marshalBufPool recycles scratch buffers used by proto.MarshalAppend,
// avoiding a fresh heap allocation on every row in the hot path.
// The pool buffer is never returned to callers — data is copied out.
var marshalBufPool = sync.Pool{
	New: func() interface{} {
		buf := make([]byte, 0, 256)
		return &buf
	},
}

// messagePool caches *dynamicpb.Message instances keyed by MessageDescriptor
// identity to avoid per-row heap allocations in the hot path.
// We key by the descriptor itself (not FullName) because dynamicpb.Message.Set
// requires the field descriptor to belong to the exact same MessageDescriptor
// instance that was used to create the message.
var messagePool sync.Map // protoreflect.MessageDescriptor → *sync.Pool

// getPooledMessage retrieves a dynamicpb.Message from the pool,
// or creates a new one if the pool is empty.
func getPooledMessage(md protoreflect.MessageDescriptor) *dynamicpb.Message {
	p, ok := messagePool.Load(md)
	if !ok {
		p, _ = messagePool.LoadOrStore(md, &sync.Pool{
			New: func() interface{} {
				return dynamicpb.NewMessage(md)
			},
		})
	}
	return p.(*sync.Pool).Get().(*dynamicpb.Message)
}

// cleanupMsgPool removes all messagePool entries for the descriptor tree
// rooted at md. Call this when an outputConfig is torn down to prevent
// pool entries (and their associated descriptor references) from accumulating
// across init/exit cycles.
func cleanupMsgPool(md protoreflect.MessageDescriptor) {
	cleanupMsgPoolRecursive(md, make(map[protoreflect.MessageDescriptor]bool))
}

func cleanupMsgPoolRecursive(md protoreflect.MessageDescriptor, visited map[protoreflect.MessageDescriptor]bool) {
	if visited[md] {
		return
	}
	visited[md] = true
	messagePool.Delete(md)
	fields := md.Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.Kind() == protoreflect.MessageKind || f.Kind() == protoreflect.GroupKind {
			cleanupMsgPoolRecursive(f.Message(), visited)
		}
	}
}

// putPooledMessage clears all populated fields and returns the message to the pool.
// Clearing via Range+Clear preserves internal map bucket memory so that
// subsequent reuse avoids re-growing the map.
// Sub-messages (RECORD fields) are recursively returned to the pool before
// their parent field is cleared, enabling full reuse across the message tree.
func putPooledMessage(msg *dynamicpb.Message) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			if fd.IsList() {
				list := v.List()
				for i := 0; i < list.Len(); i++ {
					if subMsg, ok := list.Get(i).Message().(*dynamicpb.Message); ok {
						putPooledMessage(subMsg)
					}
				}
			} else {
				if subMsg, ok := v.Message().(*dynamicpb.Message); ok {
					putPooledMessage(subMsg)
				}
			}
		}
		msg.Clear(fd)
		return true
	})
	if p, ok := messagePool.Load(msg.Descriptor()); ok {
		p.(*sync.Pool).Put(msg)
	}
}

// marshalAndRelease marshals a pooled dynamicpb.Message to bytes,
// then returns the message to the pool. Handles buffer pooling internally.
func marshalAndRelease(msg *dynamicpb.Message) ([]byte, error) {
	bufp := marshalBufPool.Get().(*[]byte)
	b, err := proto.MarshalOptions{}.MarshalAppend((*bufp)[:0], msg)

	// Return message to pool (clears fields, preserves map capacity)
	putPooledMessage(msg)

	if err != nil {
		*bufp = b
		marshalBufPool.Put(bufp)
		return nil, err
	}

	result := make([]byte, len(b))
	copy(result, b)

	*bufp = b // preserve grown capacity for next use
	marshalBufPool.Put(bufp)

	return result, nil
}

// mapToBinary converts a map[string]interface{} directly to proto binary format,
// bypassing intermediate JSON serialization for better performance.
//
// cache may be nil — in that case every field lookup falls back to
// ByName + linear JSON-name scan (still correct, just slower).
func mapToBinary(md protoreflect.MessageDescriptor, data map[string]interface{}, cache fieldLookupCache) ([]byte, error) {
	// Get a pooled top-level message to avoid per-row allocation
	msg := getPooledMessage(md)

	if err := populateMessage(msg, md, data, cache); err != nil {
		putPooledMessage(msg)
		return nil, err
	}

	return marshalAndRelease(msg)
}

// rawMapToBinary converts a map[interface{}]interface{} (raw Fluent Bit record)
// directly to proto binary, avoiding intermediate map allocation.
// It also uses a pooled top-level message
func rawMapToBinary(md protoreflect.MessageDescriptor, rawData map[interface{}]interface{}, cache fieldLookupCache) ([]byte, error) {
	msg := getPooledMessage(md)

	if err := rawPopulateMessage(msg, md, rawData, cache); err != nil {
		putPooledMessage(msg)
		return nil, err
	}

	return marshalAndRelease(msg)
}

// mapToMessage retrieves a pooled dynamicpb.Message and populates it from a map[string]interface{}.
// Used for nested sub-messages. The caller must not release the returned message directly;
// it will be recursively released when the top-level message is passed to marshalAndRelease.
func mapToMessage(md protoreflect.MessageDescriptor, data map[string]interface{}, cache fieldLookupCache) (*dynamicpb.Message, error) {
	msg := getPooledMessage(md)
	if err := populateMessage(msg, md, data, cache); err != nil {
		putPooledMessage(msg)
		return nil, err
	}
	return msg, nil
}

// toSubMessage converts val (map[string]interface{} or map[interface{}]interface{})
// into a populated *dynamicpb.Message using a pooled message.
// It is the single authoritative path for building sub-messages from either the
// typed (mapToBinary) or raw (rawMapToBinary) Fluent Bit record paths.
func toSubMessage(val interface{}, md protoreflect.MessageDescriptor, cache fieldLookupCache) (*dynamicpb.Message, error) {
	switch sub := val.(type) {
	case map[interface{}]interface{}:
		subMsg := getPooledMessage(md)
		if err := rawPopulateMessage(subMsg, md, sub, cache); err != nil {
			putPooledMessage(subMsg)
			return nil, err
		}
		return subMsg, nil
	case map[string]interface{}:
		return mapToMessage(md, sub, cache)
	default:
		return nil, fmt.Errorf("expected map for message field, got %T", val)
	}
}

// populateMessage fills an existing dynamicpb.Message from a map[string]interface{}.
// Extracted from mapToMessage so that callers can supply a pooled message
func populateMessage(msg *dynamicpb.Message, md protoreflect.MessageDescriptor, data map[string]interface{}, cache fieldLookupCache) error {
	// Use pre-built field map when available (O(1) per key).
	var fieldMap map[string]protoreflect.FieldDescriptor
	if cache != nil {
		fieldMap = cache[md.FullName()]
	}

	for key, val := range data {
		if val == nil {
			continue
		}

		var fd protoreflect.FieldDescriptor
		if fieldMap != nil {
			fd = fieldMap[key]
		} else {
			fd = findFieldDescriptor(md.Fields(), key)
		}
		if fd == nil {
			// Skip unknown fields (consistent with protojson DiscardUnknown behavior)
			continue
		}

		if fd.IsList() {
			if err := setRepeatedField(msg, fd, val, cache); err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
		} else if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			subMsg, err := toSubMessage(val, fd.Message(), cache)
			if err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			msg.Set(fd, protoreflect.ValueOfMessage(subMsg))
		} else {
			pv, err := goToProtoScalar(fd, val)
			if err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			msg.Set(fd, pv)
		}
	}
	return nil
}

// rawPopulateMessage fills a dynamicpb.Message directly from a raw Fluent Bit
// record (map[interface{}]interface{}), handling key and value type conversions
// inline.
func rawPopulateMessage(msg *dynamicpb.Message, md protoreflect.MessageDescriptor, rawData map[interface{}]interface{}, cache fieldLookupCache) error {
	var fieldMap map[string]protoreflect.FieldDescriptor
	if cache != nil {
		fieldMap = cache[md.FullName()]
	}

	for k, val := range rawData {
		if val == nil {
			continue
		}

		// Fluent Bit msgpack keys are always string-typed
		key, ok := k.(string)
		if !ok {
			continue
		}

		var fd protoreflect.FieldDescriptor
		if fieldMap != nil {
			fd = fieldMap[key]
		} else {
			fd = findFieldDescriptor(md.Fields(), key)
		}
		if fd == nil {
			continue
		}

		if fd.IsList() {
			if err := setRepeatedField(msg, fd, val, cache); err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
		} else if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			subMsg, err := toSubMessage(val, fd.Message(), cache)
			if err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			msg.Set(fd, protoreflect.ValueOfMessage(subMsg))
		} else {
			pv, err := goToProtoScalar(fd, val)
			if err != nil {
				return fmt.Errorf("field %q: %w", key, err)
			}
			msg.Set(fd, pv)
		}
	}
	return nil
}

// findFieldDescriptor looks up a field descriptor by name.
// It first tries an exact match on the proto field name, then falls back
// to matching by JSON name (which the adapt library sets to the original
// BigQuery column name).
func findFieldDescriptor(fields protoreflect.FieldDescriptors, key string) protoreflect.FieldDescriptor {
	// Fast path: exact match on proto field name
	fd := fields.ByName(protoreflect.Name(key))
	if fd != nil {
		return fd
	}
	// Fallback: match by JSON name (handles cases where proto name differs
	// from the original BigQuery column name)
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.JSONName() == key {
			return f
		}
	}
	return nil
}

// setRepeatedField populates a repeated (list) proto field from a Go slice.
func setRepeatedField(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, val interface{}, cache fieldLookupCache) error {
	slice, ok := val.([]interface{})
	if !ok {
		return fmt.Errorf("expected []interface{} for repeated field, got %T", val)
	}
	if len(slice) == 0 {
		return nil
	}

	list := msg.Mutable(fd).List()
	for i, item := range slice {
		if item == nil {
			continue
		}
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			subMsg, err := toSubMessage(item, fd.Message(), cache)
			if err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			list.Append(protoreflect.ValueOfMessage(subMsg))
		} else {
			pv, err := goToProtoScalar(fd, item)
			if err != nil {
				return fmt.Errorf("element %d: %w", i, err)
			}
			list.Append(pv)
		}
	}
	return nil
}

// goToProtoScalar converts a Go interface{} value to the appropriate
// protoreflect.Value for the given field descriptor's kind.
// Handles all proto scalar types that appear in BigQuery Storage Write API schemas.
func goToProtoScalar(fd protoreflect.FieldDescriptor, val interface{}) (protoreflect.Value, error) {
	switch fd.Kind() {
	case protoreflect.StringKind:
		return toProtoString(val)
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return toProtoInt64(val)
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return toProtoInt32(val)
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return toProtoUint64(val)
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return toProtoUint32(val)
	case protoreflect.DoubleKind:
		return toProtoDouble(val)
	case protoreflect.FloatKind:
		return toProtoFloat(val)
	case protoreflect.BoolKind:
		return toProtoBool(val)
	case protoreflect.BytesKind:
		return toProtoBytes(val)
	case protoreflect.EnumKind:
		return toProtoEnum(val)
	default:
		return protoreflect.Value{}, fmt.Errorf("unsupported proto kind: %v", fd.Kind())
	}
}

func toProtoString(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case string:
		return protoreflect.ValueOfString(v), nil
	case []byte:
		return protoreflect.ValueOfString(unsafe.String(unsafe.SliceData(v), len(v))), nil
	case int:
		return protoreflect.ValueOfString(strconv.Itoa(v)), nil
	case int64:
		return protoreflect.ValueOfString(strconv.FormatInt(v, 10)), nil
	case uint64:
		return protoreflect.ValueOfString(strconv.FormatUint(v, 10)), nil
	case float64:
		return protoreflect.ValueOfString(strconv.FormatFloat(v, 'f', -1, 64)), nil
	case bool:
		return protoreflect.ValueOfString(strconv.FormatBool(v)), nil
	default:
		return protoreflect.ValueOfString(fmt.Sprintf("%v", v)), nil
	}
}

func toProtoInt64(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case int64:
		return protoreflect.ValueOfInt64(v), nil
	case int:
		return protoreflect.ValueOfInt64(int64(v)), nil
	case int32:
		return protoreflect.ValueOfInt64(int64(v)), nil
	case uint64:
		return protoreflect.ValueOfInt64(int64(v)), nil
	case uint32:
		return protoreflect.ValueOfInt64(int64(v)), nil
	case float64:
		return protoreflect.ValueOfInt64(int64(v)), nil
	case float32:
		return protoreflect.ValueOfInt64(int64(v)), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 64)
		if err != nil {
			// Try parsing as float then truncating
			f, ferr := strconv.ParseFloat(v, 64)
			if ferr != nil {
				return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to int64: %w", v, err)
			}
			return protoreflect.ValueOfInt64(int64(f)), nil
		}
		return protoreflect.ValueOfInt64(i), nil
	case []byte:
		s := unsafe.String(unsafe.SliceData(v), len(v))
		i, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			f, ferr := strconv.ParseFloat(s, 64)
			if ferr != nil {
				return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to int64: %w", v, err)
			}
			return protoreflect.ValueOfInt64(int64(f)), nil
		}
		return protoreflect.ValueOfInt64(i), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to int64", val)
	}
}

func toProtoInt32(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case int32:
		return protoreflect.ValueOfInt32(v), nil
	case int:
		return protoreflect.ValueOfInt32(int32(v)), nil
	case int64:
		return protoreflect.ValueOfInt32(int32(v)), nil
	case uint32:
		return protoreflect.ValueOfInt32(int32(v)), nil
	case uint64:
		return protoreflect.ValueOfInt32(int32(v)), nil
	case float64:
		return protoreflect.ValueOfInt32(int32(v)), nil
	case float32:
		return protoreflect.ValueOfInt32(int32(v)), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to int32: %w", v, err)
		}
		return protoreflect.ValueOfInt32(int32(i)), nil
	case []byte:
		i, err := strconv.ParseInt(unsafe.String(unsafe.SliceData(v), len(v)), 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to int32: %w", v, err)
		}
		return protoreflect.ValueOfInt32(int32(i)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to int32", val)
	}
}

func toProtoUint64(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case uint64:
		return protoreflect.ValueOfUint64(v), nil
	case uint32:
		return protoreflect.ValueOfUint64(uint64(v)), nil
	case int:
		return protoreflect.ValueOfUint64(uint64(v)), nil
	case int64:
		return protoreflect.ValueOfUint64(uint64(v)), nil
	case float64:
		return protoreflect.ValueOfUint64(uint64(v)), nil
	case string:
		u, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to uint64: %w", v, err)
		}
		return protoreflect.ValueOfUint64(u), nil
	case []byte:
		u, err := strconv.ParseUint(unsafe.String(unsafe.SliceData(v), len(v)), 10, 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to uint64: %w", v, err)
		}
		return protoreflect.ValueOfUint64(u), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to uint64", val)
	}
}

func toProtoUint32(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case uint32:
		return protoreflect.ValueOfUint32(v), nil
	case uint64:
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case int:
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case int64:
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case float64:
		return protoreflect.ValueOfUint32(uint32(v)), nil
	case string:
		u, err := strconv.ParseUint(v, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to uint32: %w", v, err)
		}
		return protoreflect.ValueOfUint32(uint32(u)), nil
	case []byte:
		u, err := strconv.ParseUint(unsafe.String(unsafe.SliceData(v), len(v)), 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to uint32: %w", v, err)
		}
		return protoreflect.ValueOfUint32(uint32(u)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to uint32", val)
	}
}

func toProtoDouble(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case float64:
		return protoreflect.ValueOfFloat64(v), nil
	case float32:
		return protoreflect.ValueOfFloat64(float64(v)), nil
	case int:
		return protoreflect.ValueOfFloat64(float64(v)), nil
	case int64:
		return protoreflect.ValueOfFloat64(float64(v)), nil
	case uint64:
		return protoreflect.ValueOfFloat64(float64(v)), nil
	case string:
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to float64: %w", v, err)
		}
		return protoreflect.ValueOfFloat64(f), nil
	case []byte:
		f, err := strconv.ParseFloat(unsafe.String(unsafe.SliceData(v), len(v)), 64)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to float64: %w", v, err)
		}
		return protoreflect.ValueOfFloat64(f), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to float64", val)
	}
}

func toProtoFloat(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case float32:
		return protoreflect.ValueOfFloat32(v), nil
	case float64:
		return protoreflect.ValueOfFloat32(float32(v)), nil
	case int:
		return protoreflect.ValueOfFloat32(float32(v)), nil
	case int64:
		return protoreflect.ValueOfFloat32(float32(v)), nil
	case string:
		f, err := strconv.ParseFloat(v, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to float32: %w", v, err)
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	case []byte:
		f, err := strconv.ParseFloat(unsafe.String(unsafe.SliceData(v), len(v)), 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to float32: %w", v, err)
		}
		return protoreflect.ValueOfFloat32(float32(f)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to float32", val)
	}
}

func toProtoBool(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case bool:
		return protoreflect.ValueOfBool(v), nil
	case string:
		b, err := strconv.ParseBool(v)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to bool: %w", v, err)
		}
		return protoreflect.ValueOfBool(b), nil
	case int:
		return protoreflect.ValueOfBool(v != 0), nil
	case int64:
		return protoreflect.ValueOfBool(v != 0), nil
	case uint64:
		return protoreflect.ValueOfBool(v != 0), nil
	case float64:
		return protoreflect.ValueOfBool(v != 0), nil
	case []byte:
		b, err := strconv.ParseBool(unsafe.String(unsafe.SliceData(v), len(v)))
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to bool: %w", v, err)
		}
		return protoreflect.ValueOfBool(b), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to bool", val)
	}
}

func toProtoBytes(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case []byte:
		return protoreflect.ValueOfBytes(v), nil
	case string:
		// protojson expects base64-encoded strings for bytes fields.
		// Since Fluent Bit's msgpack decoder may produce []byte for string values,
		// attempt base64 decode first.
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			// Try URL-safe base64
			b, err = base64.URLEncoding.DecodeString(v)
			if err != nil {
				// Fall back to raw string bytes
				return protoreflect.ValueOfBytes([]byte(v)), nil
			}
		}
		return protoreflect.ValueOfBytes(b), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to bytes", val)
	}
}

func toProtoEnum(val interface{}) (protoreflect.Value, error) {
	switch v := val.(type) {
	case int:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)), nil
	case int32:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)), nil
	case int64:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)), nil
	case float64:
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(int32(v))), nil
	case string:
		i, err := strconv.ParseInt(v, 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert string %q to enum: %w", v, err)
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(i)), nil
	case []byte:
		i, err := strconv.ParseInt(unsafe.String(unsafe.SliceData(v), len(v)), 10, 32)
		if err != nil {
			return protoreflect.Value{}, fmt.Errorf("cannot convert []byte %q to enum: %w", v, err)
		}
		return protoreflect.ValueOfEnum(protoreflect.EnumNumber(i)), nil
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to enum", val)
	}
}
