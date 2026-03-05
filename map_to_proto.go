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

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// mapToBinary converts a map[string]interface{} directly to proto binary format,
// bypassing the intermediate JSON serialization (json.Marshal + protojson.Unmarshal)
// that the previous jsonToBinary implementation used.
// This eliminates two of the three serialization steps in the hot path.
func mapToBinary(md protoreflect.MessageDescriptor, data map[string]interface{}) ([]byte, error) {
	msg, err := mapToMessage(md, data)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(msg)
}

// mapToMessage populates a dynamicpb.Message directly from a map[string]interface{}.
// It iterates the message descriptor's fields, looks up each field's name in the map,
// and sets the corresponding proto value.
func mapToMessage(md protoreflect.MessageDescriptor, data map[string]interface{}) (*dynamicpb.Message, error) {
	msg := dynamicpb.NewMessage(md)
	fields := md.Fields()

	for key, val := range data {
		if val == nil {
			continue
		}

		fd := findFieldDescriptor(fields, key)
		if fd == nil {
			// Skip unknown fields (consistent with protojson DiscardUnknown behavior)
			continue
		}

		if fd.IsList() {
			if err := setRepeatedField(msg, fd, val); err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
		} else if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			subMap, ok := val.(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("field %q: expected map for message field, got %T", key, val)
			}
			subMsg, err := mapToMessage(fd.Message(), subMap)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
			msg.Set(fd, protoreflect.ValueOfMessage(subMsg))
		} else {
			pv, err := goToProtoScalar(fd, val)
			if err != nil {
				return nil, fmt.Errorf("field %q: %w", key, err)
			}
			msg.Set(fd, pv)
		}
	}
	return msg, nil
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
func setRepeatedField(msg *dynamicpb.Message, fd protoreflect.FieldDescriptor, val interface{}) error {
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
			subMap, ok := item.(map[string]interface{})
			if !ok {
				return fmt.Errorf("element %d: expected map for repeated message, got %T", i, item)
			}
			subMsg, err := mapToMessage(fd.Message(), subMap)
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
		return protoreflect.ValueOfString(string(v)), nil
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
		// Since parseMap converts []byte → string, attempt base64 decode first.
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
	default:
		return protoreflect.Value{}, fmt.Errorf("cannot convert %T to enum", val)
	}
}
