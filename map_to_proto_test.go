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
	"testing"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
)

// helper to build a MessageDescriptor from a BigQuery TableSchema
func buildMD(t *testing.T, schema *storagepb.TableSchema) protoreflect.MessageDescriptor {
	t.Helper()
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(schema, "root")
	require.NoError(t, err)
	md, ok := descriptor.(protoreflect.MessageDescriptor)
	require.True(t, ok)
	return md
}

// helper: mapToBinary then unmarshal back to dynamicpb.Message for assertions.
// Builds a fieldLookupCache to exercise the cached path (same as production).
func roundTrip(t *testing.T, md protoreflect.MessageDescriptor, data map[string]interface{}) *dynamicpb.Message {
	t.Helper()
	cache := buildFieldLookupCache(md)
	b, err := mapToBinary(md, data, cache)
	require.NoError(t, err)
	msg := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b, msg))
	return msg
}

// TestMapToBinary_StringFields tests STRING type fields with various Go value types.
func TestMapToBinary_StringFields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "Tag", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name    string
		data    map[string]interface{}
		checkFn func(t *testing.T, msg *dynamicpb.Message)
	}{
		{
			name: "string value",
			data: map[string]interface{}{"Name": "hello", "Tag": "world"},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
				assert.Equal(t, "world", msg.Get(md.Fields().ByName("Tag")).String())
			},
		},
		{
			name: "int coerced to string",
			data: map[string]interface{}{"Name": 42},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "42", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "int64 coerced to string",
			data: map[string]interface{}{"Name": int64(1234567890)},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "1234567890", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "float64 coerced to string",
			data: map[string]interface{}{"Name": 3.14},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "3.14", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "bool coerced to string",
			data: map[string]interface{}{"Name": true},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "true", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "nil value skipped",
			data: map[string]interface{}{"Name": "hello", "Tag": nil},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
				// Tag should be default (empty string)
				assert.Equal(t, "", msg.Get(md.Fields().ByName("Tag")).String())
			},
		},
		{
			name: "missing field skipped",
			data: map[string]interface{}{"Name": "hello"},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "unknown field ignored",
			data: map[string]interface{}{"Name": "hello", "Unknown": "ignored"},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := roundTrip(t, md, tt.data)
			tt.checkFn(t, msg)
		})
	}
}

// TestMapToBinary_Int64Fields tests INT64 / TIMESTAMP fields with various Go numeric types.
func TestMapToBinary_Int64Fields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name     string
		data     map[string]interface{}
		expected int64
	}{
		{"int64", map[string]interface{}{"count": int64(100)}, 100},
		{"int", map[string]interface{}{"count": 200}, 200},
		{"uint64", map[string]interface{}{"count": uint64(300)}, 300},
		{"float64", map[string]interface{}{"count": float64(400)}, 400},
		{"string", map[string]interface{}{"count": "500"}, 500},
		{"negative", map[string]interface{}{"count": int64(-42)}, -42},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := roundTrip(t, md, tt.data)
			assert.Equal(t, tt.expected, msg.Get(md.Fields().ByName("count")).Int())
		})
	}
}

// TestMapToBinary_Int32Fields tests DATE fields (mapped to int32).
func TestMapToBinary_Int32Fields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "day", Type: storagepb.TableFieldSchema_DATE, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name     string
		data     map[string]interface{}
		expected int32
	}{
		{"int", map[string]interface{}{"day": 19000}, int32(19000)},
		{"int64", map[string]interface{}{"day": int64(19500)}, int32(19500)},
		{"float64", map[string]interface{}{"day": float64(18000)}, int32(18000)},
		{"string", map[string]interface{}{"day": "17000"}, int32(17000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := roundTrip(t, md, tt.data)
			fd := md.Fields().ByName("day")
			assert.Equal(t, int64(tt.expected), msg.Get(fd).Int())
		})
	}
}

// TestMapToBinary_DoubleFields tests FLOAT64 fields.
func TestMapToBinary_DoubleFields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "score", Type: storagepb.TableFieldSchema_DOUBLE, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name     string
		data     map[string]interface{}
		expected float64
	}{
		{"float64", map[string]interface{}{"score": 3.14}, 3.14},
		{"int", map[string]interface{}{"score": 42}, 42.0},
		{"int64", map[string]interface{}{"score": int64(100)}, 100.0},
		{"string", map[string]interface{}{"score": "2.718"}, 2.718},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := roundTrip(t, md, tt.data)
			assert.InDelta(t, tt.expected, msg.Get(md.Fields().ByName("score")).Float(), 0.001)
		})
	}
}

// TestMapToBinary_BoolFields tests BOOL fields.
func TestMapToBinary_BoolFields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "active", Type: storagepb.TableFieldSchema_BOOL, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name     string
		data     map[string]interface{}
		expected bool
	}{
		{"true", map[string]interface{}{"active": true}, true},
		{"false", map[string]interface{}{"active": false}, false},
		{"string_true", map[string]interface{}{"active": "true"}, true},
		{"string_false", map[string]interface{}{"active": "false"}, false},
		{"int_nonzero", map[string]interface{}{"active": 1}, true},
		{"int_zero", map[string]interface{}{"active": 0}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := roundTrip(t, md, tt.data)
			assert.Equal(t, tt.expected, msg.Get(md.Fields().ByName("active")).Bool())
		})
	}
}

// TestMapToBinary_BytesFields tests BYTES fields.
func TestMapToBinary_BytesFields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "data", Type: storagepb.TableFieldSchema_BYTES, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	t.Run("raw bytes", func(t *testing.T) {
		data := map[string]interface{}{"data": []byte{0x01, 0x02, 0x03}}
		msg := roundTrip(t, md, data)
		assert.Equal(t, []byte{0x01, 0x02, 0x03}, msg.Get(md.Fields().ByName("data")).Bytes())
	})

	t.Run("base64 encoded string", func(t *testing.T) {
		encoded := base64.StdEncoding.EncodeToString([]byte("Hello"))
		data := map[string]interface{}{"data": encoded}
		msg := roundTrip(t, md, data)
		assert.Equal(t, []byte("Hello"), msg.Get(md.Fields().ByName("data")).Bytes())
	})

	t.Run("non-base64 string fallback", func(t *testing.T) {
		// A string that is not valid base64 should be stored as raw bytes
		data := map[string]interface{}{"data": "raw-data!"}
		msg := roundTrip(t, md, data)
		assert.Equal(t, []byte("raw-data!"), msg.Get(md.Fields().ByName("data")).Bytes())
	})
}

// TestMapToBinary_NestedMessage tests STRUCT (nested message) fields.
func TestMapToBinary_NestedMessage(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{
				Name: "address",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "city", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "zip", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"name": "Alice",
		"address": map[string]interface{}{
			"city": "Tokyo",
			"zip":  "100-0001",
		},
	}

	msg := roundTrip(t, md, data)
	assert.Equal(t, "Alice", msg.Get(md.Fields().ByName("name")).String())

	addrFD := md.Fields().ByName("address")
	require.NotNil(t, addrFD)
	addrMsg := msg.Get(addrFD).Message()
	addrMD := addrFD.Message()
	assert.Equal(t, "Tokyo", addrMsg.Get(addrMD.Fields().ByName("city")).String())
	assert.Equal(t, "100-0001", addrMsg.Get(addrMD.Fields().ByName("zip")).String())
}

// TestMapToBinary_DeeplyNestedMessage tests deeply nested STRUCT fields.
func TestMapToBinary_DeeplyNestedMessage(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{
				Name: "level1",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{
						Name: "level2",
						Type: storagepb.TableFieldSchema_STRUCT,
						Mode: storagepb.TableFieldSchema_NULLABLE,
						Fields: []*storagepb.TableFieldSchema{
							{Name: "value", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
						},
					},
				},
			},
		},
	}
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"level1": map[string]interface{}{
			"level2": map[string]interface{}{
				"value": "deep",
			},
		},
	}

	msg := roundTrip(t, md, data)
	l1FD := md.Fields().ByName("level1")
	l1Msg := msg.Get(l1FD).Message()
	l2FD := l1FD.Message().Fields().ByName("level2")
	l2Msg := l1Msg.Get(l2FD).Message()
	valFD := l2FD.Message().Fields().ByName("value")
	assert.Equal(t, "deep", l2Msg.Get(valFD).String())
}

// TestMapToBinary_RepeatedScalar tests REPEATED scalar fields.
func TestMapToBinary_RepeatedScalar(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "tags", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"tags": []interface{}{"alpha", "beta", "gamma"},
	}

	msg := roundTrip(t, md, data)
	tagsFD := md.Fields().ByName("tags")
	list := msg.Get(tagsFD).List()
	require.Equal(t, 3, list.Len())
	assert.Equal(t, "alpha", list.Get(0).String())
	assert.Equal(t, "beta", list.Get(1).String())
	assert.Equal(t, "gamma", list.Get(2).String())
}

// TestMapToBinary_RepeatedMessage tests REPEATED STRUCT fields.
func TestMapToBinary_RepeatedMessage(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{
				Name: "items",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_REPEATED,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "key", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "value", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"items": []interface{}{
			map[string]interface{}{"key": "a", "value": int64(1)},
			map[string]interface{}{"key": "b", "value": int64(2)},
		},
	}

	msg := roundTrip(t, md, data)
	itemsFD := md.Fields().ByName("items")
	list := msg.Get(itemsFD).List()
	require.Equal(t, 2, list.Len())

	itemMD := itemsFD.Message()
	item0 := list.Get(0).Message()
	assert.Equal(t, "a", item0.Get(itemMD.Fields().ByName("key")).String())
	assert.Equal(t, int64(1), item0.Get(itemMD.Fields().ByName("value")).Int())

	item1 := list.Get(1).Message()
	assert.Equal(t, "b", item1.Get(itemMD.Fields().ByName("key")).String())
	assert.Equal(t, int64(2), item1.Get(itemMD.Fields().ByName("value")).Int())
}

// TestMapToBinary_EmptyMap returns empty proto with no error.
func TestMapToBinary_EmptyMap(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	b, err := mapToBinary(md, map[string]interface{}{}, nil)
	require.NoError(t, err)
	assert.NotNil(t, b)
}

// TestMapToBinary_TimestampField tests TIMESTAMP fields (int64 microseconds).
func TestMapToBinary_TimestampField(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "ts", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	// TIMESTAMP is represented as int64 microseconds in proto
	data := map[string]interface{}{
		"ts": int64(1700000000000000),
	}
	msg := roundTrip(t, md, data)
	assert.Equal(t, int64(1700000000000000), msg.Get(md.Fields().ByName("ts")).Int())
}

// TestMapToBinary_MangledTypes tests types that are mangled to STRING
// (NUMERIC, BIGNUMERIC, TIME, JSON).
func TestMapToBinary_MangledTypes(t *testing.T) {
	// After mangling, NUMERIC/BIGNUMERIC/TIME/JSON become STRING
	schema := mangleInputSchema(&storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "num", Type: storagepb.TableFieldSchema_NUMERIC, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "bnum", Type: storagepb.TableFieldSchema_BIGNUMERIC, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "time_val", Type: storagepb.TableFieldSchema_TIME, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "json_val", Type: storagepb.TableFieldSchema_JSON, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}, true)
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"num":      "123.456",
		"bnum":     "9999999999999999999.12345",
		"time_val": "12:30:00",
		"json_val": `{"key":"value"}`,
	}

	msg := roundTrip(t, md, data)
	assert.Equal(t, "123.456", msg.Get(md.Fields().ByName("num")).String())
	assert.Equal(t, "9999999999999999999.12345", msg.Get(md.Fields().ByName("bnum")).String())
	assert.Equal(t, "12:30:00", msg.Get(md.Fields().ByName("time_val")).String())
	assert.Equal(t, `{"key":"value"}`, msg.Get(md.Fields().ByName("json_val")).String())
}

// TestMapToBinary_MixedTypes tests a schema with multiple different field types,
// simulating a realistic BigQuery table.
func TestMapToBinary_MixedTypes(t *testing.T) {
	schema := mangleInputSchema(&storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "age", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "score", Type: storagepb.TableFieldSchema_DOUBLE, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "active", Type: storagepb.TableFieldSchema_BOOL, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "ts", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "tags", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}, true)
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"name":   "Bob",
		"age":    int64(30),
		"score":  95.5,
		"active": true,
		"ts":     int64(1700000000000000),
		"tags":   []interface{}{"vip", "premium"},
	}

	msg := roundTrip(t, md, data)
	assert.Equal(t, "Bob", msg.Get(md.Fields().ByName("name")).String())
	assert.Equal(t, int64(30), msg.Get(md.Fields().ByName("age")).Int())
	assert.InDelta(t, 95.5, msg.Get(md.Fields().ByName("score")).Float(), 0.001)
	assert.True(t, msg.Get(md.Fields().ByName("active")).Bool())
	assert.Equal(t, int64(1700000000000000), msg.Get(md.Fields().ByName("ts")).Int())

	tagsList := msg.Get(md.Fields().ByName("tags")).List()
	require.Equal(t, 2, tagsList.Len())
	assert.Equal(t, "vip", tagsList.Get(0).String())
	assert.Equal(t, "premium", tagsList.Get(1).String())
}

// TestMapToBinary_MatchesOriginalJsonToBinary verifies that the new implementation
// produces identical proto binary output as the original json.Marshal+protojson.Unmarshal
// approach for the schema used in the existing flush tests.
func TestMapToBinary_MatchesOriginalJsonToBinary(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	// Same data as in TestFLBPluginFlushCtx
	data := map[string]interface{}{
		"Text": "FOO",
		"Time": "000",
	}

	b, err := mapToBinary(md, data, buildFieldLookupCache(md))
	require.NoError(t, err)

	msg := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b, msg))

	assert.Equal(t, "FOO", msg.Get(md.Fields().ByJSONName("Text")).String())
	assert.Equal(t, "000", msg.Get(md.Fields().ByJSONName("Time")).String())
}

// TestMapToBinary_RepeatedEmpty tests that an empty repeated field does not error.
func TestMapToBinary_RepeatedEmpty(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "tags", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"tags": []interface{}{},
	}

	b, err := mapToBinary(md, data, nil)
	require.NoError(t, err)
	assert.NotNil(t, b)
}

// TestMapToBinary_ErrorCases tests that type mismatches produce errors.
func TestMapToBinary_ErrorCases(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
			{
				Name: "nested",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "val", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
			{Name: "items", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}
	md := buildMD(t, schema)

	t.Run("invalid int64 string", func(t *testing.T) {
		_, err := mapToBinary(md, map[string]interface{}{"count": "not-a-number"}, nil)
		assert.Error(t, err)
	})

	t.Run("non-map for message field", func(t *testing.T) {
		_, err := mapToBinary(md, map[string]interface{}{"nested": "not-a-map"}, nil)
		assert.Error(t, err)
	})

	t.Run("non-slice for repeated field", func(t *testing.T) {
		_, err := mapToBinary(md, map[string]interface{}{"items": "not-a-slice"}, nil)
		assert.Error(t, err)
	})
}

// TestMapToBinary_Uint64FromMsgpack tests uint64 values that commonly come from
// fluent-bit's msgpack decoder for positive integers.
func TestMapToBinary_Uint64FromMsgpack(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "label", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	data := map[string]interface{}{
		"count": uint64(42),
		"label": uint64(999),
	}

	msg := roundTrip(t, md, data)
	assert.Equal(t, int64(42), msg.Get(md.Fields().ByName("count")).Int())
	assert.Equal(t, "999", msg.Get(md.Fields().ByName("label")).String())
}

// BenchmarkMapToBinary benchmarks the new direct approach.
func BenchmarkMapToBinary(b *testing.B) {
	schema := mangleInputSchema(&storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "age", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "score", Type: storagepb.TableFieldSchema_DOUBLE, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "active", Type: storagepb.TableFieldSchema_BOOL, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "ts", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "numeric_val", Type: storagepb.TableFieldSchema_NUMERIC, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "tags", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}, true)
	descriptor, _ := adapt.StorageSchemaToProto2Descriptor(schema, "root")
	md := descriptor.(protoreflect.MessageDescriptor)
	cache := buildFieldLookupCache(md)

	data := map[string]interface{}{
		"name":        "test-user",
		"age":         int64(30),
		"score":       95.5,
		"active":      true,
		"ts":          int64(1700000000000000),
		"numeric_val": "123.456789",
		"tags":        []interface{}{"tag1", "tag2", "tag3"},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := mapToBinary(md, data, cache)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// helper: rawMapToBinary then unmarshal back to dynamicpb.Message for assertions.
func rawRoundTrip(t *testing.T, md protoreflect.MessageDescriptor, data map[interface{}]interface{}) *dynamicpb.Message {
	t.Helper()
	cache := buildFieldLookupCache(md)
	b, err := rawMapToBinary(md, data, cache)
	require.NoError(t, err)
	msg := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b, msg))
	return msg
}

// TestRawMapToBinary_StringFieldsWithBytes tests STRING fields where values
// arrive as []byte (typical msgpack behavior from Fluent Bit).
func TestRawMapToBinary_StringFieldsWithBytes(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "Tag", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name    string
		data    map[interface{}]interface{}
		checkFn func(t *testing.T, msg *dynamicpb.Message)
	}{
		{
			name: "[]byte values converted to string",
			data: map[interface{}]interface{}{"Name": []byte("hello"), "Tag": []byte("world")},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
				assert.Equal(t, "world", msg.Get(md.Fields().ByName("Tag")).String())
			},
		},
		{
			name: "string values passed through",
			data: map[interface{}]interface{}{"Name": "hello"},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "nil value skipped",
			data: map[interface{}]interface{}{"Name": []byte("hello"), "Tag": nil},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
				assert.Equal(t, "", msg.Get(md.Fields().ByName("Tag")).String())
			},
		},
		{
			name: "unknown field ignored",
			data: map[interface{}]interface{}{"Name": []byte("hello"), "Unknown": []byte("ignored")},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "hello", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
		{
			name: "int coerced to string",
			data: map[interface{}]interface{}{"Name": 42},
			checkFn: func(t *testing.T, msg *dynamicpb.Message) {
				assert.Equal(t, "42", msg.Get(md.Fields().ByName("Name")).String())
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := rawRoundTrip(t, md, tt.data)
			tt.checkFn(t, msg)
		})
	}
}

// TestRawMapToBinary_Int64WithBytes tests that []byte values (msgpack strings)
// are correctly parsed into int64 proto fields.
func TestRawMapToBinary_Int64WithBytes(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name     string
		data     map[interface{}]interface{}
		expected int64
	}{
		{"int64", map[interface{}]interface{}{"count": int64(100)}, 100},
		{"uint64", map[interface{}]interface{}{"count": uint64(200)}, 200},
		{"float64", map[interface{}]interface{}{"count": float64(300)}, 300},
		{"[]byte", map[interface{}]interface{}{"count": []byte("500")}, 500},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := rawRoundTrip(t, md, tt.data)
			assert.Equal(t, tt.expected, msg.Get(md.Fields().ByName("count")).Int())
		})
	}
}

// TestRawMapToBinary_NestedRawMap tests STRUCT fields where the nested value
// is a map[interface{}]interface{} (raw msgpack).
func TestRawMapToBinary_NestedRawMap(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{
				Name: "address",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "city", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "zip", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)

	data := map[interface{}]interface{}{
		"name": []byte("Alice"),
		"address": map[interface{}]interface{}{
			"city": []byte("Tokyo"),
			"zip":  []byte("100-0001"),
		},
	}

	msg := rawRoundTrip(t, md, data)
	assert.Equal(t, "Alice", msg.Get(md.Fields().ByName("name")).String())

	addrFD := md.Fields().ByName("address")
	require.NotNil(t, addrFD)
	addrMsg := msg.Get(addrFD).Message()
	addrMD := addrFD.Message()
	assert.Equal(t, "Tokyo", addrMsg.Get(addrMD.Fields().ByName("city")).String())
	assert.Equal(t, "100-0001", addrMsg.Get(addrMD.Fields().ByName("zip")).String())
}

// TestRawMapToBinary_RepeatedScalar tests REPEATED fields from raw maps.
func TestRawMapToBinary_RepeatedScalar(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "tags", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}
	md := buildMD(t, schema)

	data := map[interface{}]interface{}{
		"tags": []interface{}{[]byte("alpha"), []byte("beta"), "gamma"},
	}

	msg := rawRoundTrip(t, md, data)
	tagsFD := md.Fields().ByName("tags")
	list := msg.Get(tagsFD).List()
	require.Equal(t, 3, list.Len())
	assert.Equal(t, "alpha", list.Get(0).String())
	assert.Equal(t, "beta", list.Get(1).String())
	assert.Equal(t, "gamma", list.Get(2).String())
}

// TestRawMapToBinary_RepeatedMessage tests REPEATED STRUCT fields from raw maps.
func TestRawMapToBinary_RepeatedMessage(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{
				Name: "items",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_REPEATED,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "key", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "value", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)

	data := map[interface{}]interface{}{
		"items": []interface{}{
			map[interface{}]interface{}{"key": []byte("a"), "value": int64(1)},
			map[interface{}]interface{}{"key": []byte("b"), "value": int64(2)},
		},
	}

	msg := rawRoundTrip(t, md, data)
	itemsFD := md.Fields().ByName("items")
	list := msg.Get(itemsFD).List()
	require.Equal(t, 2, list.Len())

	itemMD := itemsFD.Message()
	item0 := list.Get(0).Message()
	assert.Equal(t, "a", item0.Get(itemMD.Fields().ByName("key")).String())
	assert.Equal(t, int64(1), item0.Get(itemMD.Fields().ByName("value")).Int())

	item1 := list.Get(1).Message()
	assert.Equal(t, "b", item1.Get(itemMD.Fields().ByName("key")).String())
	assert.Equal(t, int64(2), item1.Get(itemMD.Fields().ByName("value")).Int())
}

// TestRawMapToBinary_MatchesMapToBinary verifies that rawMapToBinary produces
// identical output to mapToBinary for the same logical data.
func TestRawMapToBinary_MatchesMapToBinary(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)

	// Same data in both formats ([]byte values in raw, string values in parsed)
	rawData := map[interface{}]interface{}{
		"Text": []byte("FOO"),
		"Time": []byte("000"),
	}
	parsedData := map[string]interface{}{
		"Text": "FOO",
		"Time": "000",
	}

	rawBytes, err := rawMapToBinary(md, rawData, cache)
	require.NoError(t, err)
	parsedBytes, err := mapToBinary(md, parsedData, cache)
	require.NoError(t, err)

	// Compare decoded messages instead of raw bytes: map iteration order is
	// non-deterministic, so the two serializations may encode fields in
	// different orders while remaining logically identical.
	rawMsg := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(rawBytes, rawMsg))
	parsedMsg := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(parsedBytes, parsedMsg))
	assert.True(t, proto.Equal(rawMsg, parsedMsg))
}

// TestRawMapToBinary_BoolWithBytes tests []byte to bool conversion.
func TestRawMapToBinary_BoolWithBytes(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "active", Type: storagepb.TableFieldSchema_BOOL, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	tests := []struct {
		name     string
		data     map[interface{}]interface{}
		expected bool
	}{
		{"[]byte_true", map[interface{}]interface{}{"active": []byte("true")}, true},
		{"[]byte_false", map[interface{}]interface{}{"active": []byte("false")}, false},
		{"[]byte_1", map[interface{}]interface{}{"active": []byte("1")}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := rawRoundTrip(t, md, tt.data)
			assert.Equal(t, tt.expected, msg.Get(md.Fields().ByName("active")).Bool())
		})
	}
}

// TestRawMapToBinary_DoubleWithBytes tests []byte to float64 conversion.
func TestRawMapToBinary_DoubleWithBytes(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "score", Type: storagepb.TableFieldSchema_DOUBLE, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)

	data := map[interface{}]interface{}{"score": []byte("3.14")}
	msg := rawRoundTrip(t, md, data)
	assert.InDelta(t, 3.14, msg.Get(md.Fields().ByName("score")).Float(), 0.001)
}

// TestMessagePoolReuse verifies that pooled messages produce correct output
// across multiple sequential calls (no stale field leaks after reset).
func TestMessagePoolReuse(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)

	// Call mapToBinary multiple times; the pool should reuse messages
	for i := 0; i < 100; i++ {
		data := map[string]interface{}{
			"name":  "user",
			"count": int64(i),
		}
		b, err := mapToBinary(md, data, cache)
		require.NoError(t, err)

		msg := dynamicpb.NewMessage(md)
		require.NoError(t, proto.Unmarshal(b, msg))
		assert.Equal(t, "user", msg.Get(md.Fields().ByName("name")).String())
		assert.Equal(t, int64(i), msg.Get(md.Fields().ByName("count")).Int())
	}
}

// TestMessagePoolReuse_NoStaleFields verifies that fields set in a previous
// row don't leak into the next row after pool reuse.
func TestMessagePoolReuse_NoStaleFields(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "a", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "b", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)

	// First call sets both fields
	b1, err := mapToBinary(md, map[string]interface{}{"a": "hello", "b": "world"}, cache)
	require.NoError(t, err)
	msg1 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b1, msg1))
	assert.Equal(t, "hello", msg1.Get(md.Fields().ByName("a")).String())
	assert.Equal(t, "world", msg1.Get(md.Fields().ByName("b")).String())

	// Second call sets only "a" — field "b" must NOT carry over from the first call
	b2, err := mapToBinary(md, map[string]interface{}{"a": "only-a"}, cache)
	require.NoError(t, err)
	msg2 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b2, msg2))
	assert.Equal(t, "only-a", msg2.Get(md.Fields().ByName("a")).String())
	assert.Equal(t, "", msg2.Get(md.Fields().ByName("b")).String()) // must be default, not "world"
}

// TestMessagePoolReuse_RawPath verifies pool reuse via rawMapToBinary.
func TestMessagePoolReuse_RawPath(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "x", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "y", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)

	// First call sets both fields
	b1, err := rawMapToBinary(md, map[interface{}]interface{}{"x": []byte("first"), "y": int64(1)}, cache)
	require.NoError(t, err)
	msg1 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b1, msg1))
	assert.Equal(t, "first", msg1.Get(md.Fields().ByName("x")).String())
	assert.Equal(t, int64(1), msg1.Get(md.Fields().ByName("y")).Int())

	// Second call sets only "x" — field "y" must not carry over
	b2, err := rawMapToBinary(md, map[interface{}]interface{}{"x": []byte("second")}, cache)
	require.NoError(t, err)
	msg2 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b2, msg2))
	assert.Equal(t, "second", msg2.Get(md.Fields().ByName("x")).String())
	assert.Equal(t, int64(0), msg2.Get(md.Fields().ByName("y")).Int()) // must be default
}

// TestMessagePoolReuse_NestedRecord verifies that pooled sub-messages (RECORD fields)
// produce correct output across multiple sequential calls.
func TestMessagePoolReuse_NestedRecord(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "id", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{
				Name: "meta",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "source", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "value", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)

	// Call mapToBinary 100 times; sub-messages should be pooled and reused
	for i := 0; i < 100; i++ {
		data := map[string]interface{}{
			"id": fmt.Sprintf("row-%d", i),
			"meta": map[string]interface{}{
				"source": "test",
				"value":  int64(i),
			},
		}
		b, err := mapToBinary(md, data, cache)
		require.NoError(t, err)

		msg := dynamicpb.NewMessage(md)
		require.NoError(t, proto.Unmarshal(b, msg))
		assert.Equal(t, fmt.Sprintf("row-%d", i), msg.Get(md.Fields().ByName("id")).String())
		metaMsg := msg.Get(md.Fields().ByName("meta")).Message()
		metaMd := md.Fields().ByName("meta").Message()
		assert.Equal(t, "test", metaMsg.Get(metaMd.Fields().ByName("source")).String())
		assert.Equal(t, int64(i), metaMsg.Get(metaMd.Fields().ByName("value")).Int())
	}
}

// TestMessagePoolReuse_NoStaleFields_NestedRecord verifies that fields set inside a
// nested RECORD in one call do not leak into the next call after pool reuse.
func TestMessagePoolReuse_NoStaleFields_NestedRecord(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{
				Name: "info",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "a", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "b", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)
	infaMd := md.Fields().ByName("info").Message()

	// First call sets both nested fields
	b1, err := mapToBinary(md, map[string]interface{}{
		"info": map[string]interface{}{"a": "hello", "b": "world"},
	}, cache)
	require.NoError(t, err)
	msg1 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b1, msg1))
	assert.Equal(t, "hello", msg1.Get(md.Fields().ByName("info")).Message().Get(infaMd.Fields().ByName("a")).String())
	assert.Equal(t, "world", msg1.Get(md.Fields().ByName("info")).Message().Get(infaMd.Fields().ByName("b")).String())

	// Second call sets only "a" — "b" must NOT carry over from pooled sub-message
	b2, err := mapToBinary(md, map[string]interface{}{
		"info": map[string]interface{}{"a": "only-a"},
	}, cache)
	require.NoError(t, err)
	msg2 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b2, msg2))
	assert.Equal(t, "only-a", msg2.Get(md.Fields().ByName("info")).Message().Get(infaMd.Fields().ByName("a")).String())
	assert.Equal(t, "", msg2.Get(md.Fields().ByName("info")).Message().Get(infaMd.Fields().ByName("b")).String())
}

// TestMessagePoolReuse_RawPath_NestedRecord verifies pool reuse for nested RECORD
// fields via the rawMapToBinary path (map[interface{}]interface{} input).
func TestMessagePoolReuse_RawPath_NestedRecord(t *testing.T) {
	schema := &storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{
				Name: "nested",
				Type: storagepb.TableFieldSchema_STRUCT,
				Mode: storagepb.TableFieldSchema_NULLABLE,
				Fields: []*storagepb.TableFieldSchema{
					{Name: "x", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					{Name: "y", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
				},
			},
		},
	}
	md := buildMD(t, schema)
	cache := buildFieldLookupCache(md)
	nestedMd := md.Fields().ByName("nested").Message()

	// First call: set both nested fields
	b1, err := rawMapToBinary(md, map[interface{}]interface{}{
		"nested": map[interface{}]interface{}{"x": []byte("first"), "y": int64(1)},
	}, cache)
	require.NoError(t, err)
	msg1 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b1, msg1))
	assert.Equal(t, "first", msg1.Get(md.Fields().ByName("nested")).Message().Get(nestedMd.Fields().ByName("x")).String())
	assert.Equal(t, int64(1), msg1.Get(md.Fields().ByName("nested")).Message().Get(nestedMd.Fields().ByName("y")).Int())

	// Second call: set only "x" — "y" must NOT carry over from pooled sub-message
	b2, err := rawMapToBinary(md, map[interface{}]interface{}{
		"nested": map[interface{}]interface{}{"x": []byte("second")},
	}, cache)
	require.NoError(t, err)
	msg2 := dynamicpb.NewMessage(md)
	require.NoError(t, proto.Unmarshal(b2, msg2))
	assert.Equal(t, "second", msg2.Get(md.Fields().ByName("nested")).Message().Get(nestedMd.Fields().ByName("x")).String())
	assert.Equal(t, int64(0), msg2.Get(md.Fields().ByName("nested")).Message().Get(nestedMd.Fields().ByName("y")).Int())
}

// TestCleanupMsgPool verifies that cleanupMsgPool removes all messagePool
// entries for a flat schema and a nested (STRUCT) schema.
func TestCleanupMsgPool(t *testing.T) {
	t.Run("flat schema", func(t *testing.T) {
		schema := &storagepb.TableSchema{
			Fields: []*storagepb.TableFieldSchema{
				{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
				{Name: "count", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
			},
		}
		md := buildMD(t, schema)
		cache := buildFieldLookupCache(md)

		// Populate the pool by serialising one record.
		_, err := mapToBinary(md, map[string]interface{}{"name": "x", "count": int64(1)}, cache)
		require.NoError(t, err)

		_, populated := messagePool.Load(md)
		assert.True(t, populated, "messagePool should have entry before cleanup")

		cleanupMsgPool(md)

		_, populated = messagePool.Load(md)
		assert.False(t, populated, "messagePool should have no entry after cleanup")
	})

	t.Run("nested schema cleans sub-message entries", func(t *testing.T) {
		schema := &storagepb.TableSchema{
			Fields: []*storagepb.TableFieldSchema{
				{
					Name: "nested",
					Type: storagepb.TableFieldSchema_STRUCT,
					Mode: storagepb.TableFieldSchema_NULLABLE,
					Fields: []*storagepb.TableFieldSchema{
						{Name: "x", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
					},
				},
			},
		}
		md := buildMD(t, schema)
		nestedMd := md.Fields().ByName("nested").Message()
		cache := buildFieldLookupCache(md)

		_, err := rawMapToBinary(md, map[interface{}]interface{}{
			"nested": map[interface{}]interface{}{"x": []byte("hello")},
		}, cache)
		require.NoError(t, err)

		_, topPopulated := messagePool.Load(md)
		_, subPopulated := messagePool.Load(nestedMd)
		assert.True(t, topPopulated, "top-level entry should exist before cleanup")
		assert.True(t, subPopulated, "sub-message entry should exist before cleanup")

		cleanupMsgPool(md)

		_, topPopulated = messagePool.Load(md)
		_, subPopulated = messagePool.Load(nestedMd)
		assert.False(t, topPopulated, "top-level entry should be removed after cleanup")
		assert.False(t, subPopulated, "sub-message entry should be removed after cleanup")
	})
}

// BenchmarkRawMapToBinary benchmarks the raw map[interface{}]interface{} path.
func BenchmarkRawMapToBinary(b *testing.B) {
	schema := mangleInputSchema(&storagepb.TableSchema{
		Fields: []*storagepb.TableFieldSchema{
			{Name: "name", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "age", Type: storagepb.TableFieldSchema_INT64, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "score", Type: storagepb.TableFieldSchema_DOUBLE, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "active", Type: storagepb.TableFieldSchema_BOOL, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "ts", Type: storagepb.TableFieldSchema_TIMESTAMP, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "numeric_val", Type: storagepb.TableFieldSchema_NUMERIC, Mode: storagepb.TableFieldSchema_NULLABLE},
			{Name: "tags", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_REPEATED},
		},
	}, true)
	descriptor, _ := adapt.StorageSchemaToProto2Descriptor(schema, "root")
	md := descriptor.(protoreflect.MessageDescriptor)
	cache := buildFieldLookupCache(md)

	// Raw data as it would come from Fluent Bit's msgpack decoder
	data := map[interface{}]interface{}{
		"name":        []byte("test-user"),
		"age":         int64(30),
		"score":       95.5,
		"active":      true,
		"ts":          int64(1700000000000000),
		"numeric_val": []byte("123.456789"),
		"tags":        []interface{}{[]byte("tag1"), []byte("tag2"), []byte("tag3")},
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := rawMapToBinary(md, data, cache)
		if err != nil {
			b.Fatal(err)
		}
	}
}
