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
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestConvertTimestampFieldsRaw tests the convertTimestampFieldsRaw function
// for raw Fluent Bit records (map[interface{}]interface{}).
func TestConvertTimestampFieldsRaw(t *testing.T) {
	tests := []struct {
		name            string
		input           map[interface{}]interface{}
		timestampFields []string
		expected        map[interface{}]interface{}
	}{
		{
			name:            "int64 seconds converted to microseconds",
			input:           map[interface{}]interface{}{"time": int64(1700000000)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000)},
		},
		{
			name:            "int seconds converted to microseconds",
			input:           map[interface{}]interface{}{"time": 1700000000},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000)},
		},
		{
			name:            "uint64 seconds converted to microseconds",
			input:           map[interface{}]interface{}{"time": uint64(1700000000)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000)},
		},
		{
			name:            "float64 seconds converted to microseconds",
			input:           map[interface{}]interface{}{"time": float64(1700000000.5)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000500000)},
		},
		{
			name:            "value already in microseconds not converted",
			input:           map[interface{}]interface{}{"time": int64(1700000000000000)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000)},
		},
		{
			name:            "[]byte string timestamp converted",
			input:           map[interface{}]interface{}{"time": []byte("1700000000")},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000)},
		},
		{
			name:            "[]byte non-numeric string not converted",
			input:           map[interface{}]interface{}{"time": []byte("not-a-number")},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": []byte("not-a-number")},
		},
		{
			name:            "string timestamp converted",
			input:           map[interface{}]interface{}{"time": "1700000000"},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000)},
		},
		{
			name:            "field not in timestampFields not converted",
			input:           map[interface{}]interface{}{"time": int64(1700000000), "other": int64(1700000000)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000), "other": int64(1700000000)},
		},
		{
			name:            "multiple timestamp fields",
			input:           map[interface{}]interface{}{"time": int64(1700000000), "created_at": int64(1600000000)},
			timestampFields: []string{"time", "created_at"},
			expected:        map[interface{}]interface{}{"time": int64(1700000000000000), "created_at": int64(1600000000000000)},
		},
		{
			name: "nested RECORD timestamp field converted",
			input: map[interface{}]interface{}{
				"meta": map[interface{}]interface{}{
					"ts": int64(1700000000),
				},
			},
			timestampFields: []string{"meta.ts"},
			expected: map[interface{}]interface{}{
				"meta": map[interface{}]interface{}{
					"ts": int64(1700000000000000),
				},
			},
		},
		{
			name: "nested RECORD timestamp field missing parent not panic",
			input: map[interface{}]interface{}{
				"other": int64(1700000000),
			},
			timestampFields: []string{"meta.ts"},
			expected: map[interface{}]interface{}{
				"other": int64(1700000000),
			},
		},
		{
			name:            "field not present in data",
			input:           map[interface{}]interface{}{"other": "value"},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"other": "value"},
		},
		{
			name:            "empty timestamp fields",
			input:           map[interface{}]interface{}{"time": int64(1700000000)},
			timestampFields: []string{},
			expected:        map[interface{}]interface{}{"time": int64(1700000000)},
		},
		{
			name:            "nil timestamp fields",
			input:           map[interface{}]interface{}{"time": int64(1700000000)},
			timestampFields: nil,
			expected:        map[interface{}]interface{}{"time": int64(1700000000)},
		},
		{
			name:            "zero value not converted",
			input:           map[interface{}]interface{}{"time": int64(0)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(0)},
		},
		{
			name:            "negative value not converted",
			input:           map[interface{}]interface{}{"time": int64(-1000)},
			timestampFields: []string{"time"},
			expected:        map[interface{}]interface{}{"time": int64(-1000)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			convertTimestampFieldsRaw(tt.input, tt.timestampFields)
			assert.Equal(t, tt.expected, tt.input)
		})
	}
}
