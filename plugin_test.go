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
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ugorji/go/codec"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// encodeRecords msgpack-encodes Fluent Bit log records ([timestamp, map] pairs) exactly as
// the proxy hands them to FLBPluginFlushCtx, and returns the pointer/length pair to pass in.
func encodeRecords(t *testing.T, records ...map[string]interface{}) (unsafe.Pointer, cInt) {
	t.Helper()
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, new(codec.MsgpackHandle))
	for _, rec := range records {
		require.NoError(t, enc.Encode([]interface{}{uint64(1700000000), rec}))
	}
	require.NotEmpty(t, buf)
	return unsafe.Pointer(&buf[0]), cInt(len(buf))
}

func cTag(s string) *cChar {
	b := append([]byte(s), 0)
	return (*cChar)(unsafe.Pointer(&b[0]))
}

func TestIsTransientError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"unavailable", status.Error(codes.Unavailable, "x"), true},
		{"deadline exceeded", status.Error(codes.DeadlineExceeded, "x"), true},
		{"resource exhausted", status.Error(codes.ResourceExhausted, "x"), true},
		{"aborted", status.Error(codes.Aborted, "x"), true},
		{"internal", status.Error(codes.Internal, "x"), true},
		{"canceled", status.Error(codes.Canceled, "x"), true},
		{"context deadline", context.DeadlineExceeded, true},
		{"wrapped context canceled", errors.Join(errors.New("append"), context.Canceled), true},
		{"eof", io.EOF, true},
		{"net error", &net.OpError{Op: "dial", Err: errors.New("refused")}, true},
		{"invalid argument", status.Error(codes.InvalidArgument, "schema mismatch"), false},
		{"permission denied", status.Error(codes.PermissionDenied, "x"), false},
		{"not found", status.Error(codes.NotFound, "x"), false},
		{"unknown code", status.Error(codes.Unknown, "x"), false},
		{"plain error", errors.New("boom"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, isTransientError(tt.err))
		})
	}
}

func TestFLBPluginFlushCtxReturnCodes(t *testing.T) {
	tests := []struct {
		name           string
		appendErr      error
		retryOnFailure bool
		want           int
	}{
		{"success", nil, true, output.FLB_OK},
		{"transient failure is retried", status.Error(codes.Unavailable, "x"), true, output.FLB_RETRY},
		{"permanent failure is an error", status.Error(codes.InvalidArgument, "x"), true, output.FLB_ERROR},
		{"Retry_On_Failure=false drops transient", status.Error(codes.Unavailable, "x"), false, output.FLB_OK},
		{"Retry_On_Failure=false drops permanent", status.Error(codes.InvalidArgument, "x"), false, output.FLB_OK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) { c.retryOnFailure = tt.retryOnFailure })
			require.NoError(t, f.router.init())
			f.appendRows = func(ctx context.Context, data [][]byte) (*managedwriter.AppendResult, error) {
				return nil, tt.appendErr
			}
			ptr := registerTestConfig(t, f.cfg)
			data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

			assert.Equal(t, tt.want, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
			assert.Equal(t, int32(1), f.appendCalls.Load())
		})
	}
}

// A transient failure on an early sub-chunk must stop the flush: the whole chunk is redelivered
// by Fluent Bit, so sending the remaining sub-chunks would only create duplicates.
func TestFLBPluginFlushCtxTransientFailureAbortsChunk(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.maxChunkSize = 1 // every record becomes its own AppendRows call
	})
	require.NoError(t, f.router.init())
	f.appendRows = func(ctx context.Context, data [][]byte) (*managedwriter.AppendResult, error) {
		return nil, status.Error(codes.Unavailable, "x")
	}
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t,
		map[string]interface{}{"Text": "a"},
		map[string]interface{}{"Text": "b"},
		map[string]interface{}{"Text": "c"},
	)

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.appendCalls.Load(), "no further sub-chunks after a transient failure")
}

// A permanent failure does not abort the flush: the remaining sub-chunks are still sent and
// the chunk is reported as FLB_ERROR.
func TestFLBPluginFlushCtxPermanentFailureContinues(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.maxChunkSize = 1
	})
	require.NoError(t, f.router.init())
	f.appendRows = func(ctx context.Context, data [][]byte) (*managedwriter.AppendResult, error) {
		if f.appendCalls.Load() == 1 {
			return nil, status.Error(codes.InvalidArgument, "x")
		}
		return nil, nil
	}
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t,
		map[string]interface{}{"Text": "a"},
		map[string]interface{}{"Text": "b"},
		map[string]interface{}{"Text": "c"},
	)

	assert.Equal(t, output.FLB_ERROR, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(3), f.appendCalls.Load())
}

// In exactly-once mode the synchronous GetResult error (after the configured retries) decides
// the return code.
func TestFLBPluginFlushCtxExactlyOnceReturnCodes(t *testing.T) {
	tests := []struct {
		name      string
		resultErr error
		want      int
	}{
		{"transient", status.Error(codes.Unavailable, "x"), output.FLB_RETRY},
		{"permanent", status.Error(codes.InvalidArgument, "x"), output.FLB_ERROR},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
				c.retryOnFailure = true
				c.exactlyOnce = true
				c.streamType = managedwriter.CommittedStream
				c.numRetries = 0
			})
			require.NoError(t, f.router.init())
			origGetResult := pluginGetResult
			pluginGetResult = func(_ *managedwriter.AppendResult, _ context.Context) (int64, error) {
				return 0, tt.resultErr
			}
			t.Cleanup(func() { pluginGetResult = origGetResult })
			ptr := registerTestConfig(t, f.cfg)
			data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

			assert.Equal(t, tt.want, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
			assert.Equal(t, int64(0), f.router.static.offset(), "offset is not advanced on failure")
		})
	}
}
