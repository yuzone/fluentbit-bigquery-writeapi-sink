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
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/ugorji/go/codec"
	"google.golang.org/api/option"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/dynamicpb"
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
			f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
				if tt.appendErr != nil {
					return nil, tt.appendErr
				}
				return readyResult(-1, nil), nil
			})
			ptr := registerTestConfig(t, f.cfg)
			data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

			assert.Equal(t, tt.want, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
			assert.Equal(t, int32(1), f.appendCalls())
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
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		return nil, status.Error(codes.Unavailable, "x")
	})
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t,
		map[string]interface{}{"Text": "a"},
		map[string]interface{}{"Text": "b"},
		map[string]interface{}{"Text": "c"},
	)

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.appendCalls(), "no further sub-chunks after a transient failure")
}

// A permanent failure does not abort the flush: the remaining sub-chunks are still sent and
// the chunk is reported as FLB_ERROR.
func TestFLBPluginFlushCtxPermanentFailureContinues(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.maxChunkSize = 1
	})
	require.NoError(t, f.router.init())
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		if f.appendCalls() == 1 {
			return nil, status.Error(codes.InvalidArgument, "x")
		}
		return readyResult(-1, nil), nil
	})
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t,
		map[string]interface{}{"Text": "a"},
		map[string]interface{}{"Text": "b"},
		map[string]interface{}{"Text": "c"},
	)

	assert.Equal(t, output.FLB_ERROR, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(3), f.appendCalls())
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
			f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
				return readyResult(0, tt.resultErr), nil
			})
			ptr := registerTestConfig(t, f.cfg)
			data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

			assert.Equal(t, tt.want, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
			assert.Equal(t, int64(0), f.router.static.offset(), "offset is not advanced on failure")
		})
	}
}

// defaultInitKeys is the minimal configuration accepted by FLBPluginInit.
func defaultInitKeys(overrides map[string]string) map[string]string {
	keys := map[string]string{
		"ProjectID":          "DummyProjectId",
		"DatasetID":          "DummyDatasetId",
		"TableID":            "DummyTableId",
		"Max_Queue_Requests": "0",
		"Max_Queue_Bytes":    "0",
		"Exactly_Once":       "False",
	}
	for k, v := range overrides {
		keys[k] = v
	}
	return keys
}

// fiveRecords returns five records whose Text/Time fields the fake stream can verify.
func fiveRecords(t *testing.T) (unsafe.Pointer, cInt) {
	t.Helper()
	records := make([]map[string]interface{}, 5)
	for i := range records {
		records[i] = map[string]interface{}{"Text": "FOO", "Time": "000"}
	}
	return encodeRecords(t, records...)
}

// decodeRows unmarshals the serialized rows of one AppendRows call against the schema.
func decodeRows(t *testing.T, data [][]byte) []*dynamicpb.Message {
	t.Helper()
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(defaultTestSchema, "root")
	require.NoError(t, err)
	md := descriptor.(protoreflect.MessageDescriptor)
	msgs := make([]*dynamicpb.Message, 0, len(data))
	for _, row := range data {
		m := dynamicpb.NewMessage(md)
		require.NoError(t, proto.Unmarshal(row, m))
		msgs = append(msgs, m)
	}
	return msgs
}

func TestFLBPluginRegister(t *testing.T) {
	d := installTestDeps(t, nil)
	assert.Equal(t, 0, FLBPluginRegister(nil))
	assert.Equal(t, []string{"writeapi"}, d.bridge.registered)
}

func TestFLBPluginInit(t *testing.T) {
	for _, exactlyOnce := range []string{"False", "True"} {
		t.Run("Exactly_Once="+exactlyOnce, func(t *testing.T) {
			d := installTestDeps(t, defaultInitKeys(map[string]string{"Exactly_Once": exactlyOnce, "Max_Chunk_Size": "0"}))

			plugin, id := initPlugin(t)

			for _, key := range []string{"ProjectID", "DatasetID", "TableID", "Max_Chunk_Size", "Max_Queue_Requests", "Max_Queue_Bytes", "Exactly_Once"} {
				assert.Equal(t, 1, d.bridge.keysRead[key], key)
			}
			assert.Equal(t, int32(1), d.newClients.Load())
			assert.Equal(t, 1, d.client.getWriteStreamCalls("projects/DummyProjectId/datasets/DummyDatasetId/tables/DummyTableId/streams/_default"))
			assert.Equal(t, 1, d.client.newStreamCalls())
			assert.Len(t, d.client.newStreamOpts[0], 8, "every writer option must be passed to NewManagedStream")
			assert.Equal(t, 1, d.bridge.setContexts)
			assert.Equal(t, id, instanceID(plugin))
			assert.Equal(t, exactlyOnce == "True", configMap[id].exactlyOnce)
		})
	}
}

func TestFLBPluginInitFailures(t *testing.T) {
	t.Run("invalid configuration", func(t *testing.T) {
		installTestDeps(t, defaultInitKeys(map[string]string{"Max_Chunk_Size": "abc"}))
		assert.Equal(t, output.FLB_ERROR, FLBPluginInit(unsafe.Pointer(new(int))))
	})
	t.Run("client creation fails", func(t *testing.T) {
		d := installTestDeps(t, defaultInitKeys(nil))
		d.newClient = func(context.Context, string, ...option.ClientOption) (ManagedWriterClient, error) {
			return nil, errors.New("no credentials")
		}
		assert.Equal(t, output.FLB_ERROR, FLBPluginInit(unsafe.Pointer(new(int))))
	})
	t.Run("missing table", func(t *testing.T) {
		d := installTestDeps(t, defaultInitKeys(nil))
		d.client.failTables["projects/DummyProjectId/datasets/DummyDatasetId/tables/DummyTableId/streams/_default"] = status.Error(codes.NotFound, "x")
		before := getInstanceCount()
		assert.Equal(t, output.FLB_ERROR, FLBPluginInit(unsafe.Pointer(new(int))))
		assert.Equal(t, before, getInstanceCount())
	})
}

// TestFLBPluginFlushCtx covers the default (at-least-once) path end to end: records are decoded
// from msgpack, serialized against the table schema and appended; the response of the first
// flush is drained on the second one.
func TestFLBPluginFlushCtx(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(nil))
	plugin, _ := initPlugin(t)
	stream := d.client.streams[0]
	stream.mu.Lock()
	stream.onAppend = func(_ context.Context, data [][]byte, _ []managedwriter.AppendOption) (appendResult, error) {
		msgs := decodeRows(t, data)
		require.Len(t, msgs, 5)
		md := msgs[0].Descriptor()
		for _, m := range msgs {
			assert.Equal(t, "FOO", m.Get(md.Fields().ByName("Text")).String())
			assert.Equal(t, "000", m.Get(md.Fields().ByName("Time")).String())
		}
		r := readyResult(-1, nil)
		stream.results = append(stream.results, r)
		return r, nil
	}
	stream.mu.Unlock()

	getContextsBefore := d.bridge.getContexts
	data, n := fiveRecords(t)
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, int32(0), stream.getResultCalls(), "responses are checked on the next flush")
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))

	assert.Equal(t, int32(2), stream.appends.Load())
	assert.Equal(t, int32(1), stream.getResultCalls())
	assert.Equal(t, 2, d.bridge.getContexts-getContextsBefore, "each flush resolves its instance via the FLB context")
}

func TestFLBPluginFlushCtxUnknownInstance(t *testing.T) {
	installTestDeps(t, nil)
	assert.Equal(t, output.FLB_ERROR, FLBPluginFlushCtx(nil, nil, 0, nil))
	assert.Equal(t, output.FLB_ERROR, FLBPluginExitCtx(nil))
}

// TestFLBPluginFlushCtxDynamicScaling verifies that a new stream is added whenever the least
// loaded stream has more pending responses than the request-count threshold.
func TestFLBPluginFlushCtxDynamicScaling(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(nil))
	plugin, id := initPlugin(t)
	configMap[id].requestCountThreshold = 0 // scale as soon as one response is pending
	for _, s := range d.client.streams {
		s.pending = true
	}
	d.client.mu.Lock()
	d.client.stream = &fakeStream{pending: true}
	d.client.mu.Unlock()

	data, n := fiveRecords(t)
	for i := 0; i < 3; i++ {
		assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	}
	// Init creates the first stream; the 2nd and 3rd flush each observe one pending response
	// on the least loaded stream and add another one.
	assert.Equal(t, 3, d.client.newStreamCalls())
	assert.Len(t, configMap[id].router.static.streams, 3)
}

func TestFLBPluginFlushCtxDynamicScalingCap(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(nil))
	plugin, id := initPlugin(t)
	configMap[id].requestCountThreshold = 0
	d.client.mu.Lock()
	d.client.stream = &fakeStream{pending: true}
	d.client.mu.Unlock()
	d.client.streams[0].pending = true

	data, n := fiveRecords(t)
	for i := 0; i < maxNumStreamsPerInstance+3; i++ {
		FLBPluginFlushCtx(plugin, data, n, cTag("app"))
	}
	assert.Len(t, configMap[id].router.static.streams, maxNumStreamsPerInstance)
}

// TestFLBPluginFlushCtxExactlyOnce verifies the committed-stream path: responses are checked
// synchronously, the offset advances by the number of rows, and WithOffset is passed.
func TestFLBPluginFlushCtxExactlyOnce(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(map[string]string{"Exactly_Once": "True"}))
	plugin, id := initPlugin(t)
	stream := d.client.streams[0]
	var optCounts []int
	stream.mu.Lock()
	stream.onAppend = func(_ context.Context, data [][]byte, opts []managedwriter.AppendOption) (appendResult, error) {
		optCounts = append(optCounts, len(opts))
		require.Len(t, decodeRows(t, data), 5)
		r := readyResult(int64(len(data)), nil)
		stream.results = append(stream.results, r)
		return r, nil
	}
	stream.mu.Unlock()

	data, n := fiveRecords(t)
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, int64(5), getOffset(id))
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, int64(10), getOffset(id))

	assert.Equal(t, int32(2), stream.appends.Load())
	assert.Equal(t, int32(2), stream.getResultCalls(), "exactly-once checks every response synchronously")
	assert.Equal(t, []int{1, 1}, optCounts, "WithOffset is passed on every append")
}

// TestFLBPluginFlushCtxErrorHandling verifies that the offset is not advanced when the server
// rejects an append, and that the failure is retried Num_Retries times with backoff.
func TestFLBPluginFlushCtxErrorHandling(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(map[string]string{"Exactly_Once": "True", "Num_Synchronous_Retries": "2"}))
	plugin, id := initPlugin(t)
	stream := d.client.streams[0]
	var call int
	stream.mu.Lock()
	stream.onAppend = func(_ context.Context, data [][]byte, _ []managedwriter.AppendOption) (appendResult, error) {
		call++
		if call <= 3 {
			return readyResult(-1, status.Error(codes.Internal, "mocked server-side error")), nil
		}
		return readyResult(int64(len(data)), nil), nil
	}
	stream.mu.Unlock()

	data, n := fiveRecords(t)
	// Attempts 1-3 fail (initial + 2 retries) -> FLB_RETRY, offset unchanged.
	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, int64(0), getOffset(id))
	assert.Len(t, d.clock.sleeps, 2, "backoff between retries uses the injected clock")
	// Attempt 4 succeeds.
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, int64(5), getOffset(id))
	assert.Equal(t, int32(4), stream.appends.Load())
}

// TestFLBPluginExitCtxDrainsPendingResponses verifies that FLBPluginExitCtx waits for every
// pending response before closing the stream and the client.
func TestFLBPluginExitCtxDrainsPendingResponses(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(nil))
	plugin, _ := initPlugin(t)
	stream := d.client.streams[0]
	stream.pending = true

	data, n := fiveRecords(t)
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, int32(1), stream.appends.Load())
	assert.Equal(t, int32(0), stream.getResultCalls(), "pending response is not drained by the flush")

	assert.Equal(t, output.FLB_OK, FLBPluginExitCtx(plugin))
	assert.Equal(t, int32(1), stream.getResultCalls(), "drained during exit")
	assert.Equal(t, int32(1), stream.closes.Load())
	assert.Equal(t, int32(1), d.client.closes.Load())
}

// TestFLBPluginFlushCtxAppendUsesInstanceContext is a regression test for the bug where the
// per-flush context was passed to AppendRows on the default (at-least-once) path. Because appends
// are acknowledged asynchronously on a later flush, canceling the flush context at flush end aborted
// the managed writer's internal retry/reconnect. AppendRows must receive the instance-scoped
// context, which stays alive until FLBPluginExitCtx.
func TestFLBPluginFlushCtxAppendUsesInstanceContext(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(nil))
	plugin, id := initPlugin(t)
	stream := d.client.streams[0]
	stream.pending = true
	instanceCtx := configMap[id].instanceCtx

	data, n := fiveRecords(t)
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))

	require.Len(t, stream.ctxs, 1, "AppendRows should have been called")
	captured := stream.ctxs[0]
	assert.NoError(t, captured.Err(), "AppendRows context must not be canceled after the flush returns")
	assert.Equal(t, instanceCtx, captured, "AppendRows must use the instance-scoped context")

	assert.Equal(t, output.FLB_OK, FLBPluginExitCtx(plugin))
	assert.Error(t, captured.Err(), "instance-scoped context must be canceled after exit")
}

// TestFLBPluginInitMultiplexing verifies that the client is created with the multiplexing
// options only when Enable_Multiplexing is set.
func TestFLBPluginInitMultiplexing(t *testing.T) {
	tests := []struct {
		name string
		keys map[string]string
		want []option.ClientOption
	}{
		{name: "disabled by default", keys: nil, want: nil},
		{
			name: "enabled with default pool limit",
			keys: map[string]string{"Enable_Multiplexing": "true"},
			want: []option.ClientOption{managedwriter.WithMultiplexing(), managedwriter.WithMultiplexPoolLimit(1)},
		},
		{
			name: "enabled with pool limit",
			keys: map[string]string{"Enable_Multiplexing": "true", "Multiplex_Pool_Limit": "4"},
			want: []option.ClientOption{managedwriter.WithMultiplexing(), managedwriter.WithMultiplexPoolLimit(4)},
		},
		{
			name: "pool limit ignored when disabled",
			keys: map[string]string{"Multiplex_Pool_Limit": "4"},
			want: nil,
		},
		{
			name: "exactly-once only warns",
			keys: map[string]string{"Enable_Multiplexing": "true", "Exactly_Once": "True"},
			want: []option.ClientOption{managedwriter.WithMultiplexing(), managedwriter.WithMultiplexPoolLimit(1)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := installTestDeps(t, defaultInitKeys(tt.keys))
			initPlugin(t)
			require.Len(t, d.clientOpts, 1)
			assert.Equal(t, tt.want, d.clientOpts[0])
		})
	}
}

func TestFLBPluginInitMultiplexingInvalid(t *testing.T) {
	for _, keys := range []map[string]string{
		{"Enable_Multiplexing": "maybe"},
		{"Multiplex_Pool_Limit": "0"},
		{"Multiplex_Pool_Limit": "x"},
	} {
		installTestDeps(t, defaultInitKeys(keys))
		assert.Equal(t, output.FLB_ERROR, FLBPluginInit(unsafe.Pointer(new(int))), keys)
	}
}
