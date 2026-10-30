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
	"strings"
	"testing"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
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
		{"unauthenticated", status.Error(codes.Unauthenticated, "token refresh failed"), true},
		{"context deadline", context.DeadlineExceeded, true},
		{"wrapped context canceled", errors.Join(errors.New("append"), context.Canceled), true},
		{"eof", io.EOF, true},
		{"stream rebuilding", errors.Join(errors.New("flush"), errStreamRebuilding), true},
		{"net error", &net.OpError{Op: "dial", Err: errors.New("refused")}, true},
		{"invalid argument", status.Error(codes.InvalidArgument, "schema mismatch"), false},
		{"permission denied", status.Error(codes.PermissionDenied, "x"), false},
		{"not found", status.Error(codes.NotFound, "x"), false},
		{"failed precondition", status.Error(codes.FailedPrecondition, "x"), false},
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

// The synchronous retries of the committed-stream path use the same classification as the
// return code: a transient failure is retried Num_Synchronous_Retries times and then reported
// as FLB_RETRY, a permanent one is reported as FLB_ERROR without any retry.
func TestFLBPluginFlushCtxExactlyOnceSynchronousRetriesFollowClassification(t *testing.T) {
	tests := []struct {
		name        string
		resultErr   error
		want        int
		wantAppends int32
	}{
		{"resource exhausted is retried then FLB_RETRY", status.Error(codes.ResourceExhausted, "x"), output.FLB_RETRY, 3},
		{"unauthenticated is retried then FLB_RETRY", status.Error(codes.Unauthenticated, "x"), output.FLB_RETRY, 3},
		{"failed precondition is not retried and FLB_ERROR", status.Error(codes.FailedPrecondition, "x"), output.FLB_ERROR, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
				c.retryOnFailure = true
				c.exactlyOnce = true
				c.streamType = managedwriter.CommittedStream
				c.numRetries = 2
			})
			require.NoError(t, f.router.init())
			f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
				return readyResult(0, tt.resultErr), nil
			})
			ptr := registerTestConfig(t, f.cfg)
			data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

			assert.Equal(t, tt.want, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
			assert.Equal(t, tt.wantAppends, f.appendCalls())
			assert.Len(t, f.clock.sleeps, int(tt.wantAppends-1), "one backoff between attempts")
		})
	}
}

// A row whose serialized size alone reaches the 10 MB AppendRows request limit can never be
// sent; it is logged and skipped like a row that cannot be serialized, and the rest of the
// chunk is delivered normally. A row above Max_Chunk_Size but below that limit is still sent
// on its own.
func TestFLBPluginFlushCtxOversizedRowIsSkipped(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) { c.retryOnFailure = true })
	require.NoError(t, f.router.init())
	var requests [][][]byte
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		// binaryData is reused between sub-chunks, so keep a copy of this request.
		requests = append(requests, append([][]byte(nil), data...))
		return readyResult(-1, nil), nil
	})
	ptr := registerTestConfig(t, f.cfg)
	large := strings.Repeat("x", chunkSizeLimit) // above Max_Chunk_Size, below the request limit
	data, n := encodeRecords(t,
		map[string]interface{}{"Text": "a"},
		map[string]interface{}{"Text": strings.Repeat("y", appendRequestLimit)},
		map[string]interface{}{"Text": large},
		map[string]interface{}{"Text": "b"},
	)

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.Len(t, requests, 3)
	assert.Equal(t, []string{"a"}, f.textValues(t, requests[0]))
	assert.Equal(t, []string{large}, f.textValues(t, requests[1]), "a row above Max_Chunk_Size is sent alone")
	assert.Equal(t, []string{"b"}, f.textValues(t, requests[2]))
}

// A msgpack object that is not a [timestamp, map] record must not end the chunk: the decoder
// has consumed it, so the records after it are still delivered. This includes top-level
// values on which output.GetRecord panics (nil, numbers, bools).
func TestFLBPluginFlushCtxMalformedRecordIsSkipped(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) { c.retryOnFailure = true })
	require.NoError(t, f.router.init())
	var appended [][]byte
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		appended = append(appended, data...)
		return readyResult(-1, nil), nil
	})
	ptr := registerTestConfig(t, f.cfg)
	var buf []byte
	enc := codec.NewEncoderBytes(&buf, new(codec.MsgpackHandle))
	require.NoError(t, enc.Encode([]interface{}{uint64(1700000000), map[string]interface{}{"Text": "a"}}))
	require.NoError(t, enc.Encode([]interface{}{uint64(1700000000), map[string]interface{}{"Text": "x"}, "extra"}))
	require.NoError(t, enc.Encode([]interface{}{uint64(1700000000), "not a map"}))
	require.NoError(t, enc.Encode(nil))
	require.NoError(t, enc.Encode(42))
	require.NoError(t, enc.Encode(true))
	require.NoError(t, enc.Encode([]interface{}{uint64(1700000000), map[string]interface{}{"Text": "b"}}))

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, unsafe.Pointer(&buf[0]), cInt(len(buf)), cTag("app")))
	assert.Equal(t, int32(1), f.appendCalls())
	assert.Equal(t, []string{"a", "b"}, f.textValues(t, appended))
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

// An AppendRows call that does not return within the flush timeout (unreachable backend or a
// saturated in-flight queue) must surface as FLB_RETRY instead of blocking the flush forever.
// Until the blocked call completes, further flushes for the table fail fast without starting
// another append; afterwards appends resume and the late result is queued for checking.
func TestFLBPluginFlushCtxBlockedAppendReturnsRetry(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.flushTimeout = 50 * time.Millisecond
	})
	require.NoError(t, f.router.init())
	release := make(chan struct{})
	lateResult := readyResult(-1, nil)
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		select {
		case <-release:
			return lateResult, nil
		case <-ctx.Done():
			t.Error("the blocked append must keep running on the instance context")
			return nil, ctx.Err()
		}
	})
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})
	tw := f.router.static

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.appendCalls())
	assert.Equal(t, int32(1), tw.blockedAppends.Load())

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")), "fails fast while blocked")
	assert.Equal(t, int32(1), f.appendCalls(), "no new append is started while one is blocked")

	close(release)
	require.Eventually(t, func() bool { return tw.blockedAppends.Load() == 0 }, time.Second, time.Millisecond)
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) { return readyResult(-1, nil), nil })

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(2), f.appendCalls())
	assert.Equal(t, int32(1), lateResult.getCalls.Load(), "the late result is queued and checked on the next flush")
}

// Exactly-once: when a blocked append completes after the flush gave up, the rows sit at the
// offset that was used, so the counter must advance before the redelivered chunk is appended.
func TestFLBPluginFlushCtxBlockedAppendExactlyOnceAdvancesOffset(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.exactlyOnce = true
		c.streamType = managedwriter.CommittedStream
		c.numRetries = 3
		c.flushTimeout = 50 * time.Millisecond
	})
	require.NoError(t, f.router.init())
	release := make(chan struct{})
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		<-release
		return readyResult(0, nil), nil
	})
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"}, map[string]interface{}{"Text": "b"})
	tw := f.router.static

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.appendCalls(), "blocked appends are not retried synchronously")
	assert.Equal(t, int64(0), tw.offset())

	close(release)
	require.Eventually(t, func() bool { return tw.offset() == 2 }, time.Second, time.Millisecond)
	assert.Equal(t, int32(0), tw.blockedAppends.Load())
}

// Exactly-once: AppendRows returned but the server response does not arrive within the flush
// timeout. The rows are committed at the offset that was used, so the flush must report the
// append as blocked (FLB_RETRY, fail fast until the response arrives) and advance the counter
// when it does, instead of leaving the counter behind and having every later append rejected
// with OFFSET_ALREADY_EXISTS.
func TestFLBPluginFlushCtxExactlyOnceLateResponseAdvancesOffset(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.exactlyOnce = true
		c.streamType = managedwriter.CommittedStream
		c.numRetries = 3
		c.flushTimeout = 50 * time.Millisecond
	})
	require.NoError(t, f.router.init())
	pending := pendingResult()
	pending.offset = 0
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) { return pending, nil })
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"}, map[string]interface{}{"Text": "b"})
	tw := f.router.static

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.appendCalls(), "a late response is not retried synchronously")
	assert.Equal(t, int32(1), tw.blockedAppends.Load())
	assert.Equal(t, int64(0), tw.offset(), "the counter is not advanced before the response arrives")
	assert.Empty(t, f.clock.sleeps)

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")), "fails fast while the response is pending")
	assert.Equal(t, int32(1), f.appendCalls(), "no append is started at a possibly stale offset")

	close(pending.ready)
	require.Eventually(t, func() bool { return tw.offset() == 2 }, time.Second, time.Millisecond)
	require.Eventually(t, func() bool { return tw.blockedAppends.Load() == 0 }, time.Second, time.Millisecond)

	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) { return readyResult(2, nil), nil })
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(2), f.appendCalls())
	assert.Equal(t, int64(4), tw.offset(), "the redelivered chunk is accounted from the advanced offset")
}

// Exactly-once: a late response that turns out to be a rejection must not advance the counter.
func TestFLBPluginFlushCtxExactlyOnceLateRejectionKeepsOffset(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.exactlyOnce = true
		c.streamType = managedwriter.CommittedStream
		c.flushTimeout = 50 * time.Millisecond
	})
	require.NoError(t, f.router.init())
	pending := pendingResult()
	pending.err = status.Error(codes.InvalidArgument, "schema mismatch")
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) { return pending, nil })
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"})
	tw := f.router.static

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.Equal(t, int32(1), tw.blockedAppends.Load())

	close(pending.ready)
	require.Eventually(t, func() bool { return tw.blockedAppends.Load() == 0 }, time.Second, time.Millisecond)
	assert.Equal(t, int64(0), tw.offset())
}

// During an outage the managed writer's Close waits for a connection lock held by the blocked
// AppendRows, which only cancelling the instance context releases. FLBPluginExitCtx must drain
// the responses of acknowledged appends, then cancel before closing when an append is blocked,
// so that shutdown completes instead of hanging until SIGKILL. The real Close reports
// context.Canceled after such a cancel; that is not a failure.
func TestFLBPluginExitCtxBlockedAppendCancelsInstance(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.flushTimeout = 50 * time.Millisecond
	})
	require.NoError(t, f.router.init())
	instanceCtx := f.cfg.instanceCtx
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

	// An acknowledged append whose response is still pending (healthy path, before the outage).
	acked := pendingResult()
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) { return acked, nil })
	require.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))

	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	})
	f.stream.mu.Lock()
	f.stream.onClose = func() error {
		<-instanceCtx.Done() // the real Close deadlocks until the blocked append is cancelled
		return context.Canceled
	}
	f.stream.mu.Unlock()
	require.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.Equal(t, int32(1), f.router.static.blockedAppends.Load())

	start := time.Now()
	assert.Equal(t, output.FLB_OK, FLBPluginExitCtx(ptr))
	assert.Less(t, time.Since(start), f.cfg.flushTimeout, "cancels before waiting out the exit deadline")
	assert.Error(t, instanceCtx.Err())
	assert.Equal(t, int32(1), acked.getCalls.Load(), "the acknowledged append is still drained")
	assert.Equal(t, int32(1), f.stream.closes.Load())
	assert.Equal(t, int32(1), f.client.closes.Load())
}

// When closing hangs for another reason (no blocked append was observed yet), the exit deadline
// cancels the instance context and the exit still returns, reporting the failure.
func TestFLBPluginExitCtxCloseTimeoutCancelsInstance(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) { c.flushTimeout = 50 * time.Millisecond })
	require.NoError(t, f.router.init())
	instanceCtx := f.cfg.instanceCtx
	f.stream.mu.Lock()
	f.stream.onClose = func() error {
		<-instanceCtx.Done()
		return io.EOF
	}
	f.stream.mu.Unlock()
	ptr := registerTestConfig(t, f.cfg)

	start := time.Now()
	assert.Equal(t, output.FLB_ERROR, FLBPluginExitCtx(ptr))
	assert.Less(t, time.Since(start), 2*f.cfg.flushTimeout+time.Second)
	assert.Error(t, instanceCtx.Err())
	assert.Equal(t, int32(1), f.stream.closes.Load())
	assert.Equal(t, int32(1), f.client.closes.Load(), "the client is still closed after a stream close timeout")
}

// Schema fetch and stream creation retry UNAVAILABLE for minutes by default; both must give up
// after Init_Timeout_Sec so an unreachable backend cannot hang FLBPluginInit. A stream created
// after the deadline is closed rather than leaked.
func TestTableWriterInitTimeout(t *testing.T) {
	t.Run("schema fetch", func(t *testing.T) {
		f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) { c.initTimeout = 50 * time.Millisecond })
		f.client.blockGet = make(chan struct{})
		defer close(f.client.blockGet)

		start := time.Now()
		err := f.router.init()
		require.Error(t, err)
		assert.True(t, isTransientError(err), "deadline errors are transient: %v", err)
		assert.Less(t, time.Since(start), time.Second)
		assert.Equal(t, 0, f.client.newStreamCalls())
	})
	t.Run("stream creation", func(t *testing.T) {
		f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) { c.initTimeout = 50 * time.Millisecond })
		f.client.blockNew = make(chan struct{})

		start := time.Now()
		err := f.router.init()
		require.Error(t, err)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
		assert.Less(t, time.Since(start), time.Second)
		assert.Nil(t, f.router.static)

		close(f.client.blockNew)
		require.Eventually(t, func() bool { return f.stream.closes.Load() == 1 }, time.Second, time.Millisecond,
			"a stream created after the deadline is closed")
	})
}

// A transient failure while initializing a tag-routed table (backend unreachable) is not an
// unknown table: the chunk is retried even with Unknown_Table_Action drop, and the failure is
// not cached for initFailureTTL so the next flush tries again right away.
func TestFLBPluginFlushCtxTransientInitFailureRetries(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", func(c *outputConfig) {
		c.unknownTable = unknownTableDrop
		c.retryOnFailure = true
	})
	require.NoError(t, f.router.init())
	f.failTable("ds", "orders", status.Error(codes.Unavailable, "x"))
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app.orders")))
	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app.orders")))
	assert.Equal(t, int32(2), f.calls("ds", "orders"), "transient init failures are not cached")

	f.unfailTable("ds", "orders")
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app.orders")))
	assert.Equal(t, int32(1), f.appendCalls())
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
	t.Run("Flush_Timeout_Sec below 1", func(t *testing.T) {
		installTestDeps(t, defaultInitKeys(map[string]string{"Flush_Timeout_Sec": "0"}))
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

// streamCount returns the number of streams of tw under streamMu.
func streamCount(tw *tableWriter) int {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	return len(tw.streams)
}

// isScaling reports whether tw is creating an additional stream in the background.
func isScaling(tw *tableWriter) bool {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	return tw.scaling
}

// blockNewStreams makes NewManagedStream block until the returned channel is closed.
func blockNewStreams(c *fakeClient) chan struct{} {
	block := make(chan struct{})
	c.mu.Lock()
	defer c.mu.Unlock()
	c.blockNew = block
	return block
}

// TestFLBPluginFlushCtxDynamicScaling verifies that a new stream is added whenever the least
// loaded stream has more pending responses than the request-count threshold. The stream is
// created in the background; the test gates its completion to keep the sequence deterministic.
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
	tw := configMap[id].router.static
	block := blockNewStreams(d.client)

	data, n := fiveRecords(t)
	// The first flush finds an empty queue; the second observes one pending response on the
	// only stream and starts creating another one.
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.Equal(t, 1, streamCount(tw))
	assert.True(t, isScaling(tw))
	close(block)
	require.Eventually(t, func() bool { return streamCount(tw) == 2 }, time.Second, time.Millisecond)

	// The next flush lands on the new (empty) stream; the one after observes its pending
	// response and adds a third stream.
	block = blockNewStreams(d.client)
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.False(t, isScaling(tw))
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(plugin, data, n, cTag("app")))
	assert.True(t, isScaling(tw))
	close(block)
	require.Eventually(t, func() bool { return streamCount(tw) == 3 }, time.Second, time.Millisecond)
	assert.Equal(t, 3, d.client.newStreamCalls(), "init creates the first stream, scaling the other two")
}

func TestFLBPluginFlushCtxDynamicScalingCap(t *testing.T) {
	d := installTestDeps(t, defaultInitKeys(nil))
	plugin, id := initPlugin(t)
	configMap[id].requestCountThreshold = 0
	d.client.mu.Lock()
	d.client.stream = &fakeStream{pending: true}
	d.client.mu.Unlock()
	d.client.streams[0].pending = true
	tw := configMap[id].router.static

	data, n := fiveRecords(t)
	require.Eventually(t, func() bool {
		FLBPluginFlushCtx(plugin, data, n, cTag("app"))
		return streamCount(tw) == maxNumStreamsPerInstance
	}, 5*time.Second, time.Millisecond)
	for i := 0; i < 3; i++ {
		FLBPluginFlushCtx(plugin, data, n, cTag("app"))
	}
	assert.False(t, isScaling(tw))
	assert.Equal(t, maxNumStreamsPerInstance, streamCount(tw))
	assert.Equal(t, maxNumStreamsPerInstance, d.client.newStreamCalls())
}

// Stream creation can take up to Init_Timeout_Sec while the backend is degraded. It must run
// in the background: neither the flush that triggers the scaling nor concurrent flushes of the
// same table may wait for it, and only one additional stream is created at a time.
func TestFLBPluginFlushCtxDynamicScalingDoesNotBlockFlushes(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.requestCountThreshold = 0
		c.flushTimeout = 500 * time.Millisecond
		c.initTimeout = 10 * time.Second
	})
	f.stream.pending = true
	require.NoError(t, f.router.init())
	block := blockNewStreams(f.client)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})
	tw := f.router.static

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	start := time.Now()
	for i := 0; i < 3; i++ {
		assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	}
	assert.Less(t, time.Since(start), f.cfg.flushTimeout, "flushes do not wait for the stream being created")
	assert.Equal(t, int32(4), f.appendCalls(), "every flush is appended to the existing stream meanwhile")
	assert.True(t, isScaling(tw))
	assert.Equal(t, 1, streamCount(tw))

	close(block)
	require.Eventually(t, func() bool { return streamCount(tw) == 2 }, time.Second, time.Millisecond)
	assert.False(t, isScaling(tw))
	assert.Equal(t, 2, f.client.newStreamCalls(), "concurrent flushes do not start a second stream creation")
}

// A scaling attempt that fails (here: Init_Timeout_Sec exceeded) leaves the stream list as it
// was and allows a later flush to try again.
func TestFLBPluginFlushCtxDynamicScalingFailureRetriesLater(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.requestCountThreshold = 0
		c.initTimeout = 200 * time.Millisecond
	})
	f.stream.pending = true
	require.NoError(t, f.router.init())
	block := blockNewStreams(f.client)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "hello"})
	tw := f.router.static

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.True(t, isScaling(tw))
	require.Eventually(t, func() bool { return !isScaling(tw) }, time.Second, time.Millisecond, "the attempt gives up after Init_Timeout_Sec")
	assert.Equal(t, 1, streamCount(tw))

	close(block)
	require.Eventually(t, func() bool { return f.stream.closes.Load() == 1 }, time.Second, time.Millisecond,
		"a stream created after the deadline is closed")
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.Eventually(t, func() bool { return streamCount(tw) == 2 }, time.Second, time.Millisecond, "the next flush scales again")
}

// A stream whose creation completes after the writer was closed is closed rather than added.
func TestTableWriterCloseDuringScaling(t *testing.T) {
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.requestCountThreshold = 0
		c.initTimeout = 10 * time.Second
	})
	f.stream.pending = true
	require.NoError(t, f.router.init())
	block := blockNewStreams(f.client)
	tw := f.router.static

	require.NoError(t, tw.flush(context.Background(), [][]byte{{}}))
	tw.maybeScale()
	require.True(t, isScaling(tw))

	assert.False(t, tw.close(context.Background()))
	assert.Equal(t, int32(1), f.stream.closes.Load())

	close(block)
	require.Eventually(t, func() bool { return f.stream.closes.Load() == 2 }, time.Second, time.Millisecond,
		"the late stream is closed instead of being added")
	assert.Equal(t, 1, streamCount(tw))
	assert.False(t, isScaling(tw))
	tw.maybeScale()
	assert.False(t, isScaling(tw), "a closed writer does not scale")
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

// invalidStreamTypeErr builds the AppendRows rejection that means the committed stream itself
// is invalid: an INVALID_ARGUMENT status carrying a StorageError INVALID_STREAM_TYPE detail.
func invalidStreamTypeErr(t *testing.T) error {
	t.Helper()
	return storageErr(t, storagepb.StorageError_INVALID_STREAM_TYPE)
}

// storageErr builds an INVALID_ARGUMENT status carrying the given StorageError code.
func storageErr(t *testing.T, code storagepb.StorageError_StorageErrorCode) error {
	t.Helper()
	st, err := status.New(codes.InvalidArgument, code.String()).WithDetails(&storagepb.StorageError{Code: code})
	require.NoError(t, err)
	return st.Err()
}

func TestRebuildPredicate(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{"invalid stream type detail", invalidStreamTypeErr(t), true},
		{"offset already exists detail", storageErr(t, storagepb.StorageError_OFFSET_ALREADY_EXISTS), true},
		{"offset out of range detail", storageErr(t, storagepb.StorageError_OFFSET_OUT_OF_RANGE), true},
		{"other storage error detail", storageErr(t, storagepb.StorageError_STREAM_NOT_FOUND), false},
		{"status without details", status.Error(codes.InvalidArgument, "x"), false},
		{"plain error", errors.New("boom"), false},
		{"nil", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, rebuildPredicate(tt.err))
		})
	}
}

// currentStream returns the managed stream at index 0 under streamMu (nil while rebuilding).
func currentStream(tw *tableWriter) MWManagedStream {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	return tw.streams[0].managedstream
}

// newExactlyOnceFixture returns an initialised exactly-once fixture whose NewManagedStream hands
// out a distinct fakeStream per call from now on, so a rebuilt stream can be told apart from the
// initial one (f.stream).
func newExactlyOnceFixture(t *testing.T, mutate func(*outputConfig)) *routerFixture {
	t.Helper()
	f := newRouterFixture(t, "ds", "tbl", func(c *outputConfig) {
		c.retryOnFailure = true
		c.exactlyOnce = true
		c.streamType = managedwriter.CommittedStream
		c.numRetries = 0
		if mutate != nil {
			mutate(c)
		}
	})
	require.NoError(t, f.router.init())
	f.client.mu.Lock()
	f.client.stream = nil
	f.client.mu.Unlock()
	return f
}

// An append rejected with INVALID_STREAM_TYPE replaces the committed stream: the old one is
// finalized and closed, the chunk is appended to the new one within the same flush, and the
// offset counter restarts at 0 because a new committed stream does.
func TestFLBPluginFlushCtxExactlyOnceRebuildsInvalidStream(t *testing.T) {
	f := newExactlyOnceFixture(t, nil)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"}, map[string]interface{}{"Text": "b"})
	tw := f.router.static
	rejections := 0
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		if rejections == 0 {
			return readyResult(0, nil), nil
		}
		return readyResult(-1, invalidStreamTypeErr(t)), nil
	})

	require.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.Equal(t, int64(2), tw.offset())
	rejections++

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(2), f.stream.appends.Load())
	assert.Equal(t, int32(1), f.stream.finalizes.Load(), "the invalid stream is finalized")
	assert.Equal(t, int32(1), f.stream.closes.Load(), "the invalid stream is closed")
	require.Len(t, f.client.streams, 2, "one stream is rebuilt")
	rebuilt := f.client.streams[1]
	assert.Same(t, rebuilt, currentStream(tw))
	assert.Equal(t, int32(1), rebuilt.appends.Load(), "the chunk is appended to the new stream in the same flush")
	assert.Equal(t, int64(2), tw.offset(), "the offset restarts at 0 on the new stream")
	assert.Empty(t, f.clock.sleeps, "the rebuild does not count as a retry attempt")
}

// Safety net: should the offset counter ever diverge from the backend's, the rejection
// (OFFSET_ALREADY_EXISTS) replaces the committed stream instead of dropping this and every
// later chunk of the table with FLB_ERROR.
func TestFLBPluginFlushCtxExactlyOnceRebuildsOnOffsetMismatch(t *testing.T) {
	f := newExactlyOnceFixture(t, nil)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"})
	tw := f.router.static
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		return readyResult(-1, storageErr(t, storagepb.StorageError_OFFSET_ALREADY_EXISTS)), nil
	})

	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.stream.finalizes.Load())
	require.Len(t, f.client.streams, 2, "one stream is rebuilt")
	assert.Same(t, f.client.streams[1], currentStream(tw))
	assert.Equal(t, int32(1), f.client.streams[1].appends.Load())
	assert.Equal(t, int64(1), tw.offset(), "the counter restarts with the new stream")
}

// A rebuilt stream that is rejected the same way is not rebuilt again within the flush: the
// INVALID_ARGUMENT status is returned as is and is a permanent failure.
func TestFLBPluginFlushCtxExactlyOnceRebuildsOnlyOncePerFlush(t *testing.T) {
	f := newExactlyOnceFixture(t, nil)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"})
	tw := f.router.static
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		return readyResult(-1, invalidStreamTypeErr(t)), nil
	})
	rejected := &fakeStream{}
	rejected.onAppend = f.stream.onAppend
	f.client.mu.Lock()
	f.client.stream = rejected
	f.client.mu.Unlock()

	assert.Equal(t, output.FLB_ERROR, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, 2, f.client.newStreamCalls(), "only one rebuild per flush")
	assert.Equal(t, int32(1), f.stream.appends.Load())
	assert.Equal(t, int32(1), rejected.appends.Load())
	assert.Same(t, rejected, currentStream(tw), "the rebuilt stream stays in place")
	assert.Equal(t, int32(0), rejected.closes.Load())
	assert.Equal(t, int64(0), tw.offset())
	assert.Empty(t, f.clock.sleeps)
}

// When the rebuild itself fails (backend unreachable past Init_Timeout_Sec) the closed stream
// must not stay in place: the flush is transient, the table has no stream until the next flush
// rebuilds it, and that flush succeeds on the new stream once the backend is back.
func TestFLBPluginFlushCtxExactlyOnceRebuildFailureRecovers(t *testing.T) {
	f := newExactlyOnceFixture(t, func(c *outputConfig) { c.initTimeout = 100 * time.Millisecond })
	block := blockNewStreams(f.client)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"})
	tw := f.router.static
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		return readyResult(-1, invalidStreamTypeErr(t)), nil
	})

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Equal(t, int32(1), f.stream.closes.Load())
	assert.Nil(t, currentStream(tw), "the closed stream is not left in place")

	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")), "still unreachable")
	assert.Equal(t, int32(1), f.stream.appends.Load(), "the closed stream is never appended to again")
	assert.Equal(t, int32(1), f.stream.closes.Load(), "and not closed twice")

	close(block)
	require.Eventually(t, func() bool { return f.client.newStreamCalls() == 3 }, time.Second, time.Millisecond,
		"the two timed-out creations complete in the background")
	assert.Equal(t, output.FLB_OK, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	require.Equal(t, 4, f.client.newStreamCalls(), "init, two timed-out rebuilds, one successful rebuild")
	rebuilt := f.client.streams[3]
	assert.Same(t, rebuilt, currentStream(tw))
	assert.Equal(t, int32(1), rebuilt.appends.Load())
	assert.Equal(t, int64(1), tw.offset())
	for _, late := range f.client.streams[1:3] {
		require.Eventually(t, func() bool { return late.closes.Load() == 1 }, time.Second, time.Millisecond,
			"streams created after the deadline are closed")
	}
}

// Finalize/Close/NewManagedStream of a rebuild run outside streamMu: a concurrent flush of the
// same table (workers > 1) fails fast with a transient error instead of waiting for the rebuild
// (up to Init_Timeout_Sec), and only one rebuild is started.
func TestFLBPluginFlushCtxExactlyOnceRebuildDoesNotBlockFlushes(t *testing.T) {
	f := newExactlyOnceFixture(t, func(c *outputConfig) {
		c.initTimeout = 10 * time.Second
		c.flushTimeout = 500 * time.Millisecond
	})
	block := blockNewStreams(f.client)
	ptr := registerTestConfig(t, f.cfg)
	data, n := encodeRecords(t, map[string]interface{}{"Text": "a"})
	tw := f.router.static
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		return readyResult(-1, invalidStreamTypeErr(t)), nil
	})

	first := make(chan int, 1)
	go func() { first <- FLBPluginFlushCtx(ptr, data, n, cTag("app")) }()
	require.Eventually(t, func() bool { return currentStream(tw) == nil }, time.Second, time.Millisecond, "the first flush starts rebuilding")

	start := time.Now()
	assert.Equal(t, output.FLB_RETRY, FLBPluginFlushCtx(ptr, data, n, cTag("app")))
	assert.Less(t, time.Since(start), f.cfg.flushTimeout, "the concurrent flush does not wait for the rebuild")
	assert.Equal(t, int32(1), f.stream.appends.Load(), "the concurrent flush does not touch the closed stream")

	close(block)
	assert.Equal(t, output.FLB_OK, <-first)
	assert.Equal(t, 2, f.client.newStreamCalls(), "the concurrent flush does not start a second rebuild")
	assert.Equal(t, int32(1), f.client.streams[1].appends.Load())
	assert.Equal(t, int64(1), tw.offset())
}

// A stream whose rebuild completes after the writer was closed is closed rather than installed.
func TestTableWriterCloseDuringRebuild(t *testing.T) {
	f := newExactlyOnceFixture(t, func(c *outputConfig) { c.initTimeout = 10 * time.Second })
	block := blockNewStreams(f.client)
	tw := f.router.static
	f.onAppend(func(ctx context.Context, data [][]byte) (appendResult, error) {
		return readyResult(-1, invalidStreamTypeErr(t)), nil
	})

	flushErr := make(chan error, 1)
	go func() { flushErr <- tw.flush(context.Background(), [][]byte{{}}) }()
	require.Eventually(t, func() bool { return currentStream(tw) == nil }, time.Second, time.Millisecond)

	assert.False(t, tw.close(context.Background()))
	assert.Equal(t, int32(1), f.stream.closes.Load(), "close does not touch the detached stream again")

	close(block)
	err := <-flushErr
	require.Error(t, err)
	assert.True(t, isTransientError(err), "%v", err)
	require.Eventually(t, func() bool { return f.client.streams[1].closes.Load() == 1 }, time.Second, time.Millisecond,
		"the late stream is closed instead of being installed")
	assert.Nil(t, currentStream(tw))
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

func TestFLBPluginInitEndpoint(t *testing.T) {
	t.Run("endpoint only", func(t *testing.T) {
		d := installTestDeps(t, defaultInitKeys(map[string]string{"Endpoint": " emulator:9060 "}))
		initPlugin(t)
		require.Len(t, d.clientOpts, 1)
		assert.Equal(t, []option.ClientOption{option.WithEndpoint("emulator:9060")}, d.clientOpts[0])
	})
	t.Run("insecure endpoint", func(t *testing.T) {
		d := installTestDeps(t, defaultInitKeys(map[string]string{"Endpoint": "emulator:9060", "Insecure": "true"}))
		initPlugin(t)
		require.Len(t, d.clientOpts, 1)
		// The dial option wraps a fresh credentials object, so only the shape is checked.
		require.Len(t, d.clientOpts[0], 3)
		assert.Equal(t, option.WithEndpoint("emulator:9060"), d.clientOpts[0][0])
		assert.Equal(t, option.WithoutAuthentication(), d.clientOpts[0][1])
	})
	t.Run("insecure without endpoint", func(t *testing.T) {
		installTestDeps(t, defaultInitKeys(map[string]string{"Insecure": "true"}))
		assert.Equal(t, output.FLB_ERROR, FLBPluginInit(unsafe.Pointer(new(int))))
	})
	t.Run("invalid insecure", func(t *testing.T) {
		installTestDeps(t, defaultInitKeys(map[string]string{"Endpoint": "emulator:9060", "Insecure": "maybe"}))
		assert.Equal(t, output.FLB_ERROR, FLBPluginInit(unsafe.Pointer(new(int))))
	})
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
