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
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

// Test doubles for the seams declared in client.go and plugin.go. They replace the gomonkey
// patches the test-suite used to rely on, so the tests run with -race and without disabling
// inlining.

// fakeBridge stands in for the Fluent Bit C bridge. Configuration keys come from `keys`, and
// SetContext/GetContext share a map keyed by the plugin pointer, exactly like Fluent Bit does.
type fakeBridge struct {
	mu          sync.Mutex
	keys        map[string]string
	contexts    map[unsafe.Pointer]interface{}
	registered  []string
	keysRead    map[string]int
	setContexts int
	getContexts int
}

func newFakeBridge(keys map[string]string) *fakeBridge {
	if keys == nil {
		keys = map[string]string{}
	}
	return &fakeBridge{keys: keys, contexts: map[unsafe.Pointer]interface{}{}, keysRead: map[string]int{}}
}

func (b *fakeBridge) Register(_ unsafe.Pointer, name, _ string) int {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.registered = append(b.registered, name)
	return 0
}

func (b *fakeBridge) ConfigKey(_ unsafe.Pointer, key string) string {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.keysRead[key]++
	return b.keys[key]
}

func (b *fakeBridge) SetContext(plugin unsafe.Pointer, ctx interface{}) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setContexts++
	b.contexts[plugin] = ctx
}

func (b *fakeBridge) GetContext(ctx unsafe.Pointer) interface{} {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.getContexts++
	return b.contexts[ctx]
}

// fakeClock is a controllable clock; sleep returns immediately and records the requested durations.
type fakeClock struct {
	mu     sync.Mutex
	t      time.Time
	sleeps []time.Duration
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Unix(1_700_000_000, 0)}
}

func (c *fakeClock) clock() clock {
	return clock{
		now: func() time.Time {
			c.mu.Lock()
			defer c.mu.Unlock()
			return c.t
		},
		sleep: func(d time.Duration) {
			c.mu.Lock()
			defer c.mu.Unlock()
			c.sleeps = append(c.sleeps, d)
		},
	}
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// fakeResult is a controllable appendResult. A ready result reports offset/err immediately; a
// pending one is skipped by isReady but still answers GetResult (as the exit drain does).
type fakeResult struct {
	ready    chan struct{}
	offset   int64
	err      error
	resp     *storagepb.AppendRowsResponse
	getCalls atomic.Int32
}

func readyResult(offset int64, err error) *fakeResult {
	r := &fakeResult{ready: make(chan struct{}), offset: offset, err: err}
	close(r.ready)
	return r
}

func pendingResult() *fakeResult {
	return &fakeResult{ready: make(chan struct{}), offset: -1}
}

func (r *fakeResult) Ready() <-chan struct{} { return r.ready }

func (r *fakeResult) GetResult(context.Context) (int64, error) {
	r.getCalls.Add(1)
	return r.offset, r.err
}

func (r *fakeResult) FullResponse(context.Context) (*storagepb.AppendRowsResponse, error) {
	return r.resp, r.err
}

// fakeStream records AppendRows calls. onAppend, when set, decides the result of each call;
// otherwise a ready, successful result is returned (or a pending one when pending is true).
// Close returns io.EOF like the real ManagedStream does on a normal close, unless onClose is set.
type fakeStream struct {
	mu        sync.Mutex
	name      string
	pending   bool
	onAppend  func(ctx context.Context, data [][]byte, opts []managedwriter.AppendOption) (appendResult, error)
	onClose   func() error
	appends   atomic.Int32
	closes    atomic.Int32
	finalizes atomic.Int32
	results   []*fakeResult
	ctxs      []context.Context
}

func (s *fakeStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error) {
	s.appends.Add(1)
	s.mu.Lock()
	s.ctxs = append(s.ctxs, ctx)
	onAppend, pending := s.onAppend, s.pending
	s.mu.Unlock()
	if onAppend != nil {
		return onAppend(ctx, data, opts)
	}
	var r *fakeResult
	if pending {
		r = pendingResult()
	} else {
		r = readyResult(-1, nil)
	}
	s.mu.Lock()
	s.results = append(s.results, r)
	s.mu.Unlock()
	return r, nil
}

func (s *fakeStream) Close() error {
	s.closes.Add(1)
	s.mu.Lock()
	onClose := s.onClose
	s.mu.Unlock()
	if onClose != nil {
		return onClose()
	}
	return io.EOF
}

func (s *fakeStream) Finalize(context.Context, ...gax.CallOption) (int64, error) {
	s.finalizes.Add(1)
	return 0, nil
}

func (s *fakeStream) StreamName() string { return s.name }

// getResultCalls sums GetResult calls over every result this stream produced.
func (s *fakeStream) getResultCalls() int32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int32
	for _, r := range s.results {
		n += r.getCalls.Load()
	}
	return n
}

// fakeClient answers GetWriteStream from `schema` (or fails tables listed in failTables) and
// hands out fakeStreams from NewManagedStream. When `stream` is set every table shares it.
// blockGet / blockNew, when set, make GetWriteStream / NewManagedStream wait on the channel (or
// on ctx) first, simulating an unreachable backend.
type fakeClient struct {
	mu             sync.Mutex
	schema         *storagepb.TableSchema
	failTables     map[string]error
	getWriteStream map[string]int
	stream         *fakeStream
	streams        []*fakeStream
	newStreamOpts  [][]managedwriter.WriterOption
	newStreamErr   error
	blockGet       chan struct{}
	blockNew       chan struct{}
	closes         atomic.Int32
}

var defaultTestSchema = &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
	{Name: "Time", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
	{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
}}

func newFakeClient() *fakeClient {
	return &fakeClient{
		schema:         defaultTestSchema,
		failTables:     map[string]error{},
		getWriteStream: map[string]int{},
	}
}

// wait blocks until block is closed or ctx expires. A nil block does not wait.
func wait(ctx context.Context, block chan struct{}) error {
	if block == nil {
		return nil
	}
	select {
	case <-block:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *fakeClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
	c.mu.Lock()
	block := c.blockNew
	c.mu.Unlock()
	if err := wait(ctx, block); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.newStreamOpts = append(c.newStreamOpts, opts)
	if c.newStreamErr != nil {
		return nil, c.newStreamErr
	}
	s := c.stream
	if s == nil {
		s = &fakeStream{}
	}
	c.streams = append(c.streams, s)
	return s, nil
}

func (c *fakeClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, _ ...gax.CallOption) (*storagepb.WriteStream, error) {
	c.mu.Lock()
	c.getWriteStream[req.GetName()]++
	block := c.blockGet
	c.mu.Unlock()
	if err := wait(ctx, block); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err, ok := c.failTables[req.GetName()]; ok {
		return nil, err
	}
	return &storagepb.WriteStream{Name: req.GetName(), TableSchema: c.schema}, nil
}

func (c *fakeClient) Close() error { c.closes.Add(1); return nil }

func (c *fakeClient) newStreamCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.newStreamOpts)
}

func (c *fakeClient) getWriteStreamCalls(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.getWriteStream[name]
}

// appendCalls sums AppendRows calls over every stream handed out by the client.
func (c *fakeClient) appendCalls() int32 {
	c.mu.Lock()
	defer c.mu.Unlock()
	seen := map[*fakeStream]bool{}
	var n int32
	for _, s := range c.streams {
		if !seen[s] {
			seen[s] = true
			n += s.appends.Load()
		}
	}
	return n
}

// testDeps wires fakes into the package-level deps for the duration of the test.
type testDeps struct {
	bridge     *fakeBridge
	client     *fakeClient
	clock      *fakeClock
	newClients atomic.Int32
	newClient  newClientFunc
	clientOpts [][]option.ClientOption
	mu         sync.Mutex
}

func installTestDeps(t *testing.T, keys map[string]string) *testDeps {
	t.Helper()
	d := &testDeps{bridge: newFakeBridge(keys), client: newFakeClient(), clock: newFakeClock()}
	orig := deps
	deps = pluginDeps{
		flb: d.bridge,
		newClient: func(ctx context.Context, projectID string, opts ...option.ClientOption) (ManagedWriterClient, error) {
			d.newClients.Add(1)
			d.mu.Lock()
			d.clientOpts = append(d.clientOpts, opts)
			d.mu.Unlock()
			if d.newClient != nil {
				return d.newClient(ctx, projectID, opts...)
			}
			return d.client, nil
		},
		clock: d.clock.clock(),
	}
	t.Cleanup(func() { deps = orig })
	return d
}

// initPlugin runs FLBPluginInit with a unique plugin pointer and returns that pointer, which
// doubles as the ctx argument of FLBPluginFlushCtx/FLBPluginExitCtx (the fake bridge maps it
// to the instance id, as Fluent Bit does).
func initPlugin(t *testing.T) (unsafe.Pointer, int) {
	t.Helper()
	plugin := unsafe.Pointer(new(int))
	before := getInstanceCount()
	if res := FLBPluginInit(plugin); res != output.FLB_OK {
		t.Fatalf("FLBPluginInit returned %d", res)
	}
	if getInstanceCount() != before+1 {
		t.Fatalf("FLBPluginInit did not register a new instance")
	}
	id, _ := deps.flb.GetContext(plugin).(int)
	t.Cleanup(func() { delete(configMap, id) })
	return plugin, id
}

// getInstanceCount returns the number of registered output instances.
func getInstanceCount() int {
	return len(configMap)
}

// getOffset returns the offset counter of the first stream of the instance's static table writer.
func getOffset(id int) int64 {
	config, ok := configMap[id]
	if !ok || config == nil || config.router == nil || config.router.static == nil {
		return 0
	}
	return config.router.static.offset()
}
