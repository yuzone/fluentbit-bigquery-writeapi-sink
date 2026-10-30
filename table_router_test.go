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
	"sync"
	"testing"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseTagTemplateRender(t *testing.T) {
	tests := []struct {
		name       string
		template   string
		tag        string
		delimiters string
		want       string
		dynamic    bool
		wantErr    bool
	}{
		{name: "static", template: "my_table", tag: "app.orders", delimiters: ".", want: "my_table"},
		{name: "whole tag", template: "$TAG", tag: "app.orders", delimiters: ".", want: "app.orders", dynamic: true},
		{name: "indexed", template: "$TAG[1]", tag: "app.orders.us", delimiters: ".", want: "orders", dynamic: true},
		{name: "literal prefix and suffix", template: "log_$TAG[1]_v2", tag: "app.orders", delimiters: ".", want: "log_orders_v2", dynamic: true},
		{name: "two placeholders", template: "$TAG[0]_$TAG[2]", tag: "a.b.c", delimiters: ".", want: "a_c", dynamic: true},
		{name: "custom delimiters", template: "$TAG[2]", tag: "kube_var-log_orders", delimiters: "_-", want: "log", dynamic: true},
		{name: "consecutive delimiters are collapsed", template: "$TAG[1]", tag: "app..orders", delimiters: ".", want: "orders", dynamic: true},
		{name: "index out of range", template: "$TAG[3]", tag: "app.orders", delimiters: ".", dynamic: true, wantErr: true},
		{name: "empty tag with index", template: "$TAG[0]", tag: "", delimiters: ".", dynamic: true, wantErr: true},
		{name: "$TAG followed by text", template: "$TAGx", tag: "app", delimiters: ".", want: "appx", dynamic: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tpl, err := parseTagTemplate(tt.template)
			require.NoError(t, err)
			assert.Equal(t, tt.dynamic, tpl.dynamic())
			got, err := tpl.render(tt.tag, tt.delimiters)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseTagTemplateErrors(t *testing.T) {
	for _, tpl := range []string{"$TAG[", "$TAG[x]", "$TAG[-1]", "$TAG[]"} {
		_, err := parseTagTemplate(tpl)
		assert.Error(t, err, tpl)
	}
}

func TestValidIDs(t *testing.T) {
	assert.True(t, validTableID("orders"))
	assert.True(t, validTableID("orders-2024 v1_x"))
	assert.True(t, validTableID("注文"))
	assert.False(t, validTableID(""))
	assert.False(t, validTableID("orders.v1"))
	assert.False(t, validTableID("orders/v1"))
	assert.False(t, validTableID(string(make([]byte, maxBigQueryID+1))))

	assert.True(t, validDatasetID("my_dataset1"))
	assert.False(t, validDatasetID("my-dataset"))
	assert.False(t, validDatasetID(""))
}

// routerFixture builds an outputConfig + tableRouter backed by a fakeClient. All tables share
// one fakeStream so appends can be counted and controlled in one place.
type routerFixture struct {
	cfg    *outputConfig
	router *tableRouter
	deps   *testDeps
	client *fakeClient
	stream *fakeStream
	clock  *fakeClock
}

func newRouterFixture(t *testing.T, dataset, table string, mutate func(*outputConfig)) *routerFixture {
	t.Helper()
	d := installTestDeps(t, nil)
	f := &routerFixture{deps: d, client: d.client, clock: d.clock, stream: &fakeStream{}}
	f.client.stream = f.stream
	f.client.schema = &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
	}}

	datasetTpl, err := parseTagTemplate(dataset)
	require.NoError(t, err)
	tableTpl, err := parseTagTemplate(table)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	f.cfg = &outputConfig{
		projectID:             "p",
		datasetTemplate:       datasetTpl,
		tableTemplate:         tableTpl,
		tagDelimiters:         tagDelimitersDefault,
		streamType:            managedwriter.DefaultStream,
		enableRetry:           true,
		maxChunkSize:          chunkSizeLimit,
		maxQueueBytes:         queueByteDefault,
		maxQueueRequests:      queueRequestDefault,
		requestCountThreshold: requestCountThreshold(queueRequestDefault),
		dateTimeStringType:    true,
		flushTimeout:          time.Second,
		client:                f.client,
		clock:                 d.clock.clock(),
		instanceCtx:           ctx,
		instanceCancel:        cancel,
	}
	if mutate != nil {
		mutate(f.cfg)
	}
	f.router = newTableRouter(f.cfg, 0)
	f.cfg.router = f.router
	return f
}

// failTable makes GetWriteStream fail for the given table.
func (f *routerFixture) failTable(dataset, table string, err error) {
	f.client.mu.Lock()
	defer f.client.mu.Unlock()
	f.client.failTables[streamName(dataset, table)] = err
}

func (f *routerFixture) unfailTable(dataset, table string) {
	f.client.mu.Lock()
	defer f.client.mu.Unlock()
	delete(f.client.failTables, streamName(dataset, table))
}

// onAppend installs the AppendRows handler of the shared stream.
func (f *routerFixture) onAppend(fn func(ctx context.Context, data [][]byte) (appendResult, error)) {
	f.stream.mu.Lock()
	defer f.stream.mu.Unlock()
	f.stream.onAppend = func(ctx context.Context, data [][]byte, _ []managedwriter.AppendOption) (appendResult, error) {
		return fn(ctx, data)
	}
}

func (f *routerFixture) appendCalls() int32 { return f.stream.appends.Load() }

func streamName(dataset, table string) string {
	return "projects/p/datasets/" + dataset + "/tables/" + table + "/streams/_default"
}

func (f *routerFixture) calls(dataset, table string) int32 {
	return int32(f.client.getWriteStreamCalls(streamName(dataset, table)))
}

func TestTableRouterStaticTableIgnoresTag(t *testing.T) {
	f := newRouterFixture(t, "ds", "static_table", nil)
	require.NoError(t, f.router.init())
	assert.False(t, f.router.dynamic())
	assert.Equal(t, int32(1), f.calls("ds", "static_table"))

	a, err := f.router.resolve("app.orders")
	require.NoError(t, err)
	b, err := f.router.resolve("completely.different")
	require.NoError(t, err)
	assert.Same(t, a, b)
	assert.Equal(t, "projects/p/datasets/ds/tables/static_table", a.tableRef)
	assert.Equal(t, int32(1), f.calls("ds", "static_table"), "static table is initialized once at init")
}

func TestTableRouterStaticInitFailure(t *testing.T) {
	f := newRouterFixture(t, "ds", "missing", nil)
	f.failTable("ds", "missing", errors.New("rpc error: code = NotFound"))
	assert.Error(t, f.router.init())
}

func TestTableRouterResolvesFromTag(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", nil)
	require.NoError(t, f.router.init())
	assert.True(t, f.router.dynamic())
	assert.Equal(t, int32(0), f.calls("ds", "orders"), "dynamic destinations are not initialized at init")

	orders, err := f.router.resolve("app.orders")
	require.NoError(t, err)
	assert.Equal(t, "projects/p/datasets/ds/tables/orders", orders.tableRef)

	users, err := f.router.resolve("app.users.extra")
	require.NoError(t, err)
	assert.Equal(t, "projects/p/datasets/ds/tables/users", users.tableRef)
	assert.NotSame(t, orders, users)

	again, err := f.router.resolve("app.orders")
	require.NoError(t, err)
	assert.Same(t, orders, again)
	assert.Equal(t, int32(1), f.calls("ds", "orders"))
	assert.Len(t, f.router.writers(), 2)
}

func TestTableRouterDynamicDataset(t *testing.T) {
	f := newRouterFixture(t, "$TAG[0]", "$TAG[1]", nil)
	require.NoError(t, f.router.init())
	tw, err := f.router.resolve("sales.orders")
	require.NoError(t, err)
	assert.Equal(t, "projects/p/datasets/sales/tables/orders", tw.tableRef)

	_, err = f.router.resolve("bad-dataset.orders")
	assert.Error(t, err, "dataset IDs must not contain dashes")
}

func TestTableRouterUnknownTable(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", nil)
	require.NoError(t, f.router.init())

	_, err := f.router.resolve("onlyonepart")
	assert.Error(t, err, "index out of range")

	_, err = f.router.resolve("app.has/slash")
	assert.Error(t, err, "invalid table id")
	assert.Equal(t, int32(0), f.calls("ds", "has/slash"), "invalid IDs never reach BigQuery")
}

func TestTableRouterCustomDelimiters(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[2]", func(c *outputConfig) { c.tagDelimiters = "_-" })
	require.NoError(t, f.router.init())
	tw, err := f.router.resolve("kube_var-orders_v1.x")
	require.NoError(t, err)
	assert.Equal(t, "projects/p/datasets/ds/tables/orders", tw.tableRef, "dots are not delimiters here and the 3rd part is selected")
}

func TestTableRouterLazyInitOnceUnderConcurrency(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", nil)
	require.NoError(t, f.router.init())

	const n = 32
	var wg sync.WaitGroup
	writers := make([]*tableWriter, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tw, err := f.router.resolve("app.orders")
			assert.NoError(t, err)
			writers[i] = tw
		}(i)
	}
	wg.Wait()
	assert.Equal(t, int32(1), f.calls("ds", "orders"), "GetWriteStream must be called once per table")
	for i := 1; i < n; i++ {
		assert.Same(t, writers[0], writers[i])
	}
}

func TestTableRouterNegativeCache(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", nil)
	require.NoError(t, f.router.init())
	f.failTable("ds", "typo", errors.New("rpc error: code = NotFound"))

	for i := 0; i < 5; i++ {
		_, err := f.router.resolve("app.typo")
		assert.Error(t, err)
	}
	assert.Equal(t, int32(1), f.calls("ds", "typo"), "failures are cached within the TTL")
	assert.Empty(t, f.router.writers())

	f.clock.advance(initFailureTTL)
	f.unfailTable("ds", "typo")
	tw, err := f.router.resolve("app.typo")
	require.NoError(t, err)
	assert.NotNil(t, tw)
	assert.Equal(t, int32(2), f.calls("ds", "typo"), "retried after the TTL expired")
}

func TestTableRouterPreloadTables(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", func(c *outputConfig) { c.preloadTables = []string{"orders", "users"} })
	require.NoError(t, f.router.init())
	assert.Equal(t, int32(1), f.calls("ds", "orders"))
	assert.Equal(t, int32(1), f.calls("ds", "users"))
	assert.Len(t, f.router.writers(), 2)

	tw, err := f.router.resolve("app.orders")
	require.NoError(t, err)
	assert.Equal(t, int32(1), f.calls("ds", "orders"), "preloaded table is reused")
	assert.NotNil(t, tw)
}

func TestTableRouterPreloadTablesFailures(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", func(c *outputConfig) { c.preloadTables = []string{"missing"} })
	f.failTable("ds", "missing", errors.New("rpc error: code = NotFound"))
	assert.Error(t, f.router.init(), "a missing preload table fails init like a static table does")

	f = newRouterFixture(t, "$TAG[0]", "$TAG[1]", func(c *outputConfig) { c.preloadTables = []string{"orders"} })
	assert.Error(t, f.router.init(), "Preload_Tables requires a static DatasetId")

	f = newRouterFixture(t, "ds", "$TAG[1]", func(c *outputConfig) { c.preloadTables = []string{"bad/id"} })
	assert.Error(t, f.router.init())
}

func TestTableRouterCloseAll(t *testing.T) {
	f := newRouterFixture(t, "ds", "$TAG[1]", nil)
	require.NoError(t, f.router.init())
	for _, tag := range []string{"app.a", "app.b", "app.c"} {
		_, err := f.router.resolve(tag)
		require.NoError(t, err)
	}
	assert.False(t, f.router.closeAll(context.Background()))
	assert.Equal(t, int32(3), f.stream.closes.Load())
}

// registerTestConfig puts cfg into configMap (as FLBPluginInit would) and returns a pointer
// that the fake bridge resolves to its id. The caller passes it as the ctx of FLBPluginFlushCtx.
func registerTestConfig(t *testing.T, cfg *outputConfig) unsafe.Pointer {
	t.Helper()
	bridge, ok := deps.flb.(*fakeBridge)
	require.True(t, ok, "installTestDeps must be called first")
	id := int(configID.Add(1) - 1)
	configMap[id] = cfg
	ptr := unsafe.Pointer(new(int))
	bridge.SetContext(ptr, id)
	t.Cleanup(func() { delete(configMap, id) })
	return ptr
}

func TestFLBPluginFlushCtxUnknownTableAction(t *testing.T) {
	tests := []struct {
		name   string
		action unknownTableAction
		want   int
	}{
		{name: "drop acknowledges the chunk", action: unknownTableDrop, want: output.FLB_OK},
		{name: "retry asks Fluent Bit to redeliver", action: unknownTableRetry, want: output.FLB_RETRY},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newRouterFixture(t, "ds", "$TAG[1]", func(c *outputConfig) { c.unknownTable = tt.action })
			require.NoError(t, f.router.init())
			f.failTable("ds", "typo", errors.New("rpc error: code = NotFound"))
			ptr := registerTestConfig(t, f.cfg)

			tag := []byte("app.typo\x00")
			got := FLBPluginFlushCtx(ptr, nil, 0, (*cChar)(unsafe.Pointer(&tag[0])))
			assert.Equal(t, tt.want, got)
			assert.Equal(t, int32(1), f.calls("ds", "typo"))
		})
	}
}
