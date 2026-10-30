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
	"sync/atomic"
	"testing"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
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

// routerFixture builds an outputConfig + tableRouter backed by a mock client whose
// GetWriteStream calls are counted per table and can be failed selectively.
type routerFixture struct {
	cfg            *outputConfig
	router         *tableRouter
	getWriteStream map[string]*atomic.Int32 // table ref -> call count
	failTables     map[string]error         // table ref -> error returned by GetWriteStream
	closedStreams  atomic.Int32
	mu             sync.Mutex
	restore        func()
}

func newRouterFixture(t *testing.T, dataset, table string, mutate func(*outputConfig)) *routerFixture {
	t.Helper()
	f := &routerFixture{
		getWriteStream: map[string]*atomic.Int32{},
		failTables:     map[string]error{},
	}
	schema := &storagepb.TableSchema{Fields: []*storagepb.TableFieldSchema{
		{Name: "Text", Type: storagepb.TableFieldSchema_STRING, Mode: storagepb.TableFieldSchema_NULLABLE},
	}}
	client := &MockManagedWriterClient{
		GetWriteStreamFunc: func(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
			f.mu.Lock()
			defer f.mu.Unlock()
			c, ok := f.getWriteStream[req.GetName()]
			if !ok {
				c = &atomic.Int32{}
				f.getWriteStream[req.GetName()] = c
			}
			c.Add(1)
			if err, ok := f.failTables[req.GetName()]; ok {
				return nil, err
			}
			return &storagepb.WriteStream{Name: req.GetName(), TableSchema: schema}, nil
		},
		CloseFunc: func() error { return nil },
	}
	mockStream := &MockManagedStream{
		AppendRowsFunc: func(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error) {
			return nil, nil
		},
		CloseFunc:      func() error { f.closedStreams.Add(1); return nil },
		FinalizeFunc:   func(ctx context.Context, opts ...gax.CallOption) (int64, error) { return 0, nil },
		FlushRowsFunc:  func(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error) { return 0, nil },
		StreamNameFunc: func() string { return "" },
	}
	origGetWriter := getWriter
	getWriter = func(client ManagedWriterClient, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
		return mockStream, nil
	}
	f.restore = func() { getWriter = origGetWriter }
	t.Cleanup(f.restore)

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
		requestCountThreshold: setThreshold(queueRequestDefault),
		dateTimeStringType:    true,
		flushTimeout:          time.Second,
		client:                client,
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

func streamName(dataset, table string) string {
	return "projects/p/datasets/" + dataset + "/tables/" + table + "/streams/_default"
}

func (f *routerFixture) calls(dataset, table string) int32 {
	f.mu.Lock()
	defer f.mu.Unlock()
	if c, ok := f.getWriteStream[streamName(dataset, table)]; ok {
		return c.Load()
	}
	return 0
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
	f.failTables[streamName("ds", "missing")] = errors.New("rpc error: code = NotFound")
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
	now := time.Unix(1_700_000_000, 0)
	f.router.now = func() time.Time { return now }
	f.failTables[streamName("ds", "typo")] = errors.New("rpc error: code = NotFound")

	for i := 0; i < 5; i++ {
		_, err := f.router.resolve("app.typo")
		assert.Error(t, err)
	}
	assert.Equal(t, int32(1), f.calls("ds", "typo"), "failures are cached within the TTL")
	assert.Empty(t, f.router.writers())

	now = now.Add(initFailureTTL)
	delete(f.failTables, streamName("ds", "typo"))
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
	f.failTables[streamName("ds", "missing")] = errors.New("rpc error: code = NotFound")
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
	assert.Equal(t, int32(3), f.closedStreams.Load())
}

// registerTestConfig puts cfg into configMap and makes getFLBPluginContext resolve the
// returned pointer to its id. The caller passes the pointer as the ctx of FLBPluginFlushCtx.
func registerTestConfig(t *testing.T, cfg *outputConfig) unsafe.Pointer {
	t.Helper()
	id := int(configID.Add(1) - 1)
	configMap[id] = cfg
	idPtr := new(int)
	*idPtr = id
	orig := getFLBPluginContext
	getFLBPluginContext = func(ctx unsafe.Pointer) int {
		if ctx != nil {
			return *(*int)(ctx)
		}
		return 0
	}
	t.Cleanup(func() {
		getFLBPluginContext = orig
		delete(configMap, id)
	})
	return unsafe.Pointer(idPtr)
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
			f.failTables[streamName("ds", "typo")] = errors.New("rpc error: code = NotFound")
			ptr := registerTestConfig(t, f.cfg)

			tag := []byte("app.typo\x00")
			got := FLBPluginFlushCtx(ptr, nil, 0, (*cChar)(unsafe.Pointer(&tag[0])))
			assert.Equal(t, tt.want, got)
			assert.Equal(t, int32(1), f.calls("ds", "typo"))
		})
	}
}
