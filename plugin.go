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
	"C"
	"context"
	"log"
	"sync/atomic"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
	"github.com/googleapis/gax-go/v2"
)

var (
	configMap = make(map[int]*outputConfig)
	configID  atomic.Int64
)

// cChar aliases C.char so that _test.go files (which cannot import "C") can build a tag pointer.
type cChar = C.char

// ManagedWriterClient wraps *managedwriter.Client so that tests can inject a mock.
type ManagedWriterClient interface {
	NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error)
	GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	Close() error
	BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error)
	CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
}

type realManagedWriterClient struct {
	currClient *managedwriter.Client
}

func (r *realManagedWriterClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (*managedwriter.ManagedStream, error) {
	return r.currClient.NewManagedStream(ctx, opts...)
}

func (r *realManagedWriterClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.GetWriteStream(ctx, req, opts...)
}

func (r *realManagedWriterClient) Close() error {
	return r.currClient.Close()
}

func (r *realManagedWriterClient) BatchCommitWriteStreams(ctx context.Context, req *storagepb.BatchCommitWriteStreamsRequest, opts ...gax.CallOption) (*storagepb.BatchCommitWriteStreamsResponse, error) {
	return r.currClient.BatchCommitWriteStreams(ctx, req, opts...)
}

func (r *realManagedWriterClient) CreateWriteStream(ctx context.Context, req *storagepb.CreateWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.CreateWriteStream(ctx, req, opts...)
}

// getClient wraps managedwriter.NewClient; tests override it to return a mock ManagedWriterClient.
var getClient = func(ctx context.Context, projectID string) (ManagedWriterClient, error) {
	client, err := managedwriter.NewClient(ctx, projectID)
	if err != nil {
		return nil, err
	}
	return &realManagedWriterClient{currClient: client}, nil
}

// MWManagedStream is the subset of *managedwriter.ManagedStream used by the plugin.
type MWManagedStream interface {
	AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (*managedwriter.AppendResult, error)
	Close() error
	Finalize(ctx context.Context, opts ...gax.CallOption) (int64, error)
	FlushRows(ctx context.Context, offset int64, opts ...gax.CallOption) (int64, error)
	StreamName() string
}

// getWriter wraps ManagedWriterClient.NewManagedStream; tests override it to return a mock stream.
var getWriter = func(client ManagedWriterClient, ctx context.Context, projectID string, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
	return client.NewManagedStream(ctx, opts...)
}

// newDecoder is a wrapper around output.NewDecoder to allow test injection.
var newDecoder = func(data unsafe.Pointer, length int) *output.FLBDecoder {
	return output.NewDecoder(data, length)
}

// getFLBPluginContext wraps output.FLBPluginGetContext so that tests can override it.
var getFLBPluginContext = func(ctx unsafe.Pointer) int {
	if ctx != nil {
		id, ok := output.FLBPluginGetContext(ctx).(int)
		if !ok {
			log.Printf("FLBPluginGetContext returned unexpected type for context pointer %p", ctx)
			return 0
		}
		return id
	}
	return 0
}

// getInstanceCount is a test-only helper that returns the number of registered output instances.
func getInstanceCount() int {
	return len(configMap)
}

// getOffset is a test-only helper that returns the offset counter of the first stream of the
// instance's static table writer.
func getOffset(id int) int64 {
	config, ok := configMap[id]
	if !ok || config == nil || config.router == nil || config.router.static == nil {
		return 0
	}
	return config.router.static.offset()
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return output.FLBPluginRegister(def, "writeapi", "Sends data to BigQuery through WriteAPI")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	cfg, err := parseOutputConfig(plugin)
	if err != nil {
		log.Print(err)
		return output.FLB_ERROR
	}

	// Create an instance-scoped context that outlives individual flush calls. Using it for client
	// creation, stream creation (buildStream) and AppendRows ensures the managed writer's internal
	// retry/reconnect logic is not aborted when a per-flush context is canceled at flush end.
	cfg.instanceCtx, cfg.instanceCancel = context.WithCancel(context.Background())
	cfg.client, err = getClient(cfg.instanceCtx, cfg.projectID)
	if err != nil {
		cfg.instanceCancel()
		log.Printf("Creating a new managed BigQuery Storage write client scoped to: %s failed in FLBPluginInit: %s", cfg.projectID, err)
		return output.FLB_ERROR
	}

	id := int(configID.Add(1) - 1)

	// A static destination is initialized here (schema fetch + first stream), so a missing
	// table still fails plugin start-up. A tag-dependent destination only initializes
	// Preload_Tables; other tables are initialized on first flush.
	cfg.router = newTableRouter(cfg, id)
	if err := cfg.router.init(); err != nil {
		cfg.instanceCancel()
		log.Printf("FLBPluginInit: %s", err)
		return output.FLB_ERROR
	}
	if cfg.router.dynamic() {
		log.Printf("Output instance with id: %d routes by tag (DatasetId=%q TableId=%q Tag_Delimiters=%q)", id, cfg.datasetTemplate.raw, cfg.tableTemplate.raw, cfg.tagDelimiters)
	}

	configMap[id] = cfg

	// Creating FLB context for each output, enables multiinstancing
	output.FLBPluginSetContext(plugin, id)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Print("[multiinstance] Flush called for unknown instance")
	return output.FLB_OK
}

// flushChunk sends binaryData through the table writer and logs failures.
func flushChunk(ctx context.Context, tw *tableWriter, id int, binaryData [][]byte) {
	if err := tw.flush(ctx, binaryData); err != nil {
		log.Printf("Appending data for output instance with id: %d to table %s failed in FLBPluginFlushCtx: %s", id, tw.tableRef, err)
	}
}

// decodeAndSerializeRecords decodes Fluent Bit records from dec, serializes each
// to proto binary, and accumulates them in binaryData. When the accumulated size
// reaches maxChunkSize, it calls flushChunk and resets binaryData
// (preserving capacity for reuse). It returns the remaining (unsent) binaryData.
func decodeAndSerializeRecords(ctx context.Context, tw *tableWriter, id int, maxChunkSize int, dec *output.FLBDecoder, binaryData [][]byte) [][]byte {
	var currsize int

	for {
		ret, _, record := output.GetRecord(dec)
		if ret != 0 {
			break
		}

		buf, err := tw.serializeRecord(record)
		if err != nil {
			log.Printf("Transforming row with value:%v from map to binary data for output instance with id: %d failed in FLBPluginFlushCtx: %s", record, id, err)
			continue
		}

		if (currsize + len(buf)) >= maxChunkSize {
			flushChunk(ctx, tw, id, binaryData)
			// Nil out sent elements so GC can reclaim them, then reset slice.
			for i := range binaryData {
				binaryData[i] = nil
			}
			binaryData = binaryData[:0]
			currsize = 0
		}
		binaryData = append(binaryData, buf)
		// Include the protobuf overhead in the size estimate.
		currsize += (len(buf) + 2)
	}

	return binaryData
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := getFLBPluginContext(ctx)
	config, ok := configMap[id]
	if !ok {
		log.Printf("Finding configuration for output instance with id: %d failed in FLBPluginFlushCtx", id)
		return output.FLB_ERROR
	}

	// Fluent Bit delivers exactly one tag per chunk, so the whole chunk goes to one table.
	tagStr := C.GoString(tag)
	tw, err := config.router.resolve(tagStr)
	if err != nil {
		log.Printf("Resolving destination table for output instance with id: %d and tag %q failed in FLBPluginFlushCtx: %s", id, tagStr, err)
		if config.unknownTable == unknownTableRetry {
			return output.FLB_RETRY
		}
		return output.FLB_OK
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), config.flushTimeout)
	defer flushCancel()

	// Drain ready responses and check whether a new stream should be created.
	tw.drainResponses(flushCtx, false)
	tw.maybeScale()

	// binaryData is a flush-local buffer. Each worker (workers > 1) gets its own
	// stack frame, so there is no shared state and no mutex needed here.
	binaryData := make([][]byte, 0, 64)

	// Decode and serialize all records; mid-size chunks are sent inside the helper.
	binaryData = decodeAndSerializeRecords(flushCtx, tw, id, config.maxChunkSize, newDecoder(data, int(length)), binaryData)

	// Send the final (possibly partial) chunk.
	flushChunk(flushCtx, tw, id, binaryData)

	return output.FLB_OK
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[multiinstance] Exit called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	id := getFLBPluginContext(ctx)

	config, ok := configMap[id]
	if !ok {
		log.Printf("Finding configuration for output instance with id: %d failed in FLBPluginExitCtx", id)
		return output.FLB_ERROR
	}

	// Drain all pending responses before closing streams to avoid data loss.
	// waitForResponse=true blocks until every in-flight AppendRows result is received.
	// Use a timeout context (same duration as flush) so exit cannot hang indefinitely.
	exitCtx, exitCancel := context.WithTimeout(context.Background(), config.flushTimeout)
	defer exitCancel()
	writers := config.router.writers()
	errFlag := config.router.closeAll(exitCtx)

	if config.client != nil {
		if err := config.client.Close(); err != nil {
			log.Printf("Closing managed writer client for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
			errFlag = true
		}
	}

	// Cancel the instance-scoped context only after pending responses are drained and the streams
	// and client are closed, so that in-flight AppendRows are not aborted prematurely (which would
	// cause data loss and terminalize the streams during shutdown).
	if config.instanceCancel != nil {
		config.instanceCancel()
	}

	if errFlag {
		return output.FLB_ERROR
	}

	// Release messagePool entries for every writer's descriptor tree so that
	// the descriptors and their cached *dynamicpb.Message objects can be GC'd.
	for _, tw := range writers {
		cleanupMsgPool(tw.messageDescriptor)
	}

	return output.FLB_OK
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[multiinstance] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}
