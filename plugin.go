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
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/fluent/fluent-bit-go/output"
)

var (
	configMap = make(map[int]*outputConfig)
	configID  atomic.Int64
)

// cChar and cInt alias C types so that _test.go files (which cannot import "C") can call the
// exported entry points.
type (
	cChar = C.char
	cInt  = C.int
)

// flbBridge is the Fluent Bit C bridge used by the exported entry points. The real
// implementation forwards to fluent-bit-go's output package; tests supply a fake.
type flbBridge interface {
	Register(def unsafe.Pointer, name, desc string) int
	ConfigKey(plugin unsafe.Pointer, key string) string
	SetContext(plugin unsafe.Pointer, ctx interface{})
	GetContext(ctx unsafe.Pointer) interface{}
}

type outputBridge struct{}

func (outputBridge) Register(def unsafe.Pointer, name, desc string) int {
	return output.FLBPluginRegister(def, name, desc)
}

func (outputBridge) ConfigKey(plugin unsafe.Pointer, key string) string {
	return output.FLBPluginConfigKey(plugin, key)
}

func (outputBridge) SetContext(plugin unsafe.Pointer, ctx interface{}) {
	output.FLBPluginSetContext(plugin, ctx)
}

func (outputBridge) GetContext(ctx unsafe.Pointer) interface{} {
	return output.FLBPluginGetContext(ctx)
}

// pluginDeps groups every external dependency of the exported entry points. The exported
// functions have no receiver, so this single package-level value is the injection point;
// everything below it (outputConfig, tableRouter, tableWriter) receives its dependencies
// through constructors.
type pluginDeps struct {
	flb       flbBridge
	newClient newClientFunc
	clock     clock
}

var deps = pluginDeps{
	flb:       outputBridge{},
	newClient: newRealManagedWriterClient,
	clock:     realClock(),
}

// instanceID extracts the output instance id stored with SetContext.
func instanceID(ctx unsafe.Pointer) int {
	if ctx == nil {
		return 0
	}
	id, ok := deps.flb.GetContext(ctx).(int)
	if !ok {
		log.Printf("FLBPluginGetContext returned unexpected type for context pointer %p", ctx)
		return 0
	}
	return id
}

//export FLBPluginRegister
func FLBPluginRegister(def unsafe.Pointer) int {
	return deps.flb.Register(def, "writeapi", "Sends data to BigQuery through WriteAPI")
}

//export FLBPluginInit
func FLBPluginInit(plugin unsafe.Pointer) int {
	cfg, err := parseOutputConfig(plugin, deps.flb)
	if err != nil {
		log.Print(err)
		return output.FLB_ERROR
	}
	cfg.clock = deps.clock

	// Create an instance-scoped context that outlives individual flush calls. Using it for client
	// creation, stream creation (buildStream) and AppendRows ensures the managed writer's internal
	// retry/reconnect logic is not aborted when a per-flush context is canceled at flush end.
	cfg.instanceCtx, cfg.instanceCancel = context.WithCancel(context.Background())
	cfg.client, err = deps.newClient(cfg.instanceCtx, cfg.projectID, cfg.clientOptions()...)
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
		if closeErr := cfg.client.Close(); closeErr != nil {
			log.Printf("FLBPluginInit: closing client after failed initialization: %s", closeErr)
		}
		log.Printf("FLBPluginInit: %s", err)
		return output.FLB_ERROR
	}
	if cfg.router.dynamic() {
		log.Printf("Output instance with id: %d routes by tag (DatasetId=%q TableId=%q Tag_Delimiters=%q)", id, cfg.datasetTemplate.raw, cfg.tableTemplate.raw, cfg.tagDelimiters)
	}

	configMap[id] = cfg

	// Creating FLB context for each output, enables multiinstancing
	deps.flb.SetContext(plugin, id)

	return output.FLB_OK
}

//export FLBPluginFlush
func FLBPluginFlush(data unsafe.Pointer, length C.int, tag *C.char) int {
	log.Print("[multiinstance] Flush called for unknown instance")
	return output.FLB_OK
}

// flushOutcome accumulates the append failures of one FLBPluginFlushCtx call and maps
// them to the plugin return code.
type flushOutcome struct {
	transientErr error
	permanentErr error
}

func (o *flushOutcome) record(err error) {
	if isTransientError(err) {
		if o.transientErr == nil {
			o.transientErr = err
		}
	} else if o.permanentErr == nil {
		o.permanentErr = err
	}
}

// abort reports whether the rest of the chunk should be skipped: once a transient failure
// happened the whole chunk is going to be redelivered, so sending more sub-chunks would only
// add duplicates.
func (o *flushOutcome) abort() bool {
	return o.transientErr != nil
}

// code maps the outcome to FLB_OK / FLB_RETRY / FLB_ERROR. With retryOnFailure disabled
// failures are only logged and the chunk is acknowledged, as the plugin did historically.
func (o *flushOutcome) code(retryOnFailure bool) int {
	if !retryOnFailure {
		return output.FLB_OK
	}
	switch {
	case o.transientErr != nil:
		return output.FLB_RETRY
	case o.permanentErr != nil:
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

// flushChunk sends binaryData through the table writer, logs failures and records them in outcome.
func flushChunk(ctx context.Context, tw *tableWriter, id int, binaryData [][]byte, outcome *flushOutcome) {
	if err := tw.flush(ctx, binaryData); err != nil {
		log.Printf("Appending data for output instance with id: %d to table %s failed in FLBPluginFlushCtx: %s", id, tw.tableRef, err)
		outcome.record(err)
	}
}

// getRecord reads the next record from dec. It returns 0 and the record, -1 at the end of the
// data (or when the next object cannot be decoded) and another negative value for a decoded
// object that is not a [timestamp, map] record.
//
// output.GetRecord calls reflect.Value.Len before checking the kind of the decoded value, so
// a top-level nil, number or bool panics inside it. By then the object has been consumed, so
// the panic is turned into "malformed record" and the following objects can still be read.
func getRecord(dec *output.FLBDecoder) (ret int, record map[interface{}]interface{}) {
	defer func() {
		if r := recover(); r != nil {
			ret, record = -2, nil
		}
	}()
	ret, _, record = output.GetRecord(dec)
	return ret, record
}

// decodeAndSerializeRecords decodes Fluent Bit records from dec, serializes each
// to proto binary, and accumulates them in binaryData. When the accumulated size
// reaches maxChunkSize, it calls flushChunk and resets binaryData
// (preserving capacity for reuse). It returns the remaining (unsent) binaryData,
// or nil when a transient failure aborted the chunk.
//
// Records that cannot be used are skipped individually and never influence the return code:
// malformed records (not a [timestamp, map] pair), records that cannot be serialized against
// the table schema, and rows whose serialized size alone reaches appendRequestLimit (such a
// row cannot fit into any AppendRows request, so sending it would fail the whole chunk with
// INVALID_ARGUMENT). A row larger than maxChunkSize but below that limit is sent on its own.
func decodeAndSerializeRecords(ctx context.Context, tw *tableWriter, id int, maxChunkSize int, dec *output.FLBDecoder, binaryData [][]byte, outcome *flushOutcome) [][]byte {
	var currsize int
	var malformed int

	for {
		ret, record := getRecord(dec)
		if ret == -1 {
			// End of data, or a msgpack object the decoder could not decode (it cannot resync
			// past it, so the rest of the chunk is unreadable either way).
			break
		}
		if ret != 0 {
			// The object was decoded but is not a log record; the decoder has consumed it and
			// the next one can be read.
			malformed++
			continue
		}

		buf, err := tw.serializeRecord(record)
		if err != nil {
			log.Printf("Transforming row with value:%v from map to binary data for output instance with id: %d failed in FLBPluginFlushCtx: %s", record, id, err)
			continue
		}
		if len(buf) >= appendRequestLimit {
			log.Printf("Skipping row of %d bytes for output instance with id: %d in FLBPluginFlushCtx: a single AppendRows request must be less than %d bytes", len(buf), id, appendRequestLimit)
			continue
		}

		if (currsize + len(buf)) >= maxChunkSize {
			flushChunk(ctx, tw, id, binaryData, outcome)
			if outcome.abort() {
				return nil
			}
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

	if malformed > 0 {
		log.Printf("Skipped %d malformed record(s) for output instance with id: %d in FLBPluginFlushCtx: not a [timestamp, map] pair", malformed, id)
	}
	return binaryData
}

//export FLBPluginFlushCtx
func FLBPluginFlushCtx(ctx, data unsafe.Pointer, length C.int, tag *C.char) int {
	id := instanceID(ctx)
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
		switch {
		case errors.Is(err, errBadTag):
			// Decided by the tag alone; redelivering the chunk cannot change the outcome.
			return output.FLB_OK
		case config.unknownTable == unknownTableRetry:
			return output.FLB_RETRY
		case config.retryOnFailure && isTransientError(err):
			// A transient failure means the backend could not be reached, not that the table is
			// unknown, so it is handled like a transient append failure.
			return output.FLB_RETRY
		}
		return output.FLB_OK
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), config.flushTimeout)
	defer flushCancel()

	// Drain ready responses of earlier (already acknowledged) chunks and check whether a new
	// stream should be created (the stream itself is built in the background, see maybeScale).
	// Failures found here are logged by drainResponses but cannot change this chunk's return
	// code: the chunks they belong to were acknowledged already.
	tw.drainResponses(flushCtx, false)
	tw.maybeScale()

	// binaryData is a flush-local buffer. Each worker (workers > 1) gets its own
	// stack frame, so there is no shared state and no mutex needed here.
	binaryData := make([][]byte, 0, 64)
	var outcome flushOutcome

	// Decode and serialize all records; mid-size chunks are sent inside the helper.
	binaryData = decodeAndSerializeRecords(flushCtx, tw, id, config.maxChunkSize, output.NewDecoder(data, int(length)), binaryData, &outcome)

	// Send the final (possibly partial) chunk unless a transient failure already aborted the chunk.
	if !outcome.abort() {
		flushChunk(flushCtx, tw, id, binaryData, &outcome)
	}

	return outcome.code(config.retryOnFailure)
}

//export FLBPluginExit
func FLBPluginExit() int {
	log.Print("[multiinstance] Exit called for unknown instance")
	return output.FLB_OK
}

//export FLBPluginExitCtx
func FLBPluginExitCtx(ctx unsafe.Pointer) int {
	id := instanceID(ctx)

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
	errFlag := false

	// Draining honours exitCtx itself; the wrapper only guards against the library misbehaving.
	if err := awaitShutdownStep(exitCtx, config, id, "Draining pending responses", func() error {
		config.router.drainAll(exitCtx)
		return nil
	}); err != nil {
		log.Printf("Draining pending responses for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
		errFlag = true
	}

	// A blocked append is still running on the instance context: either AppendRows itself
	// (backend unreachable, or in-flight queue saturated) or, on a committed stream, the wait for
	// the response of an AppendRows that did return. When the backend is unreachable the managed
	// writer's close paths wait for a connection lock held by the blocked call, which only
	// cancelling the instance context releases. Acknowledged appends were drained above and the
	// blocked chunks were already reported as FLB_RETRY, so cancelling now loses nothing; the
	// rows of a pending committed-stream response were sent before the cancel and stay written.
	for _, tw := range writers {
		if tw.blockedAppends.Load() > 0 {
			log.Printf("Output instance with id: %d has blocked appends, cancelling them before closing", id)
			config.cancelInstance()
			break
		}
	}

	if err := awaitShutdownStep(exitCtx, config, id, "Closing managed streams", func() error {
		if config.router.closeAll(exitCtx) {
			return errors.New("one or more streams failed to close")
		}
		return nil
	}); err != nil {
		log.Printf("Closing managed streams for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
		errFlag = true
	}

	if config.client != nil {
		if err := awaitShutdownStep(exitCtx, config, id, "Closing managed writer client", config.client.Close); err != nil {
			log.Printf("Closing managed writer client for output instance with id: %d failed in FLBPluginExitCtx: %s", id, err)
			errFlag = true
		}
	}

	// Cancel the instance-scoped context only after pending responses are drained and the streams
	// and client are closed, so that in-flight AppendRows are not aborted prematurely (which would
	// cause data loss and terminalize the streams during shutdown).
	config.cancelInstance()

	// Release messagePool entries for every writer's descriptor tree so that
	// the descriptors and their cached *dynamicpb.Message objects can be GC'd.
	for _, tw := range writers {
		cleanupMsgPool(tw.messageDescriptor)
	}

	if errFlag {
		return output.FLB_ERROR
	}
	return output.FLB_OK
}

// awaitShutdownStep runs step and waits for it until exitCtx expires. The managed writer's
// close paths can wait forever on a connection whose reconnect loop is retrying an unreachable
// backend; only cancelling the instance context ends that loop. So on expiry the instance
// context is cancelled and the step is given the same time again to finish. A step that still
// does not finish is abandoned (the process is exiting anyway). Both cases are reported as an
// error. Once exitCtx has expired, later steps only get the bounded grace period.
func awaitShutdownStep(exitCtx context.Context, config *outputConfig, id int, what string, step func() error) error {
	done := make(chan error, 1)
	go func() { done <- step() }()
	timedOut := false
	if exitCtx.Err() == nil {
		select {
		case err := <-done:
			return err
		case <-exitCtx.Done():
		}
		log.Printf("%s for output instance with id: %d did not finish within %s, cancelling in-flight AppendRows", what, id, config.flushTimeout)
		timedOut = true
	}
	config.cancelInstance()
	timer := time.NewTimer(config.flushTimeout)
	defer timer.Stop()
	select {
	case err := <-done:
		if err == nil && timedOut {
			return fmt.Errorf("finished only after %s and cancelling in-flight AppendRows", config.flushTimeout)
		}
		return err
	case <-timer.C:
		return fmt.Errorf("still blocked %s after cancelling in-flight AppendRows, giving up", config.flushTimeout)
	}
}

//export FLBPluginUnregister
func FLBPluginUnregister(def unsafe.Pointer) {
	log.Print("[multiinstance] Unregister called")
	output.FLBPluginUnregister(def)
}

func main() {
}
