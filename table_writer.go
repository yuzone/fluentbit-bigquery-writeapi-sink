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
	"fmt"
	"io"
	"log"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"cloud.google.com/go/bigquery/storage/managedwriter/adapt"
	"github.com/googleapis/gax-go/v2/apierror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
)

// streamConfig holds the state of one managed stream. A tableWriter owns one or more.
type streamConfig struct {
	// managedstream is nil while a committed stream is being rebuilt, or after a rebuild
	// failed (see rebuildStream); the next flush then rebuilds it before appending.
	managedstream MWManagedStream
	appendResults []appendResult
	offsetCounter int64
	// rebuilding is set while rebuildStream is replacing managedstream outside streamMu, so
	// that concurrent flushes fail fast instead of starting a second rebuild.
	rebuilding bool
}

// tableWriter owns every piece of per-table state: the destination table, its
// schema descriptors, and the managed streams (with dynamic scaling) that write to it.
// Instance-wide settings are read from cfg; the client and ctx are shared with the
// owning output instance.
type tableWriter struct {
	id       int // output instance id, used for log messages only
	ctx      context.Context
	client   ManagedWriterClient
	cfg      *outputConfig
	tableRef string

	messageDescriptor protoreflect.MessageDescriptor
	schemaDesc        *descriptorpb.DescriptorProto
	timestampFields   []string
	fieldCache        fieldLookupCache
	// maxRowBytes is the largest serialized row that still fits into an AppendRows request of
	// its own, see rowFits.
	maxRowBytes int

	streamMu sync.Mutex // guards streams and its elements (streamConfig), scaling and closed
	streams  []*streamConfig
	// scaling is set while maybeScale is creating an additional stream in the background,
	// so that concurrent flushes do not start a second one.
	scaling bool
	// closed is set by close. A stream whose creation completes afterwards is closed
	// instead of being added, so that it does not leak.
	closed bool

	// blockedAppends counts AppendRows calls that did not return within the flush timeout and
	// are still running in the background (see appendRows). While it is non-zero every flush
	// for this table fails fast with errAppendBlocked instead of starting another append.
	blockedAppends atomic.Int32
}

// errAppendBlocked is returned (wrapped) when AppendRows does not return within the flush
// timeout, and for every flush of that table until the blocked call completes. It is treated
// as transient so that Fluent Bit keeps the chunk and retries.
var errAppendBlocked = errors.New("AppendRows is blocked, the backend is probably unreachable")

// errStreamRebuilding is returned (wrapped) by the exactly-once path while the committed
// stream has no usable managed stream: a rebuild is in progress in another flush, or a
// previous rebuild failed and this flush could not complete a new one. It is treated as
// transient so that Fluent Bit keeps the chunk and retries.
var errStreamRebuilding = errors.New("the committed stream is being rebuilt")

// newTableWriter fetches the table schema and opens the first managed stream.
// ctx must be the instance-scoped context: it is retained for AppendRows and stream
// (re)creation so that the managed writer's internal retry/reconnect is not tied to a
// per-flush context. Both steps are unary RPCs whose library defaults retry UNAVAILABLE for
// 10 minutes, so each is bounded by cfg.initTimeout to keep plugin start-up (and the first
// flush of a tag-routed table) from hanging while the backend is unreachable.
func newTableWriter(ctx context.Context, client ManagedWriterClient, project, dataset, table string, cfg *outputConfig, id int) (*tableWriter, error) {
	tableRef := fmt.Sprintf("projects/%s/datasets/%s/tables/%s", project, dataset, table)

	schemaCtx, cancel := context.WithTimeout(ctx, cfg.initTimeout)
	defer cancel()
	md, descriptor, timestampFields, err := getDescriptors(schemaCtx, client, project, dataset, table, cfg.dateTimeStringType)
	if err != nil {
		return nil, fmt.Errorf("Getting message descriptor and descriptor proto for table: %s failed: %w", tableRef, err)
	}
	log.Printf("Detected TIMESTAMP fields from schema: %v", timestampFields)

	tw := &tableWriter{
		id:                id,
		ctx:               ctx,
		client:            client,
		cfg:               cfg,
		tableRef:          tableRef,
		messageDescriptor: md,
		schemaDesc:        descriptor,
		timestampFields:   timestampFields,
		fieldCache:        buildFieldLookupCache(md),
		streams:           []*streamConfig{{}},
	}
	tw.maxRowBytes = appendRequestLimit - appendRequestOverhead(tableRef, descriptor)

	if err := tw.buildStream(ctx, 0); err != nil {
		return nil, fmt.Errorf("Creating a new managed stream with destination table: %s failed: %w", tableRef, err)
	}
	return tw, nil
}

// This function mangles the top-level and complex (struct) BigQuery schema to convert NUMERIC, BIGNUMERIC, DATETIME, TIME, and JSON fields to STRING.
func mangleInputSchema(input *storagepb.TableSchema, dataTimeString bool) *storagepb.TableSchema {
	if input == nil {
		return nil
	}
	// Create a clone of the table schema
	newMsg := proto.Clone(input).(*storagepb.TableSchema)
	newMsg.Fields = make([]*storagepb.TableFieldSchema, len(input.GetFields()))
	for k, f := range input.GetFields() {
		// Create a clone of the field
		newF := proto.Clone(f).(*storagepb.TableFieldSchema)
		switch newF.GetType() {
		// Overwrite the field to be string
		case storagepb.TableFieldSchema_NUMERIC,
			storagepb.TableFieldSchema_BIGNUMERIC,
			storagepb.TableFieldSchema_TIME,
			storagepb.TableFieldSchema_JSON:
			newF.Type = storagepb.TableFieldSchema_STRING

		}
		// If datatimestring is true, then set the field type to string
		if newF.GetType() == storagepb.TableFieldSchema_DATETIME && dataTimeString {
			newF.Type = storagepb.TableFieldSchema_STRING
		}
		// If the field is a struct type it will have a non-zero number of fields
		if len(newF.GetFields()) > 0 {
			// Call mangeInputSchema on the fields in the struct
			newF.Fields = mangleInputSchema(&storagepb.TableSchema{Fields: newF.Fields}, dataTimeString).Fields
		}
		newMsg.Fields[k] = newF
	}
	return newMsg
}

// This function extracts TIMESTAMP field names from the BigQuery table schema
func extractTimestampFields(schema *storagepb.TableSchema, prefix string) []string {
	var fields []string
	if schema == nil {
		return fields
	}
	for _, f := range schema.GetFields() {
		fieldName := f.GetName()
		if prefix != "" {
			fieldName = prefix + "." + fieldName
		}
		if f.GetType() == storagepb.TableFieldSchema_TIMESTAMP {
			fields = append(fields, fieldName)
		}
		// Recursively extract from nested RECORD fields
		if len(f.GetFields()) > 0 {
			nestedFields := extractTimestampFields(&storagepb.TableSchema{Fields: f.GetFields()}, fieldName)
			fields = append(fields, nestedFields...)
		}
	}
	return fields
}

// This function handles getting data on the schema of the table data is being written to.
// The getDescriptors function returns the message descriptor (which describes the schema of the corresponding table) as well as a descriptor proto
func getDescriptors(curr_ctx context.Context, mw_client ManagedWriterClient, project string, dataset string, table string, dataTimeString bool) (protoreflect.MessageDescriptor, *descriptorpb.DescriptorProto, []string, error) {
	// Create streamID specific to the project, dataset, and table
	curr_stream := fmt.Sprintf("projects/%s/datasets/%s/tables/%s/streams/_default", project, dataset, table)

	// Create the getwritestreamrequest to have View_FULL so that the schema can be obtained
	req := storagepb.GetWriteStreamRequest{
		Name: curr_stream,
		View: storagepb.WriteStreamView_FULL,
	}

	// Call getwritestream to get data on the table
	table_data, err := mw_client.GetWriteStream(curr_ctx, &req)
	if err != nil {
		return nil, nil, nil, err
	}
	// Get the schema from table data
	init_table_schema := table_data.GetTableSchema()

	// Extract TIMESTAMP field names before mangling the schema
	timestampFields := extractTimestampFields(init_table_schema, "")

	table_schema := mangleInputSchema(init_table_schema, dataTimeString)
	// Storage schema -> proto descriptor
	descriptor, err := adapt.StorageSchemaToProto2Descriptor(table_schema, "root")
	if err != nil {
		return nil, nil, nil, err
	}
	// Proto descriptor -> message descriptor
	messageDescriptor, ok := descriptor.(protoreflect.MessageDescriptor)
	if !ok {
		return nil, nil, nil, errors.New("Message descriptor could not be created from table's proto descriptor")
	}

	// Message descriptor -> descriptor proto
	dp, err := adapt.NormalizeDescriptor(messageDescriptor)
	if err != nil {
		return nil, nil, nil, err
	}

	return messageDescriptor, dp, timestampFields, nil
}

// convertTimestampFieldsRaw converts timestamp fields in a raw Fluent Bit record
// (map[interface{}]interface{}) from seconds to microseconds for BigQuery Storage Write API.
// BigQuery TIMESTAMP type expects microseconds since Unix epoch.
// Field paths may be dotted (e.g. "outer.inner") for nested RECORD fields.
func convertTimestampFieldsRaw(data map[interface{}]interface{}, timestampFields []string) {
	for _, field := range timestampFields {
		if idx := strings.IndexByte(field, '.'); idx >= 0 {
			// Nested path: navigate into the sub-map and recurse with the remainder.
			parent, remainder := field[:idx], field[idx+1:]
			if sub, ok := data[parent]; ok {
				if subMap, ok := sub.(map[interface{}]interface{}); ok {
					convertTimestampFieldsRaw(subMap, []string{remainder})
				}
			}
		} else {
			// In Go, map[interface{}]interface{} lookup with a string key works
			// because interface comparison uses underlying type+value equality.
			if val, ok := data[field]; ok {
				convertTimestampValueRaw(data, field, val)
			}
		}
	}
}

// convertTimestampValueRaw handles the value conversion for map[interface{}]interface{}
func convertTimestampValueRaw(data map[interface{}]interface{}, field string, val interface{}) {
	switch v := val.(type) {
	case int64:
		if v > 0 && v < maxUnixSeconds {
			data[field] = v * 1000000
		}
	case int:
		if v > 0 && v < maxUnixSeconds {
			data[field] = int64(v) * 1000000
		}
	case uint64:
		if v > 0 && v < maxUnixSeconds {
			data[field] = int64(v) * 1000000
		}
	case float64:
		if v > 0 && v < maxUnixSeconds {
			data[field] = int64(v * 1000000)
		}
	case string:
		if v != "" {
			if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
				if floatVal > 0 && floatVal < maxUnixSeconds {
					data[field] = int64(floatVal * 1000000)
				} else {
					data[field] = int64(floatVal)
				}
			}
		}
	case []byte:
		// Handle msgpack string values that arrive as []byte
		s := string(v)
		if s != "" {
			if floatVal, err := strconv.ParseFloat(s, 64); err == nil {
				if floatVal > 0 && floatVal < maxUnixSeconds {
					data[field] = int64(floatVal * 1000000)
				} else {
					data[field] = int64(floatVal)
				}
			}
		}
	}
}

// serializeRecord converts one decoded Fluent Bit record to the table's proto binary
// form, converting TIMESTAMP fields on the way.
func (tw *tableWriter) serializeRecord(record map[interface{}]interface{}) ([]byte, error) {
	convertTimestampFieldsRaw(record, tw.timestampFields)
	return rawMapToBinary(tw.messageDescriptor, record, tw.fieldCache)
}

// appendRequestFixedOverhead bounds the parts of an AppendRows request that do not scale
// with the table: the stream name suffix ("/streams/" plus a stream ID), the trace ID, the
// offset, and the protobuf tags and length prefixes of the nested ProtoData / ProtoRows /
// serialized_rows wrappers. The real values are well below 1 KiB.
const appendRequestFixedOverhead = 1024

// appendRequestOverhead estimates the bytes an AppendRows request carrying a single row adds
// on top of the serialized row: the writer schema (sent by the managed writer on the first
// request of every connection, so it has to fit together with any row), the stream name and
// the fixed wrappers. The estimate errs on the large side.
func appendRequestOverhead(tableRef string, schema *descriptorpb.DescriptorProto) int {
	return proto.Size(schema) + len(tableRef) + appendRequestFixedOverhead
}

// rowFits reports whether a serialized row can be sent in an AppendRows request of its own
// without exceeding appendRequestLimit. A row that does not fit can never be delivered.
func (tw *tableWriter) rowFits(buf []byte) bool {
	return len(buf) < tw.maxRowBytes
}

func isReady(result appendResult) bool {
	select {
	case <-result.Ready():
		return true
	default:
		return false
	}
}

// getResult waits for the server response of one append and logs any per-row errors.
func getResult(result appendResult, ctx context.Context) (int64, error) {
	offset, err := result.GetResult(ctx)
	if err != nil {
		// Try to get detailed row errors from FullResponse
		if fullResp, respErr := result.FullResponse(ctx); respErr == nil && fullResp != nil {
			if rowErrors := fullResp.GetRowErrors(); len(rowErrors) > 0 {
				for _, rowErr := range rowErrors {
					log.Printf("Row error at index %d: code=%v, message=%s",
						rowErr.GetIndex(), rowErr.GetCode(), rowErr.GetMessage())
				}
			}
		}
	}
	return offset, err
}

// checkResponses drains the asynchronous WriteAPI responses queued on one stream.
// With waitForResponse it blocks on every queued result; otherwise only ready results are consumed.
// It returns the queue length after draining. Caller must hold tw.streamMu.
func (tw *tableWriter) checkResponses(ctx context.Context, stream *streamConfig, waitForResponse bool) int {
	currQueue := stream.appendResults
	if tw.cfg.exactlyOnce && len(currQueue) > 0 {
		log.Printf("Asynchronous response queue has non-zero size when exactly-once is configured")
		return len(currQueue)
	}
	writeIdx := 0
	for readIdx := 0; readIdx < len(currQueue); readIdx++ {
		result := currQueue[readIdx]
		if waitForResponse || isReady(result) {
			_, err := getResult(result, ctx)
			if err != nil {
				log.Printf("Encountered error:%s while verifying the server response to a data append for output instance with id: %d", err, tw.id)
			}
		} else {
			currQueue[writeIdx] = result
			writeIdx++
		}
	}
	// nil out drained slots to allow GC to reclaim AppendResult pointers
	for i := writeIdx; i < len(currQueue); i++ {
		currQueue[i] = nil
	}
	stream.appendResults = currQueue[:writeIdx]
	return writeIdx
}

// drainResponses checks the queued responses of every stream of this table.
func (tw *tableWriter) drainResponses(ctx context.Context, waitForResponse bool) {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	for _, stream := range tw.streams {
		tw.checkResponses(ctx, stream, waitForResponse)
	}
}

// buildStream creates the managed stream at streamIndex. Caller must hold tw.streamMu unless
// no other goroutine can reach tw.streams yet (newTableWriter).
func (tw *tableWriter) buildStream(ctx context.Context, streamIndex int) error {
	ms, err := tw.newManagedStream(ctx)
	if err != nil {
		return err
	}
	tw.streams[streamIndex].managedstream = ms
	return nil
}

// newManagedStream creates a managed stream for this table. NewManagedStream performs unary
// RPCs (GetWriteStream, plus CreateWriteStream for committed streams) that retry for minutes
// by default. The call is bounded by cfg.initTimeout; ctx itself cannot carry the deadline
// because the stream retains it for its whole lifetime. A stream that is created after the
// deadline is closed so that it does not leak. It does not touch tw.streams and may be called
// without holding streamMu.
func (tw *tableWriter) newManagedStream(ctx context.Context) (MWManagedStream, error) {
	type created struct {
		ms  MWManagedStream
		err error
	}
	done := make(chan created, 1)
	go func() {
		ms, err := tw.client.NewManagedStream(ctx,
			managedwriter.WithType(tw.cfg.streamType),
			managedwriter.WithDestinationTable(tw.tableRef),
			// Use the descriptor proto when creating the new managed stream
			managedwriter.WithSchemaDescriptor(tw.schemaDesc),
			managedwriter.EnableWriteRetries(tw.cfg.enableRetry),
			managedwriter.WithMaxInflightBytes(tw.cfg.maxQueueBytes),
			managedwriter.WithMaxInflightRequests(tw.cfg.maxQueueRequests),
			managedwriter.WithDefaultMissingValueInterpretation(storagepb.AppendRowsRequest_DEFAULT_VALUE),
			managedwriter.WithTraceID("FluentBit"),
		)
		done <- created{ms, err}
	}()

	timer := time.NewTimer(tw.cfg.initTimeout)
	defer timer.Stop()
	select {
	case c := <-done:
		return c.ms, c.err
	case <-timer.C:
		go func() {
			if c := <-done; c.err == nil {
				c.ms.Close()
			}
		}()
		return nil, fmt.Errorf("stream creation did not complete within %s: %w", tw.cfg.initTimeout, context.DeadlineExceeded)
	}
}

// rebuildPredicate reports whether an append was rejected because the committed stream can no
// longer be used as is, in which case it must be replaced: the stream itself is invalid
// (StorageError INVALID_STREAM_TYPE), or the offset counter has diverged from the offset the
// backend expects (OFFSET_ALREADY_EXISTS, OFFSET_OUT_OF_RANGE). The latter should not happen
// since late appends advance the counter, but if it does a new stream (offset 0) costs at most
// one duplicated chunk, whereas keeping the stale counter would drop every later chunk.
// ExtractProtoMessage returns nil when the detail was found.
func rebuildPredicate(err error) bool {
	if apiErr, ok := apierror.FromError(err); ok {
		storageErr := &storagepb.StorageError{}
		if e := apiErr.Details().ExtractProtoMessage(storageErr); e == nil {
			switch storageErr.GetCode() {
			case storagepb.StorageError_INVALID_STREAM_TYPE,
				storagepb.StorageError_OFFSET_ALREADY_EXISTS,
				storagepb.StorageError_OFFSET_OUT_OF_RANGE:
				return true
			}
		}
	}
	return false
}

// rebuildStream replaces the committed stream at streamIndex after the backend rejected it
// with INVALID_STREAM_TYPE. The old stream is detached under streamMu and finalized, closed
// and replaced outside it, so that concurrent flushes of this table fail fast with
// errStreamRebuilding instead of waiting up to cfg.initTimeout for the lock. A new committed
// stream starts at offset 0, so the offset counter is reset with it.
//
// On failure managedstream stays nil: the closed stream is never used again (its AppendRows
// would report io.EOF forever) and the next flush retries the rebuild. A stream that is
// created after close ran is closed instead of being installed.
func (tw *tableWriter) rebuildStream(ctx context.Context, streamIndex int) error {
	tw.streamMu.Lock()
	stream := tw.streams[streamIndex]
	if stream.rebuilding {
		tw.streamMu.Unlock()
		return fmt.Errorf("%w: another flush is rebuilding it", errStreamRebuilding)
	}
	stream.rebuilding = true
	old := stream.managedstream
	stream.managedstream = nil
	tw.streamMu.Unlock()

	if old != nil {
		log.Printf("Rebuilding the committed stream for table %s for output instance with id: %d", tw.tableRef, tw.id)
		if _, err := old.Finalize(ctx); err != nil {
			log.Printf("Finalizing the invalid committed stream for table %s for output instance with id: %d failed: %s", tw.tableRef, tw.id, err)
		}
		if err := old.Close(); err != nil && !errors.Is(err, io.EOF) {
			log.Printf("Closing the invalid committed stream for table %s for output instance with id: %d failed: %s", tw.tableRef, tw.id, err)
		}
	}
	ms, err := tw.newManagedStream(tw.ctx)

	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	stream.rebuilding = false
	if err != nil {
		return fmt.Errorf("rebuilding the committed stream for table %s failed: %w", tw.tableRef, err)
	}
	if tw.closed {
		ms.Close()
		return fmt.Errorf("%w: table writer was closed while rebuilding the committed stream", context.Canceled)
	}
	stream.managedstream = ms
	stream.offsetCounter = 0
	return nil
}

// sendRequestExactlyOnce sends and checks the response for data through a committed stream.
// On success the stream's offsetCounter is incremented by len(data) within the same streamMu
// critical section as the append, preventing TOCTOU races when workers > 1.
//
// The response wait is bounded by the per-flush ctx like the AppendRows call itself. The rows
// of an append whose response has not arrived by then are usually committed at the offset
// that was used, so the flush must not simply fail: the counter would stay behind and every
// later append, starting with the redelivered chunk, would be rejected with
// OFFSET_ALREADY_EXISTS. Instead the append is treated as blocked (errAppendBlocked, see
// appendRows) and late advances the counter once the response arrives.
func (tw *tableWriter) sendRequestExactlyOnce(ctx context.Context, data [][]byte, streamIndex int) error {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()

	currStream := tw.streams[streamIndex]
	ms := currStream.managedstream
	if ms == nil {
		return fmt.Errorf("%w: no usable stream", errStreamRebuilding)
	}

	// If the append completes after the flush timed out, the rows have been written at this
	// offset; advance the counter so the chunk redelivered by Fluent Bit lands at the next one.
	// The counter belongs to the stream the rows were appended to: once that stream has been
	// rebuilt the new one starts at offset 0 and must not be advanced.
	late := func(result appendResult, err error) {
		if err != nil {
			return
		}
		if _, err := getResult(result, tw.ctx); err != nil {
			log.Printf("Blocked exactly-once append to table %s for output instance with id: %d was rejected: %s", tw.tableRef, tw.id, err)
			return
		}
		tw.streamMu.Lock()
		defer tw.streamMu.Unlock()
		if currStream.managedstream == ms {
			currStream.offsetCounter += int64(len(data))
		}
	}
	started := time.Now()
	result, err := tw.appendRows(ctx, ms, data, late, managedwriter.WithOffset(currStream.offsetCounter))
	if err != nil {
		return err
	}
	// Synchronously check the response immediately after appending data with exactly once semantics
	select {
	case <-result.Ready():
	case <-ctx.Done():
		// Both may be ready at once; only give up when the response is really missing.
		if !isReady(result) {
			return tw.blockAppend(started, len(data), "the response of AppendRows", func() appendOutcome {
				<-result.Ready()
				return appendOutcome{result: result}
			}, late)
		}
	}
	// The response is in, so GetResult returns at once; tw.ctx keeps an expired flush ctx from
	// turning a committed append into a failure that leaves the counter behind.
	if _, err = getResult(result, tw.ctx); err != nil {
		return err
	}
	// Increment the offset inside the same lock so no other worker can read a stale offset value.
	currStream.offsetCounter += int64(len(data))
	return nil
}

// sendRequestRetries enables synchronous retries and rebuilding a valid stream based on the server response.
// Transient failures (isTransientError, the same classification that decides FLB_RETRY vs
// FLB_ERROR) are retried up to cfg.numRetries times with a short backoff; permanent ones are
// returned at once.
// An append rejected with a StorageError recognised by rebuildPredicate (INVALID_STREAM_TYPE,
// OFFSET_ALREADY_EXISTS, OFFSET_OUT_OF_RANGE), or made while the stream has no usable managed
// stream, triggers rebuildStream and is retried once on the new stream without counting as an
// attempt. At most one rebuild is performed per flush so that a backend that keeps rejecting
// the new stream cannot keep this flush looping: the second rejection is returned as is, and
// being INVALID_ARGUMENT it is a permanent failure (the chunk is dropped with FLB_ERROR, or
// only logged when Retry_On_Failure is off). The next flush rebuilds the stream again.
func (tw *tableWriter) sendRequestRetries(ctx context.Context, data [][]byte, streamIndex int) error {
	retryer := newStatelessRetryer(tw.cfg.numRetries)
	attempt := 0
	rebuilt := false
	for {
		err := tw.sendRequestExactlyOnce(ctx, data, streamIndex)
		if err == nil {
			break
		}
		// Unsuccessful data append
		if errors.Is(err, errAppendBlocked) {
			// Retrying within this flush would only block again or fail fast; hand the chunk
			// back to Fluent Bit.
			return err
		}
		if errors.Is(err, errStreamRebuilding) || rebuildPredicate(err) {
			if rebuilt {
				return err
			}
			if buildErr := tw.rebuildStream(ctx, streamIndex); buildErr != nil {
				return buildErr
			}
			rebuilt = true
			// Retry sending data without incrementing number of attempts or waiting between attempts
		} else {
			backoffPeriod, shouldRetry := retryer.Retry(err, attempt)
			if !shouldRetry {
				return err
			}
			// A done context is classified as transient but cannot be recovered by retrying
			// within this flush: another attempt would fail at once after a pointless pause.
			if ctx.Err() != nil || tw.ctx.Err() != nil {
				return err
			}
			// Retry sending data after incrementing attempt count and waiting for designated amount of time
			attempt++
			tw.cfg.clock.sleep(backoffPeriod)
		}

	}
	return nil
}

// sendRequestDefault sends data through a default stream and queues the response to be checked
// asynchronously (at-least-once). The append uses the instance-scoped context (tw.ctx), not the
// per-flush ctx, so the managed writer's internal retry/reconnect for this asynchronously-acknowledged
// append is not aborted when the flush context is canceled at flush end. The per-flush ctx only
// bounds how long this flush waits for AppendRows to return (see appendRows): on a saturated
// inflight queue (Max_Queue_Requests / Max_Queue_Bytes) or an unreachable backend the flush is
// reported as transient instead of blocking the output worker.
func (tw *tableWriter) sendRequestDefault(ctx context.Context, data [][]byte, streamIndex int) error {
	// Hold streamMu only for the slice access; AppendRows is goroutine-safe inside
	// the managed writer and must not be called under the lock to avoid blocking all
	// concurrent flushes when the inflight queue (Max_Queue_Requests / Max_Queue_Bytes)
	// is saturated.
	currStream := tw.getStream(streamIndex)

	// An append that completes after the flush timed out still needs its response checked.
	late := func(result appendResult, err error) {
		if err == nil {
			tw.queueResult(currStream, result)
		}
	}
	result, err := tw.appendRows(ctx, currStream.managedstream, data, late)
	if err != nil {
		return err
	}

	tw.queueResult(currStream, result)
	return nil
}

type appendOutcome struct {
	result appendResult
	err    error
}

// appendRows calls AppendRows on stream with the instance context and waits for it to return or
// for ctx (the per-flush deadline) to expire.
//
// While the backend is unreachable the managed writer re-opens its gRPC stream indefinitely
// (UNAVAILABLE is retried without an attempt limit and the request context is not consulted),
// so AppendRows blocks for the whole outage instead of failing. The same happens when the
// in-flight queue (Max_Queue_Requests / Max_Queue_Bytes) is saturated. On expiry the call is
// left running in the background: its rows are written once the backend answers and late
// receives its outcome. Until then flush fails fast with errAppendBlocked, so Fluent Bit
// redelivers the chunk (FLB_RETRY) without piling up more blocked appends. The redelivered
// chunk is appended again after the blocked one completes, which is the usual
// at-least-once duplicate.
func (tw *tableWriter) appendRows(ctx context.Context, ms MWManagedStream, data [][]byte, late func(appendResult, error), opts ...managedwriter.AppendOption) (appendResult, error) {
	done := make(chan appendOutcome, 1)
	started := time.Now()
	go func() {
		result, err := ms.AppendRows(tw.ctx, data, opts...)
		done <- appendOutcome{result: result, err: err}
	}()

	select {
	case o := <-done:
		return o.result, o.err
	case <-ctx.Done():
	}

	return nil, tw.blockAppend(started, len(data), "AppendRows", func() appendOutcome { return <-done }, late)
}

// blockAppend records an append whose outcome is still unknown when the flush deadline expired
// and returns the errAppendBlocked the flush reports. wait runs in the background until the
// outcome is known; it is handed to late before appends for the table resume. The outcome
// becomes known either when AppendRows returns (see appendRows) or, for committed streams,
// when the response of a returned AppendRows arrives (see sendRequestExactlyOnce).
func (tw *tableWriter) blockAppend(started time.Time, rows int, what string, wait func() appendOutcome, late func(appendResult, error)) error {
	tw.blockedAppends.Add(1)
	go func() {
		o := wait()
		log.Printf("Blocked %s for %d rows to table %s for output instance with id: %d returned after %s (error: %v); resuming appends", what, rows, tw.tableRef, tw.id, time.Since(started).Round(time.Millisecond), o.err)
		// Record the outcome before appends resume so that the next flush sees it.
		late(o.result, o.err)
		tw.blockedAppends.Add(-1)
	}()
	return fmt.Errorf("%w: %s did not return within %s (%d rows left in flight)", errAppendBlocked, what, time.Since(started).Round(time.Millisecond), rows)
}

// flush picks the least-loaded stream and sends data through it, using the exactly-once or
// at-least-once path according to the configuration.
func (tw *tableWriter) flush(ctx context.Context, data [][]byte) error {
	if len(data) == 0 {
		return nil
	}
	if n := tw.blockedAppends.Load(); n > 0 {
		return fmt.Errorf("%w: %d append(s) still in flight", errAppendBlocked, n)
	}
	streamIndex := tw.leastLoadedStreamIndex()
	if tw.cfg.exactlyOnce {
		return tw.sendRequestRetries(ctx, data, streamIndex)
	}
	return tw.sendRequestDefault(ctx, data, streamIndex)
}

// getLeastLoadedStream finds the stream index with the shortest response queue. Caller must hold streamMu.
func getLeastLoadedStream(streams []*streamConfig) int {
	m := len(streams[0].appendResults)
	minStreamIndex := 0
	for streamIndex, stream := range streams {
		if len(stream.appendResults) < m {
			m = len(stream.appendResults)
			minStreamIndex = streamIndex
		}
	}
	return minStreamIndex
}

// getStream returns the streamConfig at the given index, holding streamMu only
// for the slice access so that the caller can invoke AppendRows without holding the lock.
func (tw *tableWriter) getStream(index int) *streamConfig {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	return tw.streams[index]
}

// queueResult appends an append result to the stream's result queue under streamMu.
func (tw *tableWriter) queueResult(stream *streamConfig, result appendResult) {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	stream.appendResults = append(stream.appendResults, result)
}

// leastLoadedStreamIndex returns the index of the least-loaded stream under streamMu.
func (tw *tableWriter) leastLoadedStreamIndex() int {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	return getLeastLoadedStream(tw.streams)
}

// offset returns the offset counter of the first stream (test helper).
func (tw *tableWriter) offset() int64 {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	if len(tw.streams) == 0 {
		return 0
	}
	return tw.streams[0].offsetCounter
}

// requestCountThreshold returns the per-stream queue length above which a new stream is added.
func requestCountThreshold(maxQueueSize int) int {
	requestCountThreshold := int(math.Floor(queueRequestScalingPercent * float64(maxQueueSize)))
	if requestCountThreshold < minQueueRequests {
		requestCountThreshold = minQueueRequests
	}
	return requestCountThreshold
}

// maybeScale checks whether there is room for scaling and, when it detects back pressure from
// the queue, starts creating an additional stream in the background. Stream creation can take
// up to cfg.initTimeout while the backend is degraded, so it must not run under streamMu (every
// flush of this table needs the lock) nor in the calling flush (which is bounded by
// Flush_Timeout_Sec). The scaling flag keeps concurrent flushes from starting a second one.
func (tw *tableWriter) maybeScale() {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	if tw.scaling || tw.closed || len(tw.streams) >= maxNumStreamsPerInstance {
		return
	}
	// Gets stream with least values in queue
	mostEfficient := getLeastLoadedStream(tw.streams)
	if len(tw.streams[mostEfficient].appendResults) <= tw.cfg.requestCountThreshold {
		return
	}
	tw.scaling = true
	go tw.addStream()
}

// addStream creates one more managed stream and adds it to tw.streams. It uses tw.ctx (not a
// per-flush context) so that the new stream's lifetime is not tied to the flush that triggered
// the scaling. Caller must have set tw.scaling; it is cleared here.
func (tw *tableWriter) addStream() {
	ms, err := tw.newManagedStream(tw.ctx)

	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	tw.scaling = false
	if err != nil {
		log.Printf("Creating an additional managed stream with destination table: %s for output instance with id: %d failed: %s", tw.tableRef, tw.id, err)
		return
	}
	if tw.closed {
		// close ran while the stream was being created; nothing was appended to it.
		ms.Close()
		return
	}
	tw.streams = append(tw.streams, &streamConfig{managedstream: ms})
}

// close finalizes (exactly-once only) and closes all streams of this table.
// It returns true if any stream failed to finalize or close. ManagedStream.Close reports a
// normal close as io.EOF, and context.Canceled once the instance context has been cancelled
// on purpose during shutdown; neither is a failure.
func (tw *tableWriter) close(ctx context.Context) bool {
	tw.streamMu.Lock()
	defer tw.streamMu.Unlock()
	tw.closed = true
	errFlag := false
	for i, stream := range tw.streams {
		if stream.managedstream == nil {
			continue
		}
		if tw.cfg.exactlyOnce {
			if _, err := stream.managedstream.Finalize(ctx); err != nil {
				log.Printf("Finalizing managed stream for output instance with id %d and stream index %d failed in FLBPluginExit: %s", tw.id, i, err)
				errFlag = true
			}
		}
		err := stream.managedstream.Close()
		if err == nil || errors.Is(err, io.EOF) || (errors.Is(err, context.Canceled) && tw.ctx.Err() != nil) {
			continue
		}
		log.Printf("Closing managed stream for output instance with id %d and stream index %d failed in FLBPluginExitCtx: %s", tw.id, i, err)
		errFlag = true
	}
	return errFlag
}
