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
	"fmt"
	"log"
	"strconv"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/fluent/fluent-bit-go/output"
)

const (
	chunkSizeLimit             = 9 * 1024 * 1024   // BigQuery Storage Write API AppendRows hard limit (10MB minus overhead)
	queueRequestDefault        = 1000              // default Max_Queue_Requests (max in-flight AppendRows per stream)
	queueByteDefault           = 100 * 1024 * 1024 // default Max_Queue_Bytes: 100 MB
	exactlyOnceDefault         = false
	queueRequestScalingPercent = 0.8 // queue utilization threshold (80%) above which a new stream is created
	numRetriesDefault          = 4
	maxNumStreamsPerInstance   = 10 // upper bound on dynamic stream count per table writer
	minQueueRequests           = 10 // minimum Max_Queue_Requests accepted (prevents starvation during scaling)
	dateTimeDefault            = true
	maxUnixSeconds             = 4102444800 // 2100-01-01 00:00:00 UTC; values above this are treated as already in microseconds
	flushTimeoutSecDefault     = 10         // default Flush_Timeout_Sec: 10 seconds per flush call
)

// outputConfig holds the instance-wide settings of one output instance together with the
// shared client and the table writer(s) that belong to it.
type outputConfig struct {
	projectID string
	datasetID string
	tableID   string

	exactlyOnce           bool
	streamType            managedwriter.StreamType
	enableRetry           bool
	numRetries            int
	maxChunkSize          int
	maxQueueBytes         int
	maxQueueRequests      int
	requestCountThreshold int
	dateTimeStringType    bool
	flushTimeout          time.Duration

	client ManagedWriterClient
	writer *tableWriter

	// instanceCtx is a long-lived context, scoped to the lifetime of the output instance,
	// used for AppendRows and managed stream creation. Unlike the per-flush context, it is not
	// canceled at the end of a flush, so the managed writer's internal retry/reconnect logic for
	// asynchronously-acknowledged appends (default/at-least-once path) is not aborted prematurely.
	// It is canceled in FLBPluginExitCtx after pending responses are drained and streams are closed.
	instanceCtx    context.Context
	instanceCancel context.CancelFunc
}

// This function gets the value of various configuration fields and returns an error if the field could not be parsed
func getConfigField[T int | bool](plugin unsafe.Pointer, key string, defaultval T) (T, error) {
	currstr := output.FLBPluginConfigKey(plugin, key)
	finval := defaultval
	if currstr != "" {
		switch any(defaultval).(type) {
		case int:
			intval, err := strconv.Atoi(currstr)
			if err != nil {
				return defaultval, err
			} else {
				finval = any(intval).(T)
			}
		case bool:
			boolval, err := strconv.ParseBool(currstr)
			if err != nil {
				return defaultval, err
			} else {
				finval = any(boolval).(T)
			}
		}
	}
	return finval, nil
}

// parseOutputConfig reads every configuration key of the output instance. The returned
// error message is intended to be logged as-is by FLBPluginInit.
func parseOutputConfig(plugin unsafe.Pointer) (*outputConfig, error) {
	cfg := &outputConfig{
		projectID: output.FLBPluginConfigKey(plugin, "ProjectID"),
		datasetID: output.FLBPluginConfigKey(plugin, "DatasetID"),
		tableID:   output.FLBPluginConfigKey(plugin, "TableID"),
	}

	var err error
	if cfg.exactlyOnce, err = getConfigField(plugin, "Exactly_Once", exactlyOnceDefault); err != nil {
		return nil, fmt.Errorf("Invalid Exactly_Once parameter in configuration file: %w", err)
	}

	// This value is only used when the exactly-once field is configured to true (as it describes synchronous retries)
	if cfg.numRetries, err = getConfigField(plugin, "Num_Synchronous_Retries", numRetriesDefault); err != nil {
		return nil, fmt.Errorf("Invalid Num_Synchronous_Retries parameter in configuration file: %w", err)
	}

	if cfg.maxChunkSize, err = getConfigField(plugin, "Max_Chunk_Size", chunkSizeLimit); err != nil {
		return nil, fmt.Errorf("Invalid Max_Chunk_Size parameter in configuration file: %w", err)
	}
	if cfg.maxChunkSize > chunkSizeLimit {
		log.Printf("Max_Chunk_Size was set to: %d, but a single call to AppendRows cannot exceed 9 MB. Defaulting to 9 MB", cfg.maxChunkSize)
		cfg.maxChunkSize = chunkSizeLimit
	}

	if cfg.maxQueueRequests, err = getConfigField(plugin, "Max_Queue_Requests", queueRequestDefault); err != nil {
		return nil, fmt.Errorf("Invalid Max_Queue_Requests parameter in configuration file: %w", err)
	}
	cfg.requestCountThreshold = setThreshold(cfg.maxQueueRequests)

	if cfg.maxQueueBytes, err = getConfigField(plugin, "Max_Queue_Bytes", queueByteDefault); err != nil {
		return nil, fmt.Errorf("Invalid Max_Queue_Bytes parameter in configuration file: %w", err)
	}

	if cfg.dateTimeStringType, err = getConfigField(plugin, "DateTime_String_Type", dateTimeDefault); err != nil {
		return nil, fmt.Errorf("Invalid DateTime_Input_Type parameter in configuration file: %w", err)
	}

	flushTimeoutSec, err := getConfigField(plugin, "Flush_Timeout_Sec", flushTimeoutSecDefault)
	if err != nil {
		return nil, fmt.Errorf("Invalid Flush_Timeout_Sec parameter in configuration file: %w", err)
	}
	cfg.flushTimeout = time.Duration(flushTimeoutSec) * time.Second

	// Set the stream type based on exactly once parameter
	if cfg.exactlyOnce {
		cfg.streamType = managedwriter.CommittedStream
	} else {
		cfg.streamType = managedwriter.DefaultStream
		cfg.enableRetry = true
	}

	return cfg, nil
}
