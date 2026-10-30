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
	"strings"
	"time"
	"unsafe"

	"cloud.google.com/go/bigquery/storage/managedwriter"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	chunkSizeLimit             = 9 * 1024 * 1024   // upper bound for Max_Chunk_Size: keeps a request well below appendRequestLimit
	appendRequestLimit         = 10 * 1000 * 1000  // BigQuery Storage Write API: a single AppendRows request must be less than 10 MB
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
	initTimeoutSecDefault      = 30         // default Init_Timeout_Sec: schema fetch + stream creation per table
	tagDelimitersDefault       = "."        // default Tag_Delimiters used to split the tag for $TAG[n]
	retryOnFailureDefault      = true       // default Retry_On_Failure: return FLB_RETRY for transient append failures
	enableMultiplexingDefault  = false      // default Enable_Multiplexing: one gRPC connection per default stream
	multiplexPoolLimitDefault  = 1          // default Multiplex_Pool_Limit: connections per region when multiplexing
)

// unknownTableAction is what FLBPluginFlushCtx does when the table resolved from the tag
// cannot be initialized.
type unknownTableAction int

const (
	unknownTableDrop  unknownTableAction = iota // log and acknowledge the chunk (FLB_OK)
	unknownTableRetry                           // ask Fluent Bit to retry the chunk (FLB_RETRY)
)

func parseUnknownTableAction(s string) (unknownTableAction, error) {
	switch strings.ToLower(s) {
	case "", "drop":
		return unknownTableDrop, nil
	case "retry":
		return unknownTableRetry, nil
	}
	return unknownTableDrop, fmt.Errorf("expected \"drop\" or \"retry\", got %q", s)
}

// outputConfig holds the instance-wide settings of one output instance together with the
// shared client and the table writer(s) that belong to it.
type outputConfig struct {
	projectID       string
	datasetTemplate *tagTemplate // DatasetId, may reference $TAG
	tableTemplate   *tagTemplate // TableId, may reference $TAG
	tagDelimiters   string
	preloadTables   []string
	unknownTable    unknownTableAction

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
	initTimeout           time.Duration
	retryOnFailure        bool
	enableMultiplexing    bool
	multiplexPoolLimit    int
	endpoint              string
	insecure              bool

	client ManagedWriterClient
	router *tableRouter
	clock  clock

	// instanceCtx is a long-lived context, scoped to the lifetime of the output instance,
	// used for AppendRows and managed stream creation. Unlike the per-flush context, it is not
	// canceled at the end of a flush, so the managed writer's internal retry/reconnect logic for
	// asynchronously-acknowledged appends (default/at-least-once path) is not aborted prematurely.
	// It is canceled in FLBPluginExitCtx after pending responses are drained and streams are closed.
	instanceCtx    context.Context
	instanceCancel context.CancelFunc
}

// This function gets the value of various configuration fields and returns an error if the field could not be parsed
func getConfigField[T int | bool](flb flbBridge, plugin unsafe.Pointer, key string, defaultval T) (T, error) {
	currstr := flb.ConfigKey(plugin, key)
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
func parseOutputConfig(plugin unsafe.Pointer, flb flbBridge) (*outputConfig, error) {
	cfg := &outputConfig{
		projectID: flb.ConfigKey(plugin, "ProjectID"),
	}

	var err error
	if cfg.datasetTemplate, err = parseTagTemplate(flb.ConfigKey(plugin, "DatasetID")); err != nil {
		return nil, fmt.Errorf("Invalid DatasetId parameter in configuration file: %w", err)
	}
	if cfg.tableTemplate, err = parseTagTemplate(flb.ConfigKey(plugin, "TableID")); err != nil {
		return nil, fmt.Errorf("Invalid TableId parameter in configuration file: %w", err)
	}
	cfg.tagDelimiters = flb.ConfigKey(plugin, "Tag_Delimiters")
	if cfg.tagDelimiters == "" {
		cfg.tagDelimiters = tagDelimitersDefault
	}
	for _, t := range strings.Split(flb.ConfigKey(plugin, "Preload_Tables"), ",") {
		if t = strings.TrimSpace(t); t != "" {
			cfg.preloadTables = append(cfg.preloadTables, t)
		}
	}
	if cfg.unknownTable, err = parseUnknownTableAction(flb.ConfigKey(plugin, "Unknown_Table_Action")); err != nil {
		return nil, fmt.Errorf("Invalid Unknown_Table_Action parameter in configuration file: %w", err)
	}

	if cfg.exactlyOnce, err = getConfigField(flb, plugin, "Exactly_Once", exactlyOnceDefault); err != nil {
		return nil, fmt.Errorf("Invalid Exactly_Once parameter in configuration file: %w", err)
	}

	// This value is only used when the exactly-once field is configured to true (as it describes synchronous retries)
	if cfg.numRetries, err = getConfigField(flb, plugin, "Num_Synchronous_Retries", numRetriesDefault); err != nil {
		return nil, fmt.Errorf("Invalid Num_Synchronous_Retries parameter in configuration file: %w", err)
	}

	if cfg.maxChunkSize, err = getConfigField(flb, plugin, "Max_Chunk_Size", chunkSizeLimit); err != nil {
		return nil, fmt.Errorf("Invalid Max_Chunk_Size parameter in configuration file: %w", err)
	}
	if cfg.maxChunkSize > chunkSizeLimit {
		log.Printf("Max_Chunk_Size was set to: %d, but a single call to AppendRows cannot exceed 9 MB. Defaulting to 9 MB", cfg.maxChunkSize)
		cfg.maxChunkSize = chunkSizeLimit
	}

	// Max_Queue_Requests / Max_Queue_Bytes <= 0 disable the respective in-flight limit in the
	// managed writer (its flow controller only creates a semaphore for positive values).
	if cfg.maxQueueRequests, err = getConfigField(flb, plugin, "Max_Queue_Requests", queueRequestDefault); err != nil {
		return nil, fmt.Errorf("Invalid Max_Queue_Requests parameter in configuration file: %w", err)
	}
	cfg.requestCountThreshold = requestCountThreshold(cfg.maxQueueRequests)

	if cfg.maxQueueBytes, err = getConfigField(flb, plugin, "Max_Queue_Bytes", queueByteDefault); err != nil {
		return nil, fmt.Errorf("Invalid Max_Queue_Bytes parameter in configuration file: %w", err)
	}

	if cfg.dateTimeStringType, err = getConfigField(flb, plugin, "DateTime_String_Type", dateTimeDefault); err != nil {
		return nil, fmt.Errorf("Invalid DateTime_String_Type parameter in configuration file: %w", err)
	}

	flushTimeoutSec, err := getConfigField(flb, plugin, "Flush_Timeout_Sec", flushTimeoutSecDefault)
	if err != nil {
		return nil, fmt.Errorf("Invalid Flush_Timeout_Sec parameter in configuration file: %w", err)
	}
	if flushTimeoutSec < 1 {
		return nil, fmt.Errorf("Invalid Flush_Timeout_Sec parameter in configuration file: must be >= 1, got %d", flushTimeoutSec)
	}
	cfg.flushTimeout = time.Duration(flushTimeoutSec) * time.Second

	initTimeoutSec, err := getConfigField(flb, plugin, "Init_Timeout_Sec", initTimeoutSecDefault)
	if err != nil {
		return nil, fmt.Errorf("Invalid Init_Timeout_Sec parameter in configuration file: %w", err)
	}
	if initTimeoutSec < 1 {
		return nil, fmt.Errorf("Invalid Init_Timeout_Sec parameter in configuration file: must be >= 1, got %d", initTimeoutSec)
	}
	cfg.initTimeout = time.Duration(initTimeoutSec) * time.Second

	if cfg.retryOnFailure, err = getConfigField(flb, plugin, "Retry_On_Failure", retryOnFailureDefault); err != nil {
		return nil, fmt.Errorf("Invalid Retry_On_Failure parameter in configuration file: %w", err)
	}

	if cfg.enableMultiplexing, err = getConfigField(flb, plugin, "Enable_Multiplexing", enableMultiplexingDefault); err != nil {
		return nil, fmt.Errorf("Invalid Enable_Multiplexing parameter in configuration file: %w", err)
	}
	if cfg.multiplexPoolLimit, err = getConfigField(flb, plugin, "Multiplex_Pool_Limit", multiplexPoolLimitDefault); err != nil {
		return nil, fmt.Errorf("Invalid Multiplex_Pool_Limit parameter in configuration file: %w", err)
	}
	if cfg.multiplexPoolLimit < 1 {
		return nil, fmt.Errorf("Invalid Multiplex_Pool_Limit parameter in configuration file: must be >= 1, got %d", cfg.multiplexPoolLimit)
	}
	if cfg.enableMultiplexing && cfg.exactlyOnce {
		log.Printf("Enable_Multiplexing is set together with Exactly_Once; committed streams cannot be multiplexed, so the option has no effect on this output")
	}

	cfg.endpoint = strings.TrimSpace(flb.ConfigKey(plugin, "Endpoint"))
	if cfg.insecure, err = getConfigField(flb, plugin, "Insecure", false); err != nil {
		return nil, fmt.Errorf("Invalid Insecure parameter in configuration file: %w", err)
	}
	if cfg.insecure && cfg.endpoint == "" {
		return nil, fmt.Errorf("Invalid Insecure parameter in configuration file: requires Endpoint to be set")
	}
	if cfg.insecure {
		log.Printf("Insecure is set: connecting to %s without TLS or authentication", cfg.endpoint)
	}

	// Set the stream type based on exactly once parameter
	if cfg.exactlyOnce {
		cfg.streamType = managedwriter.CommittedStream
	} else {
		cfg.streamType = managedwriter.DefaultStream
		cfg.enableRetry = true
	}

	return cfg, nil
}

// clientOptions returns the managedwriter client options derived from the configuration.
func (cfg *outputConfig) clientOptions() []option.ClientOption {
	var opts []option.ClientOption
	if cfg.enableMultiplexing {
		opts = append(opts,
			managedwriter.WithMultiplexing(),
			managedwriter.WithMultiplexPoolLimit(cfg.multiplexPoolLimit),
		)
	}
	if cfg.endpoint != "" {
		opts = append(opts, option.WithEndpoint(cfg.endpoint))
	}
	if cfg.insecure {
		// Emulators speak plaintext gRPC and do not validate credentials.
		opts = append(opts,
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
		)
	}
	return opts
}

// cancelInstance cancels the instance-scoped context. It is safe to call more than once.
func (c *outputConfig) cancelInstance() {
	if c.instanceCancel != nil {
		c.instanceCancel()
	}
}
