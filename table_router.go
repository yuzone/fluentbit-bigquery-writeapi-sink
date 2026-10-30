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
	"sync"
	"time"
)

// initFailureTTL is how long a failed table initialization (e.g. NOT_FOUND) is remembered
// before the next flush for that table retries GetWriteStream.
const initFailureTTL = 60 * time.Second

// writerEntry is one slot of the router map. done is closed once initialization finished;
// tw/err/failedAt are written under tableRouter.mu before done is closed.
type writerEntry struct {
	done     chan struct{}
	tw       *tableWriter
	err      error
	failedAt time.Time
}

// tableRouter resolves the destination table for a Fluent Bit tag and lazily creates one
// tableWriter per (dataset, table). When neither DatasetId nor TableId references the tag the
// single writer is created at init and returned without any map lookup.
type tableRouter struct {
	cfg *outputConfig
	id  int

	static *tableWriter

	mu      sync.Mutex
	entries map[string]*writerEntry
}

func newTableRouter(cfg *outputConfig, id int) *tableRouter {
	return &tableRouter{
		cfg:     cfg,
		id:      id,
		entries: make(map[string]*writerEntry),
	}
}

// dynamic reports whether the destination depends on the tag.
func (r *tableRouter) dynamic() bool {
	return r.cfg.datasetTemplate.dynamic() || r.cfg.tableTemplate.dynamic()
}

// init creates the static writer, or the Preload_Tables writers for a dynamic destination.
// Any failure is returned so that FLBPluginInit can fail, matching the previous behavior for
// a static TableId.
func (r *tableRouter) init() error {
	if !r.dynamic() {
		tw, err := r.newWriter(r.cfg.datasetTemplate.raw, r.cfg.tableTemplate.raw)
		if err != nil {
			return err
		}
		r.static = tw
		return nil
	}
	if len(r.cfg.preloadTables) > 0 && r.cfg.datasetTemplate.dynamic() {
		return fmt.Errorf("Preload_Tables requires a static DatasetId, but DatasetId is %q", r.cfg.datasetTemplate.raw)
	}
	for _, table := range r.cfg.preloadTables {
		if !validTableID(table) {
			return fmt.Errorf("Preload_Tables contains an invalid table ID %q", table)
		}
		if _, err := r.writerFor(r.cfg.datasetTemplate.raw, table); err != nil {
			return err
		}
	}
	return nil
}

// resolve renders the dataset and table templates for tag and returns the matching writer,
// creating it on first use. Errors indicate an unknown or uninitializable table.
func (r *tableRouter) resolve(tag string) (*tableWriter, error) {
	if r.static != nil {
		return r.static, nil
	}
	dataset, err := r.cfg.datasetTemplate.render(tag, r.cfg.tagDelimiters)
	if err != nil {
		return nil, err
	}
	table, err := r.cfg.tableTemplate.render(tag, r.cfg.tagDelimiters)
	if err != nil {
		return nil, err
	}
	if !validDatasetID(dataset) {
		return nil, fmt.Errorf("tag %q resolved to invalid dataset ID %q", tag, dataset)
	}
	if !validTableID(table) {
		return nil, fmt.Errorf("tag %q resolved to invalid table ID %q", tag, table)
	}
	return r.writerFor(dataset, table)
}

// writerFor returns the writer for (dataset, table), initializing it exactly once even under
// concurrent flushes. Other tables are not blocked while one table initializes. A failed
// initialization is cached for initFailureTTL.
func (r *tableRouter) writerFor(dataset, table string) (*tableWriter, error) {
	key := dataset + "/" + table

	r.mu.Lock()
	e, ok := r.entries[key]
	if ok {
		select {
		case <-e.done:
			if e.err != nil && r.cfg.clock.now().Sub(e.failedAt) >= initFailureTTL {
				ok = false
			}
		default:
		}
	}
	if !ok {
		e = &writerEntry{done: make(chan struct{})}
		r.entries[key] = e
		r.mu.Unlock()

		tw, err := r.newWriter(dataset, table)

		r.mu.Lock()
		e.tw, e.err = tw, err
		if err != nil {
			e.failedAt = r.cfg.clock.now()
		}
		r.mu.Unlock()
		close(e.done)
		return tw, err
	}
	r.mu.Unlock()

	<-e.done
	return e.tw, e.err
}

func (r *tableRouter) newWriter(dataset, table string) (*tableWriter, error) {
	return newTableWriter(r.cfg.instanceCtx, r.cfg.client, r.cfg.projectID, dataset, table, r.cfg, r.id)
}

// writers returns every successfully initialized writer.
func (r *tableRouter) writers() []*tableWriter {
	if r.static != nil {
		return []*tableWriter{r.static}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]*tableWriter, 0, len(r.entries))
	for _, e := range r.entries {
		select {
		case <-e.done:
			if e.tw != nil {
				out = append(out, e.tw)
			}
		default:
		}
	}
	return out
}

// closeAll drains pending responses and closes every writer. It returns true if any close failed.
func (r *tableRouter) closeAll(ctx context.Context) bool {
	errFlag := false
	for _, tw := range r.writers() {
		tw.drainResponses(ctx, true)
		if tw.close(ctx) {
			errFlag = true
		}
	}
	return errFlag
}
