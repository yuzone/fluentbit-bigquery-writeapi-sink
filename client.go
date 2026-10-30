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
	"time"

	"cloud.google.com/go/bigquery/storage/apiv1/storagepb"
	"cloud.google.com/go/bigquery/storage/managedwriter"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
)

// ManagedWriterClient is the subset of *managedwriter.Client used by the plugin. Streams are
// returned as MWManagedStream so that tests can supply fakes.
type ManagedWriterClient interface {
	NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (MWManagedStream, error)
	GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error)
	Close() error
}

// MWManagedStream is the subset of *managedwriter.ManagedStream used by the plugin.
type MWManagedStream interface {
	AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error)
	Close() error
	Finalize(ctx context.Context, opts ...gax.CallOption) (int64, error)
	StreamName() string
}

// appendResult is the subset of *managedwriter.AppendResult used by the plugin.
type appendResult interface {
	Ready() <-chan struct{}
	GetResult(ctx context.Context) (int64, error)
	FullResponse(ctx context.Context) (*storagepb.AppendRowsResponse, error)
}

// newClientFunc creates the managed writer client for an output instance.
type newClientFunc func(ctx context.Context, projectID string, opts ...option.ClientOption) (ManagedWriterClient, error)

// clock groups the time functions used for retry backoff and the failed-init cache so that
// tests can run without sleeping.
type clock struct {
	now   func() time.Time
	sleep func(time.Duration)
}

func realClock() clock {
	return clock{now: time.Now, sleep: time.Sleep}
}

type realManagedWriterClient struct {
	currClient *managedwriter.Client
}

func newRealManagedWriterClient(ctx context.Context, projectID string, opts ...option.ClientOption) (ManagedWriterClient, error) {
	client, err := managedwriter.NewClient(ctx, projectID, opts...)
	if err != nil {
		return nil, err
	}
	return &realManagedWriterClient{currClient: client}, nil
}

func (r *realManagedWriterClient) NewManagedStream(ctx context.Context, opts ...managedwriter.WriterOption) (MWManagedStream, error) {
	ms, err := r.currClient.NewManagedStream(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return &realManagedStream{ms: ms}, nil
}

func (r *realManagedWriterClient) GetWriteStream(ctx context.Context, req *storagepb.GetWriteStreamRequest, opts ...gax.CallOption) (*storagepb.WriteStream, error) {
	return r.currClient.GetWriteStream(ctx, req, opts...)
}

func (r *realManagedWriterClient) Close() error {
	return r.currClient.Close()
}

// realManagedStream adapts *managedwriter.ManagedStream to MWManagedStream. The adapter is
// needed because AppendRows must return the appendResult interface (a nil *AppendResult
// must not become a non-nil interface).
type realManagedStream struct {
	ms *managedwriter.ManagedStream
}

func (s *realManagedStream) AppendRows(ctx context.Context, data [][]byte, opts ...managedwriter.AppendOption) (appendResult, error) {
	res, err := s.ms.AppendRows(ctx, data, opts...)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (s *realManagedStream) Close() error { return s.ms.Close() }

func (s *realManagedStream) Finalize(ctx context.Context, opts ...gax.CallOption) (int64, error) {
	return s.ms.Finalize(ctx, opts...)
}

func (s *realManagedStream) StreamName() string { return s.ms.StreamName() }
