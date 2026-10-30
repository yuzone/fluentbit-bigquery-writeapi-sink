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

// These functions are based on: https://github.com/googleapis/google-cloud-go/blob/6a9c12a395245d8500c267437c2dfa897049a719/bigquery/storage/managedwriter/retry.go
import (
	"context"
	"errors"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// isTransientError classifies an AppendRows / GetResult failure. It is the single predicate
// behind both retry decisions of the plugin: the synchronous retries of the committed-stream
// path (statelessRetryer) and the FLB_RETRY / FLB_ERROR return code of FLBPluginFlushCtx.
// Transient errors (connectivity, deadlines, server-side throttling or hiccups, token refresh
// failures) are worth retrying and asking Fluent Bit to redeliver the chunk; everything else
// (INVALID_ARGUMENT, PERMISSION_DENIED, NOT_FOUND, FAILED_PRECONDITION, schema/row errors, ...)
// is permanent and retrying would not help.
func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) ||
		errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, errAppendBlocked) || errors.Is(err, errStreamRebuilding) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	if s, ok := status.FromError(err); ok {
		switch s.Code() {
		case codes.Unavailable,
			codes.DeadlineExceeded,
			codes.ResourceExhausted,
			codes.Aborted,
			codes.Internal,
			codes.Canceled,
			codes.Unauthenticated:
			return true
		}
	}
	return false
}

// statelessRetryer decides whether a failed synchronous append (committed stream) is retried
// within the same flush and how long to pause before the next attempt. Only transient errors
// (isTransientError) are retried, so an append that exhausts its attempts is reported as
// FLB_RETRY and one that fails permanently is reported as FLB_ERROR right away.
type statelessRetryer struct {
	mu sync.Mutex // guards r
	r  *rand.Rand

	minBackoff  time.Duration
	jitter      time.Duration
	maxAttempts int
}

func newStatelessRetryer(numRetryAttempts int) *statelessRetryer {
	return &statelessRetryer{
		r:           rand.New(rand.NewSource(time.Now().UnixNano())),
		minBackoff:  50 * time.Millisecond,
		jitter:      time.Second,
		maxAttempts: numRetryAttempts,
	}
}

func (sr *statelessRetryer) pause() time.Duration {
	jitter := sr.jitter.Nanoseconds()
	if jitter > 0 {
		sr.mu.Lock()
		jitter = sr.r.Int63n(jitter)
		sr.mu.Unlock()
	}
	pause := sr.minBackoff.Nanoseconds() + jitter

	return time.Duration(pause)
}

func (sr *statelessRetryer) Retry(err error, attemptCount int) (time.Duration, bool) {
	if attemptCount >= sr.maxAttempts {
		return 0, false
	}
	if isTransientError(err) {
		return sr.pause(), true
	}
	return 0, false
}
