//go:build e2e

// Package e2e drives the Docker-based end-to-end harness: it starts a BigQuery emulator and a
// real Fluent Bit that loads the compiled out_writeapi.so, pushes records through in_http and
// counts the rows that reach each table.
//
// Run it with `make e2e` (builds the plugin first) or, once hack/e2e/build/out_writeapi.so
// exists, `go test -tags e2e -count=1 -v ./hack/e2e`.
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigquery"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

const (
	project      = "e2e"
	dataset      = "logs"
	restEndpoint = "http://127.0.0.1:9050"
	proxyAPI     = "http://127.0.0.1:8474"
	httpInput    = "http://127.0.0.1:9880"
	startTimeout = 90 * time.Second
	rowTimeout   = 60 * time.Second
)

type harness struct {
	t   *testing.T
	dir string
	bq  *bigquery.Client
}

func TestE2E(t *testing.T) {
	h := newHarness(t)
	h.up()

	t.Run("static table", h.testStaticTable)
	t.Run("tag routing", h.testTagRouting)
	t.Run("unknown table", h.testUnknownTable)
	t.Run("unknown table retry", h.testUnknownTableRetry)
	t.Run("backend outage", h.testBackendOutage)
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	dir, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, "build", "out_writeapi.so")); err != nil {
		t.Fatalf("hack/e2e/build/out_writeapi.so not found; run `make e2e` (or `make e2e-build`) first: %v", err)
	}
	return &harness{t: t, dir: dir}
}

// compose runs `docker compose` in the harness directory and fails the test on error.
func (h *harness) compose(args ...string) string {
	h.t.Helper()
	out, err := h.composeOutput(args...)
	if err != nil {
		h.t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

func (h *harness) composeOutput(args ...string) (string, error) {
	cmd := exec.Command("docker", append([]string{"compose"}, args...)...)
	cmd.Dir = h.dir
	var buf bytes.Buffer
	cmd.Stdout, cmd.Stderr = &buf, &buf
	err := cmd.Run()
	return buf.String(), err
}

func (h *harness) up() {
	h.t.Helper()
	h.compose("down", "--remove-orphans", "--volumes", "--timeout", "5")
	h.t.Cleanup(func() {
		if h.t.Failed() {
			h.t.Logf("fluent-bit logs:\n%s", h.fluentBitLogs())
		}
		if os.Getenv("E2E_KEEP") == "" {
			h.composeOutput("down", "--remove-orphans", "--volumes", "--timeout", "5")
		}
	})

	h.compose("up", "-d", "bigquery")
	h.waitFor("bigquery emulator REST API", func() bool {
		resp, err := http.Get(restEndpoint + "/bigquery/v2/projects/" + project + "/datasets")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	bq, err := bigquery.NewClient(context.Background(), project,
		option.WithEndpoint(restEndpoint), option.WithoutAuthentication())
	if err != nil {
		h.t.Fatal(err)
	}
	h.bq = bq
	h.t.Cleanup(func() { bq.Close() })

	h.compose("up", "-d", "bq-proxy")
	h.waitFor("toxiproxy API", func() bool {
		resp, err := http.Get(proxyAPI + "/proxies/bq")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})

	h.compose("up", "-d", "fluent-bit")
	h.waitFor("fluent-bit in_http listener", func() bool {
		c, err := net.DialTimeout("tcp", "127.0.0.1:9880", time.Second)
		if err != nil {
			return false
		}
		c.Close()
		return true
	})
}

func (h *harness) waitFor(what string, ready func() bool) {
	h.t.Helper()
	deadline := time.Now().Add(startTimeout)
	for time.Now().Before(deadline) {
		if ready() {
			return
		}
		time.Sleep(500 * time.Millisecond)
	}
	h.t.Fatalf("timed out waiting for %s\n%s", what, h.compose("logs", "--no-color"))
}

func (h *harness) fluentBitLogs() string {
	out, _ := h.composeOutput("logs", "--no-color", "--no-log-prefix", "fluent-bit")
	return out
}

// send posts n records with the given tag through in_http. seq starts at from.
func (h *harness) send(tag string, from, n int) {
	h.t.Helper()
	records := make([]map[string]interface{}, 0, n)
	for i := 0; i < n; i++ {
		records = append(records, map[string]interface{}{"message": tag, "seq": from + i})
	}
	body, err := json.Marshal(records)
	if err != nil {
		h.t.Fatal(err)
	}
	var lastErr error
	for attempt := 0; attempt < 20; attempt++ {
		resp, err := http.Post(httpInput+"/"+tag, "application/json", bytes.NewReader(body))
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode/100 == 2 {
				return
			}
			err = fmt.Errorf("status %s", resp.Status)
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	h.t.Fatalf("posting %d records with tag %s: %v", n, tag, lastErr)
}

func (h *harness) count(table string) int64 {
	h.t.Helper()
	q := h.bq.Query(fmt.Sprintf("SELECT COUNT(*) FROM `%s.%s.%s`", project, dataset, table))
	it, err := q.Read(context.Background())
	if err != nil {
		h.t.Fatalf("counting %s: %v", table, err)
	}
	var row []bigquery.Value
	if err := it.Next(&row); err != nil && err != iterator.Done {
		h.t.Fatalf("counting %s: %v", table, err)
	}
	if len(row) == 0 {
		return 0
	}
	n, _ := row[0].(int64)
	return n
}

// waitCount blocks until table holds exactly want rows (or fails after rowTimeout).
func (h *harness) waitCount(table string, want int64, timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	var got int64
	for time.Now().Before(deadline) {
		got = h.count(table)
		if got == want {
			return
		}
		time.Sleep(time.Second)
	}
	h.t.Fatalf("table %s: want %d rows, got %d after %s", table, want, got, timeout)
}

// assertStableCount verifies that table still holds want rows after a settle period.
func (h *harness) assertStableCount(table string, want int64) {
	h.t.Helper()
	time.Sleep(4 * time.Second)
	if got := h.count(table); got != want {
		h.t.Fatalf("table %s: want %d rows, got %d", table, want, got)
	}
}

func (h *harness) testStaticTable(t *testing.T) {
	h.send("static.app", 0, 5)
	h.waitCount("static_logs", 5, rowTimeout)
}

func (h *harness) testTagRouting(t *testing.T) {
	h.send("app.orders", 0, 3)
	h.send("app.users", 0, 2)
	h.waitCount("orders", 3, rowTimeout)
	h.waitCount("users", 2, rowTimeout)
	h.assertStableCount("static_logs", 5)
}

func (h *harness) testUnknownTable(t *testing.T) {
	h.send("app.missing", 0, 2)
	// Unknown_Table_Action drop: the chunk is acknowledged, nothing else is affected and the
	// output keeps working for known tables.
	h.send("app.orders", 3, 1)
	h.waitCount("orders", 4, rowTimeout)
	h.assertStableCount("users", 2)
	if logs := h.fluentBitLogs(); !strings.Contains(logs, "missing") {
		t.Fatalf("expected the unknown table to be logged; logs:\n%s", logs)
	}
}

// testUnknownTableRetry covers the FLB_RETRY path: with Unknown_Table_Action retry a chunk for a
// table that cannot be initialised is handed back to Fluent Bit, which redelivers it. Once the
// table exists the plugin retries the initialisation (after initFailureTTL) and the rows land.
func (h *harness) testUnknownTableRetry(t *testing.T) {
	h.send("late.events", 0, 3)
	h.waitForLog(`tag "late.events" failed`, rowTimeout)
	h.waitForLog("retry in", rowTimeout)
	h.assertStableCount("users", 2)

	h.createTable("events")
	// The failed initialisation is cached for initFailureTTL (60s) before it is retried.
	h.waitCount("events", 3, 90*time.Second)
}

// testBackendOutage disables the proxy between the plugin and the emulator: the connection is
// dropped and reconnects are refused. The managed writer retries the reconnect internally for as
// long as the outage lasts, so AppendRows blocks; the plugin gives up after Flush_Timeout_Sec,
// returns FLB_RETRY and Fluent Bit redelivers the chunk until the backend is back. The emulator
// itself stays up, so the seeded tables and the REST port used by count are unaffected.
func (h *harness) testBackendOutage(t *testing.T) {
	h.setProxyEnabled(false)
	defer h.setProxyEnabled(true)

	h.send("outage.app", 0, 3)
	h.waitForLog("AppendRows is blocked", rowTimeout)
	h.waitForLog("output=writeapi.3", rowTimeout) // Fluent Bit scheduled a retry for this output
	h.assertStableCount("outage_logs", 0)

	h.setProxyEnabled(true)
	// The blocked append and the redelivered chunk both land: at-least-once duplicates.
	h.waitCount("outage_logs", 6, rowTimeout)
}

// createTable creates dataset.table with the shared e2e schema through the REST API.
func (h *harness) createTable(table string) {
	h.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err := h.bq.Dataset(dataset).Table(table).Create(ctx, &bigquery.TableMetadata{
		Schema: bigquery.Schema{
			{Name: "message", Type: bigquery.StringFieldType},
			{Name: "seq", Type: bigquery.IntegerFieldType},
		},
	})
	if err != nil {
		h.t.Fatalf("creating table %s: %v", table, err)
	}
}

// waitForLog waits until the Fluent Bit logs contain substr.
func (h *harness) waitForLog(substr string, timeout time.Duration) {
	h.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if strings.Contains(h.fluentBitLogs(), substr) {
			return
		}
		time.Sleep(time.Second)
	}
	h.t.Fatalf("Fluent Bit did not log %q within %s; logs:\n%s", substr, timeout, h.fluentBitLogs())
}

// setProxyEnabled toggles the toxiproxy proxy in front of the emulator's gRPC port.
func (h *harness) setProxyEnabled(enabled bool) {
	h.t.Helper()
	body := fmt.Sprintf(`{"enabled":%t}`, enabled)
	resp, err := http.Post(proxyAPI+"/proxies/bq", "application/json", strings.NewReader(body))
	if err != nil {
		h.t.Fatalf("toggling proxy: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		h.t.Fatalf("toggling proxy: status %s", resp.Status)
	}
}
