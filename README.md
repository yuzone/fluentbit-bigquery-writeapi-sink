# fluentbit-bigquery-writeapi-sink

A [Fluent Bit](https://fluentbit.io/) Go output plugin (`writeapi`) that streams
records into Google BigQuery through the
[BigQuery Storage Write API](https://cloud.google.com/bigquery/docs/write-api).

This project is derived from
[`GoogleCloudPlatform/fluentbit-bigquery-writeapi-sink`](https://github.com/GoogleCloudPlatform/fluentbit-bigquery-writeapi-sink)
and is maintained independently. See [Attribution](#attribution).

## Features

- Streams JSON records from any Fluent Bit input into an existing BigQuery table
  using the table schema fetched on first use.
- Routes one output to many tables by resolving `TableId` (and `DatasetId`)
  from the Fluent Bit tag (`TableId $TAG[1]`).
- At-least-once delivery via the default stream (asynchronous acknowledgement),
  or exactly-once delivery via committed streams (synchronous acknowledgement).
- Dynamic scale-out of default streams when the in-flight queue fills up.
- Pre-built `linux/amd64` and `linux/arm64` shared objects published on every
  release.

## Installation

### Download a release

Each GitHub Release ships the plugin as

```
out_writeapi-<tag>-linux-amd64.so
out_writeapi-<tag>-linux-arm64.so
```

Download the file matching your platform, rename it to `out_writeapi.so` if you
like, and point Fluent Bit at it via a plugins file:

```
[PLUGINS]
    Path /path/to/out_writeapi.so
```

The Go runtime embedded in the plugin must be compatible with the Fluent Bit
binary's C library; the release artifacts are built on Debian bookworm
(glibc).

### Build from source

CGO is required.

```sh
go build -buildmode=c-shared -o out_writeapi.so
```

To build the same artifact that the release workflow publishes (Linux, glibc),
use `Dockerfile.build`:

```sh
docker buildx build \
  --platform linux/amd64 \        # or linux/arm64
  --target artifact \
  --output type=local,dest=out \
  -f Dockerfile.build .
# -> out/out_writeapi.so
```

## Configuration

BigQuery does not create the dataset or table for you; create them first
([datasets](https://cloud.google.com/bigquery/docs/datasets),
[tables](https://cloud.google.com/bigquery/docs/tables)). Field names in the
incoming records must match the table schema.

Authentication uses
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials).

A minimal configuration (see also `expipeline.conf`, `plugins.conf` and
`jsonparser.conf`):

```
[SERVICE]
    Flush           1
    Daemon          off
    Log_Level       error
    Parsers_File    jsonparser.conf
    plugins_file    plugins.conf

[INPUT]
    Name    tail
    Path    /var/log/app/*.log
    Parser  json
    Tag     app

[OUTPUT]
    Name        writeapi
    Match       app
    ProjectId   my-project
    DatasetId   my_dataset
    TableId     my_table
```

### `[OUTPUT]` parameters

Keys are matched case-insensitively by Fluent Bit.

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `ProjectId` | string | (required) | GCP project that owns the table. |
| `DatasetId` | string | (required) | BigQuery dataset. May contain `$TAG` / `$TAG[n]` (see [Routing by tag](#routing-by-tag)). |
| `TableId` | string | (required) | BigQuery table. May contain `$TAG` / `$TAG[n]`. Without placeholders the schema is fetched once at startup and a missing table fails initialisation. |
| `Tag_Delimiters` | string | `.` | Characters on which the tag is split for `$TAG[n]`. Every character in the value is a delimiter; empty parts are dropped. |
| `Preload_Tables` | string | *(empty)* | Comma-separated table IDs to initialise at startup when `TableId` is tag-based. A missing preload table fails initialisation, restoring the start-up validation a static `TableId` provides. Requires a static `DatasetId`. |
| `Unknown_Table_Action` | string | `drop` | What to do with a chunk whose table cannot be resolved or initialised: `drop` logs and acknowledges the chunk (`FLB_OK`); `retry` returns `FLB_RETRY` so Fluent Bit redelivers it. A **transient** initialisation failure (backend unreachable, `Init_Timeout_Sec` exceeded) is not an unknown table: it is retried like a transient append failure regardless of this setting. |
| `Exactly_Once` | bool | `false` | `true` uses a committed stream with synchronous response checking (exactly-once). `false` uses the default stream with asynchronous response checking (at-least-once). |
| `Num_Synchronous_Retries` | int | `4` | Maximum synchronous retries per append when `Exactly_Once` is `true`. Ignored otherwise. |
| `Max_Chunk_Size` | int (bytes) | `9437184` (9 MiB) | Upper bound on the serialized size of a single `AppendRows` request. Values above 9 MiB are clamped to 9 MiB. |
| `Max_Queue_Requests` | int | `1000` | Maximum number of in-flight (unacknowledged) `AppendRows` requests per stream. When the queue reaches 80 % of this value a new default stream is created (up to 10 streams per output instance). |
| `Max_Queue_Bytes` | int (bytes) | `104857600` (100 MiB) | Maximum number of in-flight bytes per stream. |
| `DateTime_String_Type` | bool | `true` | `true` accepts `DATETIME` values as strings. `false` accepts civil-time encoded `int64`. Applies to nested `DATETIME` fields as well; `RANGE<DATETIME>` always expects `int64`. |
| `Flush_Timeout_Sec` | int (seconds) | `10` | Deadline for a single flush call (and for each step of the shutdown, see [Shutdown](#shutdown)). An `AppendRows` call that has not returned by then is left running in the background and the chunk is reported as `FLB_RETRY` (see [Delivery semantics](#delivery-semantics)). |
| `Init_Timeout_Sec` | int (seconds) | `30` | Deadline for initialising one table (schema fetch and stream creation). Without it the client library retries an unreachable backend for 10 minutes, blocking Fluent Bit's start-up (static table) or the first flush of a tag-routed table. Must be ≥ 1. |
| `Retry_On_Failure` | bool | `true` | `true` returns `FLB_RETRY` / `FLB_ERROR` to Fluent Bit when an append fails (see [Delivery semantics](#delivery-semantics)). `false` restores the historical behaviour of logging the failure and acknowledging the chunk. |
| `Enable_Multiplexing` | bool | `false` | **EXPERIMENTAL.** Share gRPC connections between default streams of this output (see [Connection multiplexing](#connection-multiplexing)). |
| `Multiplex_Pool_Limit` | int | `1` | **EXPERIMENTAL.** Maximum shared connections per BigQuery region when `Enable_Multiplexing` is `true`; ignored otherwise. Must be `>= 1`. |
| `Endpoint` | string | _(Google default)_ | `host:port` of the Storage Write API gRPC endpoint. Only needed to target an emulator or a private endpoint. |
| `Insecure` | bool | `false` | `true` disables TLS **and** authentication for `Endpoint` (plaintext gRPC, as spoken by emulators). Requires `Endpoint`; never use against Google's endpoints. |

Boolean values are parsed with Go's `strconv.ParseBool` (`true`/`false`,
`1`/`0`, `t`/`f`, …). An unparsable value fails plugin initialisation.

### Routing by tag

`DatasetId` and `TableId` accept the placeholders used by Fluent Bit's `out_s3`
`s3_key_format`:

| Placeholder | Meaning |
|---|---|
| `$TAG` | The whole tag. |
| `$TAG[n]` | The n-th part (0-based) of the tag after splitting on `Tag_Delimiters`. |

Any other text is kept literally. Fluent Bit delivers exactly one tag per
chunk, so a chunk is always written to a single table.

```yaml
pipeline:
  outputs:
    - name: writeapi
      match: 'app.*'
      ProjectId: my-project
      DatasetId: my_dataset
      TableId: '$TAG[1]'          # app.orders.us -> table "orders"
      Preload_Tables: orders,users
      Unknown_Table_Action: retry
```

Behaviour:

- A table is initialised (schema fetch + first stream) on the first chunk
  carrying its tag. Initialisation happens once per table even when several
  Fluent Bit workers flush concurrently; other tables are not blocked.
- Resolved IDs are validated against BigQuery naming rules (table: Unicode
  letters/marks/numbers, `_`, `-`, space, ≤ 1024 bytes; dataset: ASCII
  letters/digits/`_`, ≤ 1024 bytes). Invalid IDs and `$TAG[n]` indexes out of
  range are treated as unknown tables and never reach BigQuery.
- A failed initialisation (for example `NOT_FOUND` for a mistyped tag) is
  remembered for 60 s so a bad tag does not call `GetWriteStream` on every
  flush. Transient failures (backend unreachable) are not remembered; the next
  flush tries again.
- `Max_Queue_Requests`, `Max_Queue_Bytes` and dynamic stream scaling (up to 10
  streams) apply **per table**, as they did per output before. Fluent Bit's
  own buffer limits (`storage.total_limit_size`, `Mem_Buf_Limit`) remain per
  output, so keep very high-volume tables on dedicated outputs.
- Shutdown drains and closes every table's streams (see [Shutdown](#shutdown)).

## Accepted input type per BigQuery field type

| BigQuery type | Accepted input |
|:-------------:|:--------------:|
| BOOL | bool |
| BYTES | bytes |
| DATE | int32 |
| DATETIME | string or int64 (see `DateTime_String_Type`) |
| FLOAT | float64 |
| GEOGRAPHY | string |
| INTEGER | int64 |
| JSON | string |
| NUMERIC | string |
| BIGNUMERIC | string |
| STRING | string |
| TIME | string |
| TIMESTAMP | int64 |
| RANGE | struct |
| RECORD | struct |

`integration_test.go` contains an example record for every type. Encoding
details: [Storage Write API data type
conversions](https://cloud.google.com/bigquery/docs/write-api#data_type_conversions).

## Delivery semantics

Each call of the output (one Fluent Bit chunk, one tag) ends with one of the
Fluent Bit return codes:

| Code | When | Fluent Bit reaction |
|---|---|---|
| `FLB_OK` | Every `AppendRows` call of the chunk was accepted (or handed to the default stream). Also returned when `Retry_On_Failure` is `false`, and for unknown tables with `Unknown_Table_Action drop`. | Chunk is acknowledged and released. |
| `FLB_RETRY` | A **transient** failure: gRPC `UNAVAILABLE`, `DEADLINE_EXCEEDED`, `RESOURCE_EXHAUSTED`, `ABORTED`, `INTERNAL`, `CANCELLED`, connection errors, `io.EOF`, context deadline/cancellation. Also returned for unknown tables with `Unknown_Table_Action retry`. | Chunk is kept in the buffer and redelivered according to `Retry_Limit` / `scheduler.*`. |
| `FLB_ERROR` | A **permanent** failure: `INVALID_ARGUMENT` (for example schema mismatch), `PERMISSION_DENIED`, `NOT_FOUND`, and any other error. | Chunk is dropped and counted in the output's error metrics. |

Which failures are visible at flush time depends on the mode:

- **Default stream (`Exactly_Once false`, at-least-once)**: only the
  synchronous error returned by `AppendRows` itself (connection problems,
  request too large, stream closed, ...) can change the return code. The
  server's answer arrives asynchronously and is checked on a later flush or at
  exit; by then the chunk has already been acknowledged, so those errors are
  **logged only** (`Encountered error ... while verifying the server
  response`). The managed writer retries transient failures internally for
  this path.
- **Blocked append (both modes)**: while the endpoint is unreachable the
  managed writer re-opens the gRPC stream indefinitely (`UNAVAILABLE` is
  retried without limit), and when the in-flight queue is saturated it waits
  for room, so `AppendRows` **blocks** instead of failing. The plugin waits at
  most `Flush_Timeout_Sec`, then returns `FLB_RETRY` and leaves the call
  running in the background; until it completes, every flush for that table
  returns `FLB_RETRY` immediately without starting another append. When the
  backend answers, the blocked rows are written and the redelivered chunk is
  appended again, so **one chunk per table and outage is duplicated**. Size
  Fluent Bit's buffer (`storage.*`, `Mem_Buf_Limit`, `Retry_Limit`) for the
  outages you want to survive. Verified by the `backend outage` scenario of
  the [end-to-end harness](#end-to-end-harness). Initialising a table while the
  backend is unreachable fails after `Init_Timeout_Sec` and is treated the same
  way (`FLB_RETRY`, not `Unknown_Table_Action`).
- **Committed stream (`Exactly_Once true`)**: every append is confirmed
  synchronously and retried up to `Num_Synchronous_Retries` times for
  transient errors before the chunk is reported as `FLB_RETRY`.

A chunk larger than `Max_Chunk_Size` is sent as several `AppendRows` requests.
When a transient failure occurs the plugin stops sending the remaining
requests, because Fluent Bit redelivers the **whole** chunk; the requests that
were already accepted are therefore written twice. This is the usual
at-least-once trade-off and also applies to committed streams (the retried
chunk is appended at the next offset). Permanent failures do not stop the
remaining requests.

Records that cannot be converted to the table schema on the client side are
logged and skipped individually; retrying would not help, so they never
influence the return code.

## Shutdown

On `FLBPluginExitCtx` the plugin, per output instance:

1. waits up to `Flush_Timeout_Sec` for the pending responses of
   already-acknowledged appends (default stream),
2. closes every stream,
3. closes the client,
4. cancels the instance context.

While the backend is unreachable the client library's close paths wait on the
same connection lock that the blocked reconnect loop holds, so step 2 would
never return. To keep shutdown bounded the plugin cancels the instance context
*before* step 2 when an `AppendRows` call is known to be blocked (see above),
and otherwise whenever a step exceeds `Flush_Timeout_Sec`; a cancelled step is
then given another `Flush_Timeout_Sec` before it is abandoned. Acknowledged
appends are drained in step 1 and blocked chunks were already reported as
`FLB_RETRY`, so cancelling loses nothing on the plugin side: whether the
retried chunks survive the restart depends on Fluent Bit's buffer
(`storage.type filesystem`). Responses still outstanding for acknowledged
appends after step 1 cannot be waited for and are lost, as they would be with
any restart during an outage. A step that had to be cancelled because it
exceeded the deadline is logged and reported as `FLB_ERROR`. Verified by the
`shutdown during outage` scenario of the
[end-to-end harness](#end-to-end-harness).

Plan Fluent Bit's `Grace` and the process manager's stop timeout for roughly
`Grace + 4 × Flush_Timeout_Sec` per output in the worst case (one shared
deadline plus one grace period for each of the three steps); a healthy
shutdown takes well under a second per output.

## Backpressure and buffering

When the in-flight queue of a default stream fills up (see
`Max_Queue_Requests`) the plugin opens additional streams. In practice this only
happens for default streams; with `Exactly_Once` every append is acknowledged
synchronously, so the queue never grows beyond a single flush.

Backpressure towards inputs is handled by Fluent Bit's own buffering
(`Mem_Buf_Limit`, `storage.type filesystem`); see
[Buffering & Storage](https://docs.fluentbit.io/manual/administration/buffering-and-storage).
A flush whose `AppendRows` call stays blocked for `Flush_Timeout_Sec` (all
streams saturated, or the backend unreachable) is reported as `FLB_RETRY` and
redelivered later rather than blocking the output worker; see
[Delivery semantics](#delivery-semantics).

### Connection multiplexing

By default every default stream owns its own gRPC connection. With
[routing by tag](#routing-by-tag) one output may hold `tables × up to 10`
streams, and each connection counts against the Storage Write API
concurrent-connection quota of the project/region.

`Enable_Multiplexing true` passes `managedwriter.WithMultiplexing()` when the
client is created, so default streams of the output share a per-region
connection pool whose size is capped by `Multiplex_Pool_Limit`
(`WithMultiplexPoolLimit`). The pool grows up to the limit only when there is
enough traffic on the shared connection(s).

```
[OUTPUT]
    Name                 writeapi
    Match                app.*
    ProjectID            my-project
    DatasetId            logs
    TableId              $TAG[1]
    Enable_Multiplexing  true
    Multiplex_Pool_Limit 2
```

Notes:

- Google marks these client options as **EXPERIMENTAL and subject to change**;
  the plugin keeps them opt-in for that reason.
- Only default streams can be multiplexed. With `Exactly_Once true` (committed
  streams) the option has no effect and a warning is logged at start-up.
- `Max_Queue_Requests` / `Max_Queue_Bytes` still apply per stream.

## Releases and versioning

`.github/workflows/release.yml` builds `linux/amd64` and `linux/arm64`
artifacts with `Dockerfile.build` and publishes them:

- every push to `main` creates a **pre-release** tagged
  `vYYYYMMDD-HHMMSS-<short-sha>`;
- every `v*` tag pushed to the repository creates a **release** with that tag.

Tags are date-based; there is no SemVer guarantee between releases.

## Development

```sh
go vet ./...
go build -buildmode=c-shared -o out_writeapi.so
go test -race -skip "^Test(Pipeline|ExactlyOnce|ErrorHandlingDefault|ErrorHandlingExactlyOnce|DefaultValue)$" ./...
```

The skipped tests are integration tests that require a real BigQuery table.
See [CONTRIBUTING.md](CONTRIBUTING.md).

### End-to-end harness

`hack/e2e` runs the compiled plugin inside a real Fluent Bit container against
the [goccy/bigquery-emulator](https://github.com/goccy/bigquery-emulator) and
asserts on the rows that reach each table. It needs Docker (with Compose v2)
and nothing else, no GCP project or credentials:

```sh
make e2e          # builds hack/e2e/build/out_writeapi.so with Dockerfile.build, then runs the scenarios
make e2e-down     # tears down a stack kept with E2E_KEEP=1
```

Scenarios (`hack/e2e/e2e_test.go`): static table, `$TAG[n]` routing to several
tables, unknown table with `Unknown_Table_Action drop`, unknown table with
`Unknown_Table_Action retry` (the table is created while Fluent Bit retries the
chunk), a backend outage simulated with a
[toxiproxy](https://github.com/Shopify/toxiproxy) in front of the emulator
(`FLB_RETRY` after `Flush_Timeout_Sec`, redelivery once the backend is back),
stopping Fluent Bit during that outage (`docker compose stop` exits cleanly
within `Grace` plus a few seconds instead of being killed), and starting Fluent
Bit during the outage (initialisation fails after `Init_Timeout_Sec`).
Set `E2E_KEEP=1` to leave the stack running after a failure for inspection
(`docker compose -f hack/e2e/docker-compose.yml logs fluent-bit`).

The harness uses the `Endpoint` / `Insecure` parameters and enables
`Enable_Multiplexing` on every output: emulator releases up to 0.8.1 pick an
arbitrary stream when a request omits `write_stream`, which the managed writer
does on non-multiplexed connections (fixed upstream in
goccy/bigquery-emulator#491, not released yet). The emulator also decodes
every row with the first schema seen on a connection, so all e2e tables share
one schema.

## Attribution

This repository began as a fork of
[`GoogleCloudPlatform/fluentbit-bigquery-writeapi-sink`](https://github.com/GoogleCloudPlatform/fluentbit-bigquery-writeapi-sink),
Copyright Google LLC, licensed under the Apache License 2.0. The original
license headers are retained in the source files. Substantial parts have since
been rewritten (JSON-to-protobuf conversion, concurrency and context lifecycle
handling), and this project is not affiliated with or endorsed by Google.

## License

[Apache License 2.0](LICENSE)
