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
| `Unknown_Table_Action` | string | `drop` | What to do with a chunk whose table cannot be resolved or initialised: `drop` logs and acknowledges the chunk (`FLB_OK`); `retry` returns `FLB_RETRY` so Fluent Bit redelivers it. |
| `Exactly_Once` | bool | `false` | `true` uses a committed stream with synchronous response checking (exactly-once). `false` uses the default stream with asynchronous response checking (at-least-once). |
| `Num_Synchronous_Retries` | int | `4` | Maximum synchronous retries per append when `Exactly_Once` is `true`. Ignored otherwise. |
| `Max_Chunk_Size` | int (bytes) | `9437184` (9 MiB) | Upper bound on the serialized size of a single `AppendRows` request. Values above 9 MiB are clamped to 9 MiB. |
| `Max_Queue_Requests` | int | `1000` | Maximum number of in-flight (unacknowledged) `AppendRows` requests per stream. When the queue reaches 80 % of this value a new default stream is created (up to 10 streams per output instance). |
| `Max_Queue_Bytes` | int (bytes) | `104857600` (100 MiB) | Maximum number of in-flight bytes per stream. |
| `DateTime_String_Type` | bool | `true` | `true` accepts `DATETIME` values as strings. `false` accepts civil-time encoded `int64`. Applies to nested `DATETIME` fields as well; `RANGE<DATETIME>` always expects `int64`. |
| `Flush_Timeout_Sec` | int (seconds) | `10` | Deadline for a single flush call (and for draining pending responses on exit). |
| `Retry_On_Failure` | bool | `true` | `true` returns `FLB_RETRY` / `FLB_ERROR` to Fluent Bit when an append fails (see [Delivery semantics](#delivery-semantics)). `false` restores the historical behaviour of logging the failure and acknowledging the chunk. |
| `Enable_Multiplexing` | bool | `false` | **EXPERIMENTAL.** Share gRPC connections between default streams of this output (see [Connection multiplexing](#connection-multiplexing)). |
| `Multiplex_Pool_Limit` | int | `1` | **EXPERIMENTAL.** Maximum shared connections per BigQuery region when `Enable_Multiplexing` is `true`; ignored otherwise. Must be `>= 1`. |

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
  flush.
- `Max_Queue_Requests`, `Max_Queue_Bytes` and dynamic stream scaling (up to 10
  streams) apply **per table**, as they did per output before. Fluent Bit's
  own buffer limits (`storage.total_limit_size`, `Mem_Buf_Limit`) remain per
  output, so keep very high-volume tables on dedicated outputs.
- Shutdown drains and closes every table's streams.

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

## Backpressure and buffering

When the in-flight queue of a default stream fills up (see
`Max_Queue_Requests`) the plugin opens additional streams. In practice this only
happens for default streams; with `Exactly_Once` every append is acknowledged
synchronously, so the queue never grows beyond a single flush.

Backpressure towards inputs is handled by Fluent Bit's own buffering
(`Mem_Buf_Limit`, `storage.type filesystem`); see
[Buffering & Storage](https://docs.fluentbit.io/manual/administration/buffering-and-storage).

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

## Attribution

This repository began as a fork of
[`GoogleCloudPlatform/fluentbit-bigquery-writeapi-sink`](https://github.com/GoogleCloudPlatform/fluentbit-bigquery-writeapi-sink),
Copyright Google LLC, licensed under the Apache License 2.0. The original
license headers are retained in the source files. Substantial parts have since
been rewritten (JSON-to-protobuf conversion, concurrency and context lifecycle
handling), and this project is not affiliated with or endorsed by Google.

## License

[Apache License 2.0](LICENSE)
