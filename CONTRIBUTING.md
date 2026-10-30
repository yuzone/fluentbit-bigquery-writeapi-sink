# Contributing

Thanks for your interest in improving this plugin. Bug reports, feature
requests and pull requests are welcome.

## Reporting issues

Open an issue at
<https://github.com/yuzone/fluentbit-bigquery-writeapi-sink/issues>. Please
include the plugin version (release tag or commit), the Fluent Bit version, the
relevant `[OUTPUT]` configuration (with project/dataset/table redacted as
needed) and any log output.

Issues and pull requests are written in English. Japanese notes may be added
inside `<details>` blocks.

## Pull requests

1. Fork the repository and create a topic branch from `main`.
2. Make your change. Keep unrelated changes out of the same pull request.
3. Make sure the following pass locally (CGO is required):

   ```sh
   go vet ./...
   go build -buildmode=c-shared -o out_writeapi.so
   go test -race -skip "^Test(Pipeline|ExactlyOnce|ErrorHandlingDefault|ErrorHandlingExactlyOnce|DefaultValue)$" ./...
   ```

   The skipped tests are integration tests that need a real BigQuery table;
   see `integration_test.go` for the environment variables they read.
4. Open a pull request against `main`. All changes, including those from
   maintainers, go through pull request review.

Every push to `main` publishes a pre-release and every `v*` tag publishes a
release via `.github/workflows/release.yml`.

## License

By contributing you agree that your contributions will be licensed under the
[Apache License 2.0](LICENSE) that covers this project. Keep the existing
license headers in source files.
