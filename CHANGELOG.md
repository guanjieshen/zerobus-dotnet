# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added
- Initial release of the pure-managed Zerobus .NET SDK.
- `ZerobusSdk` entry point with reusable gRPC channel and OAuth M2M authentication.
- JSON ingest (`ZerobusStream`) including a `System.Text.Json` POCO overload.
- Protobuf ingest (`ZerobusStream<T>`) with automatic descriptor derivation.
- Per-record (`WaitForOffsetAsync`), drain (`FlushAsync`), and callback (`IAckCallback`) durability tracking.
- Automatic reconnect with exponential backoff and at-least-once replay of unacknowledged records.
- Batch ingest (`IngestRecordBatchAsync`) for JSON and Protobuf.
- `netstandard2.1` and `net8.0` targets.
