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
- High-level bulk writers (`ZerobusBulkWriter<T>`, `ZerobusBulkWriter`) that accept single records
  or lists, auto-batch them (bounded by row count and byte size), and fan out across a configurable
  number of parallel connections (`BulkWriterOptions.Parallelism`).
- Interface-based public API (`IZerobusSdk`, `IZerobusStream<T>`, `IZerobusJsonStream`,
  `IZerobusBulkWriter<T>`, `IZerobusJsonBulkWriter`) for dependency injection and testing.
- `netstandard2.1` and `net8.0` targets.

### Notes
- The `descriptor_proto` sent on stream creation is the message-level `DescriptorProto`
  (validated against a live Zerobus server).
- Target tables must not have CHECK constraints / the `checkConstraints` table feature.
- proto3 scalar defaults are omitted on the wire; declare required-but-zero-valued fields `optional`.
