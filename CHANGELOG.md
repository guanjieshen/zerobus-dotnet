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
- `GetUnacknowledgedRecords()` / `GetUnacknowledgedBatches()` on streams and bulk writers, for
  custom retry after a terminal failure (parity with the Python SDK's `get_unacked_*`).
- Per-offset `IAckCallback.OnError(long offset, Exception)` (was a single stream-level callback).
- `zerobus-generate-proto` dotnet tool (`tools/Databricks.Solutions.Zerobus.ProtoGen`) that generates a
  `.proto` from a Unity Catalog table, mirroring `python -m zerobus.tools.generate_proto`.
- `DelegatingTokenProvider` for supplying tokens from a custom source (Azure.Identity, managed
  identity, the Databricks SDK, etc.). The built-in OAuth flow already covers both Databricks-managed
  and Microsoft Entra ID service principals (an Entra SP needs a Databricks OAuth secret).
- `FederatedTokenProvider` for OAuth token federation: exchanges an external identity-provider JWT
  (e.g. a Microsoft Entra ID token) for a Zerobus-scoped Databricks token, for service principals
  that authenticate through a federation policy instead of a Databricks OAuth secret.
- `ZerobusSdk.WorkspaceIdFromServerEndpoint` helper.
- `netstandard2.1` and `net8.0` targets.

### Changed
- Default options aligned with the Python SDK: `MaxInflightRecords` 1,000,000;
  `FlushTimeout` 5 min; reconnect `InitialDelay` 2 s; reconnect `MaxAttempts` 3.

### Fixed
- `offset_id` is now numbered per ephemeral stream (0-based) and assigned at send time, then
  mapped back to the logical offset for acks. Previously the offset was a stream-lifetime counter,
  so after a reconnect the SDK replayed unacknowledged records with non-zero offsets and the server
  rejected the new stream with "Non-sequential offset_id: expected 0" (error 4002). The in-memory
  test server now enforces sequential 0-based offsets so this path is covered.

### Notes
- The `descriptor_proto` sent on stream creation is the message-level `DescriptorProto`
  (validated against a live Zerobus server).
- Target tables must not have CHECK constraints / the `checkConstraints` table feature.
- proto3 scalar defaults are omitted on the wire; declare required-but-zero-valued fields `optional`.
