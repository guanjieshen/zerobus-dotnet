# Databricks Zerobus .NET SDK

A pure-managed .NET client library for [Databricks Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest) — stream records directly into Unity Catalog managed Delta tables over gRPC, with no message bus in between.

- **Pure managed** — built on `Grpc.Net.Client` + `Google.Protobuf`. A single AnyCPU assembly, no native binaries. Targets `net8.0` and `netstandard2.1`, so it drops into any .NET Core 3.1+/.NET 5+ app.
- **JSON or Protobuf** records over a persistent bidirectional stream.
- **Durability acknowledgments** with per-offset waits, flush, and a fire-and-forget callback.
- **Automatic reconnect** with exponential backoff and at-least-once replay of unacknowledged records.
- **High-throughput bulk writer** — pass single records or whole lists; it auto-batches and fans out across a configurable number of parallel connections.
- **Async-only, interface-based API** — depend on `IZerobusSdk` and friends for clean DI and testing.

## Install

```bash
dotnet add package Databricks.Zerobus.Sdk
```

## Concepts

| Type | Role |
|------|------|
| `IZerobusSdk` / `ZerobusSdk` | Entry point. Construct once per workspace endpoint; create streams and writers from it. |
| `IZerobusStream` (JSON) / `IZerobusStream<T>` (Protobuf) | A single ingest stream — lowest-level, full control over offsets and flushing. |
| `IZerobusBulkWriter<T>` / `IZerobusJsonBulkWriter` | High-level writer that auto-batches and parallelizes. **Start here for throughput.** |
| `TableProperties` / `TableProperties<T>` | The target table (and, for Protobuf, the record type). |
| `BulkWriterOptions` / `StreamConfigurationOptions` | Tuning: parallelism, batch size, backpressure, reconnect. |
| `ITokenProvider` | Auth abstraction. `OAuthTokenProvider` (M2M client-credentials) is the default. |

## Quick start — bulk writer (recommended)

The bulk writer is the easiest way to get high throughput. You give it single records or lists; it batches them and spreads them across parallel connections.

### Protobuf

Add your record `.proto` to your project (generate it from the table so fields match exactly) and compile it with `Grpc.Tools`:

```xml
<ItemGroup>
  <Protobuf Include="Protos/sensor_reading.proto" GrpcServices="None" />
  <PackageReference Include="Grpc.Tools" PrivateAssets="All" />
  <PackageReference Include="Google.Protobuf" />
</ItemGroup>
```

```csharp
using Databricks.Zerobus;

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

var options = new BulkWriterOptions
{
    Parallelism   = 4,                // independent gRPC connections (default)
    BatchSize     = 10_000,           // max rows per batch    (default)
    MaxBatchBytes = 8 * 1024 * 1024,  // flush early before 10 MB (default)
};

await using IZerobusBulkWriter<SensorReading> writer =
    await sdk.CreateBulkWriterAsync(
        new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
        clientId, clientSecret, options);

// one record
await writer.WriteAsync(new SensorReading { DeviceId = "sensor-1", TempC = 22.5 });

// or a whole list — auto-batched and fanned out across the connections
await writer.WriteAsync(myReadings);     // IEnumerable<SensorReading>

await writer.FlushAsync();               // wait until everything is durable
// disposing (await using) also flushes + closes all connections
```

### JSON

```csharp
await using IZerobusJsonBulkWriter writer =
    await sdk.CreateBulkWriterAsync(
        new TableProperties("main.telemetry.events"),
        clientId, clientSecret,
        new BulkWriterOptions { Parallelism = 4, BatchSize = 10_000 });

await writer.WriteAsync("{\"device_id\":\"sensor-1\",\"temp_c\":22.5}");  // raw JSON string
await writer.WriteAsync(new { device_id = "sensor-2", temp_c = 23.0 });     // POCO -> JSON
await writer.WriteAsync(manyJsonStrings);                                   // IEnumerable<string>
await writer.FlushAsync();
```

## Lower-level: a single stream

When you want explicit control over offsets and acknowledgments:

```csharp
await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

IZerobusJsonStream stream = await sdk.CreateStreamAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

long offset = await stream.IngestRecordAsync("{\"device_id\":\"sensor-1\"}");
await stream.WaitForOffsetAsync(offset);   // this record is now durable
await stream.FlushAsync();
await stream.CloseAsync();
```

Protobuf is identical with `CreateStreamAsync<T>(new TableProperties<T>(...), …)` and `IngestRecordAsync(T)`.

## Integrating in a C# application (DI)

Register the SDK as a singleton (the gRPC channel is expensive to create and meant to be reused) and depend on `IZerobusSdk`:

```csharp
// Program.cs
builder.Services.AddSingleton<IZerobusSdk>(_ =>
    new ZerobusSdk(
        builder.Configuration["Zerobus:ServerEndpoint"]!,
        builder.Configuration["Zerobus:WorkspaceUrl"]!));
```

```csharp
public sealed class TelemetryIngestor
{
    private readonly IZerobusSdk _sdk;
    public TelemetryIngestor(IZerobusSdk sdk) => _sdk = sdk;

    public async Task IngestAsync(IEnumerable<SensorReading> readings, CancellationToken ct)
    {
        await using var writer = await _sdk.CreateBulkWriterAsync(
            new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
            clientId, clientSecret, cancellationToken: ct);
        await writer.WriteAsync(readings, ct);
        await writer.FlushAsync(ct);
    }
}
```

Because the public surface is interfaces (`IZerobusSdk`, `IZerobusBulkWriter<T>`, …), you can mock them in unit tests without touching gRPC.

> For a long-lived producer, keep one bulk writer (or stream) open and reuse it across calls rather than opening one per request — opening a stream pays the auth + handshake cost each time.

## Durability model

The server acknowledges records cumulatively. The SDK assigns each record/batch a monotonically increasing offset and gives you three ways to track durability:

| Pattern | API | Use when |
|---|---|---|
| Per-record | `await stream.WaitForOffsetAsync(offset)` | You need confirmation for a specific record. |
| Drain | `await writer.FlushAsync()` / `stream.FlushAsync()` | You want everything so far to be durable (e.g. before close). |
| Fire-and-forget | `StreamConfigurationOptions.AckCallback` | High throughput; ingest without awaiting and react to `OnAck(offset)`. |

Delivery is **at-least-once**: after a reconnect the SDK replays unacknowledged records, so design downstream consumers to tolerate duplicates.

## Authentication & grants

The SDK uses OAuth 2.0 client credentials (M2M). The service principal needs explicit grants on the target table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<sp-client-id>`;
GRANT USE SCHEMA  ON SCHEMA main.telemetry TO `<sp-client-id>`;
GRANT MODIFY, SELECT ON TABLE main.telemetry.sensor_readings TO `<sp-client-id>`;
```

Schema-level inherited grants may be insufficient — grant `MODIFY` and `SELECT` directly on the table. For custom auth, implement `ITokenProvider` and pass it instead of the client id/secret.

## Target table requirements

Zerobus does **not** create or alter tables. Pre-create a **managed Delta table** whose schema matches your records, and note:

- **No CHECK constraints.** Zerobus refuses ingestion into tables that have CHECK constraints or the `checkConstraints` table feature. Enforce value rules in the producer instead.
- **proto3 field presence.** In proto3, a scalar equal to its default (`0`, `0.0`, `""`) is *not* serialized, and the server treats the field as absent — so a `NOT NULL` column will reject it. For required fields that can legitimately be a default value, declare them `optional` in the `.proto` (and always set them).

## Limits & throughput

- **10 MB** per message, **2,000** columns per table. The bulk writer's `MaxBatchBytes` keeps batches safely under the message limit automatically.
- Per-stream throughput guidance is on the order of tens of thousands of rows/s; **scale by increasing `Parallelism`** (each stream is its own connection). Throughput grows roughly linearly until your client uplink or account quotas bound it. Note each open stream counts against your concurrency quota.

## Configuration reference

`BulkWriterOptions`: `Parallelism` (default 4), `BatchSize` (10,000), `MaxBatchBytes` (8 MB), `StreamOptions`.

`StreamConfigurationOptions`: `RecordType`, `AckCallback`, `MaxInflightRecords` (10,000), `FlushTimeout` (30 s), `Recovery` (`BackoffPolicy`).

`BackoffPolicy`: `InitialDelay` (1 s), `Multiplier` (2.0), `MaxDelay` (30 s), `MaxAttempts` (10).

## Building from source

```bash
dotnet build -c Release
dotnet test
```

Tests run against an in-memory gRPC server (no Databricks credentials required). The `examples/` projects are environment-variable gated for live runs.

## License

Apache 2.0 — see [LICENSE](LICENSE).
