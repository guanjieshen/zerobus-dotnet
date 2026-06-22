# Databricks Zerobus .NET SDK

A pure-managed .NET client library for [Databricks Zerobus Ingest](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest) — stream records directly into Unity Catalog managed Delta tables over gRPC, with no message bus in between.

- **Pure managed** — built on `Grpc.Net.Client` + `Google.Protobuf`. A single AnyCPU assembly, no native binaries. Drop it into any .NET Core 3.1+/.NET 5+ app (targets `net8.0` and `netstandard2.1`).
- **JSON or Protobuf** records over a persistent bidirectional stream.
- **Durability acknowledgments** with per-offset waits, flush, and a fire-and-forget callback.
- **Automatic reconnect** with exponential backoff and at-least-once replay of unacknowledged records.
- **Async-only**, idiomatic `Task`/`CancellationToken`/`IAsyncDisposable` API.

> Status: built from the public Zerobus wire protocol (`databricks/zerobus-sdk`). Verify end-to-end against your workspace before production use.

## Install

```bash
dotnet add package Databricks.Zerobus.Sdk
```

## Quick start (JSON)

```csharp
using Databricks.Zerobus;

await using var sdk = new ZerobusSdk(
    serverEndpoint: "1234567890.zerobus.us-west-2.cloud.databricks.com",
    workspaceUrl:   "https://dbc-….cloud.databricks.com");

var stream = await sdk.CreateStreamAsync(
    new TableProperties("main.sales.events"),
    clientId, clientSecret);          // service principal OAuth credentials

for (var i = 0; i < 100; i++)
{
    long offset = await stream.IngestRecordAsync($"{{\"id\":{i}}}");
    // optionally: await stream.WaitForOffsetAsync(offset);
}

await stream.FlushAsync();            // wait until everything is durable
await stream.CloseAsync();
```

You can also pass a POCO and let the SDK serialize it with `System.Text.Json`:

```csharp
await stream.IngestRecordAsync(new { device_name = "sensor-1", temp = 22, humidity = 55 });
```

## Quick start (Protobuf)

Add the record `.proto` to your project (generate it from the table schema so the fields match exactly) and compile it with `Grpc.Tools`:

```xml
<ItemGroup>
  <Protobuf Include="Protos/air_quality.proto" GrpcServices="None" />
  <PackageReference Include="Grpc.Tools" PrivateAssets="All" />
  <PackageReference Include="Google.Protobuf" />
</ItemGroup>
```

```csharp
var stream = await sdk.CreateStreamAsync(
    new TableProperties<AirQuality>("main.sales.air_quality"),
    clientId, clientSecret);

long offset = await stream.IngestRecordAsync(
    new AirQuality { DeviceName = "sensor-1", Temp = 22, Humidity = 55 });
await stream.WaitForOffsetAsync(offset);
await stream.CloseAsync();
```

The wire descriptor is derived automatically from the generated message type.

## Durability model

The server acknowledges records cumulatively. The SDK assigns each record a monotonically increasing offset and exposes three ways to track durability:

| Pattern | API | Use when |
|---|---|---|
| Per-record | `await stream.WaitForOffsetAsync(offset)` | You need confirmation for a specific record. |
| Drain | `await stream.FlushAsync()` | You want everything ingested so far to be durable (e.g. before close). |
| Fire-and-forget | `StreamConfigurationOptions.AckCallback` | High throughput; ingest without awaiting and react to `OnAck(offset)`. |

Delivery is **at-least-once**: after a reconnect the SDK replays unacknowledged records, so design downstream consumers to tolerate duplicates.

## Configuration

```csharp
var options = new StreamConfigurationOptions
{
    RecordType        = RecordType.Json,        // or Proto
    MaxInflightRecords = 10_000,                // backpressure bound
    FlushTimeout       = TimeSpan.FromSeconds(30),
    AckCallback        = myCallback,            // optional IAckCallback
    Recovery           = new BackoffPolicy { InitialDelay = TimeSpan.FromSeconds(1), MaxDelay = TimeSpan.FromSeconds(30), MaxAttempts = 10 },
};
var stream = await sdk.CreateStreamAsync(tableProperties, clientId, clientSecret, options);
```

Custom authentication: implement `ITokenProvider` and pass it instead of the client id/secret.

## Authentication & grants

The SDK uses OAuth 2.0 client credentials (M2M). The service principal needs explicit grants on the target table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<sp-id>`;
GRANT USE SCHEMA  ON SCHEMA main.sales TO `<sp-id>`;
GRANT MODIFY, SELECT ON TABLE main.sales.events TO `<sp-id>`;
```

Schema-level inherited grants may be insufficient — grant `MODIFY` and `SELECT` directly on the table.

## Limits

100 MB/s and 15,000 rows/s per stream; 10 MB per message; 2,000 columns per table. Open multiple streams to scale beyond a single stream's limits.

## Building from source

```bash
dotnet build -c Release
dotnet test
```

Tests run against an in-memory gRPC server (no Databricks credentials required). The `examples/` projects are environment-variable gated for live runs.

## License

Apache 2.0 — see [LICENSE](LICENSE).
