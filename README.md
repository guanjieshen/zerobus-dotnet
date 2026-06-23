# Databricks Zerobus .NET SDK

Stream records straight into a Unity Catalog Delta table over gRPC — no Kafka, no Event Hub, no message bus in between.

This is a pure-managed .NET library (`Grpc.Net.Client` + `Google.Protobuf`, no native bits). It targets `net8.0` and `netstandard2.1`, so it works in any .NET Core 3.1+/.NET 5+ app.

```bash
dotnet add package Databricks.Zerobus.Sdk
```

## Get started

The bulk writer is the easiest way in: hand it records — one at a time or a whole list — and it batches them and sends them over several connections in parallel.

```csharp
using Databricks.Zerobus;

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret);

await writer.WriteAsync(new SensorReading { DeviceId = "sensor-1", TempC = 22.5 }); // one
await writer.WriteAsync(myReadings);                                                // or a list
await writer.FlushAsync();                                                          // now it's durable
```

That's the whole happy path. Disposing the writer flushes and closes everything, so an `await using` is usually all the cleanup you need.

`SensorReading` is a generated protobuf type. Generate the `.proto` from your table so the fields line up, and compile it with `Grpc.Tools`:

```xml
<Protobuf Include="Protos/sensor_reading.proto" GrpcServices="None" />
<PackageReference Include="Grpc.Tools" PrivateAssets="All" />
<PackageReference Include="Google.Protobuf" />
```

Prefer JSON? Same shape, no `.proto` needed:

```csharp
await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

await writer.WriteAsync("{\"device_id\":\"sensor-1\",\"temp_c\":22.5}"); // raw JSON
await writer.WriteAsync(new { device_id = "sensor-2", temp_c = 23.0 });   // or a POCO
await writer.FlushAsync();
```

## Tuning throughput

Throughput scales with the number of parallel connections. The defaults are a good starting point; raise `Parallelism` if you have the uplink and quota for it.

```csharp
var options = new BulkWriterOptions
{
    Parallelism   = 4,                // parallel connections        (default 4)
    BatchSize     = 10_000,           // max rows per batch          (default 10,000)
    MaxBatchBytes = 8 * 1024 * 1024,  // flush early to stay <10 MB  (default 8 MB)
};
```

Each connection is its own stream and counts against your account's concurrency quota, so don't go wider than you need.

## Using it in an app

Register the SDK once as a singleton — the gRPC channel is meant to be reused — and depend on the `IZerobusSdk` interface so your code stays testable:

```csharp
builder.Services.AddSingleton<IZerobusSdk>(_ =>
    new ZerobusSdk(config["Zerobus:ServerEndpoint"]!, config["Zerobus:WorkspaceUrl"]!));
```

```csharp
public sealed class TelemetryIngestor(IZerobusSdk sdk)
{
    public async Task IngestAsync(IEnumerable<SensorReading> readings, CancellationToken ct)
    {
        await using var writer = await sdk.CreateBulkWriterAsync(
            new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
            clientId, clientSecret, cancellationToken: ct);
        await writer.WriteAsync(readings, ct);
        await writer.FlushAsync(ct);
    }
}
```

For a long-running producer, keep one writer open and reuse it rather than creating one per request — each new stream pays the auth and handshake cost.

## Need finer control?

Drop down to a single stream when you want to manage offsets yourself:

```csharp
var stream = await sdk.CreateStreamAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

long offset = await stream.IngestRecordAsync("{\"device_id\":\"sensor-1\"}");
await stream.WaitForOffsetAsync(offset);   // this record is durable
await stream.CloseAsync();
```

Delivery is **at-least-once** — after a reconnect the SDK replays anything that wasn't acknowledged, so make your downstream tolerant of duplicates. A record (or batch) is durable once the call that waits on it — `WaitForOffsetAsync` or `FlushAsync` — returns. For fire-and-forget, set an `AckCallback` and don't await each write.

## Before your first write

Zerobus never creates or changes tables — you pre-create a **managed Delta table** whose columns match your records. Two things trip people up:

- **No CHECK constraints.** Zerobus rejects tables that have CHECK constraints or the `checkConstraints` feature. Validate values in your producer instead.
- **proto3 drops default values.** A scalar equal to `0`, `0.0`, or `""` isn't sent on the wire, and the server reads it as missing — so a `NOT NULL` column rejects it. If a required field can legitimately be zero/empty, mark it `optional` in the `.proto` and always set it.

The service principal needs explicit grants on the table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<sp-client-id>`;
GRANT USE SCHEMA  ON SCHEMA main.telemetry TO `<sp-client-id>`;
GRANT MODIFY, SELECT ON TABLE main.telemetry.sensor_readings TO `<sp-client-id>`;
```

(Custom auth? Implement `ITokenProvider` and pass it instead of the id/secret.)

## Good to know

- Limits: 10 MB per message, 2,000 columns per table. The bulk writer's byte cap keeps batches under the message limit for you.
- The public API is all interfaces (`IZerobusSdk`, `IZerobusBulkWriter<T>`, `IZerobusStream<T>`, …), so it mocks cleanly in tests.

## Building from source

```bash
dotnet build -c Release
dotnet test          # runs against an in-memory gRPC server — no credentials needed
```

The `examples/` projects show JSON, Protobuf, and Azure Functions usage; they read connection settings from environment variables for live runs.

## License

Apache 2.0 — see [LICENSE](LICENSE).
