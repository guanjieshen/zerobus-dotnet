# Databricks Zerobus .NET SDK

Stream records straight into a Unity Catalog Delta table over gRPC. No Kafka, no Event Hub, no message bus in between.

It is a pure-managed .NET library (built on `Grpc.Net.Client` and `Google.Protobuf`, with no native dependencies). It targets `net8.0` and `netstandard2.1`, so it runs in any .NET Core 3.1+ or .NET 5+ app.

```bash
dotnet add package Databricks.Zerobus.Sdk
```

## A complete example

Say you want to land sensor readings into `main.telemetry.sensor_readings`. Here is the whole thing, end to end.

**1. Create the table** (Zerobus writes to existing managed Delta tables; it never creates them):

```sql
CREATE TABLE main.telemetry.sensor_readings (
    device_id  STRING,
    temp_c     DOUBLE,
    humidity   INT,
    reading_ts TIMESTAMP
);
```

**2. Describe the row as protobuf** in `Protos/sensor_reading.proto`. The field names and types match the table:

```protobuf
syntax = "proto3";
option csharp_namespace = "MyApp.Telemetry";

message SensorReading {
    string device_id  = 1;
    double temp_c     = 2;
    int32  humidity   = 3;
    int64  reading_ts = 4;   // epoch microseconds
}
```

Compile it by adding these to your `.csproj`:

```xml
<ItemGroup>
  <Protobuf Include="Protos/sensor_reading.proto" GrpcServices="None" />
  <PackageReference Include="Grpc.Tools" PrivateAssets="All" />
  <PackageReference Include="Google.Protobuf" />
</ItemGroup>
```

**3. Write the records.** Hand the bulk writer one record or a whole list; it batches them and sends them over several connections at once:

```csharp
using Databricks.Zerobus;
using MyApp.Telemetry;

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret);

var readings = Enumerable.Range(0, 10_000).Select(i => new SensorReading
{
    DeviceId  = $"sensor-{i % 50}",
    TempC     = 20 + i % 10,
    Humidity  = 40 + i % 30,
    ReadingTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000,
});

await writer.WriteAsync(readings);   // batched and parallelized for you
await writer.FlushAsync();           // returns once every record is durable
```

`TableProperties<SensorReading>` is simply the target table name paired with your record type. (For JSON, use the non-generic `new TableProperties("catalog.schema.table")`.)

Disposing the writer (the `await using`) flushes and closes everything, so that is usually all the cleanup you need.

The four connection values come from your workspace:

```csharp
var serverEndpoint = "1234567890.zerobus.us-west-2.cloud.databricks.com"; // gRPC endpoint
var workspaceUrl   = "https://dbc-xxxx.cloud.databricks.com";             // for OAuth
var clientId       = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID");     // service principal
var clientSecret   = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET");
```

## JSON instead of protobuf

If you would rather not define a `.proto`, write JSON. Everything else is the same:

```csharp
await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

await writer.WriteAsync("{\"device_id\":\"sensor-1\",\"temp_c\":22.5}"); // raw JSON
await writer.WriteAsync(new { device_id = "sensor-2", temp_c = 23.0 });   // or a POCO, serialized for you
await writer.FlushAsync();
```

## Tuning throughput

All the calls so far used the defaults. To tune the writer, build a `BulkWriterOptions` and pass it as the last argument to `CreateBulkWriterAsync`:

```csharp
var options = new BulkWriterOptions
{
    Parallelism   = 8,                // parallel connections        (default 4)
    BatchSize     = 20_000,           // max rows per batch          (default 10,000)
    MaxBatchBytes = 8 * 1024 * 1024,  // flush early to stay <10 MB  (default 8 MB)
};

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId,
    clientSecret,
    options);          // <- options go here; omit the argument to use the defaults
```

The full signature is:

```csharp
Task<IZerobusBulkWriter<T>> CreateBulkWriterAsync<T>(
    TableProperties<T> table,
    string clientId,
    string clientSecret,
    BulkWriterOptions? options = null,        // optional
    CancellationToken cancellationToken = default);
```

Throughput scales with `Parallelism`. Each connection is a separate stream that counts against your account's concurrency quota, so use what you need and no more. (The JSON `CreateBulkWriterAsync(TableProperties, ...)` overload takes the same `BulkWriterOptions` in the same position.)

## Using it in an app

Register the SDK once as a singleton, since the gRPC channel is built to be reused, and depend on the `IZerobusSdk` interface so your code stays easy to test:

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

For a long-running producer, keep one writer open and reuse it instead of creating one per request. Each new stream pays the auth and handshake cost up front.

## When you want finer control

Drop down to a single stream to manage offsets yourself:

```csharp
var stream = await sdk.CreateStreamAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

long offset = await stream.IngestRecordAsync("{\"device_id\":\"sensor-1\"}");
await stream.WaitForOffsetAsync(offset);   // this record is now durable
await stream.CloseAsync();
```

Delivery is at-least-once. After a reconnect the SDK replays anything that was not acknowledged, so make your downstream tolerant of duplicates. A record or batch is durable once the call that waits on it (`WaitForOffsetAsync` or `FlushAsync`) returns. For fire-and-forget, set an `AckCallback` and skip awaiting each write.

## Before your first write

Zerobus writes to a table you have already created, and two things commonly get in the way:

1. **No CHECK constraints.** Zerobus rejects tables that have CHECK constraints or the `checkConstraints` table feature. Validate values in your producer instead.
2. **proto3 drops default values.** A scalar equal to `0`, `0.0`, or `""` is not sent on the wire, and the server reads it as missing, so a `NOT NULL` column rejects it. If a required field can legitimately be zero or empty, mark it `optional` in the `.proto` and always set it.

The service principal needs explicit grants on the table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<sp-client-id>`;
GRANT USE SCHEMA  ON SCHEMA main.telemetry TO `<sp-client-id>`;
GRANT MODIFY, SELECT ON TABLE main.telemetry.sensor_readings TO `<sp-client-id>`;
```

For custom auth, implement `ITokenProvider` and pass it in place of the id and secret.

## Good to know

* Limits: 10 MB per message and 2,000 columns per table. The bulk writer's byte cap keeps batches under the message limit automatically.
* The public API is all interfaces (`IZerobusSdk`, `IZerobusBulkWriter<T>`, `IZerobusStream<T>`, and so on), so it mocks cleanly in tests.

## Building from source

```bash
dotnet build -c Release
dotnet test          # runs against an in-memory gRPC server, no credentials needed
```

The `examples/` projects show JSON, protobuf, and Azure Functions usage. They read connection settings from environment variables for live runs.

## License

Apache 2.0. See [LICENSE](LICENSE).
