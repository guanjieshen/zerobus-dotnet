# Databricks Zerobus .NET SDK

Write records straight into a Unity Catalog Delta table over gRPC. No Kafka or Event Hub in the middle.

It is a plain managed .NET library (built on `Grpc.Net.Client` and `Google.Protobuf`, no native bits), targeting `net8.0` and `netstandard2.1`, so it works in any .NET Core 3.1+ or .NET 5+ app.

```bash
dotnet add package Databricks.Zerobus.Sdk
```

## A quick example

Let's land sensor readings into `main.telemetry.sensor_readings`.

First, the table has to exist already. Zerobus writes to it, it never creates it:

```sql
CREATE TABLE main.telemetry.sensor_readings (
    device_id  STRING,
    temp_c     DOUBLE,
    humidity   INT,
    reading_ts TIMESTAMP
);
```

Next, describe a row as protobuf in `Protos/sensor_reading.proto`. The field names line up with the columns:

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

Add these to your `.csproj` so the proto gets compiled into a `SensorReading` class:

```xml
<ItemGroup>
  <Protobuf Include="Protos/sensor_reading.proto" GrpcServices="None" />
  <PackageReference Include="Grpc.Tools" PrivateAssets="All" />
  <PackageReference Include="Google.Protobuf" />
</ItemGroup>
```

Now write some records:

```csharp
using Databricks.Zerobus;
using MyApp.Telemetry;

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret);

await writer.WriteAsync(new SensorReading { DeviceId = "sensor-1", TempC = 22.5 });  // one record
await writer.WriteAsync(myReadings);   // or an IEnumerable<SensorReading>

await writer.FlushAsync();   // returns once everything is safely stored
```

That is the whole happy path. You hand the writer records, it batches them and sends them, and `FlushAsync` waits until the server has them. Disposing the writer (the `await using`) flushes and closes for you, so you usually do not need to do anything else.

`TableProperties<SensorReading>` is just the table name plus the record type. For JSON, there is a non-generic `new TableProperties("catalog.schema.table")`.

The four connection values come from your workspace:

```csharp
var serverEndpoint = "1234567890.zerobus.us-west-2.cloud.databricks.com"; // gRPC endpoint
var workspaceUrl   = "https://dbc-xxxx.cloud.databricks.com";             // used for OAuth
var clientId       = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID");     // service principal
var clientSecret   = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET");
```

## Sending a lot of data

The same writer handles bulk. You can keep calling `WriteAsync` as your data arrives, then flush once at the end. Two settings control speed: `Parallelism` (how many connections run at once) and `BatchSize` (rows per message). They go on `BulkWriterOptions`, which is the last argument to `CreateBulkWriterAsync`:

```csharp
var options = new BulkWriterOptions
{
    Parallelism = 8,        // connections running in parallel (default 4)
    BatchSize   = 10_000,   // rows per batch              (default 10,000)
};

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret, options);

foreach (var chunk in source)        // your data, in whatever chunks you have
    await writer.WriteAsync(chunk);  // IEnumerable<SensorReading>

await writer.FlushAsync();
```

More `Parallelism` means more throughput, up to your network and account limits. Each connection counts against your Zerobus concurrency quota, so pick a number you actually need. You do not have to worry about message size: the writer splits batches so they stay under the 10 MB limit on their own.

If you leave `options` off, you get the defaults (parallelism 4, batches of 10,000), which are fine for most cases.

## Prefer JSON?

Skip the proto and send JSON instead. Everything else is the same:

```csharp
await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

await writer.WriteAsync("{\"device_id\":\"sensor-1\",\"temp_c\":22.5}"); // a JSON string
await writer.WriteAsync(new { device_id = "sensor-2", temp_c = 23.0 });   // or any object
await writer.FlushAsync();
```

## Generating the proto from your table

Rather than hand-writing the `.proto`, you can generate it from the table so the fields match:

```bash
dotnet tool install --global Databricks.Zerobus.ProtoGen
zerobus-generate-proto \
  --uc-endpoint https://adb-xxxx.azuredatabricks.net \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --table main.telemetry.sensor_readings \
  --output sensor_reading.proto \
  --namespace MyApp.Telemetry
```

It marks every field `optional` so a value of `0`, `0.0`, or `""` still gets sent (more on that below).

## Using it in a service

Register the SDK once as a singleton and ask for the `IZerobusSdk` interface where you need it. The gRPC channel is meant to be reused, and depending on the interface keeps your code easy to test:

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

For a service that runs all the time, keep one writer open and reuse it instead of making a new one per request. Opening a stream costs an auth and handshake round trip.

## When you need more control

If you want to track individual records, drop down to a single stream:

```csharp
var stream = await sdk.CreateStreamAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

long offset = await stream.IngestRecordAsync("{\"device_id\":\"sensor-1\"}");
await stream.WaitForOffsetAsync(offset);   // that record is now stored
await stream.CloseAsync();
```

A record is stored once the call that waits on it (`WaitForOffsetAsync` or `FlushAsync`) comes back. Delivery is at-least-once: if a connection drops, the SDK reconnects and resends anything that was not confirmed, so expect the odd duplicate downstream. If something fails for good, `GetUnacknowledgedRecords()` hands back what never made it so you can retry it elsewhere.

## A couple of things to know first

You create the table yourself, and two things commonly get in the way:

- Zerobus will not write to a table that has CHECK constraints. Check your values in code instead.
- In proto3, a field equal to its default (`0`, `0.0`, `""`) is not sent over the wire, and the server reads that as missing, so a `NOT NULL` column rejects it. If a required field can be zero or empty, mark it `optional` in the proto and always set it. The proto generator does this for you.

The service principal needs access to the table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<sp-client-id>`;
GRANT USE SCHEMA  ON SCHEMA main.telemetry TO `<sp-client-id>`;
GRANT MODIFY, SELECT ON TABLE main.telemetry.sensor_readings TO `<sp-client-id>`;
```

If you need custom auth, implement `ITokenProvider` and pass it in place of the id and secret.

## Limits

10 MB per message and 2,000 columns per table. The bulk writer keeps batches under the message limit for you. Scale past a single stream by raising `Parallelism`.

## Building from source

```bash
dotnet build -c Release
dotnet test          # runs against an in-memory gRPC server, no credentials needed
```

The `examples/` folder has JSON, protobuf, and Azure Functions samples that read settings from environment variables.

## License

Apache 2.0. See [LICENSE](LICENSE).
