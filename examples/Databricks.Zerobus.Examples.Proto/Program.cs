using Databricks.Zerobus;
using Zerobus.Example.Proto;

// Protobuf ingestion example. Requires the same environment variables as the JSON example,
// and a target table whose schema matches the AirQuality message in Protos/air_quality.proto.

static string Required(string name) =>
    Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException($"Missing environment variable {name}.");

var serverEndpoint = Required("ZEROBUS_SERVER_ENDPOINT");
var workspaceUrl = Required("DATABRICKS_WORKSPACE_URL");
var tableName = Required("ZEROBUS_TABLE_NAME");
var clientId = Required("DATABRICKS_CLIENT_ID");
var clientSecret = Required("DATABRICKS_CLIENT_SECRET");

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

var stream = await sdk.CreateStreamAsync(
    new TableProperties<AirQuality>(tableName), clientId, clientSecret);

Console.WriteLine($"Stream created: {stream.StreamId}");

for (var i = 0; i < 100; i++)
{
    var record = new AirQuality { DeviceName = $"sensor-{i}", Temp = 20 + i % 10, Humidity = 50 + i % 25 };
    var offset = await stream.IngestRecordAsync(record);
    await stream.WaitForOffsetAsync(offset);
}

await stream.FlushAsync();
Console.WriteLine($"Flushed. Highest acknowledged offset: {stream.LastAcknowledgedOffset}");

await stream.CloseAsync();
Console.WriteLine("Done.");
