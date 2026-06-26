using System.Text.Json;
using Databricks.Solutions.Zerobus;

// JSON ingestion example. Set these environment variables before running:
//   ZEROBUS_SERVER_ENDPOINT  e.g. 1234567890.zerobus.us-west-2.cloud.databricks.com
//   DATABRICKS_WORKSPACE_URL e.g. https://dbc-….cloud.databricks.com
//   ZEROBUS_TABLE_NAME       e.g. main.sales.events   (a managed Delta table)
//   DATABRICKS_CLIENT_ID     service principal OAuth client id
//   DATABRICKS_CLIENT_SECRET service principal OAuth client secret

static string Required(string name) =>
    Environment.GetEnvironmentVariable(name) ?? throw new InvalidOperationException($"Missing environment variable {name}.");

var serverEndpoint = Required("ZEROBUS_SERVER_ENDPOINT");
var workspaceUrl = Required("DATABRICKS_WORKSPACE_URL");
var tableName = Required("ZEROBUS_TABLE_NAME");
var clientId = Required("DATABRICKS_CLIENT_ID");
var clientSecret = Required("DATABRICKS_CLIENT_SECRET");

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

var stream = await sdk.CreateStreamAsync(
    new TableProperties(tableName), clientId, clientSecret);

Console.WriteLine($"Stream created: {stream.StreamId}");

for (var i = 0; i < 100; i++)
{
    var record = JsonSerializer.Serialize(new { device_name = $"sensor-{i}", temp = 20 + i % 10, humidity = 50 + i % 25 });
    await stream.IngestRecordAsync(record);
}

await stream.FlushAsync();
Console.WriteLine($"Flushed. Highest acknowledged offset: {stream.LastAcknowledgedOffset}");

await stream.CloseAsync();
Console.WriteLine("Done.");
