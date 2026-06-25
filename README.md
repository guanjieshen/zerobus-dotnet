# Databricks Zerobus .NET SDK

[![NuGet](https://img.shields.io/nuget/v/Databricks.Zerobus.Sdk.svg)](https://www.nuget.org/packages/Databricks.Zerobus.Sdk)
[![Downloads](https://img.shields.io/nuget/dt/Databricks.Zerobus.Sdk.svg)](https://www.nuget.org/packages/Databricks.Zerobus.Sdk)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

## Overview

This repository provides a .NET client library for **Databricks Zerobus**, which ingests records directly into Unity Catalog managed Delta tables over gRPC. It is a managed library built on `Grpc.Net.Client` and `Google.Protobuf`, and it supports `net8.0` and `netstandard2.1`.

The SDK handles the connection, batching, acknowledgments, and reconnection for you, and exposes both a high-level bulk writer and a lower-level single stream.

## Installation

```bash
dotnet add package Databricks.Zerobus.Sdk
```

Or add the reference to your `.csproj`:

```xml
<PackageReference Include="Databricks.Zerobus.Sdk" Version="0.1.0" />
```

## Getting started

This example lands sensor readings into `main.telemetry.sensor_readings`.

### 1. Create the target table

Zerobus ingests into a table that already exists, so create it first:

```sql
CREATE TABLE main.telemetry.sensor_readings (
    device_id  STRING,
    temp_c     DOUBLE,
    humidity   INT,
    reading_ts TIMESTAMP
);
```

### 2. Define the record schema

Describe a row as protobuf in `Protos/sensor_reading.proto`, matching the table columns:

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

Install the two compile-time packages, then point the build at your `.proto`:

```bash
dotnet add package Grpc.Tools
dotnet add package Google.Protobuf
```

Install them this way rather than adding them version-less in the `.csproj`. A version-less `Google.Protobuf` resolves to an old `3.0.0` (which carries a known high-severity advisory) and the proto never compiles. `dotnet add package` pins a current version.

Then add the proto to your `.csproj` so it compiles into a `SensorReading` class:

```xml
<ItemGroup>
  <Protobuf Include="Protos/sensor_reading.proto" GrpcServices="None" />
</ItemGroup>
```

In the `Grpc.Tools` reference that `dotnet add package` wrote, add `PrivateAssets="All"` so it stays build-only and does not flow to anything that depends on your project.

### 3. Write records

```csharp
using Databricks.Zerobus;
using MyApp.Telemetry;

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret);

await writer.WriteAsync(new SensorReading { DeviceId = "sensor-1", TempC = 22.5 });  // one record
await writer.WriteAsync(myReadings);   // or an IEnumerable<SensorReading>

await writer.FlushAsync();   // returns once everything is stored
```

You hand the writer records, it batches and sends them, and `FlushAsync` waits until the server has them. The `await using` on the writer flushes and closes for you, so there's usually nothing else to clean up.

`TableProperties<SensorReading>` is the table name plus the record type. For JSON, use the non-generic `new TableProperties("catalog.schema.table")`.

The connection values come from your workspace:

```csharp
var serverEndpoint = "1234567890.zerobus.us-west-2.cloud.databricks.com"; // gRPC endpoint
var workspaceUrl   = "https://dbc-xxxx.cloud.databricks.com";             // used for OAuth
var clientId       = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID");     // service principal
var clientSecret   = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET");
```

## High-throughput writes

The same writer handles larger volumes. Keep calling `WriteAsync` as your data comes in, then flush once at the end. Two settings on `BulkWriterOptions` control throughput:

| Option | Default | Description |
|--------|---------|-------------|
| `Parallelism` | 4 | Number of connections running in parallel |
| `BatchSize` | 10,000 | Maximum rows per batch (one gRPC message) |
| `MaxBatchBytes` | 8 MB | Batches flush before this size to stay under the 10 MB message limit |

Here's a full example that writes a million records. Pass the options as the last argument to `CreateBulkWriterAsync`, hand the writer your records, and flush once at the end:

```csharp
using System.Diagnostics;
using Databricks.Zerobus;
using MyApp.Telemetry;

// Connection settings. You pass these in; the SDK doesn't read a config file on its own.
// Keep them wherever you store config (environment variables, appsettings.json, Key Vault).
// They're the same whether you use a Databricks-managed or an Entra ID service principal.
var serverEndpoint = Environment.GetEnvironmentVariable("ZEROBUS_SERVER_ENDPOINT")!; // e.g. 1234567890.zerobus.us-west-2.cloud.databricks.com
var workspaceUrl   = Environment.GetEnvironmentVariable("DATABRICKS_WORKSPACE_URL")!; // e.g. https://adb-xxxx.azuredatabricks.net
var clientId       = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_ID")!;     // service principal application (client) id
var clientSecret   = Environment.GetEnvironmentVariable("DATABRICKS_CLIENT_SECRET")!; // its Databricks OAuth secret

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

var options = new BulkWriterOptions
{
    Parallelism = 8,        // 8 connections in parallel
    BatchSize   = 10_000,   // rows per batch
};

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret, options);

// Your records can come from anywhere: a list, a query result, a file. This one streams
// them lazily, so they don't all sit in memory at once.
IEnumerable<SensorReading> readings = GenerateReadings(1_000_000);

var sw = Stopwatch.StartNew();
await writer.WriteAsync(readings);   // the writer batches these and spreads them across the 8 connections
await writer.FlushAsync();           // returns once every record is stored
sw.Stop();

Console.WriteLine($"Wrote 1,000,000 records in {sw.Elapsed.TotalSeconds:F1}s");

static IEnumerable<SensorReading> GenerateReadings(int count)
{
    for (var i = 0; i < count; i++)
        yield return new SensorReading
        {
            DeviceId  = $"sensor-{i % 100}",
            TempC     = 20 + (i % 15),
            Humidity  = 40 + (i % 30),
            ReadingTs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() * 1000,
        };
}
```

You can hand the whole sequence to `WriteAsync` and let it batch, or call `WriteAsync` per item or per chunk as data arrives. Either way, `FlushAsync` at the end waits until everything is stored.

A higher `Parallelism` gives more throughput, up to your network and account limits. With 8 connections this lands a million records in the tens of seconds (roughly 40,000+ rows per second from a single client). Each connection counts against your Zerobus concurrency quota, so pick a number you'll actually use.

> 💡 **Tip:** If you leave `options` off, the writer uses the defaults above, which work well for most cases.

## JSON ingestion

If you'd rather not define a proto, send JSON instead. Everything else stays the same:

```csharp
await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

await writer.WriteAsync("{\"device_id\":\"sensor-1\",\"temp_c\":22.5}"); // a JSON string
await writer.WriteAsync(new { device_id = "sensor-2", temp_c = 23.0 });   // or any object
await writer.FlushAsync();
```

## Writing into a streaming table

Zerobus can also write into a Databricks **streaming table**, which is useful when a downstream Lakeflow pipeline or Structured Streaming job reads the data incrementally. Create it with `CREATE STREAMING TABLE` and a column list (no query), so it starts empty and Zerobus fills it:

```sql
CREATE STREAMING TABLE main.telemetry.sensor_readings (
    device_id  STRING,
    temp_c     DOUBLE,
    humidity   INT,
    reading_ts TIMESTAMP
);
```

The SDK code doesn't change. Point `TableProperties` at the streaming table the same way you would a regular table:

```csharp
await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"),
    clientId, clientSecret);

await writer.WriteAsync(readings);
await writer.FlushAsync();
```

Downstream, you can read it as a streaming source, for example a `CREATE STREAMING TABLE ... AS SELECT` that aggregates it as new rows arrive.

## Generating a proto from a table

Generating the proto is optional. The SDK ingests any compiled protobuf message, so you can write the `.proto` by hand (as shown above), match the table schema yourself, or use the official Databricks Python generator (`python -m zerobus.tools.generate_proto`).

This repo also bundles a generator that reads the table and keeps the fields in sync. It isn't published to NuGet, so run it from a clone:

```bash
git clone https://github.com/guanjieshen/zerobus-dotnet
dotnet run --project zerobus-dotnet/tools/Databricks.Zerobus.ProtoGen -- \
  --uc-endpoint https://adb-xxxx.azuredatabricks.net \
  --client-id "$DATABRICKS_CLIENT_ID" \
  --client-secret "$DATABRICKS_CLIENT_SECRET" \
  --table main.telemetry.sensor_readings \
  --output sensor_reading.proto \
  --namespace MyApp.Telemetry
```

It marks every field `optional` so a value of `0`, `0.0`, or `""` still gets sent (see the note in [Before you begin](#before-you-begin)).

## Using the SDK in a service

Register the SDK once as a singleton and inject the `IZerobusSdk` interface where you need it. The gRPC channel is meant to be reused, and depending on the interface keeps your code easy to test:

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

> 💡 **Tip:** For a long-running service, keep one writer open and reuse it rather than creating one per request. Opening a stream costs an auth and handshake round trip.

## Working with a single stream

For control over individual records, use a single stream instead of the bulk writer:

```csharp
var stream = await sdk.CreateStreamAsync(
    new TableProperties("main.telemetry.events"), clientId, clientSecret);

long offset = await stream.IngestRecordAsync("{\"device_id\":\"sensor-1\"}");
await stream.WaitForOffsetAsync(offset);   // that record is now stored
await stream.CloseAsync();
```

A record is stored once the call that waits on it (`WaitForOffsetAsync` or `FlushAsync`) returns. Delivery is at-least-once: if a connection drops, the SDK reconnects and resends anything that wasn't confirmed, so expect the occasional duplicate downstream. If something fails for good, `GetUnacknowledgedRecords()` returns whatever didn't make it so you can retry it elsewhere.

## Authentication

The SDK authenticates with a Databricks service principal over OAuth (machine to machine). The examples above pass the service principal's client ID and secret, which is all most apps need.

This works for both **Databricks-managed** and **Microsoft Entra ID** service principals. The simplest path for an Entra ID service principal is to add it to the workspace and generate a Databricks OAuth secret (Settings, Identity and access, Service principals, Secrets), then pass the application (client) ID and that secret. No tenant id is needed, the token request is the same as a Databricks-managed SP, and it's the endpoint Databricks recommends for M2M.

A raw Entra ID token (from `login.microsoftonline.com`) is not accepted directly. Zerobus needs a token issued by the Databricks workspace endpoint, scoped to the Zerobus resource, so the Databricks OAuth secret above is the path to use.

### Microsoft Entra ID via token federation

If you can't issue a Databricks OAuth secret for the service principal, authenticate the Entra ID SP through [Databricks token federation](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/auth/oauth-federation-exchange). You get an Entra token; `FederatedTokenProvider` exchanges it at the workspace endpoint for a Zerobus-scoped Databricks token. No Databricks secret is stored.

**One-time setup (account admin):** create a federation policy so Databricks trusts the SP's Entra tokens.

1. In the account console, go to **User management, Service principals, your SP, Credentials & secrets, Federation policies, Create policy** (or run `databricks account service-principal-federation-policy create`).
2. Set:
   - **Issuer:** `https://sts.windows.net/<tenant-id>/`
   - **Audience:** `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d` (the Azure Databricks application id, the same for every tenant)
   - **Subject:** the service principal's object id (the `sub` claim of its Entra token)
   - **Subject claim:** `sub`
3. Grant the SP `MODIFY` and `SELECT` on the target table (see [Before you begin](#before-you-begin)).

> 💡 **Tip:** If you aren't sure of the subject, run a write once. When the policy is missing or wrong, Databricks returns the exact issuer, subject, and audience it expects in the error, which you can paste into the policy.

In code, get the Entra token with `Azure.Identity` and hand it to `FederatedTokenProvider`:

```csharp
using Azure.Core;
using Azure.Identity;
using Databricks.Zerobus;

var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

var tokenProvider = new FederatedTokenProvider(
    workspaceUrl,
    ZerobusSdk.WorkspaceIdFromServerEndpoint(serverEndpoint),
    subjectTokenProvider: async ct =>
        (await credential.GetTokenAsync(
            new TokenRequestContext(new[] { "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default" }), ct)).Token,
    clientId: clientId);   // the SP application (client) id, for service-principal federation policies

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"), tokenProvider);
```

The provider does the RFC 8693 token exchange and refreshes as needed. If you'd rather not add `Azure.Identity`, your `subjectTokenProvider` can POST to `https://login.microsoftonline.com/<tenant-id>/oauth2/v2.0/token` (grant_type `client_credentials`, scope `2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default`) and return the `access_token`. Both have been verified through Zerobus authentication.

If you'd rather supply the token from your own flow (Azure.Identity, a managed identity, the Databricks SDK, or a token you already hold), use `DelegatingTokenProvider` in place of the client id and secret:

```csharp
var tokenProvider = new DelegatingTokenProvider(ct => GetMyDatabricksTokenAsync(ct));

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<SensorReading>("main.telemetry.sensor_readings"), tokenProvider);
```

For full control, implement `ITokenProvider` directly.

## Before you begin

Since you create the table yourself, two things commonly get in the way:

> ⚠️ **CHECK constraints are not supported.** Zerobus will not ingest into a table that has CHECK constraints. Validate values in your producer instead.

> ⚠️ **proto3 drops default values.** A field equal to its default (`0`, `0.0`, `""`) is not sent over the wire, and the server reads that as missing, so a `NOT NULL` column rejects it. If a required field can be zero or empty, mark it `optional` in the proto and always set it. The proto generator does this for you.

> ⚠️ **The table can't be in Unity Catalog default storage.** Zerobus rejects a table whose catalog has no explicit managed or external storage location (error 4024, "Tables created in default storage are not supported"). Create the table in a catalog backed by managed or external storage.

The service principal needs access to the table:

```sql
GRANT USE CATALOG ON CATALOG main TO `<sp-client-id>`;
GRANT USE SCHEMA  ON SCHEMA main.telemetry TO `<sp-client-id>`;
GRANT MODIFY, SELECT ON TABLE main.telemetry.sensor_readings TO `<sp-client-id>`;
```

For custom authentication, implement `ITokenProvider` and pass it in place of the client id and secret.

## Limits

10 MB per message and 2,000 columns per table. The bulk writer keeps batches under the message limit for you, and you can scale past a single stream by raising `Parallelism`.

## Building from source

```bash
dotnet build -c Release
dotnet test          # runs against an in-memory gRPC server, no credentials needed
```

The `examples/` folder has JSON, protobuf, and Azure Functions samples that read settings from environment variables.

## Using this with an AI coding agent

This repo ships a [`SKILL.md`](SKILL.md) that follows the [Agent Skills](https://agentskills.io/specification) spec. Point your coding harness (Claude Code or similar) at it and the agent can wire Zerobus into your .NET project for you: install the package, set up the writer, configure authentication, and check the table requirements. Drop `SKILL.md` into your harness's skills directory, or copy it alongside your project so the agent picks it up.

## License

Apache 2.0. See [LICENSE](LICENSE).
