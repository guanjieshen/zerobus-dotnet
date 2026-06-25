---
name: zerobus-dotnet
description: Integrate the Databricks.Zerobus.Sdk .NET library into a .NET project to stream records into Unity Catalog Delta tables over gRPC (Databricks Zerobus Ingest). Use when a .NET app needs to write or ingest rows into Databricks through Zerobus, when setting up the bulk writer or a single ingest stream, when configuring OAuth or Microsoft Entra ID service-principal authentication, or when generating a record .proto from a Unity Catalog table.
license: Apache-2.0
compatibility: A .NET project targeting net8.0 or netstandard2.1+ (.NET Core 3.1+/.NET 5+). Requires NuGet access to nuget.org and a reachable Databricks workspace with a Zerobus endpoint.
metadata:
  author: guanjieshen
  version: "0.1.3"
  repository: https://github.com/guanjieshen/zerobus-dotnet
  package: Databricks.Zerobus.Sdk
---

# Integrate Databricks Zerobus into a .NET project

Add streaming ingestion into a Unity Catalog Delta table using `Databricks.Zerobus.Sdk`. Follow these steps when wiring Zerobus into an existing .NET app.

## 1. Install the package

```bash
dotnet add package Databricks.Zerobus.Sdk
```

It targets `net8.0` and `netstandard2.1` and pulls in `Grpc.Net.Client` and `Google.Protobuf`. No native dependencies.

## 2. Pick a serialization format

- **Protobuf** (recommended for production): define a `.proto` matching the table columns and compile it with `Grpc.Tools`.
- **JSON**: no `.proto` needed; send JSON strings or POCOs.

For Protobuf, add to the consuming project's `.csproj`:

```xml
<ItemGroup>
  <Protobuf Include="Protos/record.proto" GrpcServices="None" />
  <PackageReference Include="Grpc.Tools" PrivateAssets="All" />
  <PackageReference Include="Google.Protobuf" />
</ItemGroup>
```

## 3. Write records

Default to the **bulk writer**. It accepts a single record or a list, batches them, and fans out across parallel connections. `FlushAsync` returns once everything is durable.

```csharp
using Databricks.Zerobus;

await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);

await using var writer = await sdk.CreateBulkWriterAsync(
    new TableProperties<MyRecord>("catalog.schema.table"),   // TableProperties("...") for JSON
    clientId, clientSecret,
    new BulkWriterOptions { Parallelism = 4, BatchSize = 10_000 }); // optional; these are the defaults

await writer.WriteAsync(oneRecord);   // single
await writer.WriteAsync(manyRecords); // IEnumerable<MyRecord>
await writer.FlushAsync();
```

JSON variant: `new TableProperties("catalog.schema.table")` and `WriteAsync("{...}")` or `WriteAsync(poco)`.

For control over individual records, use a single stream instead:

```csharp
var stream = await sdk.CreateStreamAsync(new TableProperties("catalog.schema.table"), clientId, clientSecret);
long offset = await stream.IngestRecordAsync("{\"id\":1}");
await stream.WaitForOffsetAsync(offset); // durable
await stream.CloseAsync();
```

Delivery is at-least-once; design downstream consumers to tolerate duplicates. After a terminal failure, `GetUnacknowledgedRecords()` returns what was not acknowledged.

## 4. Connection settings

These are values the app supplies (the SDK does not read a config file). Source them from environment variables, `appsettings.json`, or a secret store. They are identical for Databricks-managed and Microsoft Entra ID service principals.

- `serverEndpoint`: `<workspace-id>.zerobus.<region>.cloud.databricks.com` (AWS) or `.azuredatabricks.net` (Azure)
- `workspaceUrl`: `https://<instance>.cloud.databricks.com` or `https://adb-<id>.<n>.azuredatabricks.net`
- `clientId` / `clientSecret`: service principal credentials

## 5. Authentication

- **Service principal client id + secret** (default): pass them to `CreateStreamAsync` / `CreateBulkWriterAsync`. Works for Databricks-managed SPs and for Entra ID SPs that have a Databricks OAuth secret. No tenant id needed.
- **Microsoft Entra ID via token federation** (no Databricks secret): use `FederatedTokenProvider` with an Entra token from `Azure.Identity`. Requires a Databricks token-federation policy on the SP. See the README "Authentication" section.
- **Custom**: implement `ITokenProvider`, or wrap any token source with `DelegatingTokenProvider`.

## 6. Target table requirements (verify before running)

Zerobus writes to a pre-existing table. Check these or ingestion fails:

- The table must be a **managed Delta table backed by managed or external storage**. Tables in Unity Catalog **default storage are rejected** (error 4024).
- The table must **not** have CHECK constraints (Zerobus rejects them).
- The service principal needs `USE CATALOG`, `USE SCHEMA`, and `MODIFY` + `SELECT` on the table (grant explicitly on the table).
- proto3 drops default values (`0`, `0.0`, `""`); a `NOT NULL` column rejects a missing field. Mark required-but-possibly-default fields `optional` in the `.proto` and always set them.

Generate a matching `.proto` from a table with the bundled tool:

```bash
dotnet tool install --global Databricks.Zerobus.ProtoGen
zerobus-generate-proto --uc-endpoint <workspace-url> --client-id <id> --client-secret <secret> \
  --table catalog.schema.table --output record.proto --namespace MyApp
```

## 7. Verify the integration

```bash
dotnet build
```

Then run a small write (a few records, then `FlushAsync`) against the target table and confirm the rows land. The flush completing means the server durably stored them.

## Tuning

`BulkWriterOptions`: `Parallelism` (default 4, more connections = more throughput, each counts against the Zerobus concurrency quota), `BatchSize` (default 10,000 rows), `MaxBatchBytes` (default 8 MB, keeps batches under the 10 MB message limit). Limits: 10 MB per message, 2,000 columns per table, ~15,000 rows/s per stream.

## Reference

Full usage, DI setup, and the Entra federation walkthrough are in the [README](README.md).
