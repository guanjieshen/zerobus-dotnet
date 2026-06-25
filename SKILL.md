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

- **Protobuf** (recommended for production): the SDK ingests a generated protobuf message type (`IMessage<T>`). This skill assumes you already have the record type, either as a generated C# class or as a `.proto` file. Generating one is optional and external (see step 6).
- **JSON**: no `.proto` needed; send JSON strings or POCOs. Skip the rest of this step.

You do **not** add `Google.Protobuf` yourself. The SDK already depends on a current, patched `Google.Protobuf` and it flows to your project transitively. Adding it by hand is what exposes the team: a version-less `<PackageReference Include="Google.Protobuf" />` resolves to the old `3.0.0`, which carries a known high-severity advisory.

Two cases:

- **You already have the generated C# message class** (proto compiled elsewhere, the `.cs` is in your project): add nothing. The SDK's transitive `Google.Protobuf` is all the runtime needs.
- **You have a `.proto` and want the build to compile it**: add only `Grpc.Tools` (the build-time protoc), then point the build at the file:

  ```bash
  dotnet add package Grpc.Tools
  ```

  ```xml
  <ItemGroup>
    <Protobuf Include="Protos/record.proto" GrpcServices="None" />
  </ItemGroup>
  ```

  In the `<PackageReference Include="Grpc.Tools" ... />` line that `dotnet add package` wrote, add `PrivateAssets="All"` so it stays build-only. `Grpc.Tools` is the only package you add; the generated code binds to the transitive `Google.Protobuf`.

Confirm the dependency tree is clean before shipping:

```bash
dotnet list package --vulnerable --include-transitive
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

Generating the `.proto` is optional and external to the SDK. The SDK ingests any compiled protobuf message, so produce the `.proto` however suits the project:

- Hand-write it to match the table columns (see the README for an example).
- Use the official Databricks Python generator: `python -m zerobus.tools.generate_proto`.
- This repo bundles a generator under `tools/Databricks.Zerobus.ProtoGen`. Run it from a clone:

```bash
git clone https://github.com/guanjieshen/zerobus-dotnet
dotnet run --project zerobus-dotnet/tools/Databricks.Zerobus.ProtoGen -- \
  --uc-endpoint <workspace-url> --client-id <id> --client-secret <secret> \
  --table catalog.schema.table --output record.proto --namespace MyApp
```

Whichever you pick, declare required-but-zero-valued fields `optional` (see the proto3 note above).

## 7. Verify the integration

```bash
dotnet build
```

Then run a small write (a few records, then `FlushAsync`) against the target table and confirm the rows land. The flush completing means the server durably stored them.

## Tuning

`BulkWriterOptions`: `Parallelism` (default 4, more connections = more throughput, each counts against the Zerobus concurrency quota), `BatchSize` (default 10,000 rows), `MaxBatchBytes` (default 8 MB, keeps batches under the 10 MB message limit). Limits: 10 MB per message, 2,000 columns per table, ~15,000 rows/s per stream.

## Reference

Full usage, DI setup, and the Entra federation walkthrough are in the [README](README.md).
