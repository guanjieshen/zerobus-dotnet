using Databricks.Zerobus.ProtoGen;

// Generates a .proto from a Unity Catalog table, mirroring `python -m zerobus.tools.generate_proto`.
//
//   zerobus-generate-proto \
//     --uc-endpoint https://adb-xxxx.azuredatabricks.net \
//     --client-id <sp-id> --client-secret <sp-secret> \
//     --table main.telemetry.sensor_readings \
//     --output sensor_reading.proto \
//     [--proto-msg SensorReading] [--namespace MyApp.Telemetry]

if (args.Length == 0 || args.Contains("--help") || args.Contains("-h"))
{
    Console.WriteLine(
        """
        Generates a .proto from a Unity Catalog table.

        Usage:
          zerobus-generate-proto --uc-endpoint <url> --client-id <id> --client-secret <secret> \
            --table catalog.schema.table [--output record.proto] [--proto-msg MyRecord] [--namespace MyApp]

        Required:
          --uc-endpoint    Workspace URL, e.g. https://adb-xxxx.azuredatabricks.net
          --client-id      Service principal id
          --client-secret  Service principal secret
          --table          Three-part table name (catalog.schema.table)

        Optional:
          --output         Output .proto path (default: record.proto)
          --proto-msg      Message name (default: derived from the table name)
          --namespace      csharp_namespace option in the generated .proto
        """);
    return 0;
}

var options = ParseArgs(args);

string Required(string key) =>
    options.TryGetValue(key, out var v) && v.Length > 0 ? v : throw new ArgumentException($"Missing required --{key}");

try
{
    var workspaceUrl = Required("uc-endpoint");
    var clientId = Required("client-id");
    var clientSecret = Required("client-secret");
    var table = Required("table");
    var output = options.GetValueOrDefault("output", "record.proto");
    var message = options.GetValueOrDefault("proto-msg", ProtoSchemaGenerator.MessageNameFromTable(table));
    options.TryGetValue("namespace", out var csharpNamespace);

    using var uc = new UnityCatalogClient(workspaceUrl);
    var columns = await uc.GetColumnsAsync(table, clientId, clientSecret);
    var proto = ProtoSchemaGenerator.Generate(message, columns, csharpNamespace);

    await File.WriteAllTextAsync(output, proto);
    Console.WriteLine($"Wrote {output}: message {message} with {columns.Count} field(s).");
    return 0;
}
catch (Exception ex)
{
    Console.Error.WriteLine($"error: {ex.Message}");
    return 1;
}

static Dictionary<string, string> ParseArgs(string[] args)
{
    var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
    for (var i = 0; i < args.Length; i++)
    {
        if (!args[i].StartsWith("--", StringComparison.Ordinal)) continue;
        var key = args[i].Substring(2);
        var value = i + 1 < args.Length && !args[i + 1].StartsWith("--", StringComparison.Ordinal) ? args[++i] : "";
        result[key] = value;
    }
    return result;
}
