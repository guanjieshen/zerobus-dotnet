using System.Text;

namespace Databricks.Zerobus.ProtoGen;

/// <summary>A Unity Catalog column: its name, Delta type name, and nullability.</summary>
public sealed record ColumnSchema(string Name, string TypeName, bool Nullable);

/// <summary>
/// Turns a Unity Catalog table schema into a proto3 message definition for Zerobus ingestion.
/// </summary>
public static class ProtoSchemaGenerator
{
    /// <summary>
    /// Generates a proto3 <c>.proto</c> for <paramref name="messageName"/> from <paramref name="columns"/>.
    /// Every field is emitted as <c>optional</c> so values equal to a proto3 default (0, 0.0, "") are
    /// still sent on the wire; Zerobus treats an absent field as missing and rejects NOT NULL columns.
    /// </summary>
    public static string Generate(string messageName, IReadOnlyList<ColumnSchema> columns, string? csharpNamespace = null)
    {
        if (string.IsNullOrWhiteSpace(messageName))
            throw new ArgumentException("Message name is required.", nameof(messageName));
        if (columns is null || columns.Count == 0)
            throw new ArgumentException("At least one column is required.", nameof(columns));

        var sb = new StringBuilder();
        sb.AppendLine("syntax = \"proto3\";");
        sb.AppendLine();
        if (!string.IsNullOrWhiteSpace(csharpNamespace))
        {
            sb.AppendLine($"option csharp_namespace = \"{csharpNamespace}\";");
            sb.AppendLine();
        }
        sb.AppendLine("// Generated from a Unity Catalog table by zerobus-generate-proto.");
        sb.AppendLine("// Every field is 'optional' so values equal to a proto3 default (0, 0.0, \"\") are still");
        sb.AppendLine("// sent on the wire. Zerobus treats an absent field as missing and rejects NOT NULL columns.");
        sb.AppendLine($"message {messageName} {{");
        for (var i = 0; i < columns.Count; i++)
        {
            var column = columns[i];
            sb.AppendLine($"  optional {MapType(column.TypeName)} {column.Name} = {i + 1};   // {column.TypeName}");
        }
        sb.AppendLine("}");
        return sb.ToString();
    }

    /// <summary>Maps a Unity Catalog Delta type name to its proto3 scalar type.</summary>
    public static string MapType(string deltaTypeName)
    {
        return (deltaTypeName ?? string.Empty).Trim().ToUpperInvariant() switch
        {
            "STRING" => "string",
            "INT" or "INTEGER" => "int32",
            "LONG" or "BIGINT" => "int64",
            "SHORT" or "SMALLINT" => "int32",
            "BYTE" or "TINYINT" => "int32",
            "FLOAT" => "float",
            "DOUBLE" => "double",
            "BOOLEAN" => "bool",
            "BINARY" => "bytes",
            "DATE" => "int32",                          // epoch days
            "TIMESTAMP" or "TIMESTAMP_NTZ" => "int64",  // epoch microseconds
            "DECIMAL" => "string",                      // exact decimal as text
            "VARIANT" => "string",                      // JSON-encoded
            var other => throw new NotSupportedException(
                $"Unsupported Unity Catalog column type '{other}'. Supported: STRING, INT, LONG, SHORT, " +
                "BYTE, FLOAT, DOUBLE, BOOLEAN, BINARY, DATE, TIMESTAMP, DECIMAL, VARIANT."),
        };
    }

    /// <summary>Derives a PascalCase message name from a three-part table name (uses the table segment).</summary>
    public static string MessageNameFromTable(string tableName)
    {
        var leaf = tableName.Split('.').Last();
        var parts = leaf.Split('_', StringSplitOptions.RemoveEmptyEntries);
        return string.Concat(parts.Select(p => char.ToUpperInvariant(p[0]) + p.Substring(1)));
    }
}
