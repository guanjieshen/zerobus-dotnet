using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Identifies the target table for a JSON ingest stream.
/// </summary>
public class TableProperties
{
    /// <summary>The fully qualified three-part table name: <c>catalog.schema.table</c>.</summary>
    public string TableName { get; }

    /// <summary>Creates table properties for the given three-part table name.</summary>
    /// <exception cref="ArgumentException">The name is not a three-part identifier.</exception>
    public TableProperties(string tableName)
    {
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Table name must be a non-empty three-part name 'catalog.schema.table'.", nameof(tableName));

        var parts = tableName.Split('.');
        if (parts.Length != 3 || parts.Any(string.IsNullOrWhiteSpace))
            throw new ArgumentException($"Table name '{tableName}' must be a three-part name 'catalog.schema.table'.", nameof(tableName));

        TableName = tableName;
    }
}

/// <summary>
/// Identifies the target table for a Protobuf ingest stream and carries the
/// record message type <typeparamref name="T"/> used to derive the wire descriptor.
/// </summary>
/// <typeparam name="T">The generated protobuf message type for the table's rows.</typeparam>
public sealed class TableProperties<T> : TableProperties where T : IMessage<T>, new()
{
    /// <summary>Creates protobuf table properties for the given three-part table name.</summary>
    public TableProperties(string tableName) : base(tableName) { }

    /// <summary>The protobuf message descriptor for <typeparamref name="T"/>.</summary>
    internal MessageDescriptor Descriptor => new T().Descriptor;
}
