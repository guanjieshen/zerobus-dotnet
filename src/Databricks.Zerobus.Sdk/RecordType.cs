namespace Databricks.Zerobus;

/// <summary>
/// The serialization format for records ingested over a Zerobus stream.
/// </summary>
public enum RecordType
{
    /// <summary>JSON-encoded records (a JSON string per record). No descriptor required.</summary>
    Json = 0,

    /// <summary>Protobuf-encoded records. Requires a message descriptor; recommended for production.</summary>
    Proto = 1,
}
