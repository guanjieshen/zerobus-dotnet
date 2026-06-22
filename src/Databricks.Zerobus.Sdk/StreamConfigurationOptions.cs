namespace Databricks.Zerobus;

/// <summary>
/// Configuration for a Zerobus ingest stream.
/// </summary>
public sealed class StreamConfigurationOptions
{
    /// <summary>The record serialization format. Default: <see cref="RecordType.Json"/>.</summary>
    public RecordType RecordType { get; set; } = RecordType.Json;

    /// <summary>Optional durability callback for the fire-and-forget pattern.</summary>
    public IAckCallback? AckCallback { get; set; }

    /// <summary>
    /// Maximum number of in-flight (ingested but not yet acknowledged) records.
    /// Ingestion applies backpressure once this bound is reached. Default: 10,000.
    /// </summary>
    public int MaxInflightRecords { get; set; } = 10_000;

    /// <summary>Maximum time <see cref="ZerobusStreamBase.FlushAsync"/> waits for outstanding acks. Default: 30 seconds.</summary>
    public TimeSpan FlushTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Reconnect backoff policy applied on retryable stream errors.</summary>
    public BackoffPolicy Recovery { get; set; } = BackoffPolicy.Default;

    /// <summary>A new options instance with default values.</summary>
    public static StreamConfigurationOptions Default => new();
}
