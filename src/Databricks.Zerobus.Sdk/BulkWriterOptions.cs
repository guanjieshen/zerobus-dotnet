namespace Databricks.Zerobus;

/// <summary>
/// Configuration for a <see cref="ZerobusBulkWriter{T}"/>.
/// </summary>
public sealed class BulkWriterOptions
{
    /// <summary>
    /// Number of independent streams/connections to fan out across. Throughput scales
    /// roughly linearly until the client uplink or account quotas bound it. Default: 4.
    /// </summary>
    public int Parallelism { get; set; } = 4;

    /// <summary>
    /// Maximum number of records accumulated per batch before a single gRPC message is sent.
    /// Default: 10,000. A batch is also flushed early if it would exceed <see cref="MaxBatchBytes"/>.
    /// </summary>
    public int BatchSize { get; set; } = 10_000;

    /// <summary>
    /// Soft byte ceiling per batch. A batch is dispatched before adding a record that would push
    /// its serialized size past this value, keeping batches safely under the 10 MB message limit
    /// regardless of <see cref="BatchSize"/>. Default: 8 MB.
    /// </summary>
    public int MaxBatchBytes { get; set; } = 8 * 1024 * 1024;

    /// <summary>
    /// Optional per-stream configuration (backpressure, reconnect, flush timeout). The record
    /// type is set to Protobuf internally. If omitted, defaults are used.
    /// </summary>
    public StreamConfigurationOptions? StreamOptions { get; set; }

    /// <summary>A new options instance with default values.</summary>
    public static BulkWriterOptions Default => new();
}
