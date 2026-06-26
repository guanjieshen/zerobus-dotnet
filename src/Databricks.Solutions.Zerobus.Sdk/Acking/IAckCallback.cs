namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Receives durability notifications for ingested records. Supply one via
/// <see cref="StreamConfigurationOptions.AckCallback"/> to use the high-throughput
/// "fire-and-forget" pattern: ingest records without awaiting
/// <see cref="ZerobusStreamBase.WaitForOffsetAsync"/> and react to acknowledgments here.
/// </summary>
/// <remarks>Callback methods are invoked from the stream's internal reader loop and must not block.</remarks>
public interface IAckCallback
{
    /// <summary>Invoked once per record offset as it becomes durably acknowledged, in increasing order.</summary>
    void OnAck(long offset);

    /// <summary>
    /// Invoked for each unacknowledged offset when the stream fails terminally.
    /// <paramref name="error"/> is the cause.
    /// </summary>
    void OnError(long offset, Exception error);
}
