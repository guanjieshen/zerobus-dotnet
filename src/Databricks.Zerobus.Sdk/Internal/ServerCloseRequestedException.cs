namespace Databricks.Zerobus;

/// <summary>
/// Internal signal that the server sent a <c>CloseStreamSignal</c>. Treated as a
/// retryable condition: the stream reconnects and replays unacknowledged records.
/// </summary>
internal sealed class ServerCloseRequestedException : Exception
{
    public ServerCloseRequestedException(TimeSpan? duration)
        : base($"Server requested stream close (duration: {duration?.ToString() ?? "unspecified"}).") { }
}
