namespace Databricks.Solutions.Zerobus;

/// <summary>Base type for all errors raised by the Zerobus SDK.</summary>
public class ZerobusException : Exception
{
    public ZerobusException(string message) : base(message) { }
    public ZerobusException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// The stream was terminated and could not be recovered (for example, reconnect
/// attempts were exhausted). Records that were not acknowledged may not be durable.
/// </summary>
public sealed class ZerobusStreamClosedException : ZerobusException
{
    public ZerobusStreamClosedException(string message) : base(message) { }
    public ZerobusStreamClosedException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// Authentication or authorization failed. Verify the service principal credentials
/// and that it has explicit <c>MODIFY</c> and <c>SELECT</c> grants on the target table.
/// </summary>
public sealed class ZerobusAuthException : ZerobusException
{
    public ZerobusAuthException(string message) : base(message) { }
    public ZerobusAuthException(string message, Exception innerException) : base(message, innerException) { }
}

/// <summary>
/// A non-retryable error such as a schema mismatch, an invalid table, or a record
/// that exceeds the 10 MB message limit. Retrying without changing the input will not help.
/// </summary>
public sealed class ZerobusNonRetryableException : ZerobusException
{
    public ZerobusNonRetryableException(string message) : base(message) { }
    public ZerobusNonRetryableException(string message, Exception innerException) : base(message, innerException) { }
}
