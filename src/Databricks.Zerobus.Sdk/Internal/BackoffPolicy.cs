namespace Databricks.Zerobus;

/// <summary>
/// Exponential backoff configuration for stream reconnection. Delays grow by
/// <see cref="Multiplier"/> from <see cref="InitialDelay"/> up to <see cref="MaxDelay"/>,
/// for at most <see cref="MaxAttempts"/> consecutive failures.
/// </summary>
public sealed class BackoffPolicy
{
    /// <summary>Delay before the first retry. Default: 1 second.</summary>
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>Factor applied to the delay after each failed attempt. Default: 2.0.</summary>
    public double Multiplier { get; set; } = 2.0;

    /// <summary>Maximum delay between retries. Default: 30 seconds.</summary>
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Maximum consecutive reconnect attempts before the stream fails. Default: 10.</summary>
    public int MaxAttempts { get; set; } = 10;

    /// <summary>A policy with the default settings.</summary>
    public static BackoffPolicy Default => new();

    /// <summary>Computes the delay for the given 1-based attempt number.</summary>
    public TimeSpan GetDelay(int attempt)
    {
        if (attempt < 1) attempt = 1;
        var ms = InitialDelay.TotalMilliseconds * Math.Pow(Multiplier, attempt - 1);
        ms = Math.Min(ms, MaxDelay.TotalMilliseconds);
        return TimeSpan.FromMilliseconds(ms);
    }
}
