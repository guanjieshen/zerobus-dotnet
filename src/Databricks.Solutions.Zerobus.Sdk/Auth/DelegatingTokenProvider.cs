namespace Databricks.Solutions.Zerobus;

/// <summary>
/// An <see cref="ITokenProvider"/> backed by a callback. Use this when you want to obtain the
/// token yourself, for example from the Databricks SDK, Azure.Identity, a managed identity, or a
/// token you already hold. The callback must return a Databricks OAuth access token that Zerobus
/// accepts for the target table.
/// </summary>
public sealed class DelegatingTokenProvider : ITokenProvider
{
    private readonly Func<string, CancellationToken, Task<string>> _getToken;

    /// <summary>Creates a provider from a callback that receives the target table name.</summary>
    public DelegatingTokenProvider(Func<string, CancellationToken, Task<string>> getToken)
        => _getToken = getToken ?? throw new ArgumentNullException(nameof(getToken));

    /// <summary>Creates a provider from a callback that takes only a cancellation token.</summary>
    public DelegatingTokenProvider(Func<CancellationToken, Task<string>> getToken)
    {
        if (getToken is null) throw new ArgumentNullException(nameof(getToken));
        _getToken = (_, ct) => getToken(ct);
    }

    /// <inheritdoc />
    public Task<string> GetTokenAsync(string tableName, CancellationToken cancellationToken)
        => _getToken(tableName, cancellationToken);
}
