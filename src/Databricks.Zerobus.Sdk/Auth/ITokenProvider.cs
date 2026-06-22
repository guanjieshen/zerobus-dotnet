namespace Databricks.Zerobus;

/// <summary>
/// Supplies bearer tokens for authenticating a Zerobus stream. Implement this to
/// plug in a custom credential source; the default is <see cref="OAuthTokenProvider"/>.
/// </summary>
public interface ITokenProvider
{
    /// <summary>
    /// Returns a valid OAuth access token (without the "Bearer " prefix) scoped for
    /// ingesting into <paramref name="tableName"/>. Implementations should cache and
    /// refresh tokens; this is called once per stream connection.
    /// </summary>
    Task<string> GetTokenAsync(string tableName, CancellationToken cancellationToken);
}
