using System.Net.Http;
using System.Text.Json;

namespace Databricks.Zerobus;

/// <summary>
/// Obtains a Microsoft Entra ID access token for a service principal (tenant id, client id, and
/// client secret) to authenticate to Azure Databricks. Use this when the service principal is
/// managed in Entra ID and you authenticate with its Azure AD credentials rather than a Databricks
/// OAuth secret.
/// </summary>
/// <remarks>
/// Requests a token from <c>https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token</c> with
/// the Azure Databricks resource scope (<c>2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default</c>),
/// matching how the Databricks SDKs do Entra service principal auth. If your service principal has a
/// Databricks OAuth secret, prefer passing the client id and secret directly (no tenant id needed),
/// which is the flow Databricks recommends for M2M.
/// </remarks>
public sealed class EntraTokenProvider : ITokenProvider, IDisposable
{
    // Well-known Azure Databricks application (resource) id.
    private const string DatabricksResourceId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";
    private static readonly TimeSpan RefreshSkew = TimeSpan.FromSeconds(60);

    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly string _tokenEndpoint;
    private readonly string _clientId;
    private readonly string _clientSecret;
    private readonly string _scope;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _cachedToken;
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;

    /// <param name="tenantId">The Microsoft Entra ID tenant id of the service principal.</param>
    /// <param name="clientId">The service principal's application (client) id.</param>
    /// <param name="clientSecret">The service principal's Entra ID client secret.</param>
    /// <param name="httpClient">Optional shared <see cref="HttpClient"/>; one is created if omitted.</param>
    public EntraTokenProvider(string tenantId, string clientId, string clientSecret, HttpClient? httpClient = null)
    {
        if (string.IsNullOrWhiteSpace(tenantId)) throw new ArgumentException("Tenant id is required.", nameof(tenantId));
        _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
        _clientSecret = clientSecret ?? throw new ArgumentNullException(nameof(clientSecret));
        _tokenEndpoint = $"https://login.microsoftonline.com/{tenantId}/oauth2/v2.0/token";
        _scope = $"{DatabricksResourceId}/.default";
        _http = httpClient ?? new HttpClient();
        _ownsHttp = httpClient is null;
    }

    /// <inheritdoc />
    public async Task<string> GetTokenAsync(string tableName, CancellationToken cancellationToken)
    {
        if (_cachedToken is not null && DateTimeOffset.UtcNow < _expiresAt - RefreshSkew)
            return _cachedToken;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_cachedToken is not null && DateTimeOffset.UtcNow < _expiresAt - RefreshSkew)
                return _cachedToken;

            var (token, expiresIn) = await RequestTokenAsync(cancellationToken).ConfigureAwait(false);
            _cachedToken = token;
            _expiresAt = expiresIn.HasValue ? DateTimeOffset.UtcNow + expiresIn.Value : DateTimeOffset.MinValue;
            return token;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<(string token, TimeSpan? expiresIn)> RequestTokenAsync(CancellationToken ct)
    {
        var form = new List<KeyValuePair<string, string>>
        {
            new("grant_type", "client_credentials"),
            new("client_id", _clientId),
            new("client_secret", _clientSecret),
            new("scope", _scope),
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint)
        {
            Content = new FormUrlEncodedContent(form),
        };

        using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new ZerobusAuthException(
                $"Entra ID token request failed ({(int)response.StatusCode} {response.ReasonPhrase}). " +
                $"Verify the tenant id, client id, and secret. Response: {Truncate(body, 500)}");

        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;
        if (!root.TryGetProperty("access_token", out var tokenEl) || tokenEl.GetString() is not { Length: > 0 } token)
            throw new ZerobusAuthException("Entra ID token response did not contain an access_token.");

        TimeSpan? expiresIn = null;
        if (root.TryGetProperty("expires_in", out var expEl) && expEl.TryGetInt64(out var seconds) && seconds > 0)
            expiresIn = TimeSpan.FromSeconds(seconds);

        return (token, expiresIn);
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s.Substring(0, max) + "…";

    /// <inheritdoc />
    public void Dispose()
    {
        _gate.Dispose();
        if (_ownsHttp) _http.Dispose();
    }
}
