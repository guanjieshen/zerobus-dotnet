using System.Net.Http;
using System.Text.Json;

namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Exchanges an external identity-provider JWT (for example a Microsoft Entra ID token) for a
/// Databricks OAuth token scoped to Zerobus, using Databricks OAuth token federation
/// (RFC 8693 token exchange). Use this when your service principal authenticates through a federated
/// identity provider rather than a Databricks OAuth secret.
/// </summary>
/// <remarks>
/// Requires a Databricks token federation policy for the account or service principal. The provider
/// calls your <c>subjectTokenProvider</c> to get the federated JWT, then posts a token exchange to
/// <c>{workspaceUrl}/oidc/v1/token</c> with the Zerobus resource and table-scoped
/// <c>authorization_details</c>. The Zerobus-specific resource on a token exchange is not separately
/// documented by Databricks, so verify it against your workspace.
/// </remarks>
public sealed class FederatedTokenProvider : ITokenProvider, IDisposable
{
    private const string TokenExchangeGrant = "urn:ietf:params:oauth:grant-type:token-exchange";
    private const string JwtTokenType = "urn:ietf:params:oauth:token-type:jwt";
    private static readonly TimeSpan RefreshSkew = TimeSpan.FromSeconds(60);

    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly string _tokenEndpoint;
    private readonly string _workspaceId;
    private readonly string? _clientId;
    private readonly Func<CancellationToken, Task<string>> _subjectTokenProvider;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _cachedToken;
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;

    /// <param name="workspaceUrl">Workspace URL, e.g. <c>https://adb-….azuredatabricks.net</c>.</param>
    /// <param name="workspaceId">Numeric workspace id (see <see cref="ZerobusSdk.WorkspaceIdFromServerEndpoint"/>).</param>
    /// <param name="subjectTokenProvider">Returns the federated JWT from your identity provider.</param>
    /// <param name="clientId">Service principal application (client) id, for service-principal federation policies.</param>
    /// <param name="httpClient">Optional shared <see cref="HttpClient"/>; one is created if omitted.</param>
    public FederatedTokenProvider(
        string workspaceUrl,
        string workspaceId,
        Func<CancellationToken, Task<string>> subjectTokenProvider,
        string? clientId = null,
        HttpClient? httpClient = null)
    {
        if (string.IsNullOrWhiteSpace(workspaceUrl)) throw new ArgumentException("Workspace URL is required.", nameof(workspaceUrl));
        _workspaceId = workspaceId ?? throw new ArgumentNullException(nameof(workspaceId));
        _subjectTokenProvider = subjectTokenProvider ?? throw new ArgumentNullException(nameof(subjectTokenProvider));
        _clientId = clientId;
        _tokenEndpoint = workspaceUrl.TrimEnd('/') + "/oidc/v1/token";
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

            var subjectToken = await _subjectTokenProvider(cancellationToken).ConfigureAwait(false);
            var (token, expiresIn) = await ExchangeAsync(subjectToken, tableName, cancellationToken).ConfigureAwait(false);
            _cachedToken = token;
            _expiresAt = expiresIn.HasValue ? DateTimeOffset.UtcNow + expiresIn.Value : DateTimeOffset.MinValue;
            return token;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<(string token, TimeSpan? expiresIn)> ExchangeAsync(string subjectToken, string tableName, CancellationToken ct)
    {
        var form = new List<KeyValuePair<string, string>>
        {
            new("grant_type", TokenExchangeGrant),
            new("subject_token", subjectToken),
            new("subject_token_type", JwtTokenType),
            new("scope", "all-apis"),
            new("resource", ZerobusOAuth.ResourceFor(_workspaceId)),
            new("authorization_details", ZerobusOAuth.AuthorizationDetails(tableName)),
        };
        if (!string.IsNullOrEmpty(_clientId)) form.Add(new("client_id", _clientId!));

        using var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint)
        {
            Content = new FormUrlEncodedContent(form),
        };

        using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new ZerobusAuthException(
                $"Token exchange failed ({(int)response.StatusCode} {response.ReasonPhrase}). " +
                $"Verify the federation policy and the subject token. Response: {Truncate(body, 500)}");

        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;
        if (!root.TryGetProperty("access_token", out var tokenEl) || tokenEl.GetString() is not { Length: > 0 } token)
            throw new ZerobusAuthException("Token exchange response did not contain an access_token.");

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
