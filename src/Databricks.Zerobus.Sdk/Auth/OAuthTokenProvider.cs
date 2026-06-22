using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Databricks.Zerobus;

/// <summary>
/// OAuth 2.0 client-credentials (M2M) token provider. Requests a Unity Catalog
/// scoped token from <c>{workspaceUrl}/oidc/v1/token</c> for ingesting into a
/// specific table, caching it until shortly before it expires.
/// </summary>
/// <remarks>
/// Mirrors the token flow of the official Zerobus SDKs: HTTP Basic auth with the
/// service-principal client id/secret, <c>grant_type=client_credentials</c>,
/// <c>scope=all-apis</c>, a Zerobus <c>resource</c> indicator, and an
/// <c>authorization_details</c> request scoping USE CATALOG / USE SCHEMA / SELECT+MODIFY
/// (operation <c>zerobuswrite</c>) to the catalog, schema, and table.
/// </remarks>
public sealed class OAuthTokenProvider : ITokenProvider, IDisposable
{
    private static readonly TimeSpan RefreshSkew = TimeSpan.FromSeconds(60);

    private readonly HttpClient _http;
    private readonly bool _ownsHttp;
    private readonly string _tokenEndpoint;
    private readonly string _workspaceId;
    private readonly string _clientId;
    private readonly string _clientSecret;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private string? _cachedToken;
    private DateTimeOffset _expiresAt = DateTimeOffset.MinValue;

    /// <param name="workspaceUrl">The workspace URL, e.g. <c>https://dbc-….cloud.databricks.com</c>.</param>
    /// <param name="workspaceId">The numeric workspace id (first segment of the Zerobus server endpoint).</param>
    /// <param name="clientId">Service-principal OAuth client id.</param>
    /// <param name="clientSecret">Service-principal OAuth client secret.</param>
    /// <param name="httpClient">Optional shared <see cref="HttpClient"/>; one is created if omitted.</param>
    public OAuthTokenProvider(string workspaceUrl, string workspaceId, string clientId, string clientSecret, HttpClient? httpClient = null)
    {
        if (string.IsNullOrWhiteSpace(workspaceUrl)) throw new ArgumentException("Workspace URL is required.", nameof(workspaceUrl));
        _workspaceId = workspaceId ?? throw new ArgumentNullException(nameof(workspaceId));
        _clientId = clientId ?? throw new ArgumentNullException(nameof(clientId));
        _clientSecret = clientSecret ?? throw new ArgumentNullException(nameof(clientSecret));
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

            var (token, expiresIn) = await RequestTokenAsync(tableName, cancellationToken).ConfigureAwait(false);
            _cachedToken = token;
            _expiresAt = expiresIn.HasValue ? DateTimeOffset.UtcNow + expiresIn.Value : DateTimeOffset.MinValue;
            return token;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task<(string token, TimeSpan? expiresIn)> RequestTokenAsync(string tableName, CancellationToken ct)
    {
        var parts = tableName.Split('.');
        var catalog = parts[0];
        var schema = $"{parts[0]}.{parts[1]}";
        var table = tableName;

        var authorizationDetails = JsonSerializer.Serialize(new object[]
        {
            new { type = "unity_catalog_privileges", privileges = new[] { "USE CATALOG" }, object_type = "CATALOG", object_full_path = catalog },
            new { type = "unity_catalog_privileges", privileges = new[] { "USE SCHEMA" }, object_type = "SCHEMA", object_full_path = schema },
            new { type = "unity_catalog_privileges", privileges = new[] { "SELECT", "MODIFY" }, object_type = "TABLE", object_full_path = table, operations = new[] { "zerobuswrite" } },
        });

        var form = new List<KeyValuePair<string, string>>
        {
            new("grant_type", "client_credentials"),
            new("scope", "all-apis"),
            new("resource", $"api://databricks/workspaces/{_workspaceId}/zerobusDirectWriteApi"),
            new("authorization_details", authorizationDetails),
        };

        using var request = new HttpRequestMessage(HttpMethod.Post, _tokenEndpoint)
        {
            Content = new FormUrlEncodedContent(form),
        };
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{_clientId}:{_clientSecret}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", basic);

        using var response = await _http.SendAsync(request, ct).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);

        if (!response.IsSuccessStatusCode)
        {
            throw new ZerobusAuthException(
                $"OAuth token request failed ({(int)response.StatusCode} {response.ReasonPhrase}). " +
                $"Verify the service principal credentials and table grants. Response: {Truncate(body, 500)}");
        }

        using var doc = JsonDocument.Parse(body);
        var root = doc.RootElement;
        if (!root.TryGetProperty("access_token", out var tokenEl) || tokenEl.GetString() is not { Length: > 0 } token)
            throw new ZerobusAuthException("OAuth token response did not contain an access_token.");

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
