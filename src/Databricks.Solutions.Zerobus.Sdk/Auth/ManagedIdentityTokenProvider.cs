using System.Net.Http;
using System.Text.Json;

namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Authenticates to Zerobus from an Azure workload with a <b>managed identity</b>, with no client
/// secret. It fetches the managed identity's Microsoft Entra ID token and exchanges it (via
/// Databricks OAuth token federation) for a table-scoped Databricks Zerobus token.
/// </summary>
/// <remarks>
/// This is a convenience wrapper over <see cref="FederatedTokenProvider"/>: it supplies the
/// federated exchange with a subject token obtained from the Azure managed-identity endpoint, and
/// inherits that provider's caching (the exchanged Databricks token is reused until shortly before
/// it expires, so the identity endpoint is only called on a cache miss).
/// <para>
/// It is dependency-free (no <c>Azure.Identity</c>) and speaks the identity endpoint directly:
/// <list type="bullet">
/// <item><description>
/// <b>Azure Functions / App Service</b>: uses the <c>IDENTITY_ENDPOINT</c> / <c>IDENTITY_HEADER</c>
/// environment variables (api-version <c>2019-08-01</c>, <c>X-IDENTITY-HEADER</c> header).
/// </description></item>
/// <item><description>
/// <b>Azure VM / VMSS</b>: falls back to the instance metadata service at
/// <c>169.254.169.254</c> (api-version <c>2018-02-01</c>, <c>Metadata: true</c> header).
/// </description></item>
/// </list>
/// For AKS workload identity, Azure Arc, or local development, use <c>Azure.Identity</c>
/// (<c>DefaultAzureCredential</c>) to get the Entra token and pass it to
/// <see cref="FederatedTokenProvider"/> instead.
/// </para>
/// <para>
/// Requires a Databricks token-federation policy on the account or service principal whose audience
/// matches the requested resource (the Azure Databricks application id by default). See the
/// README "Authentication" section.
/// </para>
/// </remarks>
public sealed class ManagedIdentityTokenProvider : ITokenProvider, IDisposable
{
    /// <summary>
    /// The Azure Databricks first-party application id. This is the default resource (audience)
    /// requested for the managed-identity token, and the audience a Databricks token-federation
    /// policy expects for the subject token.
    /// </summary>
    public const string DatabricksAzureAppId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

    private const string ImdsEndpoint = "http://169.254.169.254/metadata/identity/oauth2/token";
    private const string ImdsApiVersion = "2018-02-01";
    private const string AppServiceApiVersion = "2019-08-01";

    private readonly FederatedTokenProvider _federated;
    private readonly HttpClient _identityHttp;
    private readonly bool _ownsIdentityHttp;
    private readonly string _audience;
    private readonly string? _managedIdentityClientId;

    // App Service / Functions local token endpoint. When null, the VM IMDS endpoint is used.
    private readonly string? _identityEndpoint;
    private readonly string? _identityHeader;

    /// <param name="workspaceUrl">Workspace URL, e.g. <c>https://adb-….azuredatabricks.net</c>.</param>
    /// <param name="workspaceId">Numeric workspace id (see <see cref="ZerobusSdk.WorkspaceIdFromServerEndpoint"/>).</param>
    /// <param name="databricksClientId">
    /// Databricks service-principal application (client) id, for a service-principal-scoped federation
    /// policy. Leave null for an account-level federation policy.
    /// </param>
    /// <param name="managedIdentityClientId">
    /// The client id of a <b>user-assigned</b> managed identity. Leave null to use the system-assigned
    /// managed identity.
    /// </param>
    /// <param name="audience">
    /// The resource (audience) requested for the managed-identity token; must match the Databricks
    /// federation policy. Defaults to <see cref="DatabricksAzureAppId"/>.
    /// </param>
    /// <param name="identityHttpClient">Optional shared <see cref="HttpClient"/> for the identity endpoint; one is created if omitted.</param>
    /// <param name="exchangeHttpClient">Optional shared <see cref="HttpClient"/> for the token exchange; one is created if omitted.</param>
    /// <param name="identityEndpoint">
    /// Overrides the App Service / Functions identity endpoint URL. Defaults to the
    /// <c>IDENTITY_ENDPOINT</c> environment variable. Advanced / testing knob.
    /// </param>
    /// <param name="identityHeader">
    /// Overrides the App Service / Functions identity header secret. Defaults to the
    /// <c>IDENTITY_HEADER</c> environment variable. Advanced / testing knob.
    /// </param>
    public ManagedIdentityTokenProvider(
        string workspaceUrl,
        string workspaceId,
        string? databricksClientId = null,
        string? managedIdentityClientId = null,
        string audience = DatabricksAzureAppId,
        HttpClient? identityHttpClient = null,
        HttpClient? exchangeHttpClient = null,
        string? identityEndpoint = null,
        string? identityHeader = null)
    {
        if (string.IsNullOrWhiteSpace(audience)) throw new ArgumentException("Audience is required.", nameof(audience));
        _audience = audience;
        _managedIdentityClientId = managedIdentityClientId;
        _identityEndpoint = identityEndpoint ?? Environment.GetEnvironmentVariable("IDENTITY_ENDPOINT");
        _identityHeader = identityHeader ?? Environment.GetEnvironmentVariable("IDENTITY_HEADER");
        _identityHttp = identityHttpClient ?? new HttpClient();
        _ownsIdentityHttp = identityHttpClient is null;

        // The federated provider owns the RFC 8693 exchange and the token cache; we only feed it
        // the managed-identity token as the subject.
        _federated = new FederatedTokenProvider(
            workspaceUrl, workspaceId, FetchIdentityTokenAsync, databricksClientId, exchangeHttpClient);
    }

    /// <inheritdoc />
    public Task<string> GetTokenAsync(string tableName, CancellationToken cancellationToken)
        => _federated.GetTokenAsync(tableName, cancellationToken);

    private async Task<string> FetchIdentityTokenAsync(CancellationToken cancellationToken)
    {
        var usingAppService = !string.IsNullOrEmpty(_identityEndpoint);
        var baseUrl = usingAppService ? _identityEndpoint! : ImdsEndpoint;
        var apiVersion = usingAppService ? AppServiceApiVersion : ImdsApiVersion;

        var url = $"{baseUrl}?api-version={apiVersion}&resource={Uri.EscapeDataString(_audience)}";
        if (!string.IsNullOrEmpty(_managedIdentityClientId))
            url += $"&client_id={Uri.EscapeDataString(_managedIdentityClientId!)}";

        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        if (usingAppService)
            request.Headers.Add("X-IDENTITY-HEADER", _identityHeader ?? string.Empty);
        else
            request.Headers.Add("Metadata", "true");

        using var response = await _identityHttp.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new ZerobusAuthException(
                $"Managed-identity token request failed ({(int)response.StatusCode} {response.ReasonPhrase}). " +
                $"Verify the identity is assigned and the resource '{_audience}' is correct. Response: {Truncate(body, 500)}");

        using var doc = JsonDocument.Parse(body);
        if (!doc.RootElement.TryGetProperty("access_token", out var tokenEl) || tokenEl.GetString() is not { Length: > 0 } token)
            throw new ZerobusAuthException("Managed-identity token response did not contain an access_token.");

        return token;
    }

    private static string Truncate(string s, int max) => s.Length <= max ? s : s.Substring(0, max) + "…";

    /// <inheritdoc />
    public void Dispose()
    {
        _federated.Dispose();
        if (_ownsIdentityHttp) _identityHttp.Dispose();
    }
}
