using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace Databricks.Solutions.Zerobus.ProtoGen;

/// <summary>Reads a table's column schema from the Unity Catalog REST API.</summary>
public sealed class UnityCatalogClient : IDisposable
{
    private readonly HttpClient _http = new();
    private readonly string _workspaceUrl;

    public UnityCatalogClient(string workspaceUrl) => _workspaceUrl = workspaceUrl.TrimEnd('/');

    public async Task<IReadOnlyList<ColumnSchema>> GetColumnsAsync(
        string fullTableName, string clientId, string clientSecret, CancellationToken cancellationToken = default)
    {
        var token = await GetTokenAsync(clientId, clientSecret, cancellationToken).ConfigureAwait(false);

        using var request = new HttpRequestMessage(
            HttpMethod.Get, $"{_workspaceUrl}/api/2.1/unity-catalog/tables/{Uri.EscapeDataString(fullTableName)}");
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", token);

        using var response = await _http.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"Failed to read table '{fullTableName}' ({(int)response.StatusCode}): {body}");

        using var doc = JsonDocument.Parse(body);
        var columns = new List<(int position, ColumnSchema column)>();
        foreach (var c in doc.RootElement.GetProperty("columns").EnumerateArray())
        {
            var name = c.GetProperty("name").GetString()!;
            var typeName = c.GetProperty("type_name").GetString()!;
            var nullable = !c.TryGetProperty("nullable", out var n) || n.GetBoolean();
            var position = c.TryGetProperty("position", out var p) ? p.GetInt32() : columns.Count;
            columns.Add((position, new ColumnSchema(name, typeName, nullable)));
        }
        return columns.OrderBy(x => x.position).Select(x => x.column).ToList();
    }

    private async Task<string> GetTokenAsync(string clientId, string clientSecret, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, $"{_workspaceUrl}/oidc/v1/token")
        {
            Content = new FormUrlEncodedContent(new[]
            {
                new KeyValuePair<string, string>("grant_type", "client_credentials"),
                new KeyValuePair<string, string>("scope", "all-apis"),
            }),
        };
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Basic", basic);

        using var response = await _http.SendAsync(request, cancellationToken).ConfigureAwait(false);
        var body = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
        if (!response.IsSuccessStatusCode)
            throw new InvalidOperationException($"OAuth token request failed ({(int)response.StatusCode}): {body}");

        using var doc = JsonDocument.Parse(body);
        return doc.RootElement.GetProperty("access_token").GetString()
               ?? throw new InvalidOperationException("Token response did not contain an access_token.");
    }

    public void Dispose() => _http.Dispose();
}
