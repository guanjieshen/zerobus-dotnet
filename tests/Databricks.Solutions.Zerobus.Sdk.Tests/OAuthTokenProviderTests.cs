using System.Net;
using System.Net.Http;
using System.Text.Json;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class OAuthTokenProviderTests
{
    [Fact]
    public async Task Sends_client_credentials_with_table_scoped_authorization_details()
    {
        HttpRequestMessage? captured = null;
        string? capturedBody = null;
        var handler = new StubHandler(async req =>
        {
            captured = req;
            capturedBody = await req.Content!.ReadAsStringAsync();
            return Json(new { access_token = "tok-123", expires_in = 3600 });
        });

        using var http = new HttpClient(handler);
        var provider = new OAuthTokenProvider("https://ws.example.com/", "9999", "my-client", "my-secret", http);

        var token = await provider.GetTokenAsync("main.sales.events", default);

        Assert.Equal("tok-123", token);
        Assert.Equal(HttpMethod.Post, captured!.Method);
        Assert.Equal("https://ws.example.com/oidc/v1/token", captured.RequestUri!.ToString());
        Assert.Equal("Basic", captured.Headers.Authorization!.Scheme);

        var form = ParseForm(capturedBody!);
        Assert.Equal("client_credentials", form["grant_type"]);
        Assert.Equal("all-apis", form["scope"]);
        Assert.Contains("zerobusDirectWriteApi", form["resource"]);

        using var details = JsonDocument.Parse(form["authorization_details"]);
        var entries = details.RootElement.EnumerateArray().ToList();
        Assert.Equal(3, entries.Count);
        var table = entries[2];
        Assert.Equal("TABLE", table.GetProperty("object_type").GetString());
        Assert.Equal("main.sales.events", table.GetProperty("object_full_path").GetString());
        Assert.Contains("MODIFY", table.GetProperty("privileges").EnumerateArray().Select(e => e.GetString()));
        Assert.Contains("zerobuswrite", table.GetProperty("operations").EnumerateArray().Select(e => e.GetString()));
    }

    [Fact]
    public async Task Caches_token_until_near_expiry()
    {
        var calls = 0;
        var handler = new StubHandler(_ =>
        {
            calls++;
            return Task.FromResult(Json(new { access_token = "tok", expires_in = 3600 }));
        });
        using var http = new HttpClient(handler);
        var provider = new OAuthTokenProvider("https://ws.example.com", "1", "c", "s", http);

        await provider.GetTokenAsync("a.b.c", default);
        await provider.GetTokenAsync("a.b.c", default);
        await provider.GetTokenAsync("a.b.c", default);

        Assert.Equal(1, calls);
    }

    [Fact]
    public async Task Throws_auth_exception_on_error_response()
    {
        var handler = new StubHandler(_ => Task.FromResult(
            new HttpResponseMessage(HttpStatusCode.Unauthorized) { Content = new StringContent("{\"error\":\"invalid_client\"}") }));
        using var http = new HttpClient(handler);
        var provider = new OAuthTokenProvider("https://ws.example.com", "1", "c", "s", http);

        await Assert.ThrowsAsync<ZerobusAuthException>(() => provider.GetTokenAsync("a.b.c", default));
    }

    private static HttpResponseMessage Json(object payload) =>
        new(HttpStatusCode.OK) { Content = new StringContent(JsonSerializer.Serialize(payload)) };

    private static Dictionary<string, string> ParseForm(string body) =>
        body.Split('&')
            .Select(p => p.Split('=', 2))
            .ToDictionary(p => Uri.UnescapeDataString(p[0]), p => Uri.UnescapeDataString(p[1]));

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<HttpRequestMessage, Task<HttpResponseMessage>> _responder;
        public StubHandler(Func<HttpRequestMessage, Task<HttpResponseMessage>> responder) => _responder = responder;
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            _responder(request);
    }
}
