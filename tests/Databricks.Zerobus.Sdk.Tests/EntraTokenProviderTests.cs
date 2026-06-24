using System.Net;
using System.Net.Http;
using System.Text.Json;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class EntraTokenProviderTests
{
    [Fact]
    public async Task Requests_an_entra_token_with_the_databricks_resource_scope()
    {
        HttpRequestMessage? captured = null;
        string? body = null;
        var handler = new StubHandler(async req =>
        {
            captured = req;
            body = await req.Content!.ReadAsStringAsync();
            return Json(new { access_token = "entra-tok", expires_in = 3600 });
        });

        using var http = new HttpClient(handler);
        var provider = new EntraTokenProvider("my-tenant", "my-client", "my-secret", http);

        var token = await provider.GetTokenAsync("main.s.t", default);

        Assert.Equal("entra-tok", token);
        Assert.Equal("https://login.microsoftonline.com/my-tenant/oauth2/v2.0/token", captured!.RequestUri!.ToString());

        var form = ParseForm(body!);
        Assert.Equal("client_credentials", form["grant_type"]);
        Assert.Equal("my-client", form["client_id"]);
        Assert.Equal("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default", form["scope"]);
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
        var provider = new EntraTokenProvider("t", "c", "s", http);

        await provider.GetTokenAsync("a.b.c", default);
        await provider.GetTokenAsync("a.b.c", default);

        Assert.Equal(1, calls);
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
