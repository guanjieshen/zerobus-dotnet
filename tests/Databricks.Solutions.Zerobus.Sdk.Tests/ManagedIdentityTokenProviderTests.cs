using System.Net;
using System.Net.Http;
using System.Text.Json;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class ManagedIdentityTokenProviderTests
{
    private const string DatabricksAppId = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d";

    [Fact]
    public async Task App_service_path_fetches_mi_token_then_exchanges_it()
    {
        HttpRequestMessage? identityReq = null;
        HttpRequestMessage? exchangeReq = null;
        string? exchangeBody = null;

        var handler = new StubHandler(async req =>
        {
            if (IsExchange(req))
            {
                exchangeReq = req;
                exchangeBody = await req.Content!.ReadAsStringAsync();
                return Json(new { access_token = "databricks-tok", expires_in = 3600 });
            }
            identityReq = req;
            return Json(new { access_token = "entra-mi-tok", expires_in = "3600" });
        });

        using var http = new HttpClient(handler);
        using var provider = new ManagedIdentityTokenProvider(
            "https://adb-123.8.azuredatabricks.net", "123",
            identityHttpClient: http, exchangeHttpClient: http,
            identityEndpoint: "http://localhost/msi/token", identityHeader: "secret-abc");

        var token = await provider.GetTokenAsync("main.sales.events", default);

        Assert.Equal("databricks-tok", token);

        // Identity endpoint (App Service / Functions dialect).
        Assert.Equal(HttpMethod.Get, identityReq!.Method);
        Assert.Equal("secret-abc", identityReq.Headers.GetValues("X-IDENTITY-HEADER").Single());
        var identityQuery = identityReq.RequestUri!.Query;
        Assert.Contains("api-version=2019-08-01", identityQuery);
        Assert.Contains(DatabricksAppId, Uri.UnescapeDataString(identityQuery));

        // Exchange carries the MI token as the subject and Zerobus scoping.
        var form = ParseForm(exchangeBody!);
        Assert.Equal("urn:ietf:params:oauth:grant-type:token-exchange", form["grant_type"]);
        Assert.Equal("entra-mi-tok", form["subject_token"]);
        Assert.Contains("zerobusDirectWriteApi", form["resource"]);
        Assert.Contains("zerobuswrite", form["authorization_details"]);
        Assert.Contains("main.sales.events", form["authorization_details"]);
    }

    [Fact]
    public async Task Vm_path_uses_the_imds_endpoint_when_no_identity_endpoint_is_set()
    {
        HttpRequestMessage? identityReq = null;
        var handler = new StubHandler(req =>
        {
            if (IsExchange(req)) return Task.FromResult(Json(new { access_token = "databricks-tok", expires_in = 3600 }));
            identityReq = req;
            return Task.FromResult(Json(new { access_token = "entra-mi-tok", expires_in = "3600" }));
        });

        using var http = new HttpClient(handler);
        // Empty identityEndpoint forces the IMDS branch without reading process env vars.
        using var provider = new ManagedIdentityTokenProvider(
            "https://adb-123.8.azuredatabricks.net", "123",
            identityHttpClient: http, exchangeHttpClient: http, identityEndpoint: "");

        await provider.GetTokenAsync("main.s.t", default);

        Assert.Equal("169.254.169.254", identityReq!.RequestUri!.Host);
        Assert.Equal("true", identityReq.Headers.GetValues("Metadata").Single());
        Assert.Contains("api-version=2018-02-01", identityReq.RequestUri.Query);
    }

    [Fact]
    public async Task User_assigned_identity_adds_the_client_id_to_the_identity_request()
    {
        HttpRequestMessage? identityReq = null;
        var handler = new StubHandler(req =>
        {
            if (IsExchange(req)) return Task.FromResult(Json(new { access_token = "databricks-tok", expires_in = 3600 }));
            identityReq = req;
            return Task.FromResult(Json(new { access_token = "entra-mi-tok", expires_in = "3600" }));
        });

        using var http = new HttpClient(handler);
        using var provider = new ManagedIdentityTokenProvider(
            "https://adb-123.8.azuredatabricks.net", "123",
            managedIdentityClientId: "user-mi-client-id",
            identityHttpClient: http, exchangeHttpClient: http,
            identityEndpoint: "http://localhost/msi/token", identityHeader: "secret-abc");

        await provider.GetTokenAsync("main.s.t", default);

        Assert.Contains("client_id=user-mi-client-id", identityReq!.RequestUri!.Query);
    }

    [Fact]
    public async Task Caches_the_exchanged_token_so_the_identity_endpoint_is_hit_once()
    {
        var identityCalls = 0;
        var exchangeCalls = 0;
        var handler = new StubHandler(req =>
        {
            if (IsExchange(req))
            {
                Interlocked.Increment(ref exchangeCalls);
                return Task.FromResult(Json(new { access_token = "databricks-tok", expires_in = 3600 }));
            }
            Interlocked.Increment(ref identityCalls);
            return Task.FromResult(Json(new { access_token = "entra-mi-tok", expires_in = "3600" }));
        });

        using var http = new HttpClient(handler);
        using var provider = new ManagedIdentityTokenProvider(
            "https://adb-123.8.azuredatabricks.net", "123",
            identityHttpClient: http, exchangeHttpClient: http,
            identityEndpoint: "http://localhost/msi/token", identityHeader: "secret-abc");

        await provider.GetTokenAsync("main.s.t", default);
        await provider.GetTokenAsync("main.s.t", default);

        Assert.Equal(1, identityCalls);
        Assert.Equal(1, exchangeCalls);
    }

    private static bool IsExchange(HttpRequestMessage req) =>
        req.RequestUri!.AbsolutePath.Contains("oidc");

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
