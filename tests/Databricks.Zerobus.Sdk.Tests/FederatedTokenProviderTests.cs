using System.Net;
using System.Net.Http;
using System.Text.Json;
using Databricks.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class FederatedTokenProviderTests
{
    [Fact]
    public async Task Exchanges_the_subject_jwt_for_a_zerobus_scoped_token()
    {
        HttpRequestMessage? captured = null;
        string? body = null;
        var subjectCalls = 0;
        var handler = new StubHandler(async req =>
        {
            captured = req;
            body = await req.Content!.ReadAsStringAsync();
            return Json(new { access_token = "databricks-tok", expires_in = 3600 });
        });

        using var http = new HttpClient(handler);
        var provider = new FederatedTokenProvider(
            "https://adb-123.8.azuredatabricks.net", "123",
            ct => { subjectCalls++; return Task.FromResult("entra-jwt"); },
            clientId: "sp-uuid", httpClient: http);

        var token = await provider.GetTokenAsync("main.sales.events", default);

        Assert.Equal("databricks-tok", token);
        Assert.Equal(1, subjectCalls);
        Assert.Equal("https://adb-123.8.azuredatabricks.net/oidc/v1/token", captured!.RequestUri!.ToString());

        var form = ParseForm(body!);
        Assert.Equal("urn:ietf:params:oauth:grant-type:token-exchange", form["grant_type"]);
        Assert.Equal("entra-jwt", form["subject_token"]);
        Assert.Equal("urn:ietf:params:oauth:token-type:jwt", form["subject_token_type"]);
        Assert.Equal("sp-uuid", form["client_id"]);
        Assert.Contains("zerobusDirectWriteApi", form["resource"]);
        Assert.Contains("zerobuswrite", form["authorization_details"]);
    }

    [Fact]
    public async Task Works_end_to_end_with_a_mocked_exchange_and_in_memory_server()
    {
        var handler = new StubHandler(_ => Task.FromResult(Json(new { access_token = "exchanged-tok", expires_in = 3600 })));
        using var http = new HttpClient(handler);
        var provider = new FederatedTokenProvider(
            "https://adb-123.8.azuredatabricks.net", "123",
            ct => Task.FromResult("entra-jwt"), httpClient: http);

        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), provider);
        var offset = await stream.IngestRecordAsync("{\"id\":1}");
        await stream.WaitForOffsetAsync(offset).WaitAsync(TimeSpan.FromSeconds(5));
        await stream.CloseAsync();

        Assert.Equal(1, host.Behavior.TotalRows);
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
