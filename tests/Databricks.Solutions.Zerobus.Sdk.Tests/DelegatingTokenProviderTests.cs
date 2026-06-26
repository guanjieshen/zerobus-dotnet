using Databricks.Solutions.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class DelegatingTokenProviderTests
{
    [Fact]
    public async Task Passes_the_table_name_and_returns_the_callback_token()
    {
        string? seenTable = null;
        var provider = new DelegatingTokenProvider((table, _) =>
        {
            seenTable = table;
            return Task.FromResult("token-from-callback");
        });

        var token = await provider.GetTokenAsync("main.s.t", default);

        Assert.Equal("token-from-callback", token);
        Assert.Equal("main.s.t", seenTable);
    }

    [Fact]
    public async Task Works_as_an_ITokenProvider_for_a_stream()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var tokenProvider = new DelegatingTokenProvider(ct => Task.FromResult("my-own-token"));
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), tokenProvider);

        var offset = await stream.IngestRecordAsync("{\"id\":1}");
        await stream.WaitForOffsetAsync(offset).WaitAsync(TimeSpan.FromSeconds(5));
        await stream.CloseAsync();

        Assert.Equal(1, host.Behavior.TotalRows);
    }
}
