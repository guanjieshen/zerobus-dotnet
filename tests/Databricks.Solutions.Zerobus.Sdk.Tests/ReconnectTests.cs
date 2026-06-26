using Databricks.Solutions.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class ReconnectTests
{
    private static StreamConfigurationOptions FastRecovery() => new()
    {
        RecordType = RecordType.Json,
        Recovery = new BackoffPolicy { InitialDelay = TimeSpan.FromMilliseconds(25), MaxDelay = TimeSpan.FromMilliseconds(100), MaxAttempts = 20 },
    };

    [Fact]
    public async Task Reconnects_and_replays_unacked_records_after_a_disconnect()
    {
        var behavior = new ServerBehavior { AbortFirstConnectionAfterRecords = 5 };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("a.b.c"), new FakeTokenProvider(), FastRecovery());

        for (var i = 0; i < 20; i++)
            await stream.IngestRecordAsync($"{{\"id\":{i}}}");

        await stream.FlushAsync().WaitAsync(TimeSpan.FromSeconds(15));
        await stream.CloseAsync();

        // At-least-once delivery: every distinct record is durable despite the disconnect.
        // (Offsets restart at 0 on the reconnected stream, so assert on record content.)
        Assert.Equal(20, behavior.JsonRecords.Count);
        for (var i = 0; i < 20; i++) Assert.True(behavior.JsonRecords.ContainsKey($"{{\"id\":{i}}}"));
        Assert.True(behavior.ConnectionCount >= 2, $"expected a reconnect, saw {behavior.ConnectionCount} connection(s)");
    }

    [Fact]
    public async Task Honors_server_close_signal_by_reconnecting()
    {
        var behavior = new ServerBehavior { CloseSignalAfterRecords = 3 };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("a.b.c"), new FakeTokenProvider(), FastRecovery());

        for (var i = 0; i < 10; i++)
            await stream.IngestRecordAsync($"{{\"id\":{i}}}");

        await stream.FlushAsync().WaitAsync(TimeSpan.FromSeconds(15));
        await stream.CloseAsync();

        Assert.Equal(10, behavior.JsonRecords.Count);
        Assert.True(behavior.ConnectionCount >= 2, $"expected a reconnect after close signal, saw {behavior.ConnectionCount}");
    }
}
