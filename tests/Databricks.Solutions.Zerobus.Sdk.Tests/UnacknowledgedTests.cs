using System.Collections.Concurrent;
using System.Text;
using Databricks.Solutions.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class UnacknowledgedTests
{
    [Fact]
    public async Task GetUnacknowledgedRecords_returns_records_the_server_never_acked()
    {
        var behavior = new ServerBehavior { SuppressAcks = true };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();

        // Short flush timeout so CloseAsync's best-effort flush does not hang on the missing acks.
        var options = new StreamConfigurationOptions { RecordType = RecordType.Json, FlushTimeout = TimeSpan.FromSeconds(1) };
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider(), options);

        for (var i = 0; i < 5; i++) await stream.IngestRecordAsync($"{{\"id\":{i}}}");

        var unacked = stream.GetUnacknowledgedRecords();
        Assert.Equal(5, unacked.Count);
        Assert.Equal("{\"id\":0}", Encoding.UTF8.GetString(unacked[0]));
        Assert.Equal("{\"id\":4}", Encoding.UTF8.GetString(unacked[4]));

        await stream.CloseAsync();
    }

    [Fact]
    public async Task OnError_fires_per_unacked_offset_on_terminal_failure()
    {
        var behavior = new ServerBehavior { SuppressAcks = true, AbortEveryConnectionAfterRecords = 2 };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();

        var errored = new ConcurrentQueue<long>();
        var options = new StreamConfigurationOptions
        {
            RecordType = RecordType.Json,
            AckCallback = new ErrorRecordingCallback(errored),
            Recovery = new BackoffPolicy { InitialDelay = TimeSpan.FromMilliseconds(5), MaxAttempts = 1 },
        };
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider(), options);

        for (var i = 0; i < 5; i++) await stream.IngestRecordAsync($"{{\"id\":{i}}}");

        // The repeated aborts exhaust recovery; the flush surfaces the terminal failure.
        await Assert.ThrowsAsync<ZerobusStreamClosedException>(() => stream.FlushAsync().WaitAsync(TimeSpan.FromSeconds(10)));

        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, errored.OrderBy(x => x).ToArray());
    }

    private sealed class ErrorRecordingCallback : IAckCallback
    {
        private readonly ConcurrentQueue<long> _errored;
        public ErrorRecordingCallback(ConcurrentQueue<long> errored) => _errored = errored;
        public void OnAck(long offset) { }
        public void OnError(long offset, Exception error) => _errored.Enqueue(offset);
    }
}
