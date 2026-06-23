using Databricks.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class JsonIngestTests
{
    [Fact]
    public async Task Ingests_json_records_and_assigns_sequential_offsets()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider());

        for (var i = 0; i < 10; i++)
        {
            var offset = await stream.IngestRecordAsync($"{{\"id\":{i}}}");
            Assert.Equal(i, offset);
        }

        await stream.FlushAsync();
        await stream.CloseAsync();

        Assert.Equal("test-stream-1", stream.StreamId);
        Assert.Equal(10, host.Behavior.ReceivedOffsets.Count);
        Assert.Equal(2, host.Behavior.LastRecordType); // JSON == 2 on the wire
        Assert.Equal(9, stream.LastAcknowledgedOffset);
    }

    [Fact]
    public async Task WaitForOffset_completes_after_durability()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider());

        var offset = await stream.IngestRecordAsync("{\"k\":1}");
        await stream.WaitForOffsetAsync(offset).WaitAsync(TimeSpan.FromSeconds(5));

        Assert.True(stream.LastAcknowledgedOffset >= offset);
        await stream.CloseAsync();
    }

    [Fact]
    public async Task Poco_overload_serializes_with_system_text_json()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider());

        var offset = await stream.IngestRecordAsync(new { deviceName = "sensor-1", temp = 22 });
        await stream.FlushAsync();
        await stream.CloseAsync();

        var json = host.Behavior.JsonByOffset[offset];
        Assert.Contains("\"deviceName\":\"sensor-1\"", json);
        Assert.Contains("\"temp\":22", json);
    }

    [Fact]
    public async Task Ack_callback_fires_for_every_record()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var acked = new System.Collections.Concurrent.ConcurrentQueue<long>();
        var options = new StreamConfigurationOptions
        {
            RecordType = RecordType.Json,
            AckCallback = new CallbackProbe(acked),
        };
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider(), options);

        for (var i = 0; i < 5; i++) await stream.IngestRecordAsync($"{{\"id\":{i}}}");
        await stream.FlushAsync();
        await stream.CloseAsync();

        Assert.Equal(new long[] { 0, 1, 2, 3, 4 }, acked.OrderBy(x => x).ToArray());
    }

    [Fact]
    public async Task Oversized_record_is_rejected_without_sending()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider());

        var huge = new string('x', 11 * 1024 * 1024);
        await Assert.ThrowsAsync<ZerobusNonRetryableException>(() => stream.IngestRecordAsync($"{{\"v\":\"{huge}\"}}"));
        await stream.CloseAsync();
    }

    private sealed class CallbackProbe : IAckCallback
    {
        private readonly System.Collections.Concurrent.ConcurrentQueue<long> _acked;
        public CallbackProbe(System.Collections.Concurrent.ConcurrentQueue<long> acked) => _acked = acked;
        public void OnAck(long offset) => _acked.Enqueue(offset);
        public void OnError(long offset, Exception error) { }
    }
}
