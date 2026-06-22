using Databricks.Zerobus.TestProto;
using Databricks.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class BatchIngestTests
{
    [Fact]
    public async Task Json_batch_is_ingested_as_a_single_offset()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("a.b.c"), new FakeTokenProvider());

        var offset = await stream.IngestRecordBatchAsync(new[] { "{\"id\":1}", "{\"id\":2}", "{\"id\":3}" });
        await stream.WaitForOffsetAsync(offset).WaitAsync(TimeSpan.FromSeconds(5));
        await stream.CloseAsync();

        Assert.Equal(0, offset);
        Assert.True(host.Behavior.ReceivedOffsets.ContainsKey(0));
    }

    [Fact]
    public async Task Proto_batch_is_ingested_as_a_single_offset()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties<AirQuality>("a.b.air"), new FakeTokenProvider());

        var records = Enumerable.Range(0, 4).Select(i => new AirQuality { DeviceName = $"s{i}", Temp = i });
        var offset = await stream.IngestRecordBatchAsync(records);
        await stream.WaitForOffsetAsync(offset).WaitAsync(TimeSpan.FromSeconds(5));
        await stream.CloseAsync();

        Assert.True(host.Behavior.ReceivedOffsets.ContainsKey(offset));
    }
}
