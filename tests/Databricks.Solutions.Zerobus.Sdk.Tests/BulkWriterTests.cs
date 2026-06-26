using Databricks.Solutions.Zerobus.TestProto;
using Databricks.Solutions.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class BulkWriterTests
{
    [Fact]
    public async Task Writes_a_list_and_singles_auto_batched_across_parallel_streams()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var options = new BulkWriterOptions { Parallelism = 3, BatchSize = 100 };
        await using var writer = await sdk.CreateBulkWriterAsync(
            new TableProperties<AirQuality>("main.s.air"), new FakeTokenProvider(), options);

        Assert.Equal(3, writer.Parallelism);

        await writer.WriteAsync(Enumerable.Range(0, 1000).Select(i => new AirQuality { DeviceName = $"s{i}", Temp = i }));
        await writer.WriteAsync(new AirQuality { DeviceName = "single", Temp = 1 });
        await writer.FlushAsync();

        Assert.Equal(1001, host.Behavior.TotalRows);
        Assert.True(host.Behavior.ConnectionCount >= 3, $"expected >= 3 streams, saw {host.Behavior.ConnectionCount}");
    }

    [Fact]
    public async Task Buffered_singles_are_flushed_on_dispose()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var options = new BulkWriterOptions { Parallelism = 2, BatchSize = 1000 };
        var writer = await sdk.CreateBulkWriterAsync(
            new TableProperties<AirQuality>("main.s.air"), new FakeTokenProvider(), options);

        // Fewer than BatchSize, so nothing is dispatched until flush/dispose.
        for (var i = 0; i < 50; i++) await writer.WriteAsync(new AirQuality { DeviceName = $"d{i}" });
        Assert.Equal(0, host.Behavior.TotalRows);

        await writer.DisposeAsync();
        Assert.Equal(50, host.Behavior.TotalRows);
    }

    [Fact]
    public async Task Byte_cap_splits_into_multiple_batches_regardless_of_batch_size()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        // Huge row cap but a tiny byte cap, so the byte ceiling forces multiple batches.
        var options = new BulkWriterOptions { Parallelism = 1, BatchSize = 1_000_000, MaxBatchBytes = 200 };
        await using var writer = await sdk.CreateBulkWriterAsync(
            new TableProperties<AirQuality>("main.s.air"), new FakeTokenProvider(), options);

        await writer.WriteAsync(Enumerable.Range(0, 50).Select(i => new AirQuality { DeviceName = $"device-{i}", Temp = i }));
        await writer.FlushAsync();

        Assert.Equal(50, host.Behavior.TotalRows);
        Assert.True(host.Behavior.TotalReceived > 1, $"expected the byte cap to force multiple batches, saw {host.Behavior.TotalReceived}");
    }

    [Fact]
    public async Task Json_bulk_writer_sends_strings_and_pocos()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var options = new BulkWriterOptions { Parallelism = 2, BatchSize = 10 };
        await using var writer = await sdk.CreateBulkWriterAsync(
            new TableProperties("main.s.events"), new FakeTokenProvider(), options);

        await writer.WriteAsync(Enumerable.Range(0, 25).Select(i => $"{{\"id\":{i}}}"));
        await writer.WriteAsync("{\"id\":999}");                       // single string
        await writer.WriteAsync(new { device = "sensor-1", temp = 22 }); // POCO -> JSON
        await writer.FlushAsync();

        Assert.Equal(27, host.Behavior.TotalRows);
    }
}
