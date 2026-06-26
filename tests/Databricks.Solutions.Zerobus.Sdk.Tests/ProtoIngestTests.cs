using Databricks.Solutions.Zerobus.TestProto;
using Databricks.Solutions.Zerobus.Tests.Infrastructure;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class ProtoIngestTests
{
    [Fact]
    public async Task Ingests_protobuf_records_and_sends_descriptor()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var stream = await sdk.CreateStreamAsync(
            new TableProperties<AirQuality>("main.s.air"), new FakeTokenProvider());

        var offset = await stream.IngestRecordAsync(new AirQuality { DeviceName = "sensor-1", Temp = 22, Humidity = 55 });
        await stream.WaitForOffsetAsync(offset).WaitAsync(TimeSpan.FromSeconds(5));
        await stream.CloseAsync();

        Assert.Equal(1, host.Behavior.LastRecordType); // PROTO == 1 on the wire
        Assert.NotNull(host.Behavior.LastDescriptorProto);

        var decoded = AirQuality.Parser.ParseFrom(host.Behavior.ProtoByOffset[offset]);
        Assert.Equal("sensor-1", decoded.DeviceName);
        Assert.Equal(22, decoded.Temp);
        Assert.Equal(55, decoded.Humidity);
    }
}
