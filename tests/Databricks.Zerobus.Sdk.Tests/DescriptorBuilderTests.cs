using Databricks.Zerobus.TestProto;
using Google.Protobuf.Reflection;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class DescriptorBuilderTests
{
    [Fact]
    public void Build_produces_a_parseable_message_descriptor_with_the_fields()
    {
        var bytes = DescriptorBuilder.Build(AirQuality.Descriptor);

        // The Zerobus server decodes descriptor_proto as a message-level DescriptorProto.
        var message = DescriptorProto.Parser.ParseFrom(bytes);
        Assert.Equal("AirQuality", message.Name);
        Assert.Contains(message.Field, f => f.Name == "device_name");
        Assert.Contains(message.Field, f => f.Name == "temp");
        Assert.Contains(message.Field, f => f.Name == "humidity");
    }
}
