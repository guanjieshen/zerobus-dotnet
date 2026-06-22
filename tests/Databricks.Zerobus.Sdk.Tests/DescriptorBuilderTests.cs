using Databricks.Zerobus.TestProto;
using Google.Protobuf.Reflection;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class DescriptorBuilderTests
{
    [Fact]
    public void Build_produces_a_parseable_file_descriptor_containing_the_message()
    {
        var bytes = DescriptorBuilder.Build(AirQuality.Descriptor);

        var fileDescriptor = FileDescriptorProto.Parser.ParseFrom(bytes);
        Assert.Contains(fileDescriptor.MessageType, m => m.Name == "AirQuality");
        var message = fileDescriptor.MessageType.Single(m => m.Name == "AirQuality");
        Assert.Contains(message.Field, f => f.Name == "device_name");
        Assert.Contains(message.Field, f => f.Name == "temp");
        Assert.Contains(message.Field, f => f.Name == "humidity");
    }
}
