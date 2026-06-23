using Databricks.Zerobus.ProtoGen;
using Xunit;

namespace Databricks.Zerobus.Tests;

public class ProtoGenTests
{
    [Theory]
    [InlineData("STRING", "string")]
    [InlineData("INT", "int32")]
    [InlineData("LONG", "int64")]
    [InlineData("SHORT", "int32")]
    [InlineData("FLOAT", "float")]
    [InlineData("DOUBLE", "double")]
    [InlineData("BOOLEAN", "bool")]
    [InlineData("BINARY", "bytes")]
    [InlineData("DATE", "int32")]
    [InlineData("TIMESTAMP", "int64")]
    [InlineData("DECIMAL", "string")]
    public void MapType_maps_known_delta_types(string delta, string proto)
        => Assert.Equal(proto, ProtoSchemaGenerator.MapType(delta));

    [Fact]
    public void MapType_rejects_unsupported_types()
        => Assert.Throws<NotSupportedException>(() => ProtoSchemaGenerator.MapType("STRUCT"));

    [Fact]
    public void Generate_emits_proto3_with_optional_fields_in_order()
    {
        var columns = new[]
        {
            new ColumnSchema("device_id", "STRING", false),
            new ColumnSchema("temp_c", "DOUBLE", true),
            new ColumnSchema("reading_ts", "TIMESTAMP", true),
        };

        var proto = ProtoSchemaGenerator.Generate("SensorReading", columns, "MyApp.Telemetry");

        Assert.Contains("syntax = \"proto3\";", proto);
        Assert.Contains("option csharp_namespace = \"MyApp.Telemetry\";", proto);
        Assert.Contains("message SensorReading {", proto);
        Assert.Contains("optional string device_id = 1;", proto);
        Assert.Contains("optional double temp_c = 2;", proto);
        Assert.Contains("optional int64 reading_ts = 3;", proto);
    }

    [Theory]
    [InlineData("main.telemetry.sensor_readings", "SensorReadings")]
    [InlineData("cat.sch.events", "Events")]
    public void MessageNameFromTable_pascal_cases_the_leaf(string table, string expected)
        => Assert.Equal(expected, ProtoSchemaGenerator.MessageNameFromTable(table));
}
