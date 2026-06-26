using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class TablePropertiesTests
{
    [Theory]
    [InlineData("")]
    [InlineData("   ")]
    [InlineData("onlyone")]
    [InlineData("catalog.schema")]
    [InlineData("a.b.c.d")]
    [InlineData("catalog..table")]
    public void Invalid_table_names_are_rejected(string name)
    {
        Assert.Throws<ArgumentException>(() => new TableProperties(name));
    }

    [Fact]
    public void Valid_three_part_name_is_accepted()
    {
        var props = new TableProperties("main.sales.events");
        Assert.Equal("main.sales.events", props.TableName);
    }
}
