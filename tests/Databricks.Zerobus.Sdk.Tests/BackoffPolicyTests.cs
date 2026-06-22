using Xunit;

namespace Databricks.Zerobus.Tests;

public class BackoffPolicyTests
{
    [Fact]
    public void Delay_grows_exponentially_then_caps()
    {
        var policy = new BackoffPolicy
        {
            InitialDelay = TimeSpan.FromSeconds(1),
            Multiplier = 2.0,
            MaxDelay = TimeSpan.FromSeconds(8),
        };

        Assert.Equal(TimeSpan.FromSeconds(1), policy.GetDelay(1));
        Assert.Equal(TimeSpan.FromSeconds(2), policy.GetDelay(2));
        Assert.Equal(TimeSpan.FromSeconds(4), policy.GetDelay(3));
        Assert.Equal(TimeSpan.FromSeconds(8), policy.GetDelay(4));
        Assert.Equal(TimeSpan.FromSeconds(8), policy.GetDelay(5)); // capped
        Assert.Equal(TimeSpan.FromSeconds(8), policy.GetDelay(50)); // still capped
    }

    [Fact]
    public void Attempts_below_one_are_treated_as_one()
    {
        var policy = BackoffPolicy.Default;
        Assert.Equal(policy.GetDelay(1), policy.GetDelay(0));
    }
}
