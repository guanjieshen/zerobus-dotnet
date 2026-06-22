namespace Databricks.Zerobus.Tests.Infrastructure;

/// <summary>A token provider that returns a constant token without any network call.</summary>
public sealed class FakeTokenProvider : ITokenProvider
{
    private readonly string _token;
    public int CallCount;

    public FakeTokenProvider(string token = "fake-token") => _token = token;

    public Task<string> GetTokenAsync(string tableName, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref CallCount);
        return Task.FromResult(_token);
    }
}
