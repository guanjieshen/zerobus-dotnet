using Databricks.Solutions.Zerobus;

namespace Databricks.Solutions.Zerobus.Examples.Functions;

/// <summary>A no-network token provider for local development against the fake server.</summary>
public sealed class ConstantTokenProvider : ITokenProvider
{
    private readonly string _token;
    public ConstantTokenProvider(string token) => _token = token;

    public Task<string> GetTokenAsync(string tableName, CancellationToken cancellationToken) =>
        Task.FromResult(_token);
}
