using Databricks.Solutions.Zerobus.Tests.Infrastructure;
using Grpc.Core;
using Xunit;

namespace Databricks.Solutions.Zerobus.Tests;

public class ErrorHandlingTests
{
    [Fact]
    public async Task Create_with_invalid_argument_surfaces_non_retryable()
    {
        var behavior = new ServerBehavior { FailCreateWith = StatusCode.InvalidArgument };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();

        await Assert.ThrowsAsync<ZerobusNonRetryableException>(() =>
            sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider()));
    }

    [Theory]
    [InlineData(StatusCode.Unauthenticated)]
    [InlineData(StatusCode.PermissionDenied)]
    public async Task Create_with_auth_failure_surfaces_auth_exception(StatusCode code)
    {
        var behavior = new ServerBehavior { FailCreateWith = code };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();

        await Assert.ThrowsAsync<ZerobusAuthException>(() =>
            sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider()));
    }

    [Fact]
    public async Task Retryable_create_failures_exhaust_attempts_and_surface_stream_closed()
    {
        var behavior = new ServerBehavior { FailCreateWith = StatusCode.Unavailable };
        await using var host = await ZerobusTestHost.StartAsync(behavior);
        await using var sdk = host.CreateSdk();

        var options = new StreamConfigurationOptions
        {
            Recovery = new BackoffPolicy { InitialDelay = TimeSpan.FromMilliseconds(5), MaxAttempts = 2 },
        };

        await Assert.ThrowsAsync<ZerobusStreamClosedException>(() =>
            sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider(), options));
    }

    [Fact]
    public async Task Auth_failure_from_the_token_provider_is_not_retried()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();

        var attempts = 0;
        var tokenProvider = new DelegatingTokenProvider((_, _) =>
        {
            Interlocked.Increment(ref attempts);
            throw new ZerobusAuthException("bad credentials");
        });

        await Assert.ThrowsAsync<ZerobusAuthException>(() =>
            sdk.CreateStreamAsync(new TableProperties("main.s.t"), tokenProvider));

        Assert.Equal(1, attempts); // fatal, not retried
    }

    [Fact]
    public async Task Ingesting_after_close_throws_stream_closed()
    {
        await using var host = await ZerobusTestHost.StartAsync();
        await using var sdk = host.CreateSdk();
        var stream = await sdk.CreateStreamAsync(new TableProperties("main.s.t"), new FakeTokenProvider());

        await stream.IngestRecordAsync("{\"id\":1}");
        await stream.CloseAsync();

        await Assert.ThrowsAsync<ZerobusStreamClosedException>(() => stream.IngestRecordAsync("{\"id\":2}"));
    }
}
