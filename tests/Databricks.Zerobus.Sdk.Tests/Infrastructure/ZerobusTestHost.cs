using Grpc.Net.Client;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Databricks.Zerobus.Tests.Infrastructure;

/// <summary>
/// Hosts <see cref="InMemoryZerobusServer"/> on a real Kestrel HTTP/2 (cleartext) endpoint
/// on a dynamic localhost port, and builds <see cref="ZerobusSdk"/> instances pointed at it.
/// </summary>
public sealed class ZerobusTestHost : IAsyncDisposable
{
    private readonly WebApplication _app;

    public ServerBehavior Behavior { get; }
    public int Port { get; }

    private ZerobusTestHost(WebApplication app, ServerBehavior behavior, int port)
    {
        _app = app;
        Behavior = behavior;
        Port = port;
    }

    public static async Task<ZerobusTestHost> StartAsync(ServerBehavior? behavior = null)
    {
        behavior ??= new ServerBehavior();

        var builder = WebApplication.CreateBuilder();
        builder.Logging.SetMinimumLevel(LogLevel.Warning);
        builder.WebHost.ConfigureKestrel(options =>
            options.Listen(System.Net.IPAddress.Loopback, 0, listen => listen.Protocols = HttpProtocols.Http2));
        builder.Services.AddGrpc();
        builder.Services.AddSingleton(behavior);

        var app = builder.Build();
        app.MapGrpcService<InMemoryZerobusServer>();
        await app.StartAsync();

        var address = app.Urls.First();
        var port = new Uri(address).Port;
        return new ZerobusTestHost(app, behavior, port);
    }

    /// <summary>Creates an SDK pointed at this host, using a fake (no-network) token provider by default.</summary>
    public ZerobusSdk CreateSdk()
    {
        var channel = GrpcChannel.ForAddress($"http://localhost:{Port}");
        return new ZerobusSdk(channel, "https://example.databricks.com", "1234567890", ownsChannel: true);
    }

    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync();
        await _app.DisposeAsync();
    }
}
