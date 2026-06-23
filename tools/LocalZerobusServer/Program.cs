using LocalZerobusServer;
using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

// gRPC over HTTP/2 cleartext (h2c) on a fixed local port.
var port = int.TryParse(Environment.GetEnvironmentVariable("ZEROBUS_FAKE_PORT"), out var p) ? p : 5005;
builder.WebHost.ConfigureKestrel(options =>
    options.ListenLocalhost(port, listen => listen.Protocols = HttpProtocols.Http2));

builder.Services.AddGrpc();

var app = builder.Build();
app.MapGrpcService<FakeZerobusService>();

app.Logger.LogInformation("Fake Zerobus server listening on http://localhost:{Port} (h2c)", port);
app.Run();
