using Databricks.Zerobus.Examples.Functions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

var host = new HostBuilder()
    .ConfigureFunctionsWebApplication()
    .ConfigureServices(services =>
    {
        // One ingestor (and therefore one gRPC channel + reused stream) shared across all invocations.
        services.AddSingleton<IZerobusIngestor, ZerobusIngestor>();
    })
    .Build();

host.Run();
