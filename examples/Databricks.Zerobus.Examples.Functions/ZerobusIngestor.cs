using Databricks.Zerobus;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace Databricks.Zerobus.Examples.Functions;

/// <summary>Ingests JSON records into Zerobus, reusing a single SDK channel and stream across invocations.</summary>
public interface IZerobusIngestor
{
    Task<long> IngestAsync(string jsonRecord, CancellationToken cancellationToken);
    Task FlushAsync(CancellationToken cancellationToken);
}

/// <summary>
/// Singleton ingestor. The <see cref="ZerobusSdk"/> (and its gRPC channel) and the ingest
/// stream are both created lazily and reused across function invocations — opening a fresh
/// stream per request would pay the auth + handshake cost every time. On a stream fault the
/// stream is discarded and recreated on the next call.
/// </summary>
public sealed class ZerobusIngestor : IZerobusIngestor, IAsyncDisposable
{
    private readonly ZerobusSdk _sdk;
    private readonly ILogger<ZerobusIngestor> _logger;
    private readonly string _tableName;
    private readonly bool _useFakeToken;
    private readonly string? _clientId;
    private readonly string? _clientSecret;

    private readonly SemaphoreSlim _gate = new(1, 1);
    private ZerobusStream? _stream;

    public ZerobusIngestor(IConfiguration config, ILogger<ZerobusIngestor> logger)
    {
        _logger = logger;
        var serverEndpoint = Required(config, "ZEROBUS_SERVER_ENDPOINT");
        var workspaceUrl = Required(config, "DATABRICKS_WORKSPACE_URL");
        _tableName = Required(config, "ZEROBUS_TABLE_NAME");
        _useFakeToken = string.Equals(config["ZEROBUS_USE_FAKE_TOKEN"], "true", StringComparison.OrdinalIgnoreCase);
        _clientId = config["DATABRICKS_CLIENT_ID"];
        _clientSecret = config["DATABRICKS_CLIENT_SECRET"];

        _sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
    }

    public async Task<long> IngestAsync(string jsonRecord, CancellationToken cancellationToken)
    {
        var stream = await GetStreamAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return await stream.IngestRecordAsync(jsonRecord, cancellationToken).ConfigureAwait(false);
        }
        catch (ZerobusException ex)
        {
            _logger.LogWarning(ex, "Ingest failed; discarding stream so it is recreated on the next call.");
            await ResetAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async Task FlushAsync(CancellationToken cancellationToken)
    {
        var stream = Volatile.Read(ref _stream);
        if (stream is not null) await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<ZerobusStream> GetStreamAsync(CancellationToken cancellationToken)
    {
        var existing = Volatile.Read(ref _stream);
        if (existing is not null) return existing;

        await _gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (_stream is not null) return _stream;

            var options = new StreamConfigurationOptions { RecordType = RecordType.Json };
            var tableProperties = new TableProperties(_tableName);

            _stream = _useFakeToken
                ? await _sdk.CreateStreamAsync(tableProperties, new ConstantTokenProvider("local-dev-token"), options, cancellationToken).ConfigureAwait(false)
                : await _sdk.CreateStreamAsync(tableProperties, _clientId!, _clientSecret!, options, cancellationToken).ConfigureAwait(false);

            _logger.LogInformation("Opened Zerobus stream {StreamId} for {Table}", _stream.StreamId, _tableName);
            return _stream;
        }
        finally
        {
            _gate.Release();
        }
    }

    private async Task ResetAsync()
    {
        await _gate.WaitAsync().ConfigureAwait(false);
        try
        {
            if (_stream is not null)
            {
                try { await _stream.CloseAsync().ConfigureAwait(false); } catch { /* ignore */ }
                _stream = null;
            }
        }
        finally
        {
            _gate.Release();
        }
    }

    private static string Required(IConfiguration config, string key) =>
        config[key] is { Length: > 0 } value
            ? value
            : throw new InvalidOperationException($"Missing configuration value '{key}'.");

    public async ValueTask DisposeAsync()
    {
        if (_stream is not null) await _stream.CloseAsync().ConfigureAwait(false);
        await _sdk.DisposeAsync().ConfigureAwait(false);
        _gate.Dispose();
    }
}
