using Google.Protobuf;
using Grpc.Net.Client;
using Wire = Databricks.Zerobus.Grpc;

namespace Databricks.Zerobus;

/// <summary>
/// Entry point for ingesting records into Unity Catalog Delta tables via Zerobus.
/// Construct once per workspace endpoint and reuse it to create one or more streams.
/// </summary>
/// <example>
/// <code>
/// await using var sdk = new ZerobusSdk(serverEndpoint, workspaceUrl);
/// var stream = await sdk.CreateStreamAsync(
///     new TableProperties("main.sales.events"), clientId, clientSecret);
/// long offset = await stream.IngestRecordAsync("{\"id\":1}");
/// await stream.FlushAsync();
/// await stream.CloseAsync();
/// </code>
/// </example>
public sealed class ZerobusSdk : IZerobusSdk
{
    private readonly GrpcChannel _channel;
    private readonly Wire.Zerobus.ZerobusClient _client;
    private readonly string _workspaceUrl;
    private readonly string _workspaceId;
    private readonly bool _ownsChannel;
    private readonly string? _address;

    /// <summary>
    /// Creates an SDK for the given Zerobus server endpoint and workspace URL.
    /// </summary>
    /// <param name="serverEndpoint">
    /// The Zerobus gRPC endpoint, e.g. <c>1234567890.zerobus.us-west-2.cloud.databricks.com</c>.
    /// </param>
    /// <param name="workspaceUrl">
    /// The workspace URL used for OAuth, e.g. <c>https://dbc-….cloud.databricks.com</c>.
    /// </param>
    public ZerobusSdk(string serverEndpoint, string workspaceUrl)
        : this(CreateChannel(serverEndpoint), workspaceUrl, ExtractWorkspaceId(serverEndpoint), ownsChannel: true, address: BuildAddress(serverEndpoint)) { }

    internal ZerobusSdk(GrpcChannel channel, string workspaceUrl, string workspaceId, bool ownsChannel, string? address = null)
    {
        _channel = channel ?? throw new ArgumentNullException(nameof(channel));
        _client = new Wire.Zerobus.ZerobusClient(channel);
        _workspaceUrl = (workspaceUrl ?? throw new ArgumentNullException(nameof(workspaceUrl))).TrimEnd('/');
        _workspaceId = workspaceId;
        _ownsChannel = ownsChannel;
        _address = address;
    }

    /// <summary>Creates a JSON ingest stream authenticated with a service principal.</summary>
    public Task<IZerobusJsonStream> CreateStreamAsync(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var tokenProvider = new OAuthTokenProvider(_workspaceUrl, _workspaceId, clientId, clientSecret);
        return CreateStreamAsync(tableProperties, tokenProvider, options, cancellationToken);
    }

    /// <summary>Creates a JSON ingest stream authenticated with a custom token provider.</summary>
    public async Task<IZerobusJsonStream> CreateStreamAsync(
        TableProperties tableProperties,
        ITokenProvider tokenProvider,
        StreamConfigurationOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (tableProperties is null) throw new ArgumentNullException(nameof(tableProperties));
        options ??= StreamConfigurationOptions.Default;

        var createRequest = new Wire.CreateIngestStreamRequest
        {
            TableName = tableProperties.TableName,
            RecordType = Wire.RecordType.Json,
        };

        var stream = new ZerobusStream(_client, tableProperties.TableName, createRequest, tokenProvider, options);
        await stream.WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
        return stream;
    }

    /// <summary>Creates a Protobuf ingest stream authenticated with a service principal.</summary>
    public Task<IZerobusStream<T>> CreateStreamAsync<T>(
        TableProperties<T> tableProperties,
        string clientId,
        string clientSecret,
        StreamConfigurationOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IMessage<T>, new()
    {
        var tokenProvider = new OAuthTokenProvider(_workspaceUrl, _workspaceId, clientId, clientSecret);
        return CreateStreamAsync(tableProperties, tokenProvider, options, cancellationToken);
    }

    /// <summary>Creates a Protobuf ingest stream authenticated with a custom token provider.</summary>
    public async Task<IZerobusStream<T>> CreateStreamAsync<T>(
        TableProperties<T> tableProperties,
        ITokenProvider tokenProvider,
        StreamConfigurationOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IMessage<T>, new()
    {
        if (tableProperties is null) throw new ArgumentNullException(nameof(tableProperties));
        options ??= new StreamConfigurationOptions { RecordType = RecordType.Proto };

        var createRequest = new Wire.CreateIngestStreamRequest
        {
            TableName = tableProperties.TableName,
            RecordType = Wire.RecordType.Proto,
            DescriptorProto = DescriptorBuilder.Build(tableProperties.Descriptor),
        };

        var stream = new ZerobusStream<T>(_client, tableProperties.TableName, createRequest, tokenProvider, options);
        await stream.WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
        return stream;
    }

    /// <summary>
    /// Creates a high-level Protobuf bulk writer that auto-batches records and fans them out
    /// across <see cref="BulkWriterOptions.Parallelism"/> independent connections. Authenticated with a service principal.
    /// </summary>
    public Task<IZerobusBulkWriter<T>> CreateBulkWriterAsync<T>(
        TableProperties<T> tableProperties,
        string clientId,
        string clientSecret,
        BulkWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IMessage<T>, new()
    {
        var tokenProvider = new OAuthTokenProvider(_workspaceUrl, _workspaceId, clientId, clientSecret);
        return CreateBulkWriterAsync(tableProperties, tokenProvider, options, cancellationToken);
    }

    /// <summary>
    /// Creates a high-level Protobuf bulk writer that auto-batches records and fans them out
    /// across <see cref="BulkWriterOptions.Parallelism"/> independent connections. Authenticated with a custom token provider.
    /// </summary>
    public async Task<IZerobusBulkWriter<T>> CreateBulkWriterAsync<T>(
        TableProperties<T> tableProperties,
        ITokenProvider tokenProvider,
        BulkWriterOptions? options = null,
        CancellationToken cancellationToken = default)
        where T : IMessage<T>, new()
    {
        if (tableProperties is null) throw new ArgumentNullException(nameof(tableProperties));
        if (_address is null)
            throw new InvalidOperationException("CreateBulkWriterAsync requires constructing ZerobusSdk with a server endpoint.");

        options ??= BulkWriterOptions.Default;
        var parallelism = Math.Max(1, options.Parallelism);
        var batchSize = Math.Max(1, options.BatchSize);
        var maxBatchBytes = Math.Max(1, options.MaxBatchBytes);
        var descriptor = DescriptorBuilder.Build(tableProperties.Descriptor);

        var channels = new GrpcChannel[parallelism];
        var streams = new ZerobusStream<T>[parallelism];
        try
        {
            for (var i = 0; i < parallelism; i++)
            {
                // Each parallel stream gets its own channel (connection); streams multiplexed
                // over a single HTTP/2 connection would not increase throughput.
                channels[i] = GrpcChannel.ForAddress(_address);
                var client = new Wire.Zerobus.ZerobusClient(channels[i]);
                var createRequest = new Wire.CreateIngestStreamRequest
                {
                    TableName = tableProperties.TableName,
                    RecordType = Wire.RecordType.Proto,
                    DescriptorProto = descriptor,
                };
                var streamOptions = options.StreamOptions ?? new StreamConfigurationOptions();
                streamOptions.RecordType = RecordType.Proto;
                streams[i] = new ZerobusStream<T>(client, tableProperties.TableName, createRequest, tokenProvider, streamOptions);
                await streams[i].WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            foreach (var stream in streams)
                if (stream is not null) { try { await stream.CloseAsync().ConfigureAwait(false); } catch { /* best effort */ } }
            foreach (var channel in channels) channel?.Dispose();
            throw;
        }

        return new ZerobusBulkWriter<T>(streams, channels, batchSize, maxBatchBytes);
    }

    /// <summary>
    /// Creates a high-level JSON bulk writer that auto-batches records and fans them out across
    /// <see cref="BulkWriterOptions.Parallelism"/> independent connections. Authenticated with a service principal.
    /// </summary>
    public Task<IZerobusJsonBulkWriter> CreateBulkWriterAsync(
        TableProperties tableProperties,
        string clientId,
        string clientSecret,
        BulkWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        var tokenProvider = new OAuthTokenProvider(_workspaceUrl, _workspaceId, clientId, clientSecret);
        return CreateBulkWriterAsync(tableProperties, tokenProvider, options, cancellationToken);
    }

    /// <summary>
    /// Creates a high-level JSON bulk writer that auto-batches records and fans them out across
    /// <see cref="BulkWriterOptions.Parallelism"/> independent connections. Authenticated with a custom token provider.
    /// </summary>
    public async Task<IZerobusJsonBulkWriter> CreateBulkWriterAsync(
        TableProperties tableProperties,
        ITokenProvider tokenProvider,
        BulkWriterOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        if (tableProperties is null) throw new ArgumentNullException(nameof(tableProperties));
        if (_address is null)
            throw new InvalidOperationException("CreateBulkWriterAsync requires constructing ZerobusSdk with a server endpoint.");

        options ??= BulkWriterOptions.Default;
        var parallelism = Math.Max(1, options.Parallelism);
        var batchSize = Math.Max(1, options.BatchSize);
        var maxBatchBytes = Math.Max(1, options.MaxBatchBytes);

        var channels = new GrpcChannel[parallelism];
        var streams = new ZerobusStream[parallelism];
        try
        {
            for (var i = 0; i < parallelism; i++)
            {
                channels[i] = GrpcChannel.ForAddress(_address);
                var client = new Wire.Zerobus.ZerobusClient(channels[i]);
                var createRequest = new Wire.CreateIngestStreamRequest
                {
                    TableName = tableProperties.TableName,
                    RecordType = Wire.RecordType.Json,
                };
                var streamOptions = options.StreamOptions ?? new StreamConfigurationOptions();
                streamOptions.RecordType = RecordType.Json;
                streams[i] = new ZerobusStream(client, tableProperties.TableName, createRequest, tokenProvider, streamOptions);
                await streams[i].WaitUntilReadyAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            foreach (var stream in streams)
                if (stream is not null) { try { await stream.CloseAsync().ConfigureAwait(false); } catch { /* best effort */ } }
            foreach (var channel in channels) channel?.Dispose();
            throw;
        }

        return new ZerobusBulkWriter(streams, channels, batchSize, maxBatchBytes);
    }

    private static GrpcChannel CreateChannel(string serverEndpoint) => GrpcChannel.ForAddress(BuildAddress(serverEndpoint));

    private static string BuildAddress(string serverEndpoint)
    {
        if (string.IsNullOrWhiteSpace(serverEndpoint))
            throw new ArgumentException("Server endpoint is required.", nameof(serverEndpoint));
        return serverEndpoint.Contains("://") ? serverEndpoint : "https://" + serverEndpoint;
    }

    internal static string ExtractWorkspaceId(string serverEndpoint)
    {
        var host = serverEndpoint;
        var scheme = host.IndexOf("://", StringComparison.Ordinal);
        if (scheme >= 0) host = host.Substring(scheme + 3);
        var slash = host.IndexOf('/');
        if (slash >= 0) host = host.Substring(0, slash);
        var colon = host.IndexOf(':');
        if (colon >= 0) host = host.Substring(0, colon);
        var dot = host.IndexOf('.');
        return dot >= 0 ? host.Substring(0, dot) : host;
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        if (_ownsChannel) _channel.Dispose();
        return default;
    }
}
