using Google.Protobuf;

namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Entry point for creating Zerobus ingest streams and bulk writers. Depend on this interface
/// (rather than <see cref="ZerobusSdk"/>) to keep consumers testable and DI-friendly.
/// </summary>
public interface IZerobusSdk : IAsyncDisposable
{
    /// <summary>Creates a JSON ingest stream authenticated with a service principal.</summary>
    Task<IZerobusJsonStream> CreateStreamAsync(TableProperties tableProperties, string clientId, string clientSecret, StreamConfigurationOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Creates a JSON ingest stream authenticated with a custom token provider.</summary>
    Task<IZerobusJsonStream> CreateStreamAsync(TableProperties tableProperties, ITokenProvider tokenProvider, StreamConfigurationOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Creates a Protobuf ingest stream authenticated with a service principal.</summary>
    Task<IZerobusStream<T>> CreateStreamAsync<T>(TableProperties<T> tableProperties, string clientId, string clientSecret, StreamConfigurationOptions? options = null, CancellationToken cancellationToken = default) where T : IMessage<T>, new();

    /// <summary>Creates a Protobuf ingest stream authenticated with a custom token provider.</summary>
    Task<IZerobusStream<T>> CreateStreamAsync<T>(TableProperties<T> tableProperties, ITokenProvider tokenProvider, StreamConfigurationOptions? options = null, CancellationToken cancellationToken = default) where T : IMessage<T>, new();

    /// <summary>Creates a high-level JSON bulk writer authenticated with a service principal.</summary>
    Task<IZerobusJsonBulkWriter> CreateBulkWriterAsync(TableProperties tableProperties, string clientId, string clientSecret, BulkWriterOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Creates a high-level JSON bulk writer authenticated with a custom token provider.</summary>
    Task<IZerobusJsonBulkWriter> CreateBulkWriterAsync(TableProperties tableProperties, ITokenProvider tokenProvider, BulkWriterOptions? options = null, CancellationToken cancellationToken = default);

    /// <summary>Creates a high-level Protobuf bulk writer authenticated with a service principal.</summary>
    Task<IZerobusBulkWriter<T>> CreateBulkWriterAsync<T>(TableProperties<T> tableProperties, string clientId, string clientSecret, BulkWriterOptions? options = null, CancellationToken cancellationToken = default) where T : IMessage<T>, new();

    /// <summary>Creates a high-level Protobuf bulk writer authenticated with a custom token provider.</summary>
    Task<IZerobusBulkWriter<T>> CreateBulkWriterAsync<T>(TableProperties<T> tableProperties, ITokenProvider tokenProvider, BulkWriterOptions? options = null, CancellationToken cancellationToken = default) where T : IMessage<T>, new();
}
