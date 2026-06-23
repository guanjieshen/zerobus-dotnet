using Google.Protobuf;

namespace Databricks.Zerobus;

/// <summary>Members common to every Zerobus ingest stream.</summary>
public interface IZerobusStreamBase : IAsyncDisposable
{
    /// <summary>The server-assigned ephemeral stream id, available once the stream is established.</summary>
    string? StreamId { get; }

    /// <summary>The highest offset durably acknowledged by the server, or -1 if none yet.</summary>
    long LastAcknowledgedOffset { get; }

    /// <summary>Completes once the record at <paramref name="offset"/> is durably acknowledged.</summary>
    Task WaitForOffsetAsync(long offset, CancellationToken cancellationToken = default);

    /// <summary>Completes once every record ingested so far is durably acknowledged.</summary>
    Task FlushAsync(CancellationToken cancellationToken = default);

    /// <summary>Flushes outstanding records, half-closes the stream, and releases resources.</summary>
    Task CloseAsync();
}

/// <summary>A Protobuf ingest stream for records of type <typeparamref name="T"/>.</summary>
public interface IZerobusStream<T> : IZerobusStreamBase where T : IMessage<T>
{
    /// <summary>Ingests a single record. Returns the assigned offset.</summary>
    Task<long> IngestRecordAsync(T record, CancellationToken cancellationToken = default);

    /// <summary>Ingests a batch of records as a single unit. Returns the offset assigned to the batch.</summary>
    Task<long> IngestRecordBatchAsync(IEnumerable<T> records, CancellationToken cancellationToken = default);
}

/// <summary>A JSON ingest stream.</summary>
public interface IZerobusJsonStream : IZerobusStreamBase
{
    /// <summary>Ingests a single record supplied as a JSON string. Returns the assigned offset.</summary>
    Task<long> IngestRecordAsync(string jsonRecord, CancellationToken cancellationToken = default);

    /// <summary>Serializes <paramref name="record"/> to JSON and ingests it. Returns the assigned offset.</summary>
    Task<long> IngestRecordAsync<TRecord>(TRecord record, CancellationToken cancellationToken = default);

    /// <summary>Ingests a batch of JSON records as a single unit. Returns the offset assigned to the batch.</summary>
    Task<long> IngestRecordBatchAsync(IEnumerable<string> jsonRecords, CancellationToken cancellationToken = default);
}
