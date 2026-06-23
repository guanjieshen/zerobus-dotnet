using Google.Protobuf;

namespace Databricks.Zerobus;

/// <summary>A high-level Protobuf writer that auto-batches records and fans them out across parallel streams.</summary>
public interface IZerobusBulkWriter<T> : IAsyncDisposable where T : IMessage<T>
{
    /// <summary>The number of parallel streams this writer fans out across.</summary>
    int Parallelism { get; }

    /// <summary>Buffers a single record; a full batch is dispatched automatically.</summary>
    Task WriteAsync(T record, CancellationToken cancellationToken = default);

    /// <summary>Writes a collection of records, batched and fanned out across the parallel streams.</summary>
    Task WriteAsync(IEnumerable<T> records, CancellationToken cancellationToken = default);

    /// <summary>Dispatches any buffered records and waits until everything written so far is durable.</summary>
    Task FlushAsync(CancellationToken cancellationToken = default);
}

/// <summary>A high-level JSON writer that auto-batches records and fans them out across parallel streams.</summary>
public interface IZerobusJsonBulkWriter : IAsyncDisposable
{
    /// <summary>The number of parallel streams this writer fans out across.</summary>
    int Parallelism { get; }

    /// <summary>Buffers a single JSON record string; a full batch is dispatched automatically.</summary>
    Task WriteAsync(string jsonRecord, CancellationToken cancellationToken = default);

    /// <summary>Serializes <paramref name="record"/> to JSON and buffers it.</summary>
    Task WriteAsync<TRecord>(TRecord record, CancellationToken cancellationToken = default);

    /// <summary>Writes a collection of JSON record strings, batched and fanned out across the parallel streams.</summary>
    Task WriteAsync(IEnumerable<string> jsonRecords, CancellationToken cancellationToken = default);

    /// <summary>Dispatches any buffered records and waits until everything written so far is durable.</summary>
    Task FlushAsync(CancellationToken cancellationToken = default);
}
