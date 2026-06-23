using Google.Protobuf;
using Grpc.Net.Client;

namespace Databricks.Zerobus;

/// <summary>
/// High-level Protobuf writer that accepts single records or lists, accumulates them into
/// batches (bounded by row count and byte size), and fans those batches out across several
/// parallel streams/connections. Create one via
/// <see cref="ZerobusSdk.CreateBulkWriterAsync{T}(TableProperties{T}, string, string, BulkWriterOptions?, CancellationToken)"/>.
/// </summary>
/// <remarks>
/// Delivery is at-least-once. Records are durable once <see cref="FlushAsync"/> (or
/// <see cref="DisposeAsync"/>) completes. <c>WriteAsync</c> is safe to call concurrently.
/// </remarks>
/// <typeparam name="T">A generated protobuf message type matching the target table schema.</typeparam>
public sealed class ZerobusBulkWriter<T> : IZerobusBulkWriter<T> where T : IMessage<T>, new()
{
    private readonly ZerobusStream<T>[] _streams;
    private readonly GrpcChannel[] _channels;
    private readonly ParallelBatchPipeline<T> _pipeline;
    private int _disposed;

    internal ZerobusBulkWriter(ZerobusStream<T>[] streams, GrpcChannel[] channels, int batchSize, int maxBatchBytes)
    {
        _streams = streams;
        _channels = channels;
        var sinks = Array.ConvertAll(streams,
            s => (Func<IReadOnlyList<T>, CancellationToken, Task>)((batch, ct) => s.IngestRecordBatchAsync(batch, ct)));
        // +5 approximates the per-record framing inside a ProtoEncodedRecordBatch.
        _pipeline = new ParallelBatchPipeline<T>(batchSize, maxBatchBytes, static r => r.CalculateSize() + 5, sinks);
    }

    /// <summary>The number of parallel streams this writer fans out across.</summary>
    public int Parallelism => _streams.Length;

    /// <summary>Buffers a single record; a full batch is dispatched automatically.</summary>
    public Task WriteAsync(T record, CancellationToken cancellationToken = default)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));
        return _pipeline.WriteAsync(record, cancellationToken);
    }

    /// <summary>Writes a collection of records, batched and fanned out across the parallel streams.</summary>
    public Task WriteAsync(IEnumerable<T> records, CancellationToken cancellationToken = default)
    {
        if (records is null) throw new ArgumentNullException(nameof(records));
        return _pipeline.WriteManyAsync(records, cancellationToken);
    }

    /// <summary>Dispatches any buffered records and waits until everything written so far is durable.</summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        await _pipeline.FlushBufferAsync(cancellationToken).ConfigureAwait(false);
        await Task.WhenAll(Array.ConvertAll(_streams, s => s.FlushAsync(cancellationToken))).ConfigureAwait(false);
    }

    /// <summary>Flushes outstanding records, closes all streams, and disposes the connections.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        try { await FlushAsync().ConfigureAwait(false); }
        finally
        {
            foreach (var stream in _streams) await stream.CloseAsync().ConfigureAwait(false);
            foreach (var channel in _channels) channel.Dispose();
        }
    }
}
