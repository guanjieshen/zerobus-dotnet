using System.Text;
using System.Text.Json;
using Grpc.Net.Client;

namespace Databricks.Zerobus;

/// <summary>
/// High-level JSON writer that accepts single records or lists, accumulates them into batches
/// (bounded by row count and byte size), and fans those batches out across several parallel
/// streams/connections. Create one via
/// <see cref="ZerobusSdk.CreateBulkWriterAsync(TableProperties, string, string, BulkWriterOptions?, CancellationToken)"/>.
/// </summary>
/// <remarks>
/// Delivery is at-least-once. Records are durable once <see cref="FlushAsync"/> (or
/// <see cref="DisposeAsync"/>) completes. <c>WriteAsync</c> is safe to call concurrently.
/// </remarks>
public sealed class ZerobusBulkWriter : IZerobusJsonBulkWriter
{
    private static readonly JsonSerializerOptions JsonDefaults = new(JsonSerializerDefaults.Web);

    private readonly ZerobusStream[] _streams;
    private readonly GrpcChannel[] _channels;
    private readonly ParallelBatchPipeline<string> _pipeline;
    private int _disposed;

    internal ZerobusBulkWriter(ZerobusStream[] streams, GrpcChannel[] channels, int batchSize, int maxBatchBytes)
    {
        _streams = streams;
        _channels = channels;
        var sinks = Array.ConvertAll(streams,
            s => (Func<IReadOnlyList<string>, CancellationToken, Task>)((batch, ct) => s.IngestRecordBatchAsync(batch, ct)));
        _pipeline = new ParallelBatchPipeline<string>(batchSize, maxBatchBytes, static j => Encoding.UTF8.GetByteCount(j) + 5, sinks);
    }

    /// <summary>The number of parallel streams this writer fans out across.</summary>
    public int Parallelism => _streams.Length;

    /// <summary>Buffers a single JSON record string; a full batch is dispatched automatically.</summary>
    public Task WriteAsync(string jsonRecord, CancellationToken cancellationToken = default)
    {
        if (jsonRecord is null) throw new ArgumentNullException(nameof(jsonRecord));
        return _pipeline.WriteAsync(jsonRecord, cancellationToken);
    }

    /// <summary>Serializes <paramref name="record"/> to JSON (System.Text.Json) and buffers it.</summary>
    public Task WriteAsync<TRecord>(TRecord record, CancellationToken cancellationToken = default)
        => WriteAsync(JsonSerializer.Serialize(record, JsonDefaults), cancellationToken);

    /// <summary>Writes a collection of JSON record strings, batched and fanned out across the parallel streams.</summary>
    public Task WriteAsync(IEnumerable<string> jsonRecords, CancellationToken cancellationToken = default)
    {
        if (jsonRecords is null) throw new ArgumentNullException(nameof(jsonRecords));
        return _pipeline.WriteManyAsync(jsonRecords, cancellationToken);
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
