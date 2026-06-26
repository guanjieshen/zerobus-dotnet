namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Shared batching + parallel-dispatch engine used by the bulk writers. Accumulates items into
/// batches bounded by both a row count and a byte ceiling, and dispatches full batches round-robin
/// across a set of per-stream sinks. Thread-safe.
/// </summary>
internal sealed class ParallelBatchPipeline<TItem>
{
    private readonly int _batchSize;
    private readonly int _maxBatchBytes;
    private readonly Func<TItem, int> _sizeOf;
    private readonly IReadOnlyList<Func<IReadOnlyList<TItem>, CancellationToken, Task>> _sinks;

    private readonly object _lock = new();
    private List<TItem> _buffer = new();
    private long _bufferBytes;
    private int _roundRobin = -1;

    public ParallelBatchPipeline(
        int batchSize,
        int maxBatchBytes,
        Func<TItem, int> sizeOf,
        IReadOnlyList<Func<IReadOnlyList<TItem>, CancellationToken, Task>> sinks)
    {
        _batchSize = batchSize;
        _maxBatchBytes = maxBatchBytes;
        _sizeOf = sizeOf;
        _sinks = sinks;
    }

    public int Parallelism => _sinks.Count;

    public Task WriteAsync(TItem item, CancellationToken cancellationToken)
    {
        var batch = AddAndTakeFullBatch(item);
        return batch is null ? Task.CompletedTask : Dispatch(batch, cancellationToken);
    }

    public async Task WriteManyAsync(IEnumerable<TItem> items, CancellationToken cancellationToken)
    {
        var maxOutstanding = Math.Max(1, _sinks.Count * 2);
        var outstanding = new List<Task>(maxOutstanding);

        foreach (var item in items)
        {
            var batch = AddAndTakeFullBatch(item);
            if (batch is null) continue;

            outstanding.Add(Dispatch(batch, cancellationToken));
            if (outstanding.Count >= maxOutstanding)
            {
                var completed = await Task.WhenAny(outstanding).ConfigureAwait(false);
                outstanding.Remove(completed);
                await completed.ConfigureAwait(false); // surface errors promptly
            }
        }

        await Task.WhenAll(outstanding).ConfigureAwait(false);
    }

    /// <summary>Dispatches whatever is currently buffered (a partial batch), if any.</summary>
    public Task FlushBufferAsync(CancellationToken cancellationToken)
    {
        List<TItem>? remaining;
        lock (_lock)
        {
            remaining = _buffer.Count > 0 ? _buffer : null;
            if (remaining is not null)
            {
                _buffer = new List<TItem>();
                _bufferBytes = 0;
            }
        }
        return remaining is null ? Task.CompletedTask : Dispatch(remaining, cancellationToken);
    }

    private List<TItem>? AddAndTakeFullBatch(TItem item)
    {
        var size = _sizeOf(item);
        lock (_lock)
        {
            // Flush the current buffer first if adding this item would breach the byte ceiling.
            if (_buffer.Count > 0 && _bufferBytes + size > _maxBatchBytes)
            {
                var full = _buffer;
                _buffer = new List<TItem> { item };
                _bufferBytes = size;
                return full;
            }

            _buffer.Add(item);
            _bufferBytes += size;

            if (_buffer.Count >= _batchSize)
            {
                var full = _buffer;
                _buffer = new List<TItem>();
                _bufferBytes = 0;
                return full;
            }
            return null;
        }
    }

    private Task Dispatch(IReadOnlyList<TItem> batch, CancellationToken cancellationToken)
    {
        var index = (int)((uint)Interlocked.Increment(ref _roundRobin) % (uint)_sinks.Count);
        return _sinks[index](batch, cancellationToken);
    }
}
