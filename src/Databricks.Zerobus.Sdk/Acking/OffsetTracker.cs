namespace Databricks.Zerobus;

/// <summary>
/// Tracks per-record offsets and resolves waiters as the server's cumulative
/// durability acknowledgment advances. Thread-safe.
/// </summary>
internal sealed class OffsetTracker
{
    private readonly object _lock = new();
    private readonly SortedDictionary<long, List<TaskCompletionSource<bool>>> _waiters = new();
    private readonly IAckCallback? _callback;

    private long _lastAssigned = -1;
    private long _lastAcked = -1;
    private Exception? _fault;

    public OffsetTracker(IAckCallback? callback) => _callback = callback;

    /// <summary>The highest offset assigned so far, or -1 if none.</summary>
    public long LastAssigned { get { lock (_lock) return _lastAssigned; } }

    /// <summary>The highest offset durably acknowledged so far, or -1 if none.</summary>
    public long LastAcked { get { lock (_lock) return _lastAcked; } }

    /// <summary>Assigns the next monotonically increasing offset. Call under the caller's send lock to preserve order.</summary>
    public long AssignNext()
    {
        lock (_lock) return ++_lastAssigned;
    }

    /// <summary>
    /// Returns a task that completes once the cumulative ack reaches <paramref name="offset"/>.
    /// Completes immediately if already acknowledged, or faults if the stream has failed.
    /// </summary>
    public Task WaitForOffsetAsync(long offset, CancellationToken ct)
    {
        TaskCompletionSource<bool> tcs;
        lock (_lock)
        {
            if (_fault is not null) return Task.FromException(_fault);
            if (offset <= _lastAcked) return Task.CompletedTask;

            tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_waiters.TryGetValue(offset, out var list))
            {
                list = new List<TaskCompletionSource<bool>>();
                _waiters[offset] = list;
            }
            list.Add(tcs);
        }

        if (ct.CanBeCanceled)
        {
            var registration = ct.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(), tcs);
            tcs.Task.ContinueWith(static (_, state) => ((CancellationTokenRegistration)state!).Dispose(),
                registration, CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
        }
        return tcs.Task;
    }

    /// <summary>
    /// Records a cumulative durability ack. Completes every waiter at or below
    /// <paramref name="ackOffset"/> and invokes the ack callback for each newly durable offset.
    /// </summary>
    public void ReleaseUpTo(long ackOffset)
    {
        List<TaskCompletionSource<bool>>? toComplete = null;
        long previousAcked;
        long callbackCap;
        lock (_lock)
        {
            if (ackOffset <= _lastAcked) return;
            previousAcked = _lastAcked;
            _lastAcked = ackOffset;
            callbackCap = Math.Min(ackOffset, _lastAssigned);

            var releasedKeys = new List<long>();
            foreach (var key in _waiters.Keys)
            {
                if (key > ackOffset) break; // keys ascending
                releasedKeys.Add(key);
            }
            foreach (var key in releasedKeys)
            {
                (toComplete ??= new()).AddRange(_waiters[key]);
                _waiters.Remove(key);
            }
        }

        if (toComplete is not null)
            foreach (var tcs in toComplete)
                tcs.TrySetResult(true);

        if (_callback is not null)
            for (var offset = previousAcked + 1; offset <= callbackCap; offset++)
                _callback.OnAck(offset);
    }

    /// <summary>Faults the tracker and all pending waiters. Used when the stream fails terminally.</summary>
    public void Fault(Exception ex)
    {
        List<TaskCompletionSource<bool>> toFail = new();
        lock (_lock)
        {
            _fault ??= ex;
            foreach (var list in _waiters.Values) toFail.AddRange(list);
            _waiters.Clear();
        }
        foreach (var tcs in toFail) tcs.TrySetException(ex);
        // Per-offset error notification is driven by the stream (it owns the unacked set);
        // the tracker only resolves the offset waiters here.
    }
}
