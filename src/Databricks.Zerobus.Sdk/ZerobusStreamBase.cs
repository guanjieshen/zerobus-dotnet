using System.Threading.Channels;
using Databricks.Zerobus.Grpc;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Wire = Databricks.Zerobus.Grpc;

namespace Databricks.Zerobus;

/// <summary>
/// Base class for Zerobus ingest streams. Owns the bidirectional gRPC call, offset
/// and acknowledgment bookkeeping, backpressure, and automatic reconnect-and-replay.
/// Use <see cref="ZerobusStream"/> for JSON records or <see cref="ZerobusStream{T}"/> for protobuf.
/// </summary>
public abstract class ZerobusStreamBase : IAsyncDisposable
{
    private readonly Wire.Zerobus.ZerobusClient _client;
    private readonly CreateIngestStreamRequest _createRequest;
    private readonly ITokenProvider _tokenProvider;
    private readonly StreamConfigurationOptions _options;
    private readonly OffsetTracker _tracker;

    private readonly object _unackedLock = new();
    private readonly SortedDictionary<long, EphemeralStreamRequest> _unacked = new();
    private readonly Channel<long> _sendQueue = Channel.CreateUnbounded<long>(new UnboundedChannelOptions { SingleReader = true });
    private readonly SemaphoreSlim _inflight;
    private readonly CancellationTokenSource _lifetime = new();

    private readonly Task _pumpTask;
    private readonly TaskCompletionSource<bool> _ready = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private long _sentUpTo = -1;
    private volatile bool _closing;
    private volatile Exception? _fatal;
    private string? _streamId;
    private int _disposed;

    private protected ZerobusStreamBase(
        Wire.Zerobus.ZerobusClient client,
        string tableName,
        CreateIngestStreamRequest createRequest,
        ITokenProvider tokenProvider,
        StreamConfigurationOptions options)
    {
        _client = client;
        TableName = tableName;
        _createRequest = createRequest;
        _tokenProvider = tokenProvider;
        _options = options;
        _tracker = new OffsetTracker(options.AckCallback);
        _inflight = new SemaphoreSlim(Math.Max(1, options.MaxInflightRecords), Math.Max(1, options.MaxInflightRecords));
        _pumpTask = Task.Run(RunPumpAsync);
    }

    /// <summary>The fully qualified table this stream ingests into.</summary>
    public string TableName { get; }

    /// <summary>The server-assigned ephemeral stream id, available after the stream is established.</summary>
    public string? StreamId => _streamId;

    /// <summary>The highest offset durably acknowledged by the server, or -1 if none yet.</summary>
    public long LastAcknowledgedOffset => _tracker.LastAcked;

    internal async Task WaitUntilReadyAsync(CancellationToken ct)
    {
        if (!ct.CanBeCanceled)
        {
            await _ready.Task.ConfigureAwait(false);
            return;
        }

        var cancelled = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = ct.Register(static state => ((TaskCompletionSource<bool>)state!).TrySetCanceled(), cancelled);
        var completed = await Task.WhenAny(_ready.Task, cancelled.Task).ConfigureAwait(false);
        await completed.ConfigureAwait(false); // observe result / exception / cancellation
    }

    /// <summary>
    /// Enqueues a record for ingestion and returns its assigned offset. Applies
    /// backpressure when <see cref="StreamConfigurationOptions.MaxInflightRecords"/> is reached.
    /// </summary>
    private protected async Task<long> IngestAsync(EphemeralStreamRequest envelope, int size, CancellationToken ct)
    {
        ThrowIfNotWritable();
        if (size > MaxMessageBytes)
            throw new ZerobusNonRetryableException($"Record size {size} bytes exceeds the {MaxMessageBytes} byte limit.");

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token, ct);
        try
        {
            await _inflight.WaitAsync(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            throw _fatal ?? new ZerobusStreamClosedException("The stream was closed.");
        }

        long offset;
        lock (_unackedLock)
        {
            offset = _tracker.AssignNext();
            SetOffset(envelope, offset);
            _unacked[offset] = envelope;
            _sendQueue.Writer.TryWrite(offset);
        }
        return offset;
    }

    private static void SetOffset(EphemeralStreamRequest envelope, long offset)
    {
        switch (envelope.PayloadCase)
        {
            case EphemeralStreamRequest.PayloadOneofCase.IngestRecord:
                envelope.IngestRecord.OffsetId = offset;
                break;
            case EphemeralStreamRequest.PayloadOneofCase.IngestRecordBatch:
                envelope.IngestRecordBatch.OffsetId = offset;
                break;
        }
    }

    /// <summary>Completes once the record at <paramref name="offset"/> is durably acknowledged.</summary>
    public Task WaitForOffsetAsync(long offset, CancellationToken cancellationToken = default) =>
        _tracker.WaitForOffsetAsync(offset, cancellationToken);

    /// <summary>Completes once every record ingested so far is durably acknowledged.</summary>
    public async Task FlushAsync(CancellationToken cancellationToken = default)
    {
        var target = _tracker.LastAssigned;
        if (target < 0) return;

        using var timeout = new CancellationTokenSource(_options.FlushTimeout);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token, cancellationToken);
        try
        {
            await _tracker.WaitForOffsetAsync(target, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            throw new ZerobusStreamClosedException($"Flush timed out after {_options.FlushTimeout}.");
        }
    }

    /// <summary>Flushes outstanding records, half-closes the stream, and releases resources.</summary>
    public async Task CloseAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;

        try { await FlushAsync().ConfigureAwait(false); }
        catch { /* best-effort flush on close */ }

        _closing = true;
        _sendQueue.Writer.TryComplete();

        try { await _pumpTask.ConfigureAwait(false); }
        catch { /* pump exceptions surface via ingest/flush */ }

        _lifetime.Cancel();
        _lifetime.Dispose();
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync() => await CloseAsync().ConfigureAwait(false);

    private void ThrowIfNotWritable()
    {
        if (_fatal is not null) throw _fatal;
        if (_closing || _disposed != 0) throw new ZerobusStreamClosedException("The stream has been closed.");
    }

    // ---- Connection pump -------------------------------------------------

    private async Task RunPumpAsync()
    {
        var attempt = 0;
        while (!_lifetime.IsCancellationRequested && !_closing)
        {
            using var connCts = CancellationTokenSource.CreateLinkedTokenSource(_lifetime.Token);
            AsyncDuplexStreamingCall<EphemeralStreamRequest, EphemeralStreamResponse>? call = null;
            try
            {
                var token = await _tokenProvider.GetTokenAsync(TableName, connCts.Token).ConfigureAwait(false);
                var metadata = new Metadata
                {
                    { "authorization", "Bearer " + token },
                    { "x-databricks-zerobus-table-name", TableName },
                };

                call = _client.EphemeralStream(metadata, cancellationToken: connCts.Token);
                await call.RequestStream.WriteAsync(new EphemeralStreamRequest { CreateStream = _createRequest }).ConfigureAwait(false);

                if (!await call.ResponseStream.MoveNext(connCts.Token).ConfigureAwait(false))
                    throw new RpcException(new Status(StatusCode.Unavailable, "Stream closed before the create response."));

                var first = call.ResponseStream.Current;
                if (first.PayloadCase != EphemeralStreamResponse.PayloadOneofCase.CreateStreamResponse)
                    throw new ZerobusNonRetryableException($"Expected a create-stream response but received {first.PayloadCase}.");

                _streamId = first.CreateStreamResponse.StreamId;
                _sentUpTo = -1;
                attempt = 0;
                _ready.TrySetResult(true);

                var reader = ReadLoopAsync(call, connCts.Token);
                var writer = WriteLoopAsync(call, connCts.Token);
                var finished = await Task.WhenAny(reader, writer).ConfigureAwait(false);

                connCts.Cancel();
                await WhenAllSwallowed(reader, writer).ConfigureAwait(false);

                if (finished.IsFaulted)
                    throw finished.Exception!.GetBaseException();

                if (_closing) break;
                // Server completed the stream without error and we are not closing: reconnect.
            }
            catch (OperationCanceledException) when (_lifetime.IsCancellationRequested || _closing)
            {
                break;
            }
            catch (Exception ex)
            {
                var (fatal, mapped) = Classify(ex);
                if (fatal)
                {
                    Fail(mapped);
                    break;
                }

                attempt++;
                if (attempt > _options.Recovery.MaxAttempts)
                {
                    Fail(new ZerobusStreamClosedException(
                        $"Exhausted {_options.Recovery.MaxAttempts} reconnect attempts.", ex));
                    break;
                }

                try { await Task.Delay(_options.Recovery.GetDelay(attempt), _lifetime.Token).ConfigureAwait(false); }
                catch (OperationCanceledException) { break; }
            }
            finally
            {
                call?.Dispose();
            }
        }
    }

    private async Task ReadLoopAsync(
        AsyncDuplexStreamingCall<EphemeralStreamRequest, EphemeralStreamResponse> call, CancellationToken ct)
    {
        while (await call.ResponseStream.MoveNext(ct).ConfigureAwait(false))
        {
            var response = call.ResponseStream.Current;
            switch (response.PayloadCase)
            {
                case EphemeralStreamResponse.PayloadOneofCase.IngestRecordResponse:
                    ReleaseAcked(response.IngestRecordResponse.DurabilityAckUpToOffset);
                    break;
                case EphemeralStreamResponse.PayloadOneofCase.CloseStreamSignal:
                    throw new ServerCloseRequestedException(response.CloseStreamSignal.Duration?.ToTimeSpan());
            }
        }
    }

    private async Task WriteLoopAsync(
        AsyncDuplexStreamingCall<EphemeralStreamRequest, EphemeralStreamResponse> call, CancellationToken ct)
    {
        // Replay every unacknowledged record on this (re)connection, in offset order.
        List<KeyValuePair<long, EphemeralStreamRequest>> replay;
        lock (_unackedLock) replay = _unacked.ToList();
        foreach (var entry in replay)
        {
            ct.ThrowIfCancellationRequested();
            await call.RequestStream.WriteAsync(entry.Value).ConfigureAwait(false);
            _sentUpTo = entry.Key;
        }

        // Send newly ingested records as they arrive.
        var reader = _sendQueue.Reader;
        while (await reader.WaitToReadAsync(ct).ConfigureAwait(false))
        {
            while (reader.TryRead(out var offset))
            {
                if (offset <= _sentUpTo) continue; // already replayed on this connection
                EphemeralStreamRequest? envelope;
                lock (_unackedLock) _unacked.TryGetValue(offset, out envelope);
                if (envelope is null) continue; // already acknowledged
                await call.RequestStream.WriteAsync(envelope).ConfigureAwait(false);
                _sentUpTo = offset;
            }
        }

        await call.RequestStream.CompleteAsync().ConfigureAwait(false);
    }

    private void ReleaseAcked(long ackOffset)
    {
        var released = 0;
        lock (_unackedLock)
        {
            var keys = new List<long>();
            foreach (var key in _unacked.Keys)
            {
                if (key > ackOffset) break; // keys ascending
                keys.Add(key);
            }
            foreach (var key in keys)
            {
                _unacked.Remove(key);
                released++;
            }
        }
        if (released > 0) _inflight.Release(released);
        _tracker.ReleaseUpTo(ackOffset);
    }

    private void Fail(Exception ex)
    {
        _fatal = ex;
        _ready.TrySetException(ex);
        _tracker.Fault(ex);
        _lifetime.Cancel();
    }

    private static (bool fatal, Exception mapped) Classify(Exception ex)
    {
        switch (ex)
        {
            case ZerobusNonRetryableException:
                return (true, ex);
            case ServerCloseRequestedException:
                return (false, ex); // retryable: reconnect and replay
            case RpcException rpc:
                switch (rpc.StatusCode)
                {
                    case StatusCode.Unauthenticated:
                    case StatusCode.PermissionDenied:
                        return (true, new ZerobusAuthException(
                            $"Authorization failed ({rpc.StatusCode}). Verify the service principal has explicit " +
                            "MODIFY and SELECT grants on the target table.", rpc));
                    case StatusCode.InvalidArgument:
                    case StatusCode.NotFound:
                    case StatusCode.FailedPrecondition:
                    case StatusCode.OutOfRange:
                        return (true, new ZerobusNonRetryableException($"Non-retryable gRPC error ({rpc.StatusCode}): {rpc.Status.Detail}", rpc));
                    default:
                        return (false, ex); // Unavailable, Internal, Aborted, Cancelled, Unknown, DeadlineExceeded, ...
                }
            default:
                return (false, ex); // transport/IO errors: retry
        }
    }

    private static async Task WhenAllSwallowed(params Task[] tasks)
    {
        foreach (var task in tasks)
        {
            try { await task.ConfigureAwait(false); }
            catch { /* primary cause is surfaced by the caller via the finished task */ }
        }
    }

    private const int MaxMessageBytes = 10 * 1024 * 1024; // 10 MB Zerobus per-message limit
}
