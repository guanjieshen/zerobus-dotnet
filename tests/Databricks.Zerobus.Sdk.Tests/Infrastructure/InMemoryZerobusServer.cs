using Databricks.Zerobus.TestProto;
using Grpc.Core;

namespace Databricks.Zerobus.Tests.Infrastructure;

/// <summary>
/// An in-memory implementation of the Zerobus gRPC service for end-to-end tests.
/// Handles the create handshake and emits cumulative durability acks, with optional
/// fault injection driven by <see cref="ServerBehavior"/>.
/// </summary>
public sealed class InMemoryZerobusServer : Databricks.Zerobus.TestProto.Zerobus.ZerobusBase
{
    private readonly ServerBehavior _behavior;

    public InMemoryZerobusServer(ServerBehavior behavior) => _behavior = behavior;

    public override async Task EphemeralStream(
        IAsyncStreamReader<EphemeralStreamRequest> requestStream,
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        ServerCallContext context)
    {
        var connectionId = Interlocked.Increment(ref _behavior.ConnectionCount);
        long maxOffset = -1;
        var recordsThisConnection = 0;

        await foreach (var message in requestStream.ReadAllAsync(context.CancellationToken))
        {
            switch (message.PayloadCase)
            {
                case EphemeralStreamRequest.PayloadOneofCase.CreateStream:
                    if (_behavior.FailCreateWith is { } status)
                        throw new RpcException(new Status(status, "injected create failure"));
                    _behavior.LastDescriptorProto = message.CreateStream.HasDescriptorProto
                        ? message.CreateStream.DescriptorProto.ToByteArray()
                        : null;
                    _behavior.LastRecordType = (int)message.CreateStream.RecordType;
                    await responseStream.WriteAsync(new EphemeralStreamResponse
                    {
                        CreateStreamResponse = new CreateIngestStreamResponse { StreamId = $"test-stream-{connectionId}" },
                    });
                    break;

                case EphemeralStreamRequest.PayloadOneofCase.IngestRecord:
                {
                    var record = message.IngestRecord;
                    Interlocked.Increment(ref _behavior.TotalReceived);
                    Interlocked.Increment(ref _behavior.TotalRows);
                    _behavior.ReceivedOffsets[record.OffsetId] = 1;
                    if (record.RecordCase == IngestRecordRequest.RecordOneofCase.JsonRecord)
                        _behavior.JsonByOffset[record.OffsetId] = record.JsonRecord;
                    else if (record.RecordCase == IngestRecordRequest.RecordOneofCase.ProtoEncodedRecord)
                        _behavior.ProtoByOffset[record.OffsetId] = record.ProtoEncodedRecord.ToByteArray();

                    maxOffset = Math.Max(maxOffset, record.OffsetId);
                    recordsThisConnection++;

                    if ((connectionId == 1 &&
                         _behavior.AbortFirstConnectionAfterRecords is int abortAt &&
                         recordsThisConnection >= abortAt)
                        || (_behavior.AbortEveryConnectionAfterRecords is int everyAbortAt &&
                            recordsThisConnection >= everyAbortAt))
                    {
                        throw new RpcException(new Status(StatusCode.Unavailable, "simulated disconnect"));
                    }

                    if (!_behavior.SuppressAcks)
                    {
                        await responseStream.WriteAsync(new EphemeralStreamResponse
                        {
                            IngestRecordResponse = new IngestRecordResponse { DurabilityAckUpToOffset = maxOffset },
                        });
                    }

                    if (connectionId == 1 &&
                        _behavior.CloseSignalAfterRecords is int closeAt &&
                        recordsThisConnection >= closeAt)
                    {
                        await responseStream.WriteAsync(new EphemeralStreamResponse
                        {
                            CloseStreamSignal = new CloseStreamSignal(),
                        });
                    }
                    break;
                }

                case EphemeralStreamRequest.PayloadOneofCase.IngestRecordBatch:
                {
                    var batch = message.IngestRecordBatch;
                    Interlocked.Increment(ref _behavior.TotalReceived);
                    var rows = batch.BatchCase == IngestRecordBatchRequest.BatchOneofCase.ProtoEncodedBatch
                        ? batch.ProtoEncodedBatch.Records.Count
                        : batch.BatchCase == IngestRecordBatchRequest.BatchOneofCase.JsonBatch
                            ? batch.JsonBatch.Records.Count
                            : 0;
                    Interlocked.Add(ref _behavior.TotalRows, rows);
                    _behavior.ReceivedOffsets[batch.OffsetId] = 1;
                    maxOffset = Math.Max(maxOffset, batch.OffsetId);
                    recordsThisConnection++;
                    await responseStream.WriteAsync(new EphemeralStreamResponse
                    {
                        IngestRecordResponse = new IngestRecordResponse { DurabilityAckUpToOffset = maxOffset },
                    });
                    break;
                }
            }
        }
    }
}
