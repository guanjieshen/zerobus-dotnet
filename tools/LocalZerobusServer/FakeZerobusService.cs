using Databricks.Solutions.Zerobus.LocalFake;
using Grpc.Core;

namespace LocalZerobusServer;

/// <summary>
/// A local, in-memory stand-in for the Zerobus gRPC service. Performs the create
/// handshake, logs every record, and returns cumulative durability acks. Intended only
/// for local development and testing (e.g. driving the Azure Functions sample) — it does
/// not persist anything or validate schemas.
/// </summary>
public sealed class FakeZerobusService : Databricks.Solutions.Zerobus.LocalFake.Zerobus.ZerobusBase
{
    private static int _connectionCounter;
    private readonly ILogger<FakeZerobusService> _logger;

    public FakeZerobusService(ILogger<FakeZerobusService> logger) => _logger = logger;

    public override async Task EphemeralStream(
        IAsyncStreamReader<EphemeralStreamRequest> requestStream,
        IServerStreamWriter<EphemeralStreamResponse> responseStream,
        ServerCallContext context)
    {
        var connectionId = Interlocked.Increment(ref _connectionCounter);
        long maxOffset = -1;

        await foreach (var message in requestStream.ReadAllAsync(context.CancellationToken))
        {
            switch (message.PayloadCase)
            {
                case EphemeralStreamRequest.PayloadOneofCase.CreateStream:
                    _logger.LogInformation(
                        "[conn {Conn}] create stream: table={Table} recordType={RecordType} hasDescriptor={HasDescriptor}",
                        connectionId, message.CreateStream.TableName, message.CreateStream.RecordType,
                        message.CreateStream.HasDescriptorProto);
                    await responseStream.WriteAsync(new EphemeralStreamResponse
                    {
                        CreateStreamResponse = new CreateIngestStreamResponse { StreamId = $"local-{connectionId}" },
                    });
                    break;

                case EphemeralStreamRequest.PayloadOneofCase.IngestRecord:
                    maxOffset = Math.Max(maxOffset, message.IngestRecord.OffsetId);
                    _logger.LogInformation("[conn {Conn}] record offset={Offset}: {Payload}",
                        connectionId, message.IngestRecord.OffsetId,
                        message.IngestRecord.RecordCase == IngestRecordRequest.RecordOneofCase.JsonRecord
                            ? message.IngestRecord.JsonRecord
                            : $"<{message.IngestRecord.ProtoEncodedRecord.Length} proto bytes>");
                    await responseStream.WriteAsync(new EphemeralStreamResponse
                    {
                        IngestRecordResponse = new IngestRecordResponse { DurabilityAckUpToOffset = maxOffset },
                    });
                    break;

                case EphemeralStreamRequest.PayloadOneofCase.IngestRecordBatch:
                    maxOffset = Math.Max(maxOffset, message.IngestRecordBatch.OffsetId);
                    _logger.LogInformation("[conn {Conn}] batch offset={Offset}", connectionId, message.IngestRecordBatch.OffsetId);
                    await responseStream.WriteAsync(new EphemeralStreamResponse
                    {
                        IngestRecordResponse = new IngestRecordResponse { DurabilityAckUpToOffset = maxOffset },
                    });
                    break;
            }
        }
    }
}
