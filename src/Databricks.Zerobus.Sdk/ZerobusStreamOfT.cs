using Google.Protobuf;
using Wire = Databricks.Zerobus.Grpc;

namespace Databricks.Zerobus;

/// <summary>
/// A Zerobus ingest stream for strongly-typed protobuf records of type
/// <typeparamref name="T"/>. Created via
/// <see cref="ZerobusSdk.CreateStreamAsync{T}(TableProperties{T}, string, string, StreamConfigurationOptions?, CancellationToken)"/>.
/// </summary>
/// <typeparam name="T">A generated protobuf message type matching the target table schema.</typeparam>
public sealed class ZerobusStream<T> : ZerobusStreamBase where T : IMessage<T>
{
    internal ZerobusStream(
        Wire.Zerobus.ZerobusClient client,
        string tableName,
        Wire.CreateIngestStreamRequest createRequest,
        ITokenProvider tokenProvider,
        StreamConfigurationOptions options)
        : base(client, tableName, createRequest, tokenProvider, options) { }

    /// <summary>Ingests a single protobuf record. Returns the assigned offset.</summary>
    public Task<long> IngestRecordAsync(T record, CancellationToken cancellationToken = default)
    {
        if (record is null) throw new ArgumentNullException(nameof(record));
        var request = new Wire.IngestRecordRequest { ProtoEncodedRecord = record.ToByteString() };
        var envelope = new Wire.EphemeralStreamRequest { IngestRecord = request };
        return IngestAsync(envelope, request.CalculateSize(), cancellationToken);
    }

    /// <summary>
    /// Ingests a batch of protobuf records as a single unit. Returns the offset assigned to the batch;
    /// the batch is durable once that offset is acknowledged.
    /// </summary>
    public Task<long> IngestRecordBatchAsync(IEnumerable<T> records, CancellationToken cancellationToken = default)
    {
        if (records is null) throw new ArgumentNullException(nameof(records));
        var batch = new Wire.ProtoEncodedRecordBatch();
        foreach (var record in records)
            batch.Records.Add(record.ToByteString());
        var request = new Wire.IngestRecordBatchRequest { ProtoEncodedBatch = batch };
        var envelope = new Wire.EphemeralStreamRequest { IngestRecordBatch = request };
        return IngestAsync(envelope, request.CalculateSize(), cancellationToken);
    }
}
