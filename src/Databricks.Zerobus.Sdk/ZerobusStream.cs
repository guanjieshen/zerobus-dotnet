using System.Text.Json;
using Wire = Databricks.Zerobus.Grpc;

namespace Databricks.Zerobus;

/// <summary>
/// A Zerobus ingest stream for JSON records. Created via
/// <see cref="ZerobusSdk.CreateStreamAsync(TableProperties, string, string, StreamConfigurationOptions?, CancellationToken)"/>.
/// </summary>
public sealed class ZerobusStream : ZerobusStreamBase, IZerobusJsonStream
{
    private static readonly JsonSerializerOptions JsonDefaults = new(JsonSerializerDefaults.Web);

    internal ZerobusStream(
        Wire.Zerobus.ZerobusClient client,
        string tableName,
        Wire.CreateIngestStreamRequest createRequest,
        ITokenProvider tokenProvider,
        StreamConfigurationOptions options)
        : base(client, tableName, createRequest, tokenProvider, options) { }

    /// <summary>Ingests a single record supplied as a JSON string. Returns the assigned offset.</summary>
    public Task<long> IngestRecordAsync(string jsonRecord, CancellationToken cancellationToken = default)
    {
        if (jsonRecord is null) throw new ArgumentNullException(nameof(jsonRecord));
        var request = new Wire.IngestRecordRequest { JsonRecord = jsonRecord };
        var envelope = new Wire.EphemeralStreamRequest { IngestRecord = request };
        return IngestAsync(envelope, request.CalculateSize(), cancellationToken);
    }

    /// <summary>Serializes <paramref name="record"/> to JSON (System.Text.Json) and ingests it.</summary>
    public Task<long> IngestRecordAsync<TRecord>(TRecord record, CancellationToken cancellationToken = default)
    {
        var json = JsonSerializer.Serialize(record, JsonDefaults);
        return IngestRecordAsync(json, cancellationToken);
    }

    /// <summary>
    /// Ingests a batch of JSON records as a single unit. Returns the offset assigned to the batch;
    /// the batch is durable once that offset is acknowledged.
    /// </summary>
    public Task<long> IngestRecordBatchAsync(IEnumerable<string> jsonRecords, CancellationToken cancellationToken = default)
    {
        if (jsonRecords is null) throw new ArgumentNullException(nameof(jsonRecords));
        var batch = new Wire.JsonRecordBatch();
        batch.Records.AddRange(jsonRecords);
        var request = new Wire.IngestRecordBatchRequest { JsonBatch = batch };
        var envelope = new Wire.EphemeralStreamRequest { IngestRecordBatch = request };
        return IngestAsync(envelope, request.CalculateSize(), cancellationToken);
    }
}
