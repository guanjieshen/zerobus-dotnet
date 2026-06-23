using System.Collections.Concurrent;
using Grpc.Core;

namespace Databricks.Zerobus.Tests.Infrastructure;

/// <summary>Configurable, observable behavior for <see cref="InMemoryZerobusServer"/>.</summary>
public sealed class ServerBehavior
{
    /// <summary>If set, the server fails every create-stream request with this gRPC status.</summary>
    public StatusCode? FailCreateWith { get; set; }

    /// <summary>If set, the first connection aborts after receiving this many records (simulating a disconnect).</summary>
    public int? AbortFirstConnectionAfterRecords { get; set; }

    /// <summary>If set, the server emits a CloseStreamSignal after this many records on the first connection.</summary>
    public int? CloseSignalAfterRecords { get; set; }

    /// <summary>Number of connections (create requests) the server has accepted.</summary>
    public int ConnectionCount;

    /// <summary>Total ingest messages received, including replayed duplicates.</summary>
    public int TotalReceived;

    /// <summary>Total individual rows received across all messages (records + batch contents).</summary>
    public long TotalRows;

    /// <summary>The set of distinct offsets the server has received.</summary>
    public ConcurrentDictionary<long, byte> ReceivedOffsets { get; } = new();

    /// <summary>JSON payloads keyed by offset (last write wins).</summary>
    public ConcurrentDictionary<long, string> JsonByOffset { get; } = new();

    /// <summary>Protobuf payloads keyed by offset (last write wins).</summary>
    public ConcurrentDictionary<long, byte[]> ProtoByOffset { get; } = new();

    /// <summary>The descriptor_proto bytes received on the most recent create request, if any.</summary>
    public byte[]? LastDescriptorProto;

    /// <summary>The record_type received on the most recent create request.</summary>
    public int LastRecordType;
}
