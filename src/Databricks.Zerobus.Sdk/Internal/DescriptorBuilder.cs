using Google.Protobuf;
using Google.Protobuf.Reflection;

namespace Databricks.Zerobus;

/// <summary>
/// Derives the <c>descriptor_proto</c> bytes that the Zerobus server uses to validate
/// protobuf records, from a generated message's reflection descriptor.
/// </summary>
internal static class DescriptorBuilder
{
    /// <summary>
    /// Returns the serialized <see cref="DescriptorProto"/> (the message-level descriptor)
    /// for <paramref name="descriptor"/>. The Zerobus server decodes <c>descriptor_proto</c>
    /// as a <c>google.protobuf.DescriptorProto</c>, not a <c>FileDescriptorProto</c>.
    /// </summary>
    /// <remarks>
    /// v1 expects the record message to be self-contained — scalar fields, or nested
    /// message/enum types defined inline within the message (these are carried in the
    /// <see cref="DescriptorProto"/>). References to types imported from other <c>.proto</c>
    /// files are not included; regenerate a flat <c>.proto</c> for the table if needed.
    /// This matches the output of the "generate .proto from a Unity Catalog table" tools.
    /// </remarks>
    public static ByteString Build(MessageDescriptor descriptor) =>
        descriptor.ToProto().ToByteString();
}
