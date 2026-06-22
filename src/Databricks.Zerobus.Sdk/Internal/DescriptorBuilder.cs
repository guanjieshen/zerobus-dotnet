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
    /// Returns the serialized <see cref="FileDescriptorProto"/> for the file that defines
    /// <paramref name="descriptor"/>'s message.
    /// </summary>
    /// <remarks>
    /// v1 requires the record message (and any message types it references) to be
    /// self-contained in a single <c>.proto</c> file with no non-well-known imports —
    /// which is exactly what the "generate .proto from a Unity Catalog table" tools emit.
    /// A clear error is thrown otherwise.
    /// </remarks>
    public static ByteString Build(MessageDescriptor descriptor)
    {
        var file = descriptor.File;

        foreach (var dependency in file.Dependencies)
        {
            if (!IsWellKnown(dependency.Name))
            {
                throw new ZerobusNonRetryableException(
                    $"The record message '{descriptor.FullName}' is defined in '{file.Name}', which imports " +
                    $"'{dependency.Name}'. v1 requires the record type to be self-contained in a single .proto " +
                    "with no non-well-known imports. Regenerate a flat .proto for the table.");
            }
        }

        return file.ToProto().ToByteString();
    }

    private static bool IsWellKnown(string fileName) =>
        fileName.StartsWith("google/protobuf/", StringComparison.Ordinal);
}
