using System.Text.Json;

namespace Databricks.Solutions.Zerobus;

/// <summary>
/// Shared building blocks for the Zerobus OAuth token requests: the resource (audience) indicator
/// and the <c>authorization_details</c> that scope the token to a specific table.
/// </summary>
internal static class ZerobusOAuth
{
    /// <summary>The OAuth resource indicator that scopes a token to the Zerobus direct-write API.</summary>
    public static string ResourceFor(string workspaceId) =>
        $"api://databricks/workspaces/{workspaceId}/zerobusDirectWriteApi";

    /// <summary>
    /// Builds the <c>authorization_details</c> JSON granting USE CATALOG / USE SCHEMA / SELECT+MODIFY
    /// (operation <c>zerobuswrite</c>) on the three-part <paramref name="tableName"/>.
    /// </summary>
    public static string AuthorizationDetails(string tableName)
    {
        var parts = tableName.Split('.');
        var catalog = parts[0];
        var schema = $"{parts[0]}.{parts[1]}";
        var table = tableName;

        return JsonSerializer.Serialize(new object[]
        {
            new { type = "unity_catalog_privileges", privileges = new[] { "USE CATALOG" }, object_type = "CATALOG", object_full_path = catalog },
            new { type = "unity_catalog_privileges", privileges = new[] { "USE SCHEMA" }, object_type = "SCHEMA", object_full_path = schema },
            new { type = "unity_catalog_privileges", privileges = new[] { "SELECT", "MODIFY" }, object_type = "TABLE", object_full_path = table, operations = new[] { "zerobuswrite" } },
        });
    }
}
