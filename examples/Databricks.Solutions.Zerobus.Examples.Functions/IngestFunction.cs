using System.Text.Json;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace Databricks.Solutions.Zerobus.Examples.Functions;

/// <summary>
/// HTTP-triggered function that ingests its JSON request body into Zerobus.
/// POST a single JSON object or an array of objects; each becomes one record.
/// </summary>
public sealed class IngestFunction
{
    private readonly IZerobusIngestor _ingestor;
    private readonly ILogger<IngestFunction> _logger;

    public IngestFunction(IZerobusIngestor ingestor, ILogger<IngestFunction> logger)
    {
        _ingestor = ingestor;
        _logger = logger;
    }

    [Function("Ingest")]
    public async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Anonymous, "post", Route = "ingest")] HttpRequest request)
    {
        var ct = request.HttpContext.RequestAborted;

        using var reader = new StreamReader(request.Body);
        var body = await reader.ReadToEndAsync(ct);
        if (string.IsNullOrWhiteSpace(body))
            return new BadRequestObjectResult(new { error = "Request body is empty." });

        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(body);
        }
        catch (JsonException ex)
        {
            return new BadRequestObjectResult(new { error = $"Invalid JSON: {ex.Message}" });
        }

        var offsets = new List<long>();
        try
        {
            using (document)
            {
                if (document.RootElement.ValueKind == JsonValueKind.Array)
                {
                    foreach (var element in document.RootElement.EnumerateArray())
                        offsets.Add(await _ingestor.IngestAsync(element.GetRawText(), ct));
                }
                else
                {
                    offsets.Add(await _ingestor.IngestAsync(body, ct));
                }
            }

            await _ingestor.FlushAsync(ct);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Ingestion failed.");
            return new ObjectResult(new { error = ex.Message }) { StatusCode = StatusCodes.Status502BadGateway };
        }

        _logger.LogInformation("Ingested {Count} record(s); offsets {First}..{Last}.", offsets.Count, offsets.First(), offsets.Last());
        return new OkObjectResult(new { ingested = offsets.Count, offsets });
    }
}
