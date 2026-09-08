using System.Globalization;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using QsoRipper.Services;

namespace QsoRipper.Cli.Commands;

internal static class EnrichCommand
{
    public static async Task<int> RunAsync(
        GrpcChannel channel,
        string[] args,
        bool jsonOutput = false,
        CancellationToken cancellationToken = default)
    {
        if (!TryParseArgs(args, out var request, out var error))
        {
            Console.Error.WriteLine(error);
            return 1;
        }

        var client = new LogbookService.LogbookServiceClient(channel);
        using var call = client.BackfillQsoEnrichment(request, cancellationToken: cancellationToken);
        return await ConsumeResponsesAsync(call.ResponseStream, jsonOutput, cancellationToken);
    }

    internal static async Task<int> ConsumeResponsesAsync(
        IAsyncStreamReader<BackfillQsoEnrichmentResponse> responseStream,
        bool jsonOutput,
        CancellationToken cancellationToken)
    {
        BackfillQsoEnrichmentResponse? final = null;

        while (await responseStream.MoveNext(cancellationToken))
        {
            var progress = responseStream.Current;
            final = progress;
            if (!jsonOutput && !progress.Complete)
            {
                var current = progress.HasCurrentCallsign ? $" Current: {progress.CurrentCallsign}." : "";
                Console.WriteLine(
                    $"Scanned {progress.Scanned}. Candidates {progress.Candidates}. Callsigns {progress.UniqueCallsigns}.{current}");
            }
        }

        if (final is null || !final.Complete)
        {
            Console.Error.WriteLine("The engine closed the backfill stream without a complete summary.");
            return 1;
        }

        if (jsonOutput)
        {
            JsonOutput.Print(final);
        }
        else
        {
            Console.WriteLine(
                $"Complete. Found {final.Found}, not found {final.NotFound}, errors {final.Errors}. " +
                $"Changed {final.Changed}, unchanged {final.Unchanged}, concurrent edits {final.ConcurrentEdits}, " +
                $"storage errors {final.StorageErrors}.");
        }

        return final.Errors == 0 && final.StorageErrors == 0 ? 0 : 1;
    }

    internal static bool TryParseArgs(
        string[] args,
        out BackfillQsoEnrichmentRequest request,
        out string? error)
    {
        request = new BackfillQsoEnrichmentRequest
        {
            Mode = BackfillQsoEnrichmentMode.Preview,
        };
        error = null;
        var modeSpecified = false;

        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--preview":
                    if (modeSpecified && request.Mode != BackfillQsoEnrichmentMode.Preview)
                    {
                        error = "--preview and --apply cannot be combined.";
                        return false;
                    }
                    request.Mode = BackfillQsoEnrichmentMode.Preview;
                    modeSpecified = true;
                    break;
                case "--apply":
                    if (modeSpecified && request.Mode != BackfillQsoEnrichmentMode.Apply)
                    {
                        error = "--preview and --apply cannot be combined.";
                        return false;
                    }
                    request.Mode = BackfillQsoEnrichmentMode.Apply;
                    modeSpecified = true;
                    break;
                case "--after" when index < args.Length - 1:
                    if (!TryParseUtc(args[++index], out var after))
                    {
                        error = "Invalid --after value. Use ISO 8601 UTC syntax, such as 2026-08-01T00:00:00Z.";
                        return false;
                    }
                    request.After = after;
                    break;
                case "--after":
                    error = "Missing value for --after.";
                    return false;
                case "--before" when index < args.Length - 1:
                    if (!TryParseUtc(args[++index], out var before))
                    {
                        error = "Invalid --before value. Use ISO 8601 UTC syntax, such as 2026-08-31T23:59:59Z.";
                        return false;
                    }
                    request.Before = before;
                    break;
                case "--before":
                    error = "Missing value for --before.";
                    return false;
                default:
                    error = $"Unknown option: {args[index]}";
                    return false;
            }
        }

        if (request.After is not null
            && request.Before is not null
            && request.After.ToDateTimeOffset() > request.Before.ToDateTimeOffset())
        {
            error = "--after must not be later than --before.";
            return false;
        }

        return true;
    }

    private static bool TryParseUtc(string value, out Timestamp timestamp)
    {
        if (DateTimeOffset.TryParse(
                value,
                CultureInfo.InvariantCulture,
                DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.RoundtripKind,
                out var parsed)
            && parsed.Offset == TimeSpan.Zero)
        {
            timestamp = Timestamp.FromDateTimeOffset(parsed);
            return true;
        }

        timestamp = null!;
        return false;
    }
}
