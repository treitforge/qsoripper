using System.Globalization;
using System.Text;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Grpc.Net.Client;
using QsoRipper.Domain;
using QsoRipper.Services;

namespace QsoRipper.Cli.Commands;

internal static class ImportCabrilloCommand
{
    public static async Task<int> RunAsync(
        GrpcChannel channel,
        string filePath,
        string[] args,
        bool refresh,
        bool jsonOutput,
        CancellationToken cancellationToken = default)
    {
        if (!TryParseOptions(args, out var options, out var error))
        {
            Console.Error.WriteLine(error);
            return 1;
        }

        if (!File.Exists(filePath))
        {
            Console.Error.WriteLine($"File not found: {filePath}");
            return 1;
        }

        CabrilloConversionResult conversion;
        try
        {
            await using var input = File.OpenRead(filePath);
            using var reader = new StreamReader(input, Encoding.UTF8, detectEncodingFromByteOrderMarks: true);
            conversion = await CabrilloConverter.ConvertAsync(
                reader,
                options.DigitalMode,
                cancellationToken);

            if (options.AdifOutputPath is not null)
            {
                await File.WriteAllBytesAsync(
                    options.AdifOutputPath,
                    conversion.AdifBytes,
                    cancellationToken);
                Console.WriteLine($"ADIF file: {options.AdifOutputPath}");
            }
        }
        catch (IOException ex)
        {
            Console.Error.WriteLine($"File error: {ex.Message}");
            return 1;
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.Error.WriteLine($"File access error: {ex.Message}");
            return 1;
        }

        foreach (var warning in conversion.Warnings)
        {
            Console.WriteLine($"  Cabrillo warning: {warning}");
        }

        if (conversion.RecordsConverted == 0)
        {
            Console.Error.WriteLine("The Cabrillo file contains no importable QSO records.");
            return 1;
        }

        Console.WriteLine($"Converted: {conversion.RecordsConverted}");
        using var adif = new MemoryStream(conversion.AdifBytes, writable: false);
        var importResponse = await ImportAdifCommand.ImportAsync(
            channel,
            adif,
            refresh,
            cancellationToken);
        ImportAdifCommand.PrintResponse(importResponse);

        if (!options.Enrich)
        {
            return 0;
        }

        Console.WriteLine("Applying QRZ enrichment.");
        var enrichArgs = new List<string> { "--apply" };
        if (conversion.FirstQso is { } firstQso)
        {
            enrichArgs.Add("--after");
            enrichArgs.Add(firstQso.ToString("O", CultureInfo.InvariantCulture));
        }

        if (conversion.LastQso is { } lastQso)
        {
            enrichArgs.Add("--before");
            enrichArgs.Add(lastQso.ToString("O", CultureInfo.InvariantCulture));
        }

        try
        {
            return await EnrichCommand.RunAsync(
                channel,
                enrichArgs.ToArray(),
                jsonOutput,
                cancellationToken);
        }
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented)
        {
            Console.WriteLine("The engine does not support batch enrichment. Using compatible per-QSO enrichment.");
            return await EnrichCompatibleEngineAsync(channel, conversion, cancellationToken);
        }
    }

    internal static bool TryParseOptions(
        string[] args,
        out CabrilloImportOptions options,
        out string? error)
    {
        options = new CabrilloImportOptions();
        error = null;

        for (var index = 0; index < args.Length; index++)
        {
            switch (args[index])
            {
                case "--no-enrich":
                    options = options with { Enrich = false };
                    break;
                case "--adif-output" when index < args.Length - 1:
                    options = options with { AdifOutputPath = args[++index] };
                    break;
                case "--adif-output":
                    error = "Missing value for --adif-output.";
                    return false;
                case "--digital-mode" when index < args.Length - 1:
                    var value = args[++index];
                    try
                    {
                        options = options with
                        {
                            DigitalMode = EnumHelpers.FormatMode(EnumHelpers.ParseMode(value)),
                        };
                    }
                    catch (ArgumentException ex)
                    {
                        error = ex.Message;
                        return false;
                    }
                    break;
                case "--digital-mode":
                    error = "Missing value for --digital-mode.";
                    return false;
                default:
                    error = $"Unknown option: {args[index]}";
                    return false;
            }
        }

        return true;
    }

    private static async Task<int> EnrichCompatibleEngineAsync(
        GrpcChannel channel,
        CabrilloConversionResult conversion,
        CancellationToken cancellationToken)
    {
        var logbookClient = new LogbookService.LogbookServiceClient(channel);
        var lookupClient = new LookupService.LookupServiceClient(channel);
        var request = new ListQsosRequest
        {
            After = Timestamp.FromDateTimeOffset(conversion.FirstQso!.Value),
            Before = Timestamp.FromDateTimeOffset(conversion.LastQso!.Value),
            Limit = (uint)Math.Max(conversion.RecordsConverted + 100, 1000),
        };
        if (!string.IsNullOrWhiteSpace(conversion.ContestId))
        {
            request.ContestId = conversion.ContestId;
        }

        var scanned = 0;
        var changed = 0;
        var notFound = 0;
        var errors = 0;
        using var call = logbookClient.ListQsos(request, cancellationToken: cancellationToken);
        while (await call.ResponseStream.MoveNext(cancellationToken))
        {
            var qso = call.ResponseStream.Current.Qso;
            if (qso is null)
            {
                continue;
            }

            scanned++;
            try
            {
                var lookup = await lookupClient.LookupAsync(
                    new LookupRequest { Callsign = qso.WorkedCallsign },
                    cancellationToken: cancellationToken);
                if (lookup.Result?.State != LookupState.Found || lookup.Result.Record is null)
                {
                    notFound++;
                    continue;
                }

                if (!MergeMissingEnrichment(qso, lookup.Result.Record))
                {
                    continue;
                }

                var update = await logbookClient.UpdateQsoAsync(
                    new UpdateQsoRequest { Qso = qso },
                    cancellationToken: cancellationToken);
                if (update.Success)
                {
                    changed++;
                }
                else
                {
                    errors++;
                    Console.Error.WriteLine($"Enrichment update failed for {qso.WorkedCallsign}: {update.Error}");
                }
            }
            catch (RpcException ex)
            {
                errors++;
                Console.Error.WriteLine(
                    $"QRZ enrichment failed for {qso.WorkedCallsign}: {ex.Status.Detail} ({ex.StatusCode})");
            }
        }

        Console.WriteLine(
            $"Enrichment complete. Scanned {scanned}, changed {changed}, not found {notFound}, errors {errors}.");
        return errors == 0 ? 0 : 1;
    }

    private static bool MergeMissingEnrichment(QsoRecord qso, CallsignRecord record)
    {
        var changed = false;
        var fullName = !string.IsNullOrWhiteSpace(record.FormattedName)
            ? record.FormattedName
            : $"{record.FirstName} {record.LastName}".Trim();

        changed |= FillString(qso.HasWorkedOperatorName, qso.WorkedOperatorName, fullName, value => qso.WorkedOperatorName = value);
        changed |= FillString(qso.HasWorkedGrid, qso.WorkedGrid, record.GridSquare, value => qso.WorkedGrid = value);
        var country = !string.IsNullOrWhiteSpace(record.DxccCountryName)
            ? record.DxccCountryName
            : record.Country;
        changed |= FillString(qso.HasWorkedCountry, qso.WorkedCountry, country, value => qso.WorkedCountry = value);
        changed |= FillUInt32(qso.HasWorkedDxcc, record.DxccEntityId == 0 ? null : record.DxccEntityId, value => qso.WorkedDxcc = value);
        changed |= FillString(qso.HasWorkedState, qso.WorkedState, record.State, value => qso.WorkedState = value);
        changed |= FillString(qso.HasWorkedCounty, qso.WorkedCounty, record.County, value => qso.WorkedCounty = value);
        changed |= FillUInt32(qso.HasWorkedCqZone, record.HasCqZone ? record.CqZone : null, value => qso.WorkedCqZone = value);
        changed |= FillUInt32(qso.HasWorkedItuZone, record.HasItuZone ? record.ItuZone : null, value => qso.WorkedItuZone = value);
        changed |= FillString(qso.HasWorkedContinent, qso.WorkedContinent, record.DxccContinent, value => qso.WorkedContinent = value);
        changed |= FillDouble(qso.HasWorkedLatitude, record.HasLatitude ? record.Latitude : null, value => qso.WorkedLatitude = value);
        changed |= FillDouble(qso.HasWorkedLongitude, record.HasLongitude ? record.Longitude : null, value => qso.WorkedLongitude = value);
        return changed;
    }

    private static bool FillString(bool hasTarget, string target, string? source, Action<string> assign)
    {
        if ((hasTarget && !string.IsNullOrWhiteSpace(target)) || string.IsNullOrWhiteSpace(source))
        {
            return false;
        }

        assign(source.Trim());
        return true;
    }

    private static bool FillUInt32(bool hasTarget, uint? source, Action<uint> assign)
    {
        if (hasTarget || source is null)
        {
            return false;
        }

        assign(source.Value);
        return true;
    }

    private static bool FillDouble(bool hasTarget, double? source, Action<double> assign)
    {
        if (hasTarget || source is null)
        {
            return false;
        }

        assign(source.Value);
        return true;
    }
}

internal sealed record CabrilloImportOptions(
    bool Enrich = true,
    string? AdifOutputPath = null,
    string? DigitalMode = null);
