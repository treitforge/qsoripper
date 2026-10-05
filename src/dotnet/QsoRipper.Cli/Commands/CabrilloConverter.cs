using System.Globalization;
using System.Text;

namespace QsoRipper.Cli.Commands;

internal sealed record CabrilloConversionResult(
    byte[] AdifBytes,
    int RecordsConverted,
    IReadOnlyList<string> Warnings,
    DateTimeOffset? FirstQso,
    DateTimeOffset? LastQso,
    string ContestId);

internal static class CabrilloConverter
{
    public static async Task<CabrilloConversionResult> ConvertAsync(
        TextReader reader,
        string? digitalMode,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(reader);

        var adif = new StringBuilder("<ADIF_VER:5>3.1.7 <PROGRAMID:9>QsoRipper <EOH>\r\n");
        var warnings = new List<string>();
        var contestId = string.Empty;
        var sawStartOfLog = false;
        var recordsConverted = 0;
        DateTimeOffset? firstQso = null;
        DateTimeOffset? lastQso = null;
        var lineNumber = 0;

        while (await reader.ReadLineAsync(cancellationToken) is { } line)
        {
            lineNumber++;
            var trimmed = line.Trim();
            if (trimmed.Length == 0)
            {
                continue;
            }

            if (TryReadHeader(trimmed, "START-OF-LOG", out _))
            {
                sawStartOfLog = true;
                continue;
            }

            if (TryReadHeader(trimmed, "CONTEST", out var contest))
            {
                contestId = contest;
                continue;
            }

            if (trimmed.StartsWith("X-QSO:", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!trimmed.StartsWith("QSO:", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            if (!TryParseQso(line, contestId, digitalMode, out var qso, out var error))
            {
                warnings.Add($"Line {lineNumber}: {error}");
                continue;
            }

            var parsedQso = qso!;
            AppendRecord(adif, parsedQso);
            recordsConverted++;
            firstQso = firstQso is null || parsedQso.Timestamp < firstQso ? parsedQso.Timestamp : firstQso;
            lastQso = lastQso is null || parsedQso.Timestamp > lastQso ? parsedQso.Timestamp : lastQso;
        }

        if (!sawStartOfLog)
        {
            warnings.Insert(0, "The file has no START-OF-LOG header.");
        }

        return new CabrilloConversionResult(
            Encoding.UTF8.GetBytes(adif.ToString()),
            recordsConverted,
            warnings,
            firstQso,
            lastQso,
            contestId);
    }

    private static bool TryParseQso(
        string line,
        string contestId,
        string? digitalMode,
        out CabrilloQso? qso,
        out string error)
    {
        if (!TryReadFixedFields(line, out var fields)
            && !TryReadSeparatedFields(line, out fields))
        {
            qso = null;
            error = "The QSO fields do not match the Cabrillo QSO layout.";
            return false;
        }

        if (!TryParseTimestamp(fields.Date, fields.Time, out var timestamp))
        {
            qso = null;
            error = $"Invalid UTC date or time '{fields.Date} {fields.Time}'.";
            return false;
        }

        if (!TryMapMode(fields.Mode, digitalMode, out var mode, out error))
        {
            qso = null;
            return false;
        }

        if (!TryMapFrequency(fields.Frequency, out var band, out var frequencyMhz))
        {
            qso = null;
            error = $"Unsupported frequency or band '{fields.Frequency}'.";
            return false;
        }

        qso = new CabrilloQso(
            fields.StationCallsign.ToUpperInvariant(),
            fields.WorkedCallsign.ToUpperInvariant(),
            timestamp,
            band,
            mode,
            frequencyMhz,
            fields.RstSent,
            fields.RstReceived,
            fields.ExchangeSent,
            fields.ExchangeReceived,
            contestId);
        error = string.Empty;
        return true;
    }

    private static bool TryReadFixedFields(string line, out CabrilloFields fields)
    {
        fields = default;
        if (line.Length < 72
            || !line.StartsWith("QSO:", StringComparison.OrdinalIgnoreCase))
        {
            return false;
        }

        var candidate = new CabrilloFields(
            Slice(line, 5, 5),
            Slice(line, 11, 2),
            Slice(line, 14, 10),
            Slice(line, 25, 4),
            Slice(line, 30, 13),
            Slice(line, 44, 3),
            Slice(line, 48, 6),
            Slice(line, 55, 13),
            Slice(line, 69, 3),
            Slice(line, 73, 6));

        if (!IsCallsign(candidate.StationCallsign)
            || !IsCallsign(candidate.WorkedCallsign)
            || !LooksLikeReport(candidate.RstSent)
            || !LooksLikeReport(candidate.RstReceived))
        {
            return false;
        }

        fields = candidate;
        return true;
    }

    private static bool TryReadSeparatedFields(string line, out CabrilloFields fields)
    {
        fields = default;
        var tokens = line[4..].Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries);
        if (tokens.Length < 9 || !IsCallsign(tokens[4]) || !LooksLikeReport(tokens[5]))
        {
            return false;
        }

        var workedCallIndex = -1;
        for (var index = 7; index < tokens.Length - 1; index++)
        {
            if (IsCallsign(tokens[index]) && LooksLikeReport(tokens[index + 1]))
            {
                workedCallIndex = index;
                break;
            }
        }

        if (workedCallIndex < 0)
        {
            return false;
        }

        var end = tokens.Length;
        if (end > workedCallIndex + 2 && tokens[^1] is "0" or "1")
        {
            end--;
        }

        fields = new CabrilloFields(
            tokens[0],
            tokens[1],
            tokens[2],
            tokens[3],
            tokens[4],
            tokens[5],
            string.Join(' ', tokens[6..workedCallIndex]),
            tokens[workedCallIndex],
            tokens[workedCallIndex + 1],
            string.Join(' ', tokens[(workedCallIndex + 2)..end]));
        return true;
    }

    private static bool TryMapMode(
        string cabrilloMode,
        string? digitalMode,
        out string mode,
        out string error)
    {
        mode = cabrilloMode.ToUpperInvariant() switch
        {
            "CW" => "CW",
            "PH" or "SSB" => "SSB",
            "FM" => "FM",
            "RY" or "RTTY" => "RTTY",
            "AM" => "AM",
            "DG" when digitalMode is not null => digitalMode,
            _ => string.Empty,
        };

        if (mode.Length > 0)
        {
            error = string.Empty;
            return true;
        }

        error = cabrilloMode.Equals("DG", StringComparison.OrdinalIgnoreCase)
            ? "Cabrillo mode DG needs --digital-mode with an ADIF mode such as FT8."
            : $"Unsupported Cabrillo mode '{cabrilloMode}'.";
        return false;
    }

    private static bool TryMapFrequency(
        string value,
        out string band,
        out decimal? frequencyMhz)
    {
        var upper = value.ToUpperInvariant();
        band = upper switch
        {
            "1.2G" => "23CM",
            "2.3G" => "13CM",
            "3.4G" => "9CM",
            "5.7G" => "6CM",
            "10G" => "3CM",
            "24G" => "1.25CM",
            "47G" => "6MM",
            "75G" => "4MM",
            "122G" => "2.5MM",
            "134G" => "2MM",
            "241G" => "1MM",
            _ => string.Empty,
        };
        frequencyMhz = null;

        if (band.Length > 0)
        {
            return true;
        }

        if (!decimal.TryParse(value, NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture, out var numeric)
            || numeric <= 0)
        {
            return false;
        }

        frequencyMhz = numeric >= 1000 ? numeric / 1000 : numeric;
        band = frequencyMhz.Value switch
        {
            >= 0.1357m and <= 0.1378m => "2190M",
            >= 0.472m and <= 0.479m => "630M",
            >= 0.501m and <= 0.504m => "560M",
            >= 1.8m and <= 2.0m => "160M",
            >= 3.5m and <= 4.0m => "80M",
            >= 5.06m and <= 5.45m => "60M",
            >= 7.0m and <= 7.3m => "40M",
            >= 10.1m and <= 10.15m => "30M",
            >= 14.0m and <= 14.35m => "20M",
            >= 18.068m and <= 18.168m => "17M",
            >= 21.0m and <= 21.45m => "15M",
            >= 24.89m and <= 24.99m => "12M",
            >= 28.0m and <= 29.7m => "10M",
            >= 40m and <= 45m => "8M",
            >= 50m and <= 54m => "6M",
            > 54m and <= 69.9m => "5M",
            >= 70m and <= 71m => "4M",
            >= 144m and <= 148m => "2M",
            >= 222m and <= 225m => "1.25M",
            >= 420m and <= 450m => "70CM",
            >= 902m and <= 928m => "33CM",
            _ => string.Empty,
        };
        return band.Length > 0;
    }

    private static bool TryParseTimestamp(string date, string time, out DateTimeOffset timestamp)
    {
        return DateTimeOffset.TryParseExact(
            $"{date} {time}",
            "yyyy-MM-dd HHmm",
            CultureInfo.InvariantCulture,
            DateTimeStyles.AssumeUniversal | DateTimeStyles.AdjustToUniversal,
            out timestamp);
    }

    private static bool TryReadHeader(string line, string name, out string value)
    {
        var prefix = $"{name}:";
        if (line.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
        {
            value = line[prefix.Length..].Trim();
            return true;
        }

        value = string.Empty;
        return false;
    }

    private static bool IsCallsign(string value)
    {
        return value.Length is >= 3 and <= 32
            && value.Any(char.IsLetter)
            && value.Any(char.IsDigit)
            && value.All(static character => char.IsAsciiLetterOrDigit(character) || character == '/');
    }

    private static bool LooksLikeReport(string value)
    {
        return value.Length is >= 2 and <= 4
            && value.All(static character => char.IsAsciiDigit(character) || character is '+' or '-');
    }

    private static string Slice(string value, int start, int length)
    {
        if (start >= value.Length)
        {
            return string.Empty;
        }

        return value.Substring(start, Math.Min(length, value.Length - start)).Trim();
    }

    private static void AppendRecord(StringBuilder output, CabrilloQso qso)
    {
        AppendField(output, "CALL", qso.WorkedCallsign);
        AppendField(output, "STATION_CALLSIGN", qso.StationCallsign);
        AppendField(output, "QSO_DATE", qso.Timestamp.ToString("yyyyMMdd", CultureInfo.InvariantCulture));
        AppendField(output, "TIME_ON", qso.Timestamp.ToString("HHmm", CultureInfo.InvariantCulture));
        AppendField(output, "BAND", qso.Band);
        AppendField(output, "MODE", qso.Mode);
        if (qso.FrequencyMhz is { } frequency)
        {
            AppendField(output, "FREQ", frequency.ToString("0.######", CultureInfo.InvariantCulture));
        }

        AppendOptionalField(output, "RST_SENT", qso.RstSent);
        AppendOptionalField(output, "RST_RCVD", qso.RstReceived);
        AppendOptionalField(output, "CONTEST_ID", qso.ContestId);
        AppendOptionalField(output, "STX_STRING", qso.ExchangeSent);
        AppendOptionalField(output, "SRX_STRING", qso.ExchangeReceived);
        output.Append("<EOR>\r\n");
    }

    private static void AppendOptionalField(StringBuilder output, string name, string value)
    {
        if (!string.IsNullOrWhiteSpace(value))
        {
            AppendField(output, name, value);
        }
    }

    private static void AppendField(StringBuilder output, string name, string value)
    {
        output.Append('<')
            .Append(name)
            .Append(':')
            .Append(value.Length.ToString(CultureInfo.InvariantCulture))
            .Append('>')
            .Append(value)
            .Append(' ');
    }

    private readonly record struct CabrilloFields(
        string Frequency,
        string Mode,
        string Date,
        string Time,
        string StationCallsign,
        string RstSent,
        string ExchangeSent,
        string WorkedCallsign,
        string RstReceived,
        string ExchangeReceived);

    private sealed record CabrilloQso(
        string StationCallsign,
        string WorkedCallsign,
        DateTimeOffset Timestamp,
        string Band,
        string Mode,
        decimal? FrequencyMhz,
        string RstSent,
        string RstReceived,
        string ExchangeSent,
        string ExchangeReceived,
        string ContestId);
}
