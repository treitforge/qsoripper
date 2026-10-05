using System.Text;
using QsoRipper.Cli.Commands;

namespace QsoRipper.Cli.Tests;

#pragma warning disable CA1707
public sealed class CabrilloConverterTests
{
    [Fact]
    public async Task Convert_maps_standard_qso_fields_to_adif()
    {
        const string input = """
            START-OF-LOG: 3.0
            CALLSIGN: HC8N
            CONTEST: CQ-WW-SSB
            QSO:  3799 PH 1999-03-06 0711 HC8N           59 700    W1AW           59 CT     0
            END-OF-LOG:
            """;

        var result = await ConvertAsync(input);
        var adif = Encoding.UTF8.GetString(result.AdifBytes);

        Assert.Equal(1, result.RecordsConverted);
        Assert.Empty(result.Warnings);
        Assert.Contains("<CALL:4>W1AW", adif, StringComparison.Ordinal);
        Assert.Contains("<STATION_CALLSIGN:4>HC8N", adif, StringComparison.Ordinal);
        Assert.Contains("<QSO_DATE:8>19990306", adif, StringComparison.Ordinal);
        Assert.Contains("<TIME_ON:4>0711", adif, StringComparison.Ordinal);
        Assert.Contains("<BAND:3>80M", adif, StringComparison.Ordinal);
        Assert.Contains("<MODE:3>SSB", adif, StringComparison.Ordinal);
        Assert.Contains("<FREQ:5>3.799", adif, StringComparison.Ordinal);
        Assert.Contains("<CONTEST_ID:9>CQ-WW-SSB", adif, StringComparison.Ordinal);
        Assert.Contains("<STX_STRING:3>700", adif, StringComparison.Ordinal);
        Assert.Contains("<SRX_STRING:2>CT", adif, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Convert_accepts_space_separated_multi_part_exchanges()
    {
        const string input = """
            START-OF-LOG: 3.0
            CONTEST: ARRL-FD
            QSO: 14045 CW 2026-06-28 1805 K7ABC 599 2A WWA W1AW 599 3A CT 0
            END-OF-LOG:
            """;

        var result = await ConvertAsync(input);
        var adif = Encoding.UTF8.GetString(result.AdifBytes);

        Assert.Equal(1, result.RecordsConverted);
        Assert.Contains("<STX_STRING:6>2A WWA", adif, StringComparison.Ordinal);
        Assert.Contains("<SRX_STRING:5>3A CT", adif, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Convert_requires_mode_for_generic_digital_records()
    {
        const string input = """
            START-OF-LOG: 3.0
            QSO: 14074 DG 2026-09-26 1701 K7ABC 599 WA W1AW 599 CT 0
            X-QSO: 14074 DG 2026-09-26 1702 K7ABC 599 WA N0CALL 599 CO 0
            END-OF-LOG:
            """;

        var withoutMode = await ConvertAsync(input);
        var withMode = await ConvertAsync(input, "FT8");

        Assert.Equal(0, withoutMode.RecordsConverted);
        Assert.Contains(withoutMode.Warnings, static warning => warning.Contains("--digital-mode", StringComparison.Ordinal));
        Assert.Equal(1, withMode.RecordsConverted);
        Assert.Contains("<MODE:3>FT8", Encoding.UTF8.GetString(withMode.AdifBytes), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Convert_reports_invalid_qso_and_missing_header()
    {
        const string input = "QSO: invalid";

        var result = await ConvertAsync(input);

        Assert.Equal(0, result.RecordsConverted);
        Assert.Equal(2, result.Warnings.Count);
        Assert.Contains(result.Warnings, static warning => warning.Contains("START-OF-LOG", StringComparison.Ordinal));
        Assert.Contains(result.Warnings, static warning => warning.Contains("Line 1", StringComparison.Ordinal));
    }

    [Theory]
    [InlineData(new[] { "--no-enrich" }, false, null, null)]
    [InlineData(new[] { "--digital-mode", "FT8" }, true, "FT8", null)]
    [InlineData(new[] { "--adif-output", "out.adi" }, true, null, "out.adi")]
    public void Parse_options_accepts_supported_values(
        string[] args,
        bool enrich,
        string? digitalMode,
        string? outputPath)
    {
        Assert.True(ImportCabrilloCommand.TryParseOptions(args, out var options, out var error));
        Assert.Null(error);
        Assert.Equal(enrich, options.Enrich);
        Assert.Equal(digitalMode, options.DigitalMode);
        Assert.Equal(outputPath, options.AdifOutputPath);
    }

    [Theory]
    [InlineData("--digital-mode")]
    [InlineData("--digital-mode", "BOGUS")]
    [InlineData("--adif-output")]
    [InlineData("--unknown")]
    public void Parse_options_rejects_invalid_values(params string[] args)
    {
        Assert.False(ImportCabrilloCommand.TryParseOptions(args, out _, out var error));
        Assert.NotNull(error);
    }

    private static Task<CabrilloConversionResult> ConvertAsync(
        string input,
        string? digitalMode = null)
    {
        return CabrilloConverter.ConvertAsync(new StringReader(input), digitalMode);
    }
}
#pragma warning restore CA1707
