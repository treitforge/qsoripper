using QsoRipper.Cli;
using QsoRipper.EngineSelection;

namespace QsoRipper.Cli.Tests;

#pragma warning disable CA1707 // Remove underscores from member names - xUnit allows underscores in test methods
public class CliArgumentParserTests
{
    [Fact]
    public void Parse_reads_command_without_endpoint_override()
    {
        var arguments = CliArgumentParser.Parse(["status"]);

        Assert.Equal("status", arguments.Command);
        Assert.Equal(CliArgumentParser.DefaultEndpoint, arguments.Endpoint);
        Assert.Equal(KnownEngineProfiles.LocalRust, arguments.EngineProfile.ProfileId);
        Assert.False(arguments.ShowHelp);
    }

    [Fact]
    public void Parse_skips_endpoint_value_when_finding_command()
    {
        var arguments = CliArgumentParser.Parse(["--endpoint", "http://localhost:6001", "status"]);

        Assert.Equal("status", arguments.Command);
        Assert.Equal("http://localhost:6001", arguments.Endpoint);
        Assert.False(arguments.ShowHelp);
    }

    [Fact]
    public void Parse_returns_error_when_endpoint_value_is_missing()
    {
        var arguments = CliArgumentParser.Parse(["--endpoint"]);

        Assert.True(arguments.ShowHelp);
        Assert.Equal("Missing value for --endpoint.", arguments.Error);
    }

    [Fact]
    public void Parse_captures_callsign_as_second_positional_arg()
    {
        var arguments = CliArgumentParser.Parse(["lookup", "W1AW"]);

        Assert.Equal("lookup", arguments.Command);
        Assert.Equal("W1AW", arguments.Callsign);
        Assert.False(arguments.SkipCache);
    }

    [Fact]
    public void Parse_preserves_argument_case()
    {
        var arguments = CliArgumentParser.Parse(["get", "a1b2c3d4-e5f6"]);

        Assert.Equal("a1b2c3d4-e5f6", arguments.Callsign);
    }

    [Fact]
    public void Parse_captures_skip_cache_flag()
    {
        var arguments = CliArgumentParser.Parse(["lookup", "K7ABV", "--skip-cache"]);

        Assert.Equal("lookup", arguments.Command);
        Assert.Equal("K7ABV", arguments.Callsign);
        Assert.True(arguments.SkipCache);
    }

    [Fact]
    public void Parse_skip_cache_before_callsign()
    {
        var arguments = CliArgumentParser.Parse(["lookup", "--skip-cache", "N0CALL"]);

        Assert.Equal("lookup", arguments.Command);
        Assert.Equal("N0CALL", arguments.Callsign);
        Assert.True(arguments.SkipCache);
    }

    [Fact]
    public void Parse_command_without_callsign_leaves_it_null()
    {
        var arguments = CliArgumentParser.Parse(["lookup"]);

        Assert.Equal("lookup", arguments.Command);
        Assert.Null(arguments.Callsign);
    }

    [Fact]
    public void Parse_all_options_together()
    {
        var arguments = CliArgumentParser.Parse(["--endpoint", "http://host:9090", "stream-lookup", "AA1AA", "--skip-cache"]);

        Assert.Equal("stream-lookup", arguments.Command);
        Assert.Equal("http://host:9090", arguments.Endpoint);
        Assert.Equal("AA1AA", arguments.Callsign);
        Assert.True(arguments.SkipCache);
    }

    [Fact]
    public void Parse_engine_switch_updates_default_endpoint()
    {
        var arguments = CliArgumentParser.Parse(["--engine", "dotnet", "status"]);

        Assert.Equal("status", arguments.Command);
        Assert.Equal(KnownEngineProfiles.LocalDotNet, arguments.EngineProfile.ProfileId);
        Assert.Equal(EngineCatalog.DefaultDotNetEndpoint, arguments.Endpoint);
    }

    [Fact]
    public void Parse_returns_error_for_unknown_engine()
    {
        var arguments = CliArgumentParser.Parse(["--engine", "fortran", "status"]);

        Assert.True(arguments.ShowHelp);
        Assert.Equal(
            $"Unknown engine profile 'fortran'. Known values: {EngineCatalog.GetKnownProfileList()}.",
            arguments.Error);
    }

    [Fact]
    public void Parse_setup_status_flag()
    {
        var arguments = CliArgumentParser.Parse(["setup", "--status"]);

        Assert.Equal("setup", arguments.Command);
        Assert.True(arguments.SetupStatus);
        Assert.False(arguments.SetupFromEnv);
    }

    [Fact]
    public void Parse_setup_from_env_flag()
    {
        var arguments = CliArgumentParser.Parse(["setup", "--from-env"]);

        Assert.Equal("setup", arguments.Command);
        Assert.False(arguments.SetupStatus);
        Assert.True(arguments.SetupFromEnv);
    }

    [Fact]
    public void Parse_setup_without_flags_defaults_both_false()
    {
        var arguments = CliArgumentParser.Parse(["setup"]);

        Assert.Equal("setup", arguments.Command);
        Assert.False(arguments.SetupStatus);
        Assert.False(arguments.SetupFromEnv);
    }

    [Fact]
    public void Parse_setup_status_with_json()
    {
        var arguments = CliArgumentParser.Parse(["setup", "--status", "--json"]);

        Assert.Equal("setup", arguments.Command);
        Assert.True(arguments.SetupStatus);
        Assert.True(arguments.JsonOutput);
    }

    [Fact]
    public void Parse_setup_from_env_with_endpoint()
    {
        var arguments = CliArgumentParser.Parse(["--endpoint", "http://host:9090", "setup", "--from-env"]);

        Assert.Equal("setup", arguments.Command);
        Assert.Equal("http://host:9090", arguments.Endpoint);
        Assert.True(arguments.SetupFromEnv);
    }

    [Fact]
    public void Parse_space_weather_refresh_with_json()
    {
        var arguments = CliArgumentParser.Parse(["space-weather", "--refresh", "--json"]);

        Assert.Equal("space-weather", arguments.Command);
        Assert.True(arguments.Refresh);
        Assert.True(arguments.JsonOutput);
        Assert.Null(arguments.Callsign);
    }

    [Fact]
    public void Parse_contests_preserves_command_arguments()
    {
        var arguments = CliArgumentParser.Parse(["contests", "active", "--band", "20m", "--mode", "CW", "--lookahead-hours", "12"]);

        Assert.Equal("contests", arguments.Command);
        Assert.Equal(["active", "--band", "20m", "--mode", "CW", "--lookahead-hours", "12"], arguments.RemainingArgs);
        Assert.Null(arguments.Callsign);
    }

    [Fact]
    public void Parse_cabrillo_import_keeps_file_and_options()
    {
        var arguments = CliArgumentParser.Parse(
            ["import-cabrillo", "contest.log", "--digital-mode", "FT8", "--no-enrich"]);

        Assert.Equal("import-cabrillo", arguments.Command);
        Assert.Equal("contest.log", arguments.Callsign);
        Assert.Equal(["--digital-mode", "FT8", "--no-enrich"], arguments.RemainingArgs);
    }

    [Theory]
    [InlineData("http://localhost:50051", true)]
    [InlineData("https://example.com:7443", true)]
    [InlineData("localhost:50051", false)]
    [InlineData("grpc://localhost:50051", false)]
    [InlineData("not a uri", false)]
    public void Endpoint_validator_accepts_only_absolute_http_uris(string endpoint, bool expected)
    {
        var isValid = CliEndpointValidator.TryCreateEndpointUri(endpoint, out var uri);

        Assert.Equal(expected, isValid);

        if (expected)
        {
            Assert.NotNull(uri);
        }
    }
}
#pragma warning restore CA1707
