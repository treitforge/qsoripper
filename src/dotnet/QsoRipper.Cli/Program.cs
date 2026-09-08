using Grpc.Core;
using Grpc.Net.Client;
using QsoRipper.Cli;
using QsoRipper.Cli.Commands;
using QsoRipper.EngineSelection;

var arguments = CliArgumentParser.Parse(args);

if (arguments.ShowHelp)
{
    return ShowHelp(arguments.Error);
}

if (!CliEndpointValidator.TryCreateEndpointUri(arguments.Endpoint, out var endpointUri))
{
    return ShowHelp($"The endpoint '{arguments.Endpoint}' must be a valid absolute http:// or https:// URI.");
}

var needsCallsign = CliCommandMetadata.RequiresPrimaryArgument(arguments.Command);

if (CliCommandMetadata.IsCommandHelp(arguments))
{
    return ShowCommandHelp(arguments.Command);
}

if (needsCallsign && string.IsNullOrEmpty(arguments.Callsign))
{
    return ShowCommandHelp(arguments.Command);
}

try
{
    using var channel = GrpcChannel.ForAddress(endpointUri!);
    using var cancellationSource = new CancellationTokenSource();
    ConsoleCancelEventHandler cancelHandler = (_, eventArgs) =>
    {
        eventArgs.Cancel = true;
        cancellationSource.Cancel();
    };
    Console.CancelKeyPress += cancelHandler;

    try
    {
        return arguments.Command switch
        {
            "status" => await StatusCommand.RunAsync(
                channel,
                arguments.Endpoint,
                arguments.EngineProfile,
                arguments.JsonOutput),
            "space-weather" => await SpaceWeatherCommand.RunAsync(channel, arguments.Refresh, arguments.JsonOutput),
            "contests" => await ContestCalendarCommand.RunAsync(channel, arguments.RemainingArgs, arguments.Refresh, arguments.JsonOutput),
            "lookup" => await LookupCommand.RunAsync(channel, arguments.Callsign!, arguments.SkipCache, arguments.JsonOutput),
            "stream-lookup" => await StreamLookupCommand.RunAsync(channel, arguments.Callsign!, arguments.SkipCache, cancellationSource.Token),
            "cache-check" => await CacheCheckCommand.RunAsync(channel, arguments.Callsign!, arguments.JsonOutput),
            "log" => await LogQsoCommand.RunAsync(channel, arguments.Callsign!, arguments.RemainingArgs),
            "get" => await GetQsoCommand.RunAsync(channel, arguments.Callsign!, arguments.JsonOutput),
            "list" => await ListQsosCommand.RunAsync(channel, arguments.RemainingArgs, arguments.JsonOutput, cancellationSource.Token),
            "enrich" => await EnrichCommand.RunAsync(channel, arguments.RemainingArgs, arguments.JsonOutput, cancellationSource.Token),
            "update" => await UpdateQsoCommand.RunAsync(channel, arguments.Callsign!, arguments.RemainingArgs),
            "delete" => await DeleteQsoCommand.RunAsync(channel, arguments.Callsign!),
            "restore" => await RestoreQsoCommand.RunAsync(channel, arguments.Callsign!),
            "purge" => await PurgeCommand.RunAsync(channel, arguments.RemainingArgs),
            "import" => await ImportAdifCommand.RunAsync(channel, arguments.Callsign ?? arguments.RemainingArgs.FirstOrDefault() ?? "", arguments.Refresh, cancellationSource.Token),
            "export" => await ExportAdifCommand.RunAsync(channel, arguments.RemainingArgs, cancellationSource.Token),
            "config" => await ConfigCommand.RunAsync(channel, arguments.RemainingArgs, arguments.JsonOutput),
            "setup" => await SetupCommand.RunAsync(channel, arguments),
            "sync" => await SyncCommand.RunAsync(channel, arguments.Force, cancellationSource.Token),
            "sync-status" => await SyncStatusCommand.RunAsync(channel, arguments.JsonOutput),
            "rig-status" => await RigStatusCommand.RunAsync(channel, arguments.JsonOutput),
            "cw" => await CwCommand.RunAsync(channel, arguments.RemainingArgs, arguments.JsonOutput),
            "test-logbook" => await TestLogbookCommand.RunAsync(channel, arguments.RemainingArgs),
            _ => ShowHelp($"Unknown command: {arguments.Command}")
        };
    }
    finally
    {
        Console.CancelKeyPress -= cancelHandler;
    }
}
catch (RpcException ex) when (ex.StatusCode == StatusCode.Unavailable)
{
    Console.Error.WriteLine(EngineReachability.FormatUnreachableMessage(arguments.EngineProfile, arguments.Endpoint));
    return 1;
}
catch (RpcException ex) when (ex.StatusCode == StatusCode.Unimplemented && arguments.Command is "contests")
{
    Console.Error.WriteLine(EngineReachability.FormatUnimplementedServiceMessage(
        arguments.EngineProfile,
        arguments.Endpoint,
        "ContestCalendarService"));
    return 1;
}
catch (RpcException ex)
{
    Console.Error.WriteLine($"gRPC error: {ex.Status.Detail} ({ex.StatusCode})");
    return 1;
}
catch (OperationCanceledException)
{
    Console.Error.WriteLine("Operation canceled.");
    return 130;
}

static int ShowHelp(string? error = null)
{
    if (error is not null)
    {
        Console.Error.WriteLine(error);
    }

    Console.WriteLine(CliHelpText.GetGeneralHelp());

    return error is null ? 0 : 1;
}

static int ShowCommandHelp(string command)
{
    Console.WriteLine(CliHelpText.GetCommandHelp(command));
    return 0;
}
