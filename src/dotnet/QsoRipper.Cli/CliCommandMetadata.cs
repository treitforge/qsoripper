namespace QsoRipper.Cli;

internal static class CliCommandMetadata
{
    public static bool UsesPrimaryArgument(string? command)
    {
        return command is "lookup" or "stream-lookup" or "cache-check" or "log" or "get" or "update" or "delete" or "restore" or "import" or "import-cabrillo";
    }

    public static bool RequiresPrimaryArgument(string? command)
    {
        return UsesPrimaryArgument(command);
    }

    public static bool IsCommandHelp(CliArguments arguments)
    {
        return arguments.Callsign is "HELP" or "-?" or "--HELP"
            || arguments.RemainingArgs.Any(static arg => arg is "help" or "-?" or "--help");
    }
}
