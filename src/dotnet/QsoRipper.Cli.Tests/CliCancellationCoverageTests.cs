using System.Reflection;
using QsoRipper.Cli.Commands;

namespace QsoRipper.Cli.Tests;

#pragma warning disable CA1707 // Remove underscores from member names - xUnit allows underscores in test methods
public sealed class CliCancellationCoverageTests
{
    [Theory]
    [InlineData(typeof(ImportAdifCommand))]
    [InlineData(typeof(ExportAdifCommand))]
    [InlineData(typeof(ListQsosCommand))]
    [InlineData(typeof(EnrichCommand))]
    [InlineData(typeof(SyncCommand))]
    [InlineData(typeof(StreamLookupCommand))]
    public void RunAsync_methods_accept_cancellation_token(Type commandType)
    {
        ArgumentNullException.ThrowIfNull(commandType);

        var runAsync = commandType.GetMethod(
            "RunAsync",
            BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static);

        Assert.NotNull(runAsync);
        Assert.Contains(
            runAsync!.GetParameters(),
            static parameter => parameter.ParameterType == typeof(CancellationToken));
    }
}
#pragma warning restore CA1707
