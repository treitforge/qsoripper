using Grpc.Core;
using QsoRipper.Cli.Commands;
using QsoRipper.Services;

namespace QsoRipper.Cli.Tests;

#pragma warning disable CA1707
public sealed class EnrichCommandTests
{
    [Fact]
    public void Parse_defaults_to_preview()
    {
        Assert.True(EnrichCommand.TryParseArgs([], out var request, out var error));
        Assert.Null(error);
        Assert.Equal(BackfillQsoEnrichmentMode.Preview, request.Mode);
    }

    [Fact]
    public void Parse_accepts_apply_and_utc_filters()
    {
        Assert.True(EnrichCommand.TryParseArgs(
            ["--apply", "--after", "2026-08-01T00:00:00Z", "--before", "2026-08-31T23:59:59Z"],
            out var request,
            out var error));
        Assert.Null(error);
        Assert.Equal(BackfillQsoEnrichmentMode.Apply, request.Mode);
        Assert.NotNull(request.After);
        Assert.NotNull(request.Before);
    }

    [Theory]
    [InlineData("--preview", "--apply")]
    [InlineData("--after", "2026-08-01T00:00:00-07:00")]
    [InlineData("--after", "not-a-time")]
    public void Parse_rejects_invalid_options(string first, string second)
    {
        Assert.False(EnrichCommand.TryParseArgs([first, second], out _, out var error));
        Assert.NotNull(error);
    }

    [Fact]
    public async Task Consume_returns_failure_when_stream_has_no_complete_response()
    {
        var responses = new TestStreamReader(
            new BackfillQsoEnrichmentResponse { Scanned = 1 });

        var exitCode = await EnrichCommand.ConsumeResponsesAsync(
            responses,
            jsonOutput: true,
            CancellationToken.None);

        Assert.Equal(1, exitCode);
    }

    [Theory]
    [InlineData(1u, 0u)]
    [InlineData(0u, 1u)]
    public async Task Consume_returns_failure_for_reported_errors(uint lookupErrors, uint storageErrors)
    {
        var responses = new TestStreamReader(
            new BackfillQsoEnrichmentResponse
            {
                Complete = true,
                Errors = lookupErrors,
                StorageErrors = storageErrors,
            });

        var exitCode = await EnrichCommand.ConsumeResponsesAsync(
            responses,
            jsonOutput: true,
            CancellationToken.None);

        Assert.Equal(1, exitCode);
    }

    [Fact]
    public async Task Consume_allows_completed_not_found_results()
    {
        var responses = new TestStreamReader(
            new BackfillQsoEnrichmentResponse
            {
                Complete = true,
                NotFound = 2,
            });

        var exitCode = await EnrichCommand.ConsumeResponsesAsync(
            responses,
            jsonOutput: true,
            CancellationToken.None);

        Assert.Equal(0, exitCode);
    }

    private sealed class TestStreamReader(params BackfillQsoEnrichmentResponse[] responses)
        : IAsyncStreamReader<BackfillQsoEnrichmentResponse>
    {
        private int _index = -1;

        public BackfillQsoEnrichmentResponse Current => responses[_index];

        public Task<bool> MoveNext(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _index++;
            return Task.FromResult(_index < responses.Length);
        }
    }
}
#pragma warning restore CA1707
