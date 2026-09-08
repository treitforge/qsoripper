using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using QsoRipper.Domain;
using QsoRipper.Engine.DotNet;
using QsoRipper.Engine.Lookup;
using QsoRipper.Engine.Storage.Memory;
using QsoRipper.Services;

namespace QsoRipper.Engine.DotNet.Tests;

#pragma warning disable CA1707
public sealed class EnrichmentBackfillTests : IDisposable
{
    private readonly string _configPath = Path.Combine(
        Path.GetTempPath(),
        "qsoripper-backfill-tests",
        $"{Guid.NewGuid():N}.toml");

    [Fact]
    public async Task Preview_deduplicates_callsigns_and_does_not_write()
    {
        var storage = new MemoryStorage();
        await storage.Logbook.InsertQsoAsync(CreateQso("one", " w1aw "));
        await storage.Logbook.InsertQsoAsync(CreateQso("two", "W1AW"));
        await storage.Logbook.InsertQsoAsync(CreateQso("portable", "W1AW/P"));
        var coordinator = new FakeLookupCoordinator(FoundRecord());
        var state = new ManagedEngineState(_configPath, storage, coordinator);

        var updates = await CollectAsync(state, new BackfillQsoEnrichmentRequest());

        var summary = Assert.Single(updates, static update => update.Complete);
        Assert.Equal(3ul, summary.Scanned);
        Assert.Equal(3ul, summary.Candidates);
        Assert.Equal(2ul, summary.UniqueCallsigns);
        Assert.Equal(2ul, summary.Found);
        Assert.Equal(3ul, summary.Changed);
        Assert.Equal(2, coordinator.LookupCount);
        Assert.False((await storage.Logbook.GetQsoAsync("one"))!.HasWorkedGrid);
    }

    [Fact]
    public async Task Apply_fills_blank_fields_without_overwrite_or_sync_changes_and_excludes_deleted()
    {
        var storage = new MemoryStorage();
        var active = CreateQso("active", "W1AW");
        active.WorkedGrid = " ";
        active.WorkedCountry = "Keep Country";
        active.SyncStatus = SyncStatus.Synced;
        active.QrzLogid = "qrz-123";
        await storage.Logbook.InsertQsoAsync(active);

        var deleted = CreateQso("deleted", "K1ABC");
        deleted.DeletedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
        await storage.Logbook.InsertQsoAsync(deleted);

        var state = new ManagedEngineState(
            _configPath,
            storage,
            new FakeLookupCoordinator(FoundRecord()));

        var updates = await CollectAsync(state, new BackfillQsoEnrichmentRequest
        {
            Mode = BackfillQsoEnrichmentMode.Apply,
        });

        var summary = updates[^1];
        Assert.True(summary.Complete);
        Assert.Equal(1ul, summary.Scanned);
        Assert.Equal(1ul, summary.Changed);
        var saved = (await storage.Logbook.GetQsoAsync("active"))!;
        Assert.Equal("FN31", saved.WorkedGrid);
        Assert.Equal("Keep Country", saved.WorkedCountry);
        Assert.Equal(SyncStatus.Synced, saved.SyncStatus);
        Assert.Equal("qrz-123", saved.QrzLogid);
        Assert.False((await storage.Logbook.GetQsoAsync("deleted"))!.HasWorkedGrid);
    }

    [Fact]
    public async Task Apply_counts_a_concurrent_operator_edit_and_preserves_it()
    {
        var storage = new MemoryStorage();
        await storage.Logbook.InsertQsoAsync(CreateQso("active", "W1AW"));
        var coordinator = new FakeLookupCoordinator(
            FoundRecord(),
            async () =>
            {
                var edited = (await storage.Logbook.GetQsoAsync("active"))!;
                edited.Notes = "operator edit";
                await storage.Logbook.UpdateQsoAsync(edited);
            });
        var state = new ManagedEngineState(_configPath, storage, coordinator);

        var updates = await CollectAsync(state, new BackfillQsoEnrichmentRequest
        {
            Mode = BackfillQsoEnrichmentMode.Apply,
        });

        Assert.Equal(1ul, updates[^1].ConcurrentEdits);
        var saved = (await storage.Logbook.GetQsoAsync("active"))!;
        Assert.Equal("operator edit", saved.Notes);
        Assert.False(saved.HasWorkedGrid);
    }

    [Fact]
    public void Only_one_backfill_can_hold_the_engine_lease()
    {
        var state = new ManagedEngineState(
            _configPath,
            new MemoryStorage(),
            new FakeLookupCoordinator(FoundRecord()));

        Assert.True(state.TryBeginEnrichmentBackfill(out var first));
        Assert.False(state.TryBeginEnrichmentBackfill(out _));
        first!.Dispose();
        Assert.True(state.TryBeginEnrichmentBackfill(out var second));
        second!.Dispose();
    }

    [Fact]
    public async Task Cancellation_waits_for_the_active_lookup_before_the_run_finishes()
    {
        var storage = new MemoryStorage();
        await storage.Logbook.InsertQsoAsync(CreateQso("active", "W1AW"));
        var lookupStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseLookup = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var state = new ManagedEngineState(
            _configPath,
            storage,
            new FakeLookupCoordinator(
                FoundRecord(),
                async () =>
                {
                    lookupStarted.SetResult();
                    await releaseLookup.Task;
                }));
        using var cancellation = new CancellationTokenSource();
        await using var updates = state
            .RunEnrichmentBackfillAsync(
                new BackfillQsoEnrichmentRequest { Mode = BackfillQsoEnrichmentMode.Apply },
                cancellation.Token)
            .GetAsyncEnumerator();

        Assert.True(await updates.MoveNextAsync());
        var activeMove = updates.MoveNextAsync().AsTask();
        await lookupStarted.Task;
        cancellation.Cancel();

        Assert.False(activeMove.IsCompleted);
        releaseLookup.SetResult();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => activeMove);
    }

    [Theory]
    [InlineData(0L, -1)]
    [InlineData(0L, 1_000_000_000)]
    [InlineData(253_402_300_800L, 0)]
    public void Request_validation_rejects_invalid_timestamps(long seconds, int nanos)
    {
        var error = Assert.Throws<RpcException>(() =>
            ManagedLogbookGrpcService.ValidateEnrichmentBackfillRequest(
                new BackfillQsoEnrichmentRequest
                {
                    After = new Timestamp { Seconds = seconds, Nanos = nanos },
                }));

        Assert.Equal(StatusCode.InvalidArgument, error.StatusCode);
    }

    [Fact]
    public void Request_validation_rejects_reversed_range()
    {
        var error = Assert.Throws<RpcException>(() =>
            ManagedLogbookGrpcService.ValidateEnrichmentBackfillRequest(
                new BackfillQsoEnrichmentRequest
                {
                    After = new Timestamp { Seconds = 10, Nanos = 1 },
                    Before = new Timestamp { Seconds = 10, Nanos = 0 },
                }));

        Assert.Equal(StatusCode.InvalidArgument, error.StatusCode);
    }

    public void Dispose()
    {
        var directory = Path.GetDirectoryName(_configPath);
        if (directory is not null && Directory.Exists(directory))
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    private static async Task<List<BackfillQsoEnrichmentResponse>> CollectAsync(
        ManagedEngineState state,
        BackfillQsoEnrichmentRequest request)
    {
        var updates = new List<BackfillQsoEnrichmentResponse>();
        await foreach (var update in state.RunEnrichmentBackfillAsync(request, CancellationToken.None))
        {
            updates.Add(update);
        }
        return updates;
    }

    private static QsoRecord CreateQso(string id, string callsign)
    {
        return new QsoRecord
        {
            LocalId = id,
            StationCallsign = "K7RND",
            WorkedCallsign = callsign,
            UtcTimestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.Parse(
                "2026-08-15T12:00:00Z",
                System.Globalization.CultureInfo.InvariantCulture)),
            Band = Band._20M,
            Mode = Mode.Cw,
        };
    }

    private static CallsignRecord FoundRecord()
    {
        return new CallsignRecord
        {
            Callsign = "W1AW",
            FirstName = "Ada",
            LastName = "Lovelace",
            GridSquare = "FN31",
            Country = "United States",
            DxccEntityId = 291,
            State = "CT",
            County = "Hartford",
            CqZone = 5,
            ItuZone = 8,
            DxccContinent = "NA",
            Latitude = 41.7,
            Longitude = -72.7,
        };
    }

    private sealed class FakeLookupCoordinator(
        CallsignRecord record,
        Func<Task>? beforeResult = null) : ILookupCoordinator
    {
        public int LookupCount { get; private set; }

        public async Task<LookupResult> LookupAsync(
            string callsign,
            bool skipCache = false,
            CancellationToken ct = default)
        {
            LookupCount++;
            if (beforeResult is not null)
            {
                await beforeResult().WaitAsync(ct);
            }
            return new LookupResult
            {
                State = LookupState.Found,
                QueriedCallsign = callsign,
                Record = record.Clone(),
            };
        }

        public Task<LookupResult> GetCachedAsync(string callsign)
        {
            return LookupAsync(callsign);
        }

        public async Task<LookupResult[]> StreamLookupAsync(
            string callsign,
            CancellationToken ct = default)
        {
            return [await LookupAsync(callsign, ct: ct)];
        }
    }
}
#pragma warning restore CA1707
