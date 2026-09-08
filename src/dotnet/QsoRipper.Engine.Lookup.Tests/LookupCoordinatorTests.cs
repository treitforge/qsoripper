using Google.Protobuf.WellKnownTypes;
using QsoRipper.Domain;
using QsoRipper.Engine.Lookup.Qrz;
using QsoRipper.Engine.Storage;

namespace QsoRipper.Engine.Lookup.Tests;

#pragma warning disable CA1707 // Remove underscores from member names - xUnit allows underscores in test methods
public sealed class LookupCoordinatorTests
{
    private sealed class FakeProvider : ICallsignProvider
    {
        private readonly Queue<ProviderLookupResult> _responses = new();
        private int _callCount;

        public string ProviderName => "fake";
        public int CallCount => _callCount;

        public void Enqueue(ProviderLookupResult result) => _responses.Enqueue(result);

        public Task<ProviderLookupResult> LookupAsync(string callsign, CancellationToken ct = default)
        {
            Interlocked.Increment(ref _callCount);
            return Task.FromResult(_responses.Count > 0
                ? _responses.Dequeue()
                : new ProviderLookupResult { State = ProviderLookupState.NotFound });
        }
    }

    private sealed class BlockingProvider : ICallsignProvider
    {
        private readonly TaskCompletionSource<ProviderLookupResult> _completion =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public string ProviderName => "blocking";

        public Task<ProviderLookupResult> LookupAsync(string callsign, CancellationToken ct = default) =>
            _completion.Task.WaitAsync(ct);

        public void Complete(ProviderLookupResult result) => _completion.SetResult(result);
    }

    private static ProviderLookupResult FoundResult(string callsign, string firstName = "Test") =>
        new()
        {
            State = ProviderLookupState.Found,
            Record = new CallsignRecord
            {
                Callsign = callsign,
                CrossRef = callsign,
                FirstName = firstName,
                LastName = "Operator",
            },
        };

    [Fact]
    public async Task Lookup_ReturnsFoundResult()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var coordinator = new LookupCoordinator(provider);

        var result = await coordinator.LookupAsync("W1AW");

        Assert.Equal(LookupState.Found, result.State);
        Assert.False(result.CacheHit);
        Assert.Equal("W1AW", result.QueriedCallsign);
        Assert.NotNull(result.Record);
        Assert.Equal("W1AW", result.Record.Callsign);
    }

    [Fact]
    public async Task Lookup_ReturnsCacheHitOnSecondCall()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var coordinator = new LookupCoordinator(provider);

        var first = await coordinator.LookupAsync("w1aw");
        var second = await coordinator.LookupAsync("w1aw");

        Assert.Equal(LookupState.Found, first.State);
        Assert.False(first.CacheHit);
        Assert.Equal(LookupState.Found, second.State);
        Assert.True(second.CacheHit);
        Assert.Equal(1, provider.CallCount);
    }

    [Fact]
    public async Task Lookup_SkipCacheForcesProviderLookup()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW", "First"));
        provider.Enqueue(FoundResult("W1AW", "Second"));
        var coordinator = new LookupCoordinator(provider);

        _ = await coordinator.LookupAsync("w1aw");
        var second = await coordinator.LookupAsync("w1aw", skipCache: true);

        Assert.Equal(LookupState.Found, second.State);
        Assert.False(second.CacheHit);
        Assert.Equal(2, provider.CallCount);
    }

    [Fact]
    public async Task Lookup_NormalizesToUppercase()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var coordinator = new LookupCoordinator(provider);

        var result = await coordinator.LookupAsync("  w1aw  ");

        Assert.Equal("W1AW", result.QueriedCallsign);
        Assert.Equal(LookupState.Found, result.State);
    }

    [Fact]
    public async Task Lookup_NotFound_IsCachedWithNegativeTtl()
    {
        var provider = new FakeProvider();
        provider.Enqueue(new ProviderLookupResult { State = ProviderLookupState.NotFound });
        var coordinator = new LookupCoordinator(provider, negativeTtl: TimeSpan.FromMinutes(5));

        var first = await coordinator.LookupAsync("NOTEXIST");
        var second = await coordinator.LookupAsync("NOTEXIST");

        Assert.Equal(LookupState.NotFound, first.State);
        Assert.Equal(LookupState.NotFound, second.State);
        Assert.True(second.CacheHit);
        Assert.Equal(1, provider.CallCount);
    }

    [Fact]
    public async Task Lookup_SlashCallFallsBackToBase()
    {
        var provider = new FakeProvider();
        // First lookup for K7ABC/M returns not found, second for K7ABC returns found
        provider.Enqueue(new ProviderLookupResult { State = ProviderLookupState.NotFound });
        provider.Enqueue(FoundResult("K7ABC"));
        var coordinator = new LookupCoordinator(provider);

        var result = await coordinator.LookupAsync("K7ABC/M");

        Assert.Equal(LookupState.Found, result.State);
        Assert.Equal("K7ABC/M", result.QueriedCallsign);
        Assert.Equal(2, provider.CallCount);
    }

    [Fact]
    public async Task Lookup_NoSlashNoFallback()
    {
        var provider = new FakeProvider();
        provider.Enqueue(new ProviderLookupResult { State = ProviderLookupState.NotFound });
        var coordinator = new LookupCoordinator(provider);

        var result = await coordinator.LookupAsync("W1AW");

        Assert.Equal(LookupState.NotFound, result.State);
        Assert.Equal(1, provider.CallCount);
    }

    [Fact]
    public async Task Lookup_SlashCallExactFoundDoesNotFallback()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("K7ABC/M"));
        var coordinator = new LookupCoordinator(provider);

        var result = await coordinator.LookupAsync("K7ABC/M");

        Assert.Equal(LookupState.Found, result.State);
        Assert.Equal(1, provider.CallCount);
    }

    [Fact]
    public async Task GetCached_ReturnsCachedResult()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var coordinator = new LookupCoordinator(provider);

        _ = await coordinator.LookupAsync("W1AW");
        var cached = await coordinator.GetCachedAsync("W1AW");

        Assert.Equal(LookupState.Found, cached.State);
    }

    [Fact]
    public async Task GetCached_ReturnsNotFoundWhenNotCached()
    {
        var provider = new FakeProvider();
        var coordinator = new LookupCoordinator(provider);

        var cached = await coordinator.GetCachedAsync("W1AW");

        Assert.Equal(LookupState.NotFound, cached.State);
        Assert.False(cached.CacheHit);
    }

    [Fact]
    public async Task StreamLookup_EmitsLoadingThenFound()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var coordinator = new LookupCoordinator(provider);

        var updates = await coordinator.StreamLookupAsync("W1AW");

        Assert.Equal(2, updates.Length);
        Assert.Equal(LookupState.Loading, updates[0].State);
        Assert.Equal(LookupState.Found, updates[1].State);
    }

    [Fact]
    public async Task StreamLookup_YieldsLoadingBeforeProviderCompletes()
    {
        var provider = new BlockingProvider();
        var coordinator = new LookupCoordinator(provider);
        await using var updates = coordinator
            .StreamLookupIncrementallyAsync("W1AW")
            .GetAsyncEnumerator();

        Assert.True(await updates.MoveNextAsync());
        Assert.Equal(LookupState.Loading, updates.Current.State);

        provider.Complete(FoundResult("W1AW"));
        Assert.True(await updates.MoveNextAsync());
        Assert.Equal(LookupState.Found, updates.Current.State);
        Assert.False(await updates.MoveNextAsync());
    }

    [Fact]
    public async Task StreamLookup_EmitsLoadingStaleThenRefreshed()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW", "Cached"));
        provider.Enqueue(FoundResult("W1AW", "Fresh"));
        // Use 1ms positive TTL so the cache entry goes stale immediately.
        var coordinator = new LookupCoordinator(provider, positiveTtl: TimeSpan.FromMilliseconds(1));

        _ = await coordinator.LookupAsync("W1AW");
        await Task.Delay(5);
        var updates = await coordinator.StreamLookupAsync("W1AW");

        Assert.Equal(3, updates.Length);
        Assert.Equal(LookupState.Loading, updates[0].State);
        Assert.Equal(LookupState.Stale, updates[1].State);
        Assert.Equal("Cached", updates[1].Record!.FirstName);
        Assert.Equal(LookupState.Found, updates[2].State);
        Assert.Equal("Fresh", updates[2].Record!.FirstName);
    }

    [Fact]
    public async Task StreamLookup_FreshCacheReturnsCacheHit()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var coordinator = new LookupCoordinator(provider, positiveTtl: TimeSpan.FromMinutes(15));

        _ = await coordinator.LookupAsync("W1AW");
        var updates = await coordinator.StreamLookupAsync("W1AW");

        Assert.Equal(2, updates.Length);
        Assert.Equal(LookupState.Loading, updates[0].State);
        Assert.Equal(LookupState.Found, updates[1].State);
        Assert.True(updates[1].CacheHit);
        Assert.Equal(1, provider.CallCount);
    }

    [Fact]
    public async Task StreamLookup_SkipCacheBypassesFreshEntry()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW", "Cached"));
        provider.Enqueue(FoundResult("W1AW", "Fresh"));
        var coordinator = new LookupCoordinator(provider, positiveTtl: TimeSpan.FromMinutes(15));

        _ = await coordinator.LookupAsync("W1AW");
        var updates = new List<LookupResult>();
        await foreach (var update in coordinator.StreamLookupIncrementallyAsync("W1AW", skipCache: true))
        {
            updates.Add(update);
        }

        Assert.Equal(2, updates.Count);
        Assert.Equal("Fresh", updates[^1].Record!.FirstName);
        Assert.False(updates[^1].CacheHit);
        Assert.Equal(2, provider.CallCount);
    }

    [Fact]
    public async Task StreamLookup_PropagatesCancellationToProvider()
    {
        var coordinator = new LookupCoordinator(new BlockingProvider());
        using var cancellation = new CancellationTokenSource();
        await using var updates = coordinator
            .StreamLookupIncrementallyAsync("W1AW", ct: cancellation.Token)
            .GetAsyncEnumerator();

        Assert.True(await updates.MoveNextAsync());
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await updates.MoveNextAsync());
    }

    [Fact]
    public async Task Lookup_CancellationOnlyCancelsThatWaiter()
    {
        var provider = new BlockingProvider();
        var coordinator = new LookupCoordinator(provider);
        using var cancellation = new CancellationTokenSource();

        var first = coordinator.LookupAsync("W1AW", skipCache: true, cancellation.Token);
        var second = coordinator.LookupAsync("W1AW", skipCache: true);
        cancellation.Cancel();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        provider.Complete(FoundResult("W1AW"));
        var result = await second;

        Assert.Equal(LookupState.Found, result.State);
    }

    [Fact]
    public async Task Lookup_SharedProviderWorkHasAnIndependentTimeout()
    {
        var coordinator = new LookupCoordinator(
            new BlockingProvider(),
            providerTimeout: TimeSpan.FromMilliseconds(20));

        var result = await coordinator.LookupAsync("W1AW", skipCache: true);

        Assert.Equal(LookupState.Error, result.State);
        Assert.Contains("timed out", result.ErrorMessage, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public async Task Lookup_ProviderError_ReturnsErrorState()
    {
        var provider = new FakeProvider();
        provider.Enqueue(new ProviderLookupResult
        {
            State = ProviderLookupState.AuthenticationError,
            ErrorMessage = "Bad credentials",
        });
        var coordinator = new LookupCoordinator(provider);

        var result = await coordinator.LookupAsync("W1AW");

        Assert.Equal(LookupState.Error, result.State);
        Assert.Contains("Bad credentials", result.ErrorMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task DisabledProvider_ReturnsError()
    {
        var provider = new DisabledCallsignProvider();
        var result = await provider.LookupAsync("W1AW");

        Assert.Equal(ProviderLookupState.AuthenticationError, result.State);
        Assert.Contains("credentials", result.ErrorMessage, StringComparison.OrdinalIgnoreCase);
        Assert.Equal("disabled", provider.ProviderName);
    }

    [Fact]
    public async Task Lookup_PopulatesPriorQsosFromLogbook()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var logbook = new FakeLogbookStore();
        logbook.Add(MakeQso("h1", "W1AW", Band._20M, Mode.Ssb, 1_700_000_000));
        logbook.Add(MakeQso("h2", "W1AW", Band._40M, Mode.Cw, 1_700_001_000));
        logbook.Add(MakeQso("h3", "K7XYZ", Band._20M, Mode.Ssb, 1_700_002_000));
        var coordinator = new LookupCoordinator(provider, logbookStore: logbook);

        var result = await coordinator.LookupAsync("W1AW");

        Assert.Equal(LookupState.Found, result.State);
        Assert.Equal(2u, result.PriorQsoTotalCount);
        Assert.Equal(2, result.PriorQsos.Count);
        Assert.Equal("h2", result.PriorQsos[0].LocalId);
        Assert.Equal("h1", result.PriorQsos[1].LocalId);
        Assert.Equal(Band._40M, result.PriorQsos[0].Band);
    }

    [Fact]
    public async Task Lookup_NotFound_StillPopulatesPriorQsosFromLogbook()
    {
        var provider = new FakeProvider();
        provider.Enqueue(new ProviderLookupResult { State = ProviderLookupState.NotFound });
        var logbook = new FakeLogbookStore();
        logbook.Add(MakeQso("h1", "W1AW", Band._20M, Mode.Ssb, 1_700_000_000));
        var coordinator = new LookupCoordinator(provider, logbookStore: logbook);

        var result = await coordinator.LookupAsync("W1AW");

        Assert.Equal(LookupState.NotFound, result.State);
        Assert.Equal(1u, result.PriorQsoTotalCount);
        Assert.Single(result.PriorQsos);
    }

    [Fact]
    public async Task GetCached_PopulatesPriorQsosOnCacheMiss()
    {
        var provider = new FakeProvider();
        var logbook = new FakeLogbookStore();
        logbook.Add(MakeQso("h1", "W1AW", Band._20M, Mode.Ssb, 1_700_000_000));
        var coordinator = new LookupCoordinator(provider, logbookStore: logbook);

        var result = await coordinator.GetCachedAsync("W1AW");

        Assert.Equal(LookupState.NotFound, result.State);
        Assert.Equal(1u, result.PriorQsoTotalCount);
        Assert.Single(result.PriorQsos);
    }

    [Fact]
    public async Task StreamLookup_DoesNotAttachHistoryToLoadingState()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var logbook = new FakeLogbookStore();
        logbook.Add(MakeQso("h1", "W1AW", Band._20M, Mode.Ssb, 1_700_000_000));
        var coordinator = new LookupCoordinator(provider, logbookStore: logbook);

        var updates = await coordinator.StreamLookupAsync("W1AW");

        Assert.True(updates.Length >= 2);
        var loading = updates[0];
        Assert.Equal(LookupState.Loading, loading.State);
        Assert.Empty(loading.PriorQsos);
        Assert.Equal(0u, loading.PriorQsoTotalCount);

        var final = updates[^1];
        Assert.Equal(LookupState.Found, final.State);
        Assert.Equal(1u, final.PriorQsoTotalCount);
        Assert.Single(final.PriorQsos);
    }

    [Fact]
    public async Task Lookup_DoesNotPersistPriorQsosToSnapshotStore()
    {
        var provider = new FakeProvider();
        provider.Enqueue(FoundResult("W1AW"));
        var logbook = new FakeLogbookStore();
        logbook.Add(MakeQso("h1", "W1AW", Band._20M, Mode.Ssb, 1_700_000_000));
        logbook.Add(MakeQso("h2", "W1AW", Band._40M, Mode.Cw, 1_700_001_000));
        var snapshots = new InMemoryLookupSnapshotStore();
        var coordinator = new LookupCoordinator(provider, snapshots, logbookStore: logbook);

        var result = await coordinator.LookupAsync("W1AW");

        Assert.Equal(2, result.PriorQsos.Count);

        var snapshot = await snapshots.GetAsync("W1AW");
        Assert.NotNull(snapshot);
        Assert.Empty(snapshot!.Result.PriorQsos);
        Assert.Equal(0u, snapshot.Result.PriorQsoTotalCount);
    }

    private sealed class InMemoryLookupSnapshotStore : ILookupSnapshotStore
    {
        private readonly Dictionary<string, LookupSnapshot> _snapshots = new(StringComparer.OrdinalIgnoreCase);

        public ValueTask UpsertAsync(LookupSnapshot snapshot)
        {
            _snapshots[snapshot.Callsign] = snapshot;
            return ValueTask.CompletedTask;
        }

        public ValueTask<LookupSnapshot?> GetAsync(string callsign)
        {
            _snapshots.TryGetValue(callsign, out var snapshot);
            return ValueTask.FromResult<LookupSnapshot?>(snapshot);
        }

        public ValueTask<bool> DeleteAsync(string callsign)
        {
            return ValueTask.FromResult(_snapshots.Remove(callsign));
        }
    }

    private static QsoRecord MakeQso(string localId, string worked, Band band, Mode mode, long utcSeconds) =>
        new()
        {
            LocalId = localId,
            StationCallsign = "K7TEST",
            WorkedCallsign = worked,
            Band = band,
            Mode = mode,
            UtcTimestamp = new Timestamp { Seconds = utcSeconds },
        };

    private sealed class FakeLogbookStore : ILogbookStore
    {
        private readonly List<QsoRecord> _qsos = new();

        public void Add(QsoRecord qso) => _qsos.Add(qso);

        public ValueTask InsertQsoAsync(QsoRecord qso) { _qsos.Add(qso); return ValueTask.CompletedTask; }
        public ValueTask<bool> UpdateQsoAsync(QsoRecord qso) => ValueTask.FromResult(false);
        public ValueTask<bool> UpdateQsoIfUnchangedAsync(QsoRecord expected, QsoRecord replacement) => ValueTask.FromResult(false);
        public ValueTask<QsoRecord?> UpdateQrzSyncMetadataAsync(string localId, Timestamp? expectedUpdatedAt, string qrzLogid) => ValueTask.FromResult<QsoRecord?>(null);
        public ValueTask<bool> DeleteQsoAsync(string localId) => ValueTask.FromResult(false);
        public ValueTask<bool> SoftDeleteQsoAsync(string localId, DateTimeOffset deletedAt, bool pendingRemoteDelete) => ValueTask.FromResult(false);
        public ValueTask<bool> RestoreQsoAsync(string localId) => ValueTask.FromResult(false);
        public ValueTask<QsoRecord?> GetQsoAsync(string localId) => ValueTask.FromResult<QsoRecord?>(null);
        public ValueTask<IReadOnlyList<QsoRecord>> ListQsosAsync(QsoListQuery query) =>
            ValueTask.FromResult<IReadOnlyList<QsoRecord>>(_qsos);
        public ValueTask<LogbookCounts> GetCountsAsync() => ValueTask.FromResult(new LogbookCounts(_qsos.Count, 0));
        public ValueTask<int> PurgeDeletedQsosAsync(IReadOnlyList<string>? localIds, DateTimeOffset? olderThan) => ValueTask.FromResult(0);
        public ValueTask<SyncMetadata> GetSyncMetadataAsync() => ValueTask.FromResult(new SyncMetadata());
        public ValueTask UpsertSyncMetadataAsync(SyncMetadata metadata) => ValueTask.CompletedTask;

        public ValueTask<QsoHistoryPage> ListQsoHistoryAsync(string workedCallsign, int limit)
        {
            var matches = _qsos
                .Where(q => string.Equals(q.WorkedCallsign, workedCallsign, StringComparison.OrdinalIgnoreCase))
                .ToList();
            var ordered = matches
                .OrderByDescending(q => q.UtcTimestamp?.Seconds ?? 0)
                .ThenByDescending(q => q.LocalId, StringComparer.Ordinal)
                .Take(Math.Max(0, limit))
                .ToList();
            return ValueTask.FromResult(new QsoHistoryPage(ordered, matches.Count));
        }
    }
}
