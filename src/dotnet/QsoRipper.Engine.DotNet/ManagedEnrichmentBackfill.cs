using System.Runtime.CompilerServices;
using QsoRipper.Domain;
using QsoRipper.Engine.Lookup;
using QsoRipper.Engine.Storage;
using QsoRipper.Services;

namespace QsoRipper.Engine.DotNet;

internal sealed partial class ManagedEngineState
{
    private int _enrichmentBackfillActive;

    internal bool TryBeginEnrichmentBackfill(out IDisposable? lease)
    {
        if (Interlocked.CompareExchange(ref _enrichmentBackfillActive, 1, 0) != 0)
        {
            lease = null;
            return false;
        }

        lease = new BackfillLease(this);
        return true;
    }

    internal async IAsyncEnumerable<BackfillQsoEnrichmentResponse> RunEnrichmentBackfillAsync(
        BackfillQsoEnrichmentRequest request,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        ILookupCoordinator coordinator;
        lock (_gate)
        {
            coordinator = _lookupCoordinator;
        }

        var progress = new BackfillQsoEnrichmentResponse();
        IReadOnlyList<QsoRecord>? records = null;
        try
        {
            records = await _storage.Logbook.ListQsosAsync(new QsoListQuery
            {
                After = request.After?.ToDateTimeOffset(),
                Before = request.Before?.ToDateTimeOffset(),
                Sort = Engine.Storage.QsoSortOrder.OldestFirst,
                DeletedFilter = Engine.Storage.DeletedRecordsFilter.ActiveOnly,
            }).ConfigureAwait(false);
        }
        catch (StorageException)
        {
            progress.StorageErrors = 1;
            progress.Complete = true;
        }
        if (records is null)
        {
            yield return progress;
            yield break;
        }

        progress.Scanned = (uint)records.Count;
        var grouped = new SortedDictionary<string, List<QsoRecord>>(StringComparer.Ordinal);
        foreach (var record in records)
        {
            if (!NeedsEnrichment(record))
            {
                continue;
            }

            if (string.IsNullOrWhiteSpace(record.WorkedCallsign))
            {
                continue;
            }

            progress.Candidates++;
            var callsign = CallsignNormalizer.Normalize(record.WorkedCallsign);
            if (!grouped.TryGetValue(callsign, out var group))
            {
                group = [];
                grouped.Add(callsign, group);
            }
            group.Add(record);
        }
        progress.UniqueCallsigns = (uint)grouped.Count;
        yield return progress.Clone();

        var apply = request.Mode == BackfillQsoEnrichmentMode.Apply;
        foreach (var pair in grouped)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var lookupTask = coordinator.LookupAsync(pair.Key, ct: CancellationToken.None);
            LookupResult lookup;
            try
            {
                lookup = await lookupTask.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                _ = await lookupTask.ConfigureAwait(false);
                throw;
            }

            if (lookup.State == LookupState.Found && lookup.Record is not null)
            {
                progress.Found++;
                foreach (var record in pair.Value)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var replacement = record.Clone();
                    if (!MergeMissingEnrichment(replacement, lookup.Record))
                    {
                        progress.Unchanged++;
                        continue;
                    }

                    if (!apply)
                    {
                        progress.Changed++;
                        continue;
                    }

                    try
                    {
                        if (await _storage.Logbook
                            .UpdateQsoIfUnchangedAsync(record, replacement)
                            .ConfigureAwait(false))
                        {
                            progress.Changed++;
                        }
                        else
                        {
                            progress.ConcurrentEdits++;
                        }
                    }
                    catch (StorageException)
                    {
                        progress.StorageErrors++;
                    }
                }
            }
            else if (lookup.State == LookupState.NotFound)
            {
                progress.NotFound++;
                progress.Unchanged += (uint)pair.Value.Count;
            }
            else
            {
                progress.Errors++;
                progress.Unchanged += (uint)pair.Value.Count;
            }

            progress.CurrentCallsign = pair.Key;
            yield return progress.Clone();
        }

        progress.ClearCurrentCallsign();
        progress.Complete = true;
        yield return progress;
    }

    private static bool NeedsEnrichment(QsoRecord qso)
    {
        return IsMissing(qso.HasWorkedOperatorName, qso.WorkedOperatorName)
            || IsMissing(qso.HasWorkedGrid, qso.WorkedGrid)
            || IsMissing(qso.HasWorkedCountry, qso.WorkedCountry)
            || !qso.HasWorkedDxcc
            || IsMissing(qso.HasWorkedState, qso.WorkedState)
            || IsMissing(qso.HasWorkedCounty, qso.WorkedCounty)
            || !qso.HasWorkedCqZone
            || !qso.HasWorkedItuZone
            || IsMissing(qso.HasWorkedContinent, qso.WorkedContinent)
            || !qso.HasWorkedLatitude
            || !qso.HasWorkedLongitude;
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

    private static bool IsMissing(bool hasValue, string value)
    {
        return !hasValue || string.IsNullOrWhiteSpace(value);
    }

    private static bool FillString(bool hasTarget, string target, string? source, Action<string> assign)
    {
        if (!IsMissing(hasTarget, target) || string.IsNullOrWhiteSpace(source))
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

    private sealed class BackfillLease(ManagedEngineState owner) : IDisposable
    {
        private ManagedEngineState? _owner = owner;

        public void Dispose()
        {
            var activeOwner = Interlocked.Exchange(ref _owner, null);
            if (activeOwner is not null)
            {
                Interlocked.Exchange(ref activeOwner._enrichmentBackfillActive, 0);
            }
        }
    }
}
