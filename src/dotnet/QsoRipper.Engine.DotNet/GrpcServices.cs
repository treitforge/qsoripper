using System.Diagnostics.CodeAnalysis;
using Google.Protobuf;
using Grpc.Core;
using QsoRipper.Domain;
using QsoRipper.Engine.DotNet.Lotw;
using QsoRipper.Engine.Lookup;
using QsoRipper.Services;
using Timestamp = Google.Protobuf.WellKnownTypes.Timestamp;

namespace QsoRipper.Engine.DotNet;

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedEngineInfoGrpcService
    : EngineService.EngineServiceBase
{
    public override Task<GetEngineInfoResponse> GetEngineInfo(
        GetEngineInfoRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetEngineInfoResponse
        {
            Engine = ManagedEngineState.BuildEngineInfo(),
        });
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedSetupGrpcService(ManagedEngineState state)
    : SetupService.SetupServiceBase
{
    public override Task<GetSetupStatusResponse> GetSetupStatus(
        GetSetupStatusRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetSetupStatusResponse
        {
            Status = state.GetSetupStatus(),
        });
    }

    public override Task<GetSetupWizardStateResponse> GetSetupWizardState(
        GetSetupWizardStateRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(state.GetSetupWizardState());
    }

    public override Task<ValidateSetupStepResponse> ValidateSetupStep(
        ValidateSetupStepRequest request,
        ServerCallContext context)
    {
        if (!Enum.IsDefined(request.Step))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"Unknown setup wizard step value {(int)request.Step}."));
        }

        return Task.FromResult(ManagedEngineState.ValidateSetupStep(request));
    }

    public override Task<TestQrzCredentialsResponse> TestQrzCredentials(
        TestQrzCredentialsRequest request,
        ServerCallContext context)
    {
        return state.TestQrzCredentialsAsync(request.QrzXmlUsername, request.QrzXmlPassword, context.CancellationToken);
    }

    public override Task<TestQrzLogbookCredentialsResponse> TestQrzLogbookCredentials(
        TestQrzLogbookCredentialsRequest request,
        ServerCallContext context)
    {
        return state.TestQrzLogbookCredentialsAsync(request.ApiKey, context.CancellationToken);
    }

    public override Task<SaveSetupResponse> SaveSetup(
        SaveSetupRequest request,
        ServerCallContext context)
    {
        try
        {
            return Task.FromResult(state.SaveSetup(request));
        }
        catch (QrzSecretStoreException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedStationProfileGrpcService(ManagedEngineState state)
    : StationProfileService.StationProfileServiceBase
{
    public override Task<ListStationProfilesResponse> ListStationProfiles(
        ListStationProfilesRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(state.ListStationProfiles());
    }

    public override Task<GetStationProfileResponse> GetStationProfile(
        GetStationProfileRequest request,
        ServerCallContext context)
    {
        var profile = state.GetStationProfile(request.ProfileId);
        if (profile is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"Station profile '{request.ProfileId}' was not found."));
        }

        return Task.FromResult(new GetStationProfileResponse { Profile = profile });
    }

    public override Task<SaveStationProfileResponse> SaveStationProfile(
        SaveStationProfileRequest request,
        ServerCallContext context)
    {
        try
        {
            return Task.FromResult(state.SaveStationProfile(request));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public override Task<DeleteStationProfileResponse> DeleteStationProfile(
        DeleteStationProfileRequest request,
        ServerCallContext context)
    {
        switch (state.DeleteStationProfile(request.ProfileId))
        {
            case StationProfileDeleteOutcome.NotFound:
                throw new RpcException(new Status(StatusCode.NotFound, $"Station profile '{request.ProfileId}' was not found."));
            case StationProfileDeleteOutcome.ActiveProfile:
                throw new RpcException(new Status(StatusCode.FailedPrecondition, $"Station profile '{request.ProfileId}' is active and cannot be deleted."));
        }

        var response = new DeleteStationProfileResponse();
        var catalog = state.ListStationProfiles();
        if (!string.IsNullOrWhiteSpace(catalog.ActiveProfileId))
        {
            response.ActiveProfileId = catalog.ActiveProfileId;
        }

        return Task.FromResult(response);
    }

    public override Task<SetActiveStationProfileResponse> SetActiveStationProfile(
        SetActiveStationProfileRequest request,
        ServerCallContext context)
    {
        var profile = state.SetActiveStationProfile(request.ProfileId);
        if (profile is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"Station profile '{request.ProfileId}' was not found."));
        }

        return Task.FromResult(new SetActiveStationProfileResponse { Profile = profile });
    }

    public override Task<GetActiveStationContextResponse> GetActiveStationContext(
        GetActiveStationContextRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetActiveStationContextResponse
        {
            Context = state.GetActiveStationContext(),
        });
    }

    public override Task<SetSessionStationProfileOverrideResponse> SetSessionStationProfileOverride(
        SetSessionStationProfileOverrideRequest request,
        ServerCallContext context)
    {
        if (request.Profile is null)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "profile is required."));
        }

        return Task.FromResult(new SetSessionStationProfileOverrideResponse
        {
            Context = state.SetSessionStationProfileOverride(request.Profile),
        });
    }

    public override Task<ClearSessionStationProfileOverrideResponse> ClearSessionStationProfileOverride(
        ClearSessionStationProfileOverrideRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new ClearSessionStationProfileOverrideResponse
        {
            Context = state.ClearSessionStationProfileOverride(),
        });
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedDeveloperControlGrpcService(ManagedEngineState state)
    : DeveloperControlService.DeveloperControlServiceBase
{
    public override Task<GetRuntimeConfigResponse> GetRuntimeConfig(
        GetRuntimeConfigRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetRuntimeConfigResponse
        {
            Snapshot = state.GetRuntimeConfigSnapshot(),
        });
    }

    public override Task<ApplyRuntimeConfigResponse> ApplyRuntimeConfig(
        ApplyRuntimeConfigRequest request,
        ServerCallContext context)
    {
        try
        {
            return Task.FromResult(new ApplyRuntimeConfigResponse
            {
                Snapshot = state.ApplyRuntimeConfig(request.Mutations),
            });
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public override Task<ResetRuntimeConfigResponse> ResetRuntimeConfig(
        ResetRuntimeConfigRequest request,
        ServerCallContext context)
    {
        try
        {
            return Task.FromResult(new ResetRuntimeConfigResponse
            {
                Snapshot = state.ResetRuntimeConfig(request.Keys),
            });
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedLogbookGrpcService(ManagedEngineState state)
    : LogbookService.LogbookServiceBase
{
    public override Task<LogQsoResponse> LogQso(LogQsoRequest request, ServerCallContext context)
    {
        try
        {
            return Task.FromResult(state.LogQso(request));
        }
        catch (NoActiveStationProfileException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public override Task<UpdateQsoResponse> UpdateQso(UpdateQsoRequest request, ServerCallContext context)
    {
        try
        {
            return Task.FromResult(state.UpdateQso(request));
        }
        catch (QsoSoftDeletedException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
        catch (KeyNotFoundException ex)
        {
            throw new RpcException(new Status(StatusCode.NotFound, ex.Message));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public override Task<DeleteQsoResponse> DeleteQso(DeleteQsoRequest request, ServerCallContext context)
    {
        DeleteQsoOutcome outcome;
        try
        {
            outcome = state.DeleteQso(request.LocalId, request.DeleteFromQrz);
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
        if (!outcome.Found)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"QSO '{request.LocalId}' was not found."));
        }

        var response = new DeleteQsoResponse
        {
            Success = outcome.Found,
            // Legacy fields: synchronous QRZ delete is no longer performed.
            QrzDeleteSuccess = false,
            RemoteDeleteQueued = outcome.RemoteDeleteQueued,
        };

        if (outcome.MissingQrzLogid)
        {
            response.QrzDeleteError = "QSO has no QRZ logid — it may not have been synced yet.";
        }

        return Task.FromResult(response);
    }

    public override Task<RestoreQsoResponse> RestoreQso(RestoreQsoRequest request, ServerCallContext context)
    {
        if (string.IsNullOrWhiteSpace(request.LocalId))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "RestoreQso requires a non-empty local_id."));
        }

        var outcome = state.RestoreQso(request.LocalId);
        if (!outcome.Found)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"QSO '{request.LocalId}' was not found."));
        }

        return Task.FromResult(new RestoreQsoResponse
        {
            Success = true,
            Restored = outcome.Restored,
        });
    }

    public override Task<PurgeDeletedQsosResponse> PurgeDeletedQsos(
        PurgeDeletedQsosRequest request,
        ServerCallContext context)
    {
        if (!request.Confirm)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "PurgeDeletedQsos requires confirm = true."));
        }

        var syncStatus = state.GetSyncStatus();
        if (syncStatus.IsSyncing)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, "Cannot purge while a sync is in progress."));
        }

        var localIds = request.LocalIds.Count > 0 ? (IReadOnlyList<string>)request.LocalIds : null;
        var olderThan = request.OlderThan is not null ? request.OlderThan.ToDateTimeOffset() : (DateTimeOffset?)null;

        var outcome = state.PurgeDeletedQsos(
            localIds,
            olderThan,
            request.IncludePendingRemoteDeletes);

        return Task.FromResult(new PurgeDeletedQsosResponse
        {
            PurgedCount = (uint)outcome.PurgedCount,
            RemoteDeletesPushed = (uint)outcome.RemoteDeletesPushed,
            RemoteDeletesFailed = (uint)outcome.RemoteDeletesFailed,
            ErrorSummary = outcome.ErrorSummary ?? string.Empty,
        });
    }

    public override Task<GetQsoResponse> GetQso(GetQsoRequest request, ServerCallContext context)
    {
        var qso = state.GetQso(request.LocalId);
        if (qso is null)
        {
            throw new RpcException(new Status(StatusCode.NotFound, $"QSO '{request.LocalId}' was not found."));
        }

        return Task.FromResult(new GetQsoResponse { Qso = qso });
    }

    public override async Task ListQsos(
        ListQsosRequest request,
        IServerStreamWriter<ListQsosResponse> responseStream,
        ServerCallContext context)
    {
        IReadOnlyList<QsoRecord> qsos;
        try
        {
            qsos = state.ListQsos(request);
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }

        foreach (var qso in qsos)
        {
            await responseStream.WriteAsync(new ListQsosResponse { Qso = qso });
        }
    }

    public override async Task BackfillQsoEnrichment(
        BackfillQsoEnrichmentRequest request,
        IServerStreamWriter<BackfillQsoEnrichmentResponse> responseStream,
        ServerCallContext context)
    {
        ValidateEnrichmentBackfillRequest(request);
        if (!state.TryBeginEnrichmentBackfill(out var lease))
        {
            throw new RpcException(new Status(
                StatusCode.ResourceExhausted,
                "A QSO enrichment backfill is active."));
        }

        using (lease)
        {
            await foreach (var progress in state
                .RunEnrichmentBackfillAsync(request, context.CancellationToken)
                .ConfigureAwait(false))
            {
                await responseStream.WriteAsync(progress, context.CancellationToken);
            }
        }
    }

    internal static void ValidateEnrichmentBackfillRequest(BackfillQsoEnrichmentRequest request)
    {
        if (!System.Enum.IsDefined(request.Mode))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid backfill mode."));
        }
        try
        {
            ValidateTimestamp(request.After, "after");
            ValidateTimestamp(request.Before, "before");
            _ = request.After?.ToDateTimeOffset();
            _ = request.Before?.ToDateTimeOffset();
            if (request.After is not null
                && request.Before is not null
                && (request.After.Seconds > request.Before.Seconds
                    || (request.After.Seconds == request.Before.Seconds
                        && request.After.Nanos > request.Before.Nanos)))
            {
                throw new RpcException(new Status(
                    StatusCode.InvalidArgument,
                    "Backfill after must not be later than before."));
            }
        }
        catch (Exception ex) when (ex is ArgumentOutOfRangeException or InvalidOperationException)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    private static void ValidateTimestamp(Timestamp? timestamp, string name)
    {
        if (timestamp is not null
            && (timestamp.Seconds is < -62_135_596_800 or > 253_402_300_799
                || timestamp.Nanos is < 0 or > 999_999_999))
        {
            throw new InvalidOperationException(
                $"Backfill {name} must be a valid protobuf Timestamp.");
        }
    }

    public override async Task SyncWithQrz(
        SyncWithQrzRequest request,
        IServerStreamWriter<SyncWithQrzResponse> responseStream,
        ServerCallContext context)
    {
        try
        {
            state.EnsureQrzSyncAvailable();
            await responseStream.WriteAsync(new SyncWithQrzResponse
            {
                CurrentAction = "Starting QRZ sync.",
                Complete = false,
            });
            await responseStream.WriteAsync(state.SyncWithQrz(request.FullSync));
        }
        catch (QrzSyncUnavailableException ex)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, ex.Message));
        }
    }

    public override async Task SyncWithLotw(
        SyncWithLotwRequest request,
        IServerStreamWriter<SyncWithLotwResponse> responseStream,
        ServerCallContext context)
    {
        if (request.HasUpload && !request.Upload && request.HasDownload && !request.Download)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                "SyncWithLotw requires upload, download, or both."));
        }

        try
        {
            state.EnsureLotwSyncAvailable();
            await responseStream.WriteAsync(new SyncWithLotwResponse
            {
                CurrentAction = "Starting LoTW sync.",
                Complete = false,
            });
            await responseStream.WriteAsync(state.SyncWithLotw(request, context.CancellationToken));
        }
        catch (LotwConfigurationException exception)
        {
            throw new RpcException(new Status(StatusCode.FailedPrecondition, exception.Message));
        }
    }

    public override Task<GetSyncStatusResponse> GetSyncStatus(
        GetSyncStatusRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(state.GetSyncStatus());
    }

    public override async Task<ImportAdifResponse> ImportAdif(
        IAsyncStreamReader<ImportAdifRequest> requestStream,
        ServerCallContext context)
    {
        try
        {
            return await ImportAdifCoreAsync(requestStream, context);
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    public override async Task ExportAdif(
        ExportAdifRequest request,
        IServerStreamWriter<ExportAdifResponse> responseStream,
        ServerCallContext context)
    {
        try
        {
            var payload = state.ExportAdif(request);
            for (var offset = 0; offset < payload.Length; offset += ManagedAdifCodec.ChunkSize)
            {
                var chunkLength = Math.Min(ManagedAdifCodec.ChunkSize, payload.Length - offset);
                await responseStream.WriteAsync(new ExportAdifResponse
                {
                    Chunk = new AdifChunk
                    {
                        Data = ByteString.CopyFrom(payload, offset, chunkLength)
                    }
                });
            }
        }
        catch (InvalidOperationException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }

    private async Task<ImportAdifResponse> ImportAdifCoreAsync(
        IAsyncStreamReader<ImportAdifRequest> requestStream,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(requestStream);
        ArgumentNullException.ThrowIfNull(context);

        using var buffer = new MemoryStream();
        var refresh = false;

        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var request = requestStream.Current;
            if (request.Chunk is null)
            {
                throw new InvalidOperationException("chunk is required.");
            }

            request.Chunk.Data.WriteTo(buffer);
            refresh |= request.Refresh;
        }

        return state.ImportAdif(buffer.ToArray(), refresh);
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedLookupGrpcService(ManagedEngineState state)
    : LookupService.LookupServiceBase
{
    public override Task<LookupResponse> Lookup(LookupRequest request, ServerCallContext context)
    {
        return Task.FromResult(state.Lookup(request.Callsign, skipCache: request.SkipCache));
    }

    public override async Task StreamLookup(
        StreamLookupRequest request,
        IServerStreamWriter<StreamLookupResponse> responseStream,
        ServerCallContext context)
    {
        await foreach (var response in state.StreamLookup(
            request.Callsign,
            request.SkipCache,
            context.CancellationToken))
        {
            await responseStream.WriteAsync(response);
        }
    }

    public override Task<GetCachedCallsignResponse> GetCachedCallsign(
        GetCachedCallsignRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetCachedCallsignResponse
        {
            Result = state.Lookup(request.Callsign, cacheOnly: true).Result,
        });
    }

    public override Task<GetDxccEntityResponse> GetDxccEntity(
        GetDxccEntityRequest request,
        ServerCallContext context)
    {
        return request.QueryCase switch
        {
            GetDxccEntityRequest.QueryOneofCase.DxccCode
                => DxccEntityTable.TryGetByCode(request.DxccCode, out var entity)
                    ? Task.FromResult(new GetDxccEntityResponse { Entity = entity })
                    : throw new RpcException(new Status(StatusCode.NotFound, $"DXCC entity {request.DxccCode} not found.")),

            GetDxccEntityRequest.QueryOneofCase.Prefix
                => throw new RpcException(new Status(StatusCode.Unimplemented, "Prefix-based DXCC lookup is not yet supported.")),

            _ => throw new RpcException(new Status(StatusCode.InvalidArgument, "Either dxcc_code or prefix must be specified.")),
        };
    }

    public override async Task<BatchLookupResponse> BatchLookup(
        BatchLookupRequest request,
        ServerCallContext context)
    {
        var results = await state.BatchLookupAsync(
            (IReadOnlyList<string>)request.Callsigns,
            context.CancellationToken);

        var response = new BatchLookupResponse();
        response.Results.AddRange(results);
        return response;
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedRigControlGrpcService(ManagedEngineState state)
    : RigControlService.RigControlServiceBase
{
    public override Task<GetRigStatusResponse> GetRigStatus(
        GetRigStatusRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(state.CreateRigStatusResponse());
    }

    public override Task<GetRigSnapshotResponse> GetRigSnapshot(
        GetRigSnapshotRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetRigSnapshotResponse
        {
            Snapshot = state.BuildRigSnapshot(),
        });
    }

    public override Task<TestRigConnectionResponse> TestRigConnection(
        TestRigConnectionRequest request,
        ServerCallContext context)
    {
        try
        {
            return Task.FromResult(state.TestRigConnection(request));
        }
        catch (ArgumentException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedSpaceWeatherGrpcService(ManagedEngineState state)
    : SpaceWeatherService.SpaceWeatherServiceBase
{
    public override Task<GetCurrentSpaceWeatherResponse> GetCurrentSpaceWeather(
        GetCurrentSpaceWeatherRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new GetCurrentSpaceWeatherResponse
        {
            Snapshot = state.BuildSpaceWeatherSnapshot(refreshed: false),
        });
    }

    public override Task<RefreshSpaceWeatherResponse> RefreshSpaceWeather(
        RefreshSpaceWeatherRequest request,
        ServerCallContext context)
    {
        return Task.FromResult(new RefreshSpaceWeatherResponse
        {
            Snapshot = state.BuildSpaceWeatherSnapshot(refreshed: true),
        });
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedContestCalendarGrpcService(ManagedEngineState state)
    : ContestCalendarService.ContestCalendarServiceBase
{
    public override Task<GetActiveContestsResponse> GetActiveContests(
        GetActiveContestsRequest request,
        ServerCallContext context)
    {
        if (request.HasBand && !Enum.IsDefined(request.Band))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid band filter value."));
        }
        if (request.HasMode && !Enum.IsDefined(request.Mode))
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, "Invalid mode filter value."));
        }

        var snapshot = state.BuildContestCalendarSnapshot(refreshed: false);
        var response = new GetActiveContestsResponse
        {
            Status = snapshot.Status,
            FetchedAt = snapshot.FetchedAt,
            ValidUntil = snapshot.ValidUntil,
            ErrorMessage = snapshot.ErrorMessage,
        };
        response.Contests.AddRange(snapshot.Contests.Where(contest => IsActiveMatch(contest, request)));
        return Task.FromResult(response);
    }

    public override Task<RefreshContestCalendarResponse> RefreshContestCalendar(
        RefreshContestCalendarRequest request,
        ServerCallContext context)
    {
        var snapshot = state.BuildContestCalendarSnapshot(refreshed: true);
        var response = new RefreshContestCalendarResponse
        {
            Status = snapshot.Status,
            FetchedAt = snapshot.FetchedAt,
            ValidUntil = snapshot.ValidUntil,
            ErrorMessage = snapshot.ErrorMessage,
        };
        response.Contests.AddRange(snapshot.Contests);
        return Task.FromResult(response);
    }

    private static bool IsActiveMatch(ContestCalendarEntry contest, GetActiveContestsRequest request)
    {
        var at = request.AtUtc?.ToDateTimeOffset() ?? DateTimeOffset.UtcNow;
        var through = at.AddMinutes(request.LookaheadMinutes);
        if (contest.StartTimeUtc is null || contest.EndTimeUtc is null)
        {
            return false;
        }

        var start = contest.StartTimeUtc.ToDateTimeOffset();
        var end = contest.EndTimeUtc.ToDateTimeOffset();
        return start <= through
            && end >= at
            && EnumFilterMatches(request.HasBand, request.Band, contest.Bands, request.IncludePartialMatches)
            && EnumFilterMatches(request.HasMode, request.Mode, contest.Modes, request.IncludePartialMatches);
    }

    private static bool EnumFilterMatches<TEnum>(
        bool hasFilter,
        TEnum filter,
        Google.Protobuf.Collections.RepeatedField<TEnum> values,
        bool includePartialMatches)
        where TEnum : struct, Enum
    {
        return !hasFilter
            || EqualityComparer<TEnum>.Default.Equals(filter, default)
            || values.Contains(filter)
            || values.Count == 0 && includePartialMatches;
    }
}

[SuppressMessage("Performance", "CA1812:Avoid uninstantiated internal classes", Justification = "Activated by ASP.NET Core gRPC.")]
internal sealed class ManagedGreatCircleGrpcService
    : GreatCircleService.GreatCircleServiceBase
{
    public override Task<ComputeGreatCircleResponse> ComputeGreatCircle(
        ComputeGreatCircleRequest request,
        ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(request);

        var origin = ResolveReference(request.Origin, "origin");
        var target = ResolveReference(request.Target, "target");
        Geodesy.ValidatePoint(origin, "origin");
        Geodesy.ValidatePoint(target, "target");

        uint count;
        try
        {
            count = Geodesy.ResolveSampleCount(request.SampleCount);
        }
        catch (ArgumentOutOfRangeException ex)
        {
            throw new RpcException(new Status(StatusCode.InvalidArgument, ex.Message));
        }

        var samples = Geodesy.SampleGreatCircle(origin, target, count);
        var path = new GreatCirclePath
        {
            Origin = origin,
            Target = target,
            DistanceKm = Geodesy.DistanceKm(origin, target),
        };
        path.Samples.AddRange(samples);
        var initial = Geodesy.InitialBearingDeg(origin, target);
        if (initial.HasValue)
        {
            path.InitialBearingDeg = initial.Value;
        }
        var final = Geodesy.FinalBearingDeg(origin, target);
        if (final.HasValue)
        {
            path.FinalBearingDeg = final.Value;
        }

        return Task.FromResult(new ComputeGreatCircleResponse { Path = path });
    }

    private static GeoPoint ResolveReference(GeoReference? reference, string label)
    {
        if (reference is null)
        {
            throw new RpcException(new Status(
                StatusCode.InvalidArgument,
                $"{label} reference is required"));
        }
        if (reference.Coordinates is { } coords)
        {
            return coords;
        }
        if (!string.IsNullOrWhiteSpace(reference.Maidenhead))
        {
            try
            {
                return Geodesy.MaidenheadToGeoPoint(reference.Maidenhead);
            }
            catch (ArgumentException ex)
            {
                throw new RpcException(new Status(
                    StatusCode.InvalidArgument,
                    $"{label}: {ex.Message}"));
            }
        }
        throw new RpcException(new Status(
            StatusCode.InvalidArgument,
            $"{label}: must supply coordinates or maidenhead"));
    }
}
