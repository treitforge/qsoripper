using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using QsoRipper.Domain;
using QsoRipper.EngineSelection;
using QsoRipper.Gui.Services;
using QsoRipper.Gui.ViewModels;
using QsoRipper.Services;

namespace QsoRipper.Gui.Tests;

public sealed class MainWindowViewModelEngineReachabilityTests
{
    [Fact]
    public async Task CheckFirstRunAsyncMarksEngineUnreachableWhenRpcFails()
    {
        var endpoint = "http://localhost:50121";
        var profile = EngineCatalog.GetProfile(KnownEngineProfiles.LocalRust);
        var failingInner = new FakeEngineClient
        {
            FailWithRpcException = true,
        };
        using var switchable = new SwitchableEngineClient(
            profile,
            endpoint,
            _ => failingInner);
        using var viewModel = new MainWindowViewModel(switchable);

        await viewModel.CheckFirstRunAsync();

        Assert.True(viewModel.IsEngineUnreachable);
        Assert.Contains(endpoint, viewModel.EngineUnreachableMessage, StringComparison.Ordinal);
        Assert.Contains("Make sure the engine is running", viewModel.EngineUnreachableMessage, StringComparison.Ordinal);
    }

    [Fact]
    public async Task CheckFirstRunAsyncLeavesEngineReachableWhenCallsSucceed()
    {
        var engine = new FakeEngineClient
        {
            SetupStatus = new GetSetupStatusResponse
            {
                Status = new SetupStatus
                {
                    SetupComplete = true,
                    IsFirstRun = false,
                },
            },
        };

        using var viewModel = new MainWindowViewModel(engine);

        await viewModel.CheckFirstRunAsync();

        Assert.False(viewModel.IsEngineUnreachable);
    }

    [Theory]
    [InlineData(StatusCode.FailedPrecondition, false)]
    [InlineData(StatusCode.Unavailable, true)]
    [InlineData(StatusCode.DeadlineExceeded, true)]
    public async Task SyncNowDistinguishesQrzErrorsFromConnectionFailures(StatusCode code, bool unreachable)
    {
        const string detail = "Sync could not complete.";
        var engine = new FakeEngineClient { SyncFailure = new Status(code, detail) };
        using var viewModel = new MainWindowViewModel(engine);

        await viewModel.SyncNowCommand.ExecuteAsync(null);

        Assert.Equal(unreachable, viewModel.IsEngineUnreachable);
        Assert.Equal($"Sync failed: {detail}", viewModel.SyncStatusText);
    }

    private sealed class FakeEngineClient : IEngineClient
    {
        public GetSetupStatusResponse SetupStatus { get; init; } = new()
        {
            Status = new SetupStatus
            {
                SetupComplete = true,
                IsFirstRun = false,
            },
        };

        public bool FailWithRpcException { get; init; }
        public Status? SyncFailure { get; init; }

        public Task<GetSetupWizardStateResponse> GetWizardStateAsync(CancellationToken ct = default) =>
            Task.FromResult(new GetSetupWizardStateResponse { Status = SetupStatus.Status ?? new SetupStatus() });

        public Task<ValidateSetupStepResponse> ValidateStepAsync(ValidateSetupStepRequest request, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<TestQrzCredentialsResponse> TestQrzCredentialsAsync(string username, string password, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<SaveSetupResponse> SaveSetupAsync(SaveSetupRequest request, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<GetSetupStatusResponse> GetSetupStatusAsync(CancellationToken ct = default)
        {
            if (FailWithRpcException)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, "engine offline"));
            }

            return Task.FromResult(SetupStatus);
        }

        public Task<TestQrzLogbookCredentialsResponse> TestQrzLogbookCredentialsAsync(string apiKey, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<IReadOnlyList<QsoRecord>> ListQsosAsync(CancellationToken ct = default)
        {
            if (FailWithRpcException)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, "engine offline"));
            }

            return Task.FromResult<IReadOnlyList<QsoRecord>>([]);
        }

        public Task<UpdateQsoResponse> UpdateQsoAsync(QsoRecord qso, bool syncToQrz = false, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<SyncWithQrzResponse> SyncWithQrzAsync(CancellationToken ct = default) =>
            SyncFailure is { } failure
                ? Task.FromException<SyncWithQrzResponse>(new RpcException(failure))
                : Task.FromResult(new SyncWithQrzResponse());

        public Task<GetSyncStatusResponse> GetSyncStatusAsync(CancellationToken ct = default)
        {
            if (FailWithRpcException)
            {
                throw new RpcException(new Status(StatusCode.Unavailable, "engine offline"));
            }

            return Task.FromResult(new GetSyncStatusResponse());
        }

        public Task<LookupResponse> LookupCallsignAsync(string callsign, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<DeleteQsoResponse> DeleteQsoAsync(string localId, bool deleteFromQrz = false, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<LogQsoResponse> LogQsoAsync(QsoRecord qso, bool syncToQrz = false, CancellationToken ct = default) =>
            throw new NotImplementedException();

        public Task<GetRigSnapshotResponse> GetRigSnapshotAsync(CancellationToken ct = default) =>
            Task.FromResult(new GetRigSnapshotResponse());

        public Task<GetRigStatusResponse> GetRigStatusAsync(CancellationToken ct = default) =>
            Task.FromResult(new GetRigStatusResponse());

        public Task<GetCurrentSpaceWeatherResponse> GetCurrentSpaceWeatherAsync(CancellationToken ct = default) =>
            Task.FromResult(new GetCurrentSpaceWeatherResponse());

        public Task<ComputeGreatCircleResponse> ComputeGreatCircleAsync(ComputeGreatCircleRequest request, CancellationToken ct = default) => throw new NotImplementedException();
        public Task<GetActiveStationContextResponse> GetActiveStationContextAsync(CancellationToken ct = default) => throw new NotImplementedException();
        public Task<PurgeDeletedQsosResponse> PurgeDeletedQsosAsync(IReadOnlyList<string>? localIds = null, Timestamp? olderThan = null, bool includePendingRemoteDeletes = false, CancellationToken ct = default) =>
            throw new NotImplementedException();
    }
}
