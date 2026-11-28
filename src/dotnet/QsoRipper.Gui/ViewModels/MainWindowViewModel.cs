using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Avalonia;
using Avalonia.Controls.ApplicationLifetimes;
using Avalonia.Threading;
using CommunityToolkit.Mvvm.ComponentModel;
using CommunityToolkit.Mvvm.Input;
using Grpc.Core;
using QsoRipper.Domain;
using QsoRipper.EngineSelection;
using QsoRipper.Gui.Services;
using QsoRipper.Gui.Utilities;
using QsoRipper.Shared.Formatting;
using QsoRipper.Shared.Persistence;

namespace QsoRipper.Gui.ViewModels;

internal sealed partial class MainWindowViewModel : ObservableObject, IDisposable
{
    private static readonly TimeSpan PreferredEngineSwitchTimeout = TimeSpan.FromSeconds(1.5);
    private static readonly TimeSpan SpaceWeatherRefreshInterval = TimeSpan.FromHours(1);
    private static readonly TimeSpan RigPollInterval = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan EngineHealthPollInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan EngineHealthProbeTimeout = TimeSpan.FromMilliseconds(1500);

    private readonly IEngineClient _engine;
    private readonly SwitchableEngineClient? _switchableEngine;
    private readonly DispatcherTimer _utcTimer;
    private readonly DispatcherTimer _rigTimer;
    private bool _rigTickInFlight;
    private readonly DispatcherTimer _spaceWeatherTimer;
    private readonly DispatcherTimer _engineHealthTimer;
    private bool _engineHealthProbeInFlight;
    private CwDecoderProcessSampleSource? _cwSampleSource;
    private CwQsoWpmAggregator? _cwAggregator;
    private CwQsoTranscriptAggregator? _cwTranscriptAggregator;
    private CwDiagnosticsRecorder? _cwDiagnosticsRecorder;
    private bool _setupCompleteBeforeWizard;
    private string? _preferredEngineProfileId;
    private string? _preferredEngineEndpoint;
    private string? _persistedEngineProfileId;
    private string? _persistedEngineEndpoint;
    private StationProfile? _activeStationProfile;

    [ObservableProperty]
    private bool _isSettingsOpen;

    [ObservableProperty]
    private bool _isWizardOpen;

    [ObservableProperty]
    private SetupWizardViewModel? _wizardViewModel;

    [ObservableProperty]
    private string _statusMessage = "Checking engine connection...";

    [ObservableProperty]
    private bool _isSetupIncomplete;

    [ObservableProperty]
    private string _activeLogText = "Log: -";

    [ObservableProperty]
    private string _activeProfileText = "Profile: -";

    [ObservableProperty]
    private string _activeStationText = "Station: -";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(WindowTitle))]
    private string _stationCallsign = string.Empty;

    [ObservableProperty]
    private string _activeEngineText = "Engine: -";

    [ObservableProperty]
    private string _availableEnginesText = "Engines: unknown";

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(HasEngineSwitchStatus))]
    private string _engineSwitchStatusText = "Switch: idle";

    [ObservableProperty]
    [NotifyCanExecuteChangedFor(nameof(SwitchToRustEngineCommand))]
    [NotifyCanExecuteChangedFor(nameof(SwitchToDotNetEngineCommand))]
    [NotifyCanExecuteChangedFor(nameof(RefreshEngineAvailabilityCommand))]
    private bool _isEngineSwitching;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(InspectorPanelHost))]
    private bool _isInspectorOpen;

    [ObservableProperty]
    private bool _isSyncing;

    [ObservableProperty]
    private string _syncStatusText = "Sync: never";

    [ObservableProperty]
    private bool _isSortChooserOpen;

    [ObservableProperty]
    private bool _isColumnChooserOpen;

    [ObservableProperty]
    private string _currentUtcTime = string.Empty;

    [ObservableProperty]
    private string _currentUtcDate = string.Empty;

    [ObservableProperty]
    private bool _isCallsignCardOpen;

    [ObservableProperty]
    private CallsignCardViewModel? _callsignCard;

    [ObservableProperty]
    private bool _isCwStatsPaneOpen;

    [ObservableProperty]
    private CwStatsPaneViewModel? _cwStatsPane;

    [ObservableProperty]
    private bool _isRigEnabled;

    [ObservableProperty]
    private string _rigStatusText = "Rig: OFF";

    [ObservableProperty]
    private bool _isSpaceWeatherVisible;

    [ObservableProperty]
    private string _spaceWeatherText = string.Empty;

    [ObservableProperty]
    private bool _isHelpOpen;

    [ObservableProperty]
    private HelpOverlayViewModel? _helpOverlay;

    [ObservableProperty]
    private bool _isFullQsoCardOpen;

    [ObservableProperty]
    private FullQsoCardViewModel? _fullQsoCard;

    [ObservableProperty]
    private bool _isLoggerFocused;

    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CwDecoderStatusOpacity))]
    private bool _isCwDecoderEnabled;

    [ObservableProperty]
    private bool _isCwDecoderLoopback;

    [ObservableProperty]
    private string _cwDecoderStatusText = "WPM: OFF";

    [ObservableProperty]
    private string _cwDecoderDeviceOverride = string.Empty;

    /// <summary>
    /// When true, the GUI mirrors all cw-decoder NDJSON events + audio to
    /// disk under <c>%LOCALAPPDATA%\QsoRipper\diagnostics\</c> so a developer
    /// can compare what the UX displayed vs what the decoder emitted vs
    /// what got logged on the QSO. Off by default.
    /// </summary>
    [ObservableProperty]
    private bool _isCwDiagnosticsEnabled;

    /// <summary>
    /// Whether the CW WPM live readout is shown in the status bar. Toggled
    /// independently of <see cref="IsCwDecoderEnabled"/> so users can hide
    /// the readout without tearing down the decoder, or reveal it as a
    /// "(disabled)" marker as a reminder to enable the source in Settings.
    /// </summary>
    [ObservableProperty]
    [NotifyPropertyChangedFor(nameof(CwDecoderStatusOpacity))]
    private bool _isCwWpmStatusBarVisible;

    /// <summary>Dimmed (0.4) when the source is off; full opacity when running.</summary>
    public double CwDecoderStatusOpacity => IsCwDecoderEnabled ? 0.7 : 0.4;

    [ObservableProperty]
    private string _contextHintText = "F3 grid · F4 search · Ctrl+N logger · Alt+A card · F1 help";

    [ObservableProperty]
    private bool _isEngineUnreachable;

    [ObservableProperty]
    private string _engineUnreachableMessage = string.Empty;

    internal MainWindowViewModel(EngineTargetProfile engineProfile, string endpoint)
    {
        ArgumentNullException.ThrowIfNull(engineProfile);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);

        _switchableEngine = new SwitchableEngineClient(engineProfile, endpoint);
        _engine = _switchableEngine;
        RecentQsos = new RecentQsoListViewModel(_engine);
        RecentQsos.PropertyChanged += OnRecentQsosPropertyChanged;
        Logger = new QsoLoggerViewModel(_engine);
        Logger.QsoLogged += OnQsoLogged;
        Logger.LoggerFocusRequested += OnLoggerFocusRequested;
        Logger.CwEpisodeBoundary += OnCwEpisodeBoundary;
        Logger.CwEpisodeStarted += OnCwEpisodeStarted;
        Logger.CwModeChanged += OnLoggerCwModeChanged;
        ActiveEngineText = BuildEngineText(engineProfile, endpoint);
        UpdateUtcClock();
        _utcTimer = CreateUtcTimer();
        // Rig polling must stay responsive even while the UI thread is busy
        // rendering the CW waterfall/spectrogram or processing a band-change
        // burst. A default-priority DispatcherTimer is DispatcherPriority.Background
        // (the lowest non-idle lane) and gets starved for seconds behind that work,
        // so the displayed frequency lags far behind the radio. Run it at Default.
        _rigTimer = new DispatcherTimer(DispatcherPriority.Default) { Interval = RigPollInterval };
        _rigTimer.Tick += OnRigTimerTick;
        _spaceWeatherTimer = new DispatcherTimer { Interval = SpaceWeatherRefreshInterval };
        _spaceWeatherTimer.Tick += OnSpaceWeatherTimerTick;
        _engineHealthTimer = new DispatcherTimer { Interval = EngineHealthPollInterval };
        _engineHealthTimer.Tick += OnEngineHealthTimerTick;
    }

    internal MainWindowViewModel(IEngineClient engine)
    {
        ArgumentNullException.ThrowIfNull(engine);

        _switchableEngine = engine as SwitchableEngineClient;
        _engine = engine;
        RecentQsos = new RecentQsoListViewModel(engine);
        RecentQsos.PropertyChanged += OnRecentQsosPropertyChanged;
        Logger = new QsoLoggerViewModel(engine);
        Logger.QsoLogged += OnQsoLogged;
        Logger.LoggerFocusRequested += OnLoggerFocusRequested;
        Logger.CwEpisodeBoundary += OnCwEpisodeBoundary;
        Logger.CwEpisodeStarted += OnCwEpisodeStarted;
        Logger.CwModeChanged += OnLoggerCwModeChanged;
        if (_switchableEngine is not null)
        {
            ActiveEngineText = BuildEngineText(_switchableEngine.CurrentProfile, _switchableEngine.CurrentEndpoint);
        }
        else
        {
            ActiveEngineText = "Engine: fixture";
            AvailableEnginesText = "Engines: unavailable";
        }

        UpdateUtcClock();
        _utcTimer = CreateUtcTimer();
        // See the primary constructor: Default priority keeps the rig poll from
        // being starved behind UI rendering/band-change bursts.
        _rigTimer = new DispatcherTimer(DispatcherPriority.Default) { Interval = RigPollInterval };
        _rigTimer.Tick += OnRigTimerTick;
        _spaceWeatherTimer = new DispatcherTimer { Interval = SpaceWeatherRefreshInterval };
        _spaceWeatherTimer.Tick += OnSpaceWeatherTimerTick;
        _engineHealthTimer = new DispatcherTimer { Interval = EngineHealthPollInterval };
        _engineHealthTimer.Tick += OnEngineHealthTimerTick;
    }

    public RecentQsoListViewModel RecentQsos { get; }

    public QsoLoggerViewModel Logger { get; }

    /// <summary>
    /// Dynamic window title showing station callsign when available.
    /// Format: "QsoRipper — K7RND" or just "QsoRipper" if no station set.
    /// </summary>
    public string WindowTitle => string.IsNullOrWhiteSpace(StationCallsign)
        ? "QsoRipper"
        : $"QsoRipper — {StationCallsign}";

    public bool HasEngineSwitchStatus =>
        !string.IsNullOrWhiteSpace(EngineSwitchStatusText)
        && !string.Equals(EngineSwitchStatusText, "Switch: idle", StringComparison.Ordinal);

    /// <summary>
    /// Proxy for <see cref="RecentQsoListViewModel.SelectedQso"/> so the Inspector
    /// panel can bind via a single-level property path from the window DataContext.
    /// </summary>
    public RecentQsoItemViewModel? InspectorQso => RecentQsos.SelectedQso;

    public bool HasInspectorQso => InspectorQso is not null;

    public MainWindowViewModel? InspectorPanelHost => IsInspectorOpen ? this : null;

    public event EventHandler? SearchFocusRequested;

    public event EventHandler? GridFocusRequested;

    public event EventHandler? LoggerFocusRequested;

    /// <summary>
    /// Raised when the user requests the Settings dialog. The View subscribes to
    /// this event and opens the modal <see cref="Views.SettingsView"/>.
    /// </summary>
    internal event EventHandler? SettingsRequested;

    /// <summary>
    /// Called after the main window has loaded. Checks first-run state.
    /// </summary>
    public async Task CheckFirstRunAsync(bool focusSearch = false)
    {
        try
        {
            GuiPerformanceTrace.Write(nameof(CheckFirstRunAsync) + ".start");
            await ApplyPreferredEngineSelectionAsync();
            GuiPerformanceTrace.Write(nameof(CheckFirstRunAsync) + ".afterPreferredEngine");
            StatusMessage = "Loading QSOs...";
            var recentQsoRefreshTask = RecentQsos.RefreshAsync();
            var status = (await _engine.GetSetupStatusAsync()).Status;
            GuiPerformanceTrace.Write(
                nameof(CheckFirstRunAsync) + ".afterSetupStatus",
                $"firstRun={status.IsFirstRun}; setupComplete={status.SetupComplete}");
            ApplySetupContext(status);
            IsSetupIncomplete = !status.SetupComplete;

            if (status.IsFirstRun)
            {
                StatusMessage = "Welcome";
                await OpenWizardAsync();
                await recentQsoRefreshTask;
            }
            else
            {
                await ActivateDashboardAsync(focusSearch, recentQsoRefreshTask);
            }

            Dispatcher.UIThread.Post(UpdateAvailableEngineSummary, DispatcherPriority.Background);
            GuiPerformanceTrace.Write(nameof(CheckFirstRunAsync) + ".complete");
            MarkEngineReachable();
            StartEngineHealthTimer();
        }
        catch (RpcException)
        {
            StatusMessage = "Engine unavailable";
            MarkEngineUnreachable();
            StartEngineHealthTimer();
        }
    }

    [RelayCommand]
    private async Task OpenWizardAsync()
    {
        _setupCompleteBeforeWizard = !IsSetupIncomplete;
        var vm = new SetupWizardViewModel(_engine, this);
        WizardViewModel = vm;
        IsWizardOpen = true;
        await vm.LoadStateAsync();
    }

    [RelayCommand]
    private void OpenSettings()
    {
        if (!IsWizardOpen && !IsSettingsOpen)
        {
            SettingsRequested?.Invoke(this, EventArgs.Empty);
        }
    }

    [RelayCommand(CanExecute = nameof(CanSyncNow))]
    private async Task SyncNowAsync()
    {
        IsSyncing = true;
        SyncStatusText = "Syncing\u2026";
        try
        {
            var response = await _engine.SyncWithQrzAsync();
            MarkEngineReachable();

            if (!string.IsNullOrEmpty(response.Error))
            {
                SyncStatusText = $"Sync error: {response.Error}";
                return;
            }

            var up = response.UploadedRecords;
            var down = response.DownloadedRecords;
            var conflicts = response.ConflictRecords;
            SyncStatusText = conflicts > 0
                ? $"Sync needs review: {conflicts} conflict{(conflicts == 1 ? string.Empty : "s")} (\u2191{up} \u2193{down})"
                : $"Synced: \u2191{up} \u2193{down}";
            await RecentQsos.RefreshAsync();
        }
        catch (RpcException ex)
        {
            SyncStatusText = $"Sync failed: {ex.Status.Detail}";
            if (ex.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded)
            {
                MarkEngineUnreachable();
            }
            else
            {
                MarkEngineReachable();
            }
        }
        finally
        {
            IsSyncing = false;
        }
    }

    private bool CanSyncNow() => !IsSyncing && !IsWizardOpen;

    partial void OnIsSyncingChanged(bool value) => SyncNowCommand.NotifyCanExecuteChanged();

    partial void OnIsWizardOpenChanged(bool value)
    {
        SyncNowCommand.NotifyCanExecuteChanged();
        SwitchToRustEngineCommand.NotifyCanExecuteChanged();
        SwitchToDotNetEngineCommand.NotifyCanExecuteChanged();
        if (value)
        {
            _engineHealthTimer.Stop();
        }
        else if (!_engineHealthTimer.IsEnabled)
        {
            _engineHealthTimer.Start();
        }
    }

    [RelayCommand(CanExecute = nameof(CanSwitchEngines))]
    private Task SwitchToRustEngineAsync()
    {
        return SwitchEngineProfileAsync(KnownEngineProfiles.LocalRust);
    }

    [RelayCommand(CanExecute = nameof(CanSwitchEngines))]
    private Task SwitchToDotNetEngineAsync()
    {
        return SwitchEngineProfileAsync(KnownEngineProfiles.LocalDotNet);
    }

    [RelayCommand(CanExecute = nameof(CanRefreshEngineAvailability))]
    private void RefreshEngineAvailability()
    {
        UpdateAvailableEngineSummary();
    }

    private bool CanSwitchEngines()
    {
        return _switchableEngine is not null && !IsWizardOpen && !IsEngineSwitching;
    }

    private bool CanRefreshEngineAvailability()
    {
        return _switchableEngine is not null && !IsEngineSwitching;
    }

    private async Task SwitchEngineProfileAsync(string profileId)
    {
        if (_switchableEngine is null)
        {
            return;
        }

        var targetProfile = EngineCatalog.GetProfile(profileId);
        var targetEndpoint = ResolveSwitchEndpoint(targetProfile);
        IsEngineSwitching = true;
        EngineSwitchStatusText = $"Switching to {targetProfile.DisplayName}\u2026";
        try
        {
            using var timeoutSource = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            var result = await _switchableEngine.SwitchAsync(targetProfile, targetEndpoint, timeoutSource.Token);
            EngineSwitchStatusText = string.IsNullOrWhiteSpace(result.Message)
                ? "Switch: ready"
                : result.Message;
            if (!result.Success)
            {
                return;
            }

            ActiveEngineText = BuildEngineText(result.Profile, result.Endpoint);
            await RefreshSetupContextAsync();
            await RecentQsos.RefreshAsync();
            await RefreshSyncStatusAsync();
        }
        finally
        {
            IsEngineSwitching = false;
            UpdateAvailableEngineSummary();
        }
    }

    private async Task ApplyPreferredEngineSelectionAsync()
    {
        if (_switchableEngine is null)
        {
            return;
        }

        if (string.IsNullOrWhiteSpace(_preferredEngineProfileId)
            && string.IsNullOrWhiteSpace(_preferredEngineEndpoint))
        {
            return;
        }

        var targetProfile = EngineCatalog.ResolveProfile(_preferredEngineProfileId);
        var targetEndpoint = EngineCatalog.ResolveEndpoint(targetProfile, _preferredEngineEndpoint);
        if (string.Equals(targetProfile.ProfileId, _switchableEngine.CurrentProfile.ProfileId, StringComparison.OrdinalIgnoreCase)
            && string.Equals(targetEndpoint, _switchableEngine.CurrentEndpoint, StringComparison.OrdinalIgnoreCase))
        {
            _preferredEngineProfileId = null;
            _preferredEngineEndpoint = null;
            return;
        }

        using var timeoutSource = new CancellationTokenSource(PreferredEngineSwitchTimeout);
        GuiPerformanceTrace.Write(
            nameof(ApplyPreferredEngineSelectionAsync) + ".switchStart",
            $"profile={targetProfile.ProfileId}; endpoint={targetEndpoint}");
        var result = await _switchableEngine.SwitchAsync(targetProfile, targetEndpoint, timeoutSource.Token);
        if (result.Success)
        {
            ActiveEngineText = BuildEngineText(result.Profile, result.Endpoint);
        }
        else
        {
            EngineSwitchStatusText = string.IsNullOrWhiteSpace(result.Message)
                ? "Switch: ready"
                : result.Message;
        }

        GuiPerformanceTrace.Write(
            nameof(ApplyPreferredEngineSelectionAsync) + ".switchComplete",
            $"success={result.Success}; endpoint={result.Endpoint}");
        _preferredEngineProfileId = null;
        _preferredEngineEndpoint = null;
    }

    private void UpdateAvailableEngineSummary()
    {
        if (_switchableEngine is null)
        {
            AvailableEnginesText = "Engines: unavailable";
            return;
        }

        var runtimeEntries = EngineRuntimeDiscovery.DiscoverLocalEngines(new EngineRuntimeDiscoveryOptions
        {
            ValidateTcpReachability = false
        });
        var runningLabels = runtimeEntries
            .Where(static entry => entry.IsRunning)
            .Select(static entry => entry.Profile.DisplayName.Replace("QsoRipper ", string.Empty, StringComparison.Ordinal))
            .ToArray();

        AvailableEnginesText = runningLabels.Length == 0
            ? "Engines: none running"
            : $"Engines: {string.Join(", ", runningLabels)}";
    }

    private static string ResolveSwitchEndpoint(EngineTargetProfile targetProfile)
    {
        var runtimeEntries = EngineRuntimeDiscovery.DiscoverLocalEngines(new EngineRuntimeDiscoveryOptions
        {
            ValidateTcpReachability = false
        });
        var runningEntry = runtimeEntries.FirstOrDefault(entry =>
            entry.IsRunning
            && string.Equals(entry.Profile.ProfileId, targetProfile.ProfileId, StringComparison.OrdinalIgnoreCase));
        return runningEntry?.Endpoint ?? targetProfile.DefaultEndpoint;
    }

    private static string BuildEngineText(EngineTargetProfile profile, string _)
    {
        ArgumentNullException.ThrowIfNull(profile);
        return $"Engine: {profile.DisplayName}";
    }

    private static bool HasEngineEnvironmentOverride()
    {
        return !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(EngineCatalog.EngineProfileEnvironmentVariable))
            || !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(EngineCatalog.LegacyEngineProfileEnvironmentVariable))
            || !string.IsNullOrWhiteSpace(Environment.GetEnvironmentVariable(EngineCatalog.EndpointEnvironmentVariable));
    }

    /// <summary>
    /// Creates a <see cref="SettingsViewModel"/> wired to the shared engine client.
    /// Called by the View layer when handling <see cref="SettingsRequested"/>.
    /// </summary>
    internal SettingsViewModel CreateSettingsViewModel()
    {
        var vm = new SettingsViewModel(_engine)
        {
            IsSpaceWeatherVisible = IsSpaceWeatherVisible,
            IsRadioMonitorEnabled = IsCwDecoderEnabled,
            IsCwWpmStatusBarVisible = IsCwWpmStatusBarVisible,
            IsAdvancedDiagnosticsEnabled = IsCwDiagnosticsEnabled,
            PendingPreselectDeviceOverride = CwDecoderDeviceOverride,
            PendingPreselectIsLoopback = IsCwDecoderLoopback,
        };
        return vm;
    }

    /// <summary>
    /// Called by the View layer after the Settings dialog closes.
    /// </summary>
    internal async Task OnSettingsClosedAsync(bool didSave)
    {
        IsSettingsOpen = false;
        if (didSave)
        {
            await RefreshSetupContextAsync();
            await ActivateDashboardAsync(focusSearch: false);
        }
    }

    internal void ApplySettingsUiPreferences(bool isSpaceWeatherVisible)
        => ApplySettingsUiPreferences(
            isSpaceWeatherVisible,
            IsCwDecoderEnabled,
            IsCwWpmStatusBarVisible,
            IsCwDecoderLoopback,
            CwDecoderDeviceOverride,
            IsCwDiagnosticsEnabled);

    internal void ApplySettingsUiPreferences(
        bool isSpaceWeatherVisible,
        bool isRadioMonitorEnabled,
        bool isCwWpmStatusBarVisible,
        bool isCwWpmLoopback,
        string? cwDeviceOverride)
        => ApplySettingsUiPreferences(
            isSpaceWeatherVisible,
            isRadioMonitorEnabled,
            isCwWpmStatusBarVisible,
            isCwWpmLoopback,
            cwDeviceOverride,
            IsCwDiagnosticsEnabled);

    internal void ApplySettingsUiPreferences(
        bool isSpaceWeatherVisible,
        bool isRadioMonitorEnabled,
        bool isCwWpmStatusBarVisible,
        bool isCwWpmLoopback,
        string? cwDeviceOverride,
        bool isCwDiagnosticsEnabled)
    {
        IsSpaceWeatherVisible = isSpaceWeatherVisible;
        if (IsSpaceWeatherVisible && string.IsNullOrEmpty(SpaceWeatherText))
        {
            _ = FetchSpaceWeatherAsync();
        }

        IsCwWpmStatusBarVisible = isCwWpmStatusBarVisible;

        var trimmedDevice = string.IsNullOrWhiteSpace(cwDeviceOverride)
            ? string.Empty
            : cwDeviceOverride.Trim();

        var deviceChanged = !string.Equals(trimmedDevice, CwDecoderDeviceOverride, StringComparison.Ordinal);
        var loopbackChanged = isCwWpmLoopback != IsCwDecoderLoopback;
        var enableChanged = isRadioMonitorEnabled != IsCwDecoderEnabled;
        var diagnosticsChanged = isCwDiagnosticsEnabled != IsCwDiagnosticsEnabled;

        CwDecoderDeviceOverride = trimmedDevice;
        IsCwDecoderLoopback = isCwWpmLoopback;
        IsCwDiagnosticsEnabled = isCwDiagnosticsEnabled;

        if (enableChanged)
        {
            // ToggleCwDecoder flips the bool. With the new lifecycle policy
            // it only arms/disarms — the actual subprocess starts when an
            // episode begins.
            ToggleCwDecoder();
        }
        else if (IsCwDecoderEnabled && (deviceChanged || loopbackChanged || diagnosticsChanged))
        {
            // Apply new device/loopback/diagnostics. If the decoder is
            // currently running (active QSO episode), restart it in place
            // to pick up the new settings; otherwise the next episode start
            // will use them automatically.
            if (_cwSampleSource?.IsRunning == true)
            {
                StopCwDecoderProcess();
                StartCwDecoderProcessForActiveEpisode();
            }
        }

        UpdateDisabledCwStatusText();
    }

    /// <summary>
    /// Keyboard shortcut (Ctrl+Shift+W) — toggles whether the live CW WPM
    /// readout is shown in the status bar. Mirrors how Ctrl+W toggles space
    /// weather. Independent of whether the decoder is actually running.
    /// </summary>
    [RelayCommand]
    private void ToggleCwWpmStatusBar()
    {
        IsCwWpmStatusBarVisible = !IsCwWpmStatusBarVisible;
        UpdateDisabledCwStatusText();
    }

    /// <summary>
    /// Keyboard shortcut (Ctrl+Alt+W) — restarts the running cw-decoder
    /// process so the dot/dash duration estimator and confidence state
    /// machine start fresh. Useful when the decoder has latched onto a
    /// wrong baseline (e.g. one station finishes a slow exchange and the
    /// next operator starts much faster, leaving the WPM estimator
    /// "stuck" on a stale dot length). No-op when the monitor is off.
    /// </summary>
    [RelayCommand]
    private void RestartCwDecoder()
    {
        if (!IsCwDecoderEnabled || _cwSampleSource is null)
        {
            return;
        }

        // Tear down any open episode + recorder so the restart begins a fresh
        // diagnostics session aligned to the new decoder process. This avoids
        // splicing audio across two decoder lifetimes inside one WAV file.
        DisposeDiagnosticsRecorder();

        string? recordingPath = null;
        if (IsCwDiagnosticsEnabled)
        {
            try
            {
                StartDiagnosticsSession();
                recordingPath = _cwDiagnosticsRecorder?.SessionWavPath;
            }
            catch (IOException ex)
            {
                CwDecoderStatusText = $"WPM: diagnostics dir error ({ex.Message})";
                DisposeDiagnosticsRecorder();
            }
        }

        try
        {
            _cwSampleSource.Start(
                string.IsNullOrWhiteSpace(CwDecoderDeviceOverride) ? null : CwDecoderDeviceOverride.Trim(),
                IsCwDecoderLoopback,
                recordingPath);
            CwDecoderStatusText = IsCwDecoderLoopback
                ? "WPM: restarting (loopback)\u2026"
                : "WPM: restarting\u2026";
        }
        catch (InvalidOperationException ex)
        {
            CwDecoderStatusText = $"WPM: {ex.Message}";
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            CwDecoderStatusText = $"WPM: restart failed ({ex.Message})";
        }
    }

    private void UpdateDisabledCwStatusText()
    {
        if (IsCwWpmStatusBarVisible && !IsCwDecoderEnabled)
        {
            CwDecoderStatusText = "WPM: disabled (Settings → Display)";
        }
    }

    [RelayCommand]
    private void FocusSearch()
    {
        if (!IsWizardOpen)
        {
            CloseTransientPanels(restoreGridFocus: false);
            IsLoggerFocused = false;
            SearchFocusRequested?.Invoke(this, EventArgs.Empty);
        }
    }

    [RelayCommand]
    private void FocusLogger()
    {
        if (!IsWizardOpen)
        {
            CloseTransientPanels(restoreGridFocus: false);
            Logger.FocusLogger();
        }
    }

    [RelayCommand]
    private void FocusGrid()
    {
        if (!IsWizardOpen)
        {
            CloseTransientPanels(restoreGridFocus: false);
            IsLoggerFocused = false;
            GridFocusRequested?.Invoke(this, EventArgs.Empty);
        }
    }

    [RelayCommand]
    private void ToggleRigControl()
    {
        IsRigEnabled = !IsRigEnabled;
        if (IsRigEnabled)
        {
            RigStatusText = "Rig: connecting\u2026";
            _rigTimer.Start();
        }
        else
        {
            _rigTimer.Stop();
            RigStatusText = "Rig: OFF";
        }
    }

    [RelayCommand]
    private void ToggleCwDecoder()
    {
        IsCwDecoderEnabled = !IsCwDecoderEnabled;
        if (IsCwDecoderEnabled)
        {
            if (CwDecoderProcessSampleSource.LocateBinary() is null)
            {
                IsCwDecoderEnabled = false;
                CwDecoderStatusText = "WPM: decoder not built (see experiments/cw-decoder/README.md)";
                return;
            }

            EnsureCwSampleSource();

            // Decoder is now ARMED but not running. The cw-decoder subprocess
            // is launched on demand when the operator begins a QSO (typing in
            // the callsign field raises CwEpisodeStarted) AND the selected
            // mode is CW. It is stopped on every CwEpisodeBoundary (logged /
            // cleared / abandoned) and on mode-flips away from CW. This
            // matches operator expectation: "type a CW callsign → CW hunts;
            // Esc / mode swap → CW silent" and avoids the decoder reporting
            // a stale lock on ambient noise when no CW QSO is in progress.
            CwDecoderStatusText = Logger.IsLoggerOnCwMode
                ? "WPM: armed"
                : "WPM: armed (mode is not CW)";

            // If the operator already has a callsign in the field when they
            // turn the monitor on, start hunting immediately so they don't
            // have to clear and retype to wake the decoder.
            if (Logger.IsLoggerEpisodeActive)
            {
                StartCwDecoderProcessForActiveEpisode();
            }
        }
        else
        {
            StopCwDecoderProcess();
            CwDecoderStatusText = "WPM: OFF";
            UpdateDisabledCwStatusText();
        }
    }

    /// <summary>
    /// Launches the cw-decoder subprocess with the current device/loopback/
    /// diagnostics settings. Safe to call repeatedly — <see
    /// cref="CwDecoderProcessSampleSource.Start"/> stops any prior instance
    /// first. Called from <see cref="OnCwEpisodeStarted"/> when the operator
    /// begins typing a new callsign.
    /// </summary>
    private void StartCwDecoderProcessForActiveEpisode()
    {
        if (!IsCwDecoderEnabled || _cwSampleSource is null)
        {
            return;
        }
        if (_cwSampleSource.IsRunning)
        {
            return;
        }
        if (!Logger.IsLoggerOnCwMode)
        {
            // Hard gate: the WPM monitor / pitch hunter only makes sense
            // for CW QSOs. Logging an SSB or FT8 contact must not spin
            // up the decoder subprocess, capture audio, or surface a
            // stale lock badge. Mode flips back to CW will trigger
            // OnLoggerCwModeChanged which calls this method again.
            CwDecoderStatusText = "WPM: armed (mode is not CW)";
            return;
        }

        string? recordingPath = null;
        if (IsCwDiagnosticsEnabled)
        {
            try
            {
                StartDiagnosticsSession();
                recordingPath = _cwDiagnosticsRecorder?.SessionWavPath;
            }
            catch (IOException ex)
            {
                CwDecoderStatusText = $"WPM: diagnostics dir error ({ex.Message})";
                DisposeDiagnosticsRecorder();
            }
        }

        try
        {
            _cwSampleSource.Start(
                string.IsNullOrWhiteSpace(CwDecoderDeviceOverride) ? null : CwDecoderDeviceOverride.Trim(),
                IsCwDecoderLoopback,
                recordingPath);
            CwDecoderStatusText = IsCwDecoderLoopback
                ? "WPM: starting (loopback)\u2026"
                : "WPM: starting\u2026";
        }
        catch (InvalidOperationException ex)
        {
            CwDecoderStatusText = $"WPM: {ex.Message}";
            DisposeDiagnosticsRecorder();
        }
        catch (System.ComponentModel.Win32Exception ex)
        {
            CwDecoderStatusText = $"WPM: launch failed ({ex.Message})";
            DisposeDiagnosticsRecorder();
        }
    }

    /// <summary>
    /// Stops the cw-decoder subprocess (if running) and tears down any
    /// active diagnostics recorder. Leaves <see cref="IsCwDecoderEnabled"/>
    /// untouched — the monitor stays armed and will relaunch when the next
    /// QSO begins.
    /// </summary>
    private void StopCwDecoderProcess()
    {
        _cwSampleSource?.Stop();
        DisposeDiagnosticsRecorder();
    }

    private void EnsureCwSampleSource()
    {
        if (_cwSampleSource is not null)
        {
            return;
        }

        var src = new CwDecoderProcessSampleSource();
        src.SampleReceived += OnCwSampleReceived;
        src.StatusChanged += OnCwSourceStatusChanged;
        src.RawLineReceived += OnCwRawLineReceived;
        src.LockStateChanged += OnCwLockStateChanged;
        _cwSampleSource = src;
        _cwAggregator = new CwQsoWpmAggregator(src);
        _cwTranscriptAggregator = new CwQsoTranscriptAggregator(src);
        Logger.AttachCwAggregator(_cwAggregator);
        Logger.AttachCwTranscriptAggregator(_cwTranscriptAggregator);
        Logger.AttachCwResetLockHandler(src.ResetLock);
    }

    private void OnCwRawLineReceived(object? sender, string line)
    {
        // Tee to the active diagnostics recorder if one is open. Called from
        // the source's stdout pump background thread; recorder is internally
        // thread-safe.
        _cwDiagnosticsRecorder?.IngestRawLine(line);
    }

    private void StartDiagnosticsSession()
    {
        DisposeDiagnosticsRecorder();
        var sessionDir = BuildDiagnosticsSessionDirectory(DateTimeOffset.UtcNow);
        var binary = CwDecoderProcessSampleSource.LocateBinary();
        var deviceLabel = string.IsNullOrWhiteSpace(CwDecoderDeviceOverride)
            ? "(system default)"
            : CwDecoderDeviceOverride.Trim();
        _cwDiagnosticsRecorder = new CwDiagnosticsRecorder(
            sessionDir,
            DateTimeOffset.UtcNow,
            binary,
            deviceLabel,
            IsCwDecoderLoopback);
    }

    private void DisposeDiagnosticsRecorder()
    {
        var recorder = _cwDiagnosticsRecorder;
        _cwDiagnosticsRecorder = null;
        recorder?.Dispose();
    }

    private static string BuildDiagnosticsSessionDirectory(DateTimeOffset utc)
    {
        var localAppData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        var stamp = utc.ToString("yyyyMMdd-HHmmss", CultureInfo.InvariantCulture);
        return Path.Combine(localAppData, "QsoRipper", "diagnostics", $"session-{stamp}");
    }

    private void OnCwEpisodeStarted(object? sender, CwEpisodeStartedEventArgs e)
    {
        _cwDiagnosticsRecorder?.BeginEpisode(e.UtcStart);

        // Operator has begun a new QSO by typing into the callsign field.
        // If the radio monitor is armed AND the operator-selected mode is
        // CW, launch the cw-decoder subprocess so the WPM readout / F9
        // stats reflect *this* contact rather than ambient noise from
        // before the operator started typing. No-op when monitor is OFF,
        // mode isn't CW, or the process is already running.
        Dispatcher.UIThread.Post(StartCwDecoderProcessForActiveEpisode);
    }

    /// <summary>
    /// Operator changed the mode picker (e.g. cycled away from CW or onto
    /// CW mid-episode). Bring the cw-decoder lifecycle in line with the
    /// new mode without waiting for a save/clear: stop on leave, start on
    /// entry if a callsign is already in the field. This avoids the
    /// "WPM: locked" indicator burning while logging a non-CW QSO and
    /// avoids missing the start of CW copy when the operator only
    /// realises the radio is on CW after typing the callsign.
    /// </summary>
    private void OnLoggerCwModeChanged(object? sender, EventArgs e)
    {
        Dispatcher.UIThread.Post(() =>
        {
            if (!IsCwDecoderEnabled)
            {
                return;
            }

            if (Logger.IsLoggerOnCwMode)
            {
                if (Logger.IsLoggerEpisodeActive)
                {
                    StartCwDecoderProcessForActiveEpisode();
                }
                else
                {
                    CwDecoderStatusText = "WPM: armed";
                }
            }
            else
            {
                if (_cwSampleSource?.IsRunning ?? false)
                {
                    StopCwDecoderProcess();
                }
                CwStatsPane?.Reset();
                CwDecoderStatusText = "WPM: armed (mode is not CW)";
            }
        });
    }

    private void OnCwEpisodeBoundary(object? sender, CwEpisodeBoundaryEventArgs e)
    {
        // The QSO entry just ended (logged, cleared via Esc, or abandoned by
        // emptying the callsign field). Tear down the cw-decoder subprocess
        // so we go fully silent until the operator begins another QSO. This
        // is the deliberate lifecycle policy: the monitor is *armed* via
        // Settings → Radio Monitor, but only *runs* during an active episode.
        // Without this, the decoder would keep reporting a stale lock on
        // ambient noise (or on a still-transmitting station) while the
        // operator stared at a blank form, which is what users were seeing
        // as "F9 says LOCKED even though I cleared the field".
        var wasRunning = _cwSampleSource?.IsRunning ?? false;
        if (wasRunning)
        {
            StopCwDecoderProcess();
        }

        // Always reset the F9 pane's per-episode state (decoded text, last
        // garbled, WPM display) on QSO save/clear/abandoned so the next
        // QSO doesn't inherit a stale smear from the previous one.
        Dispatcher.UIThread.Post(() =>
        {
            CwStatsPane?.Reset();
            // Update the main status bar to reflect the new lifecycle. With
            // the decoder stopped, fall back to the armed/idle/disabled text
            // so the operator gets unambiguous feedback that nothing is
            // listening anymore until they start the next QSO.
            if (!IsCwDecoderEnabled)
            {
                CwDecoderStatusText = "WPM: OFF";
                UpdateDisabledCwStatusText();
            }
            else if (wasRunning)
            {
                CwDecoderStatusText = Logger.IsLoggerOnCwMode
                    ? "WPM: armed"
                    : "WPM: armed (mode is not CW)";
            }
            else if (_cwSampleSource is not null)
            {
                CwDecoderStatusText = _cwSampleSource.CurrentLockState switch
                {
                    CwLockState.Locked => "WPM: locked",
                    CwLockState.Probation => "WPM: probation",
                    CwLockState.Hunting => "WPM: hunting",
                    _ => "WPM: armed",
                };
            }
        });

        var recorder = _cwDiagnosticsRecorder;
        if (recorder is null)
        {
            return;
        }

        var samples = _cwAggregator?.GetSamplesInWindow(e.UtcStart, e.UtcEnd)
            ?? Array.Empty<CwWpmSample>();
        var aggregateMean = _cwAggregator?.GetMeanWpm(e.UtcStart, e.UtcEnd);
        var displayedWpm = _cwSampleSource?.LatestSample?.Wpm;
        var displayedStatus = CwDecoderStatusText;

        recorder.FinalizeEpisode(
            e.Reason,
            e.Qso,
            displayedWpm,
            displayedStatus,
            aggregateMean,
            samples,
            e.UtcStart,
            e.UtcEnd);
    }

    private void OnCwSampleReceived(object? sender, CwWpmSample sample)
    {
        Dispatcher.UIThread.Post(() =>
        {
            // Only display WPM as live when the decoder is locked.
            // The decoder gates wpm emission on confidence, but the
            // user's status bar is the most-glanced surface and must
            // never show a stale value alongside an unlocked state.
            if (_cwSampleSource?.CurrentLockState == CwLockState.Locked)
            {
                CwDecoderStatusText = string.Create(
                    CultureInfo.InvariantCulture,
                    $"WPM: {sample.Wpm:F1}");
            }
        });
    }

    private void OnCwLockStateChanged(object? sender, CwLockState newState)
    {
        Dispatcher.UIThread.Post(() =>
        {
            if (!IsCwDecoderEnabled || _cwSampleSource is null)
            {
                return;
            }
            CwDecoderStatusText = newState switch
            {
                CwLockState.Locked => "WPM: locking…",
                CwLockState.Probation => "WPM: probation",
                CwLockState.Hunting => "WPM: hunting",
                _ => "WPM: idle",
            };
        });
    }

    private void OnCwSourceStatusChanged(object? sender, EventArgs e)
    {
        if (_cwSampleSource is null)
        {
            return;
        }

        var running = _cwSampleSource.IsRunning;
        var lastErr = _cwSampleSource.LastStderrLine;
        Dispatcher.UIThread.Post(() =>
        {
            if (!IsCwDecoderEnabled)
            {
                CwDecoderStatusText = "WPM: OFF";
            }
            else if (!running)
            {
                CwDecoderStatusText = string.IsNullOrWhiteSpace(lastErr)
                    ? "WPM: stopped"
                    : $"WPM: stopped — {Truncate(lastErr, 120)}";
            }
        });
    }

    private static string Truncate(string text, int maxLength)
        => text.Length <= maxLength ? text : text[..(maxLength - 1)] + "…";

    [RelayCommand]
    private void ToggleSpaceWeather()
    {
        IsSpaceWeatherVisible = !IsSpaceWeatherVisible;
        if (IsSpaceWeatherVisible && string.IsNullOrEmpty(SpaceWeatherText))
        {
            _ = FetchSpaceWeatherAsync();
        }
    }

    [RelayCommand]
    private void ToggleHelp()
    {
        if (IsHelpOpen)
        {
            CloseHelp();
            return;
        }

        var vm = new HelpOverlayViewModel();
        vm.CloseRequested += OnHelpCloseRequested;
        HelpOverlay = vm;
        IsHelpOpen = true;
    }

    [RelayCommand]
    private void OpenQsoCard()
    {
        if (IsFullQsoCardOpen)
        {
            return;
        }

        FullQsoCardViewModel vm;
        var selectedQso = RecentQsos.SelectedQso;
        if (!IsLoggerFocused && selectedQso is not null)
        {
            vm = FullQsoCardViewModel.ForEdit(_engine, selectedQso.ToSourceQso());
        }
        else
        {
            vm = FullQsoCardViewModel.ForNew(_engine, Logger, _activeStationProfile?.Clone());
        }

        vm.CloseRequested += OnFullQsoCardCloseRequested;
        vm.Saved += OnFullQsoCardSaved;
        FullQsoCard = vm;
        IsFullQsoCardOpen = true;
    }

    [RelayCommand]
    private void ToggleInspector()
    {
        IsInspectorOpen = !IsInspectorOpen;
        if (!IsInspectorOpen)
        {
            GridFocusRequested?.Invoke(this, EventArgs.Empty);
        }
    }

    [RelayCommand]
    private void ToggleSortChooser()
    {
        IsSortChooserOpen = !IsSortChooserOpen;
        if (IsSortChooserOpen)
        {
            IsColumnChooserOpen = false;
        }
    }

    [RelayCommand]
    private void ToggleColumnChooser()
    {
        IsColumnChooserOpen = !IsColumnChooserOpen;
        if (IsColumnChooserOpen)
        {
            IsSortChooserOpen = false;
        }
    }

    /// <summary>
    /// Raised when the user requests a column layout reset. The View subscribes
    /// and resets DisplayIndex/width/visibility to XAML defaults.
    /// </summary>
    internal event EventHandler? ColumnLayoutResetRequested;

    [RelayCommand]
    private void ResetColumnLayout()
    {
        ColumnLayoutResetRequested?.Invoke(this, EventArgs.Empty);
        IsColumnChooserOpen = false;
    }

    [RelayCommand]
    private void OpenCallsignCard()
    {
        if (IsCallsignCardOpen)
        {
            CloseCallsignCard();
            return;
        }

        // If logger has focus and has a callsign, use that
        string? callsign = null;
        if (IsLoggerFocused && !string.IsNullOrWhiteSpace(Logger.Callsign))
        {
            callsign = Logger.Callsign.Trim().ToUpperInvariant();
        }
        else
        {
            var selectedQso = RecentQsos.SelectedQso;
            if (selectedQso is not null && !string.IsNullOrWhiteSpace(selectedQso.WorkedCallsign))
            {
                callsign = selectedQso.WorkedCallsign;
            }
        }

        if (string.IsNullOrWhiteSpace(callsign))
        {
            return;
        }

        var vm = new CallsignCardViewModel(_engine);
        vm.CloseRequested += OnCallsignCardCloseRequested;
        vm.RecordLoaded += OnCallsignCardRecordLoaded;
        CallsignCard = vm;
        IsCallsignCardOpen = true;
        _ = vm.LoadAsync(callsign);
    }

    [RelayCommand]
    private void CloseCallsignCard()
    {
        CloseCallsignCardCore(restoreFocus: true);
    }

    private void CloseCallsignCardCore(bool restoreFocus)
    {
        if (CallsignCard is { } card)
        {
            card.CloseRequested -= OnCallsignCardCloseRequested;
            card.RecordLoaded -= OnCallsignCardRecordLoaded;
        }

        var wasLoggerFocused = IsLoggerFocused;
        IsCallsignCardOpen = false;
        CallsignCard = null;

        if (!restoreFocus)
        {
            return;
        }

        if (wasLoggerFocused)
        {
            Logger.FocusLogger();
        }
        else
        {
            GridFocusRequested?.Invoke(this, EventArgs.Empty);
        }
    }

    private void OnCallsignCardCloseRequested(object? sender, EventArgs e)
    {
        CloseCallsignCard();
    }

    private void OnCallsignCardRecordLoaded(object? sender, CallsignRecord record)
    {
        Logger.AcceptLookupRecord(record);
    }

    [RelayCommand]
    private void MarkCwAnchorHeard()
    {
        if (_cwSampleSource is null)
        {
            CwDecoderStatusText = "WPM: disabled (Settings → Radio Monitor)";
            return;
        }
        if (!_cwSampleSource.IsRunning)
        {
            CwDecoderStatusText = "WPM: decoder not running";
            CwStatsPane?.MarkAnchorHeard();
            return;
        }

        if (CwStatsPane is { } pane)
        {
            pane.MarkAnchorHeard();
        }
        else
        {
            _cwSampleSource.MarkAnchorHeard();
        }
        CwDecoderStatusText = "WPM: manual anchor armed";
    }

    [RelayCommand]
    private void ToggleCwStatsPane()
    {
        if (IsCwStatsPaneOpen)
        {
            CloseCwStatsPane();
            return;
        }

        var vm = new CwStatsPaneViewModel(_cwSampleSource);
        vm.CloseRequested += OnCwStatsPaneCloseRequested;
        CwStatsPane = vm;
        IsCwStatsPaneOpen = true;
    }

    private void CloseCwStatsPane()
    {
        if (CwStatsPane is { } pane)
        {
            pane.CloseRequested -= OnCwStatsPaneCloseRequested;
            pane.Dispose();
        }
        IsCwStatsPaneOpen = false;
        CwStatsPane = null;
    }

    private void OnCwStatsPaneCloseRequested(object? sender, EventArgs e) => CloseCwStatsPane();

    private void CloseHelp()
    {
        if (HelpOverlay is { } h)
        {
            h.CloseRequested -= OnHelpCloseRequested;
        }

        IsHelpOpen = false;
        HelpOverlay = null;
        GridFocusRequested?.Invoke(this, EventArgs.Empty);
    }

    private void OnHelpCloseRequested(object? sender, EventArgs e)
    {
        CloseHelp();
    }

    private void CloseFullQsoCard()
    {
        if (FullQsoCard is { } card)
        {
            card.CloseRequested -= OnFullQsoCardCloseRequested;
            card.Saved -= OnFullQsoCardSaved;
            card.Dispose();
        }

        IsFullQsoCardOpen = false;
        FullQsoCard = null;
    }

    private void OnFullQsoCardCloseRequested(object? sender, EventArgs e)
    {
        CloseFullQsoCard();
    }

    private void OnFullQsoCardSaved(object? sender, EventArgs e)
    {
        OnQsoLogged(sender, e);
    }

    private async void OnQsoLogged(object? sender, EventArgs e)
    {
        try
        {
            await RecentQsos.RefreshAsync();
            MarkEngineReachable();
        }
        catch (Grpc.Core.RpcException)
        {
            StatusMessage = "Ready (refresh failed)";
            MarkEngineUnreachable();
        }
        catch (ObjectDisposedException)
        {
            StatusMessage = "Ready (engine restarting...)";
        }
        catch (InvalidOperationException)
        {
            StatusMessage = "Ready (refresh failed)";
        }
    }

    private void OnLoggerFocusRequested(object? sender, EventArgs e)
    {
        LoggerFocusRequested?.Invoke(this, EventArgs.Empty);
    }

    private async void OnRigTimerTick(object? sender, EventArgs e)
    {
        // The tick is async void, so it returns at the first await and the timer
        // can fire again before the previous poll completes. If the engine ever
        // stalls, overlapping polls would pile up; guard against reentrancy.
        if (_rigTickInFlight)
        {
            return;
        }

        _rigTickInFlight = true;
        try
        {
            var response = await _engine.GetRigSnapshotAsync();
            if (response.Snapshot is { } snapshot)
            {
                if (snapshot.Status == QsoRipper.Domain.RigConnectionStatus.Connected)
                {
                    var modeDisplay = ProtoEnumDisplay.ForMode(snapshot.Mode);
                    RigStatusText = $"Rig: {FrequencyFormatter.FormatMhz(snapshot.FrequencyHz)} {modeDisplay}";
                    Logger.ApplyRigSnapshot(snapshot);
                }
                else
                {
                    RigStatusText = $"Rig: {snapshot.Status}";
                }
            }
        }
        catch (Grpc.Core.RpcException)
        {
            RigStatusText = "Rig: error";
            MarkEngineUnreachable();
        }
        catch (ObjectDisposedException)
        {
            RigStatusText = "Rig: unavailable";
        }
        finally
        {
            _rigTickInFlight = false;
        }
    }

    private async void OnSpaceWeatherTimerTick(object? sender, EventArgs e)
    {
        try
        {
            // Periodic refresh: preserve stale data on failure so a transient
            // network error never clears good weather readings.
            await FetchSpaceWeatherAsync(preserveOnFailure: true);
        }
        catch (ObjectDisposedException)
        {
            // Timer fired after disposal; ignore.
        }
        catch (Grpc.Core.RpcException)
        {
            // Transient network failure — keep existing weather data.
            MarkEngineUnreachable();
        }
        catch (InvalidOperationException)
        {
            // Engine not ready — keep existing weather data.
        }
    }

    private async Task FetchSpaceWeatherAsync(bool preserveOnFailure = false)
    {
        try
        {
            var response = await _engine.GetCurrentSpaceWeatherAsync();
            if (response.Snapshot is { } sw && sw.Status == QsoRipper.Domain.SpaceWeatherStatus.Current)
            {
                var parts = new List<string>();
                if (sw.HasPlanetaryKIndex)
                {
                    parts.Add($"K:{sw.PlanetaryKIndex.ToString("F0", CultureInfo.InvariantCulture)}");
                }

                if (sw.HasSolarFluxIndex)
                {
                    parts.Add($"SFI:{sw.SolarFluxIndex.ToString("F0", CultureInfo.InvariantCulture)}");
                }

                if (sw.HasSunspotNumber)
                {
                    parts.Add($"SN:{sw.SunspotNumber.ToString(CultureInfo.InvariantCulture)}");
                }

                SpaceWeatherText = parts.Count > 0 ? string.Join(" ", parts) : "Weather: no data";
            }
            else if (!preserveOnFailure)
            {
                SpaceWeatherText = "Weather: unavailable";
            }
        }
        catch (Grpc.Core.RpcException) when (!preserveOnFailure)
        {
            SpaceWeatherText = "Weather: error";
            MarkEngineUnreachable();
        }
    }

    [RelayCommand]
    private void CloseTransientPanels(bool restoreGridFocus = true)
    {
        IsSortChooserOpen = false;
        IsColumnChooserOpen = false;
        CloseCallsignCardCore(restoreFocus: false);
        if (restoreGridFocus)
        {
            GridFocusRequested?.Invoke(this, EventArgs.Empty);
        }
    }

    [RelayCommand]
    private static void Exit()
    {
        if (Application.Current?.ApplicationLifetime is IClassicDesktopStyleApplicationLifetime lifetime)
        {
            lifetime.Shutdown();
        }
    }

    internal void CancelWizard()
    {
        IsWizardOpen = false;
        WizardViewModel = null;
        IsSetupIncomplete = !_setupCompleteBeforeWizard;
        StatusMessage = _setupCompleteBeforeWizard
            ? "Ready"
            : "Setup incomplete";

        if (_setupCompleteBeforeWizard)
        {
            _ = ActivateDashboardAsync(focusSearch: true);
        }
    }

    internal void CloseWizard(bool setupComplete)
    {
        IsWizardOpen = false;
        WizardViewModel = null;
        IsSetupIncomplete = !setupComplete;
        StatusMessage = setupComplete
            ? "Ready"
            : "Setup incomplete";

        _ = RefreshSetupContextAsync();

        if (setupComplete)
        {
            _ = ActivateDashboardAsync(focusSearch: true);
        }
    }

    public void Dispose()
    {
        _utcTimer.Stop();
        _utcTimer.Tick -= UtcTimerOnTick;

        _rigTimer.Stop();
        _rigTimer.Tick -= OnRigTimerTick;

        _spaceWeatherTimer.Stop();
        _spaceWeatherTimer.Tick -= OnSpaceWeatherTimerTick;

        _engineHealthTimer.Stop();
        _engineHealthTimer.Tick -= OnEngineHealthTimerTick;

        if (_cwSampleSource is not null)
        {
            _cwSampleSource.SampleReceived -= OnCwSampleReceived;
            _cwSampleSource.StatusChanged -= OnCwSourceStatusChanged;
            _cwSampleSource.RawLineReceived -= OnCwRawLineReceived;
            _cwSampleSource.LockStateChanged -= OnCwLockStateChanged;
            _cwSampleSource.Stop();
            _cwSampleSource.Dispose();
            _cwSampleSource = null;
        }
        _cwAggregator?.Dispose();
        _cwAggregator = null;
        _cwTranscriptAggregator?.Dispose();
        _cwTranscriptAggregator = null;
        DisposeDiagnosticsRecorder();
        CwStatsPane?.Dispose();
        CwStatsPane = null;

        if (_switchableEngine is not null)
        {
            _switchableEngine.Dispose();
        }
        else if (_engine is IDisposable disposable)
        {
            disposable.Dispose();
        }
    }

    /// <summary>
    /// Restores persisted UI preferences (rig control, space weather, inspector).
    /// Call after construction but before or during <see cref="ActivateDashboardAsync"/>.
    /// </summary>
    internal void ApplyPreferences(UiPreferences? prefs)
    {
        if (prefs is null)
        {
            return;
        }

        if (prefs.IsRigEnabled)
        {
            ToggleRigControl();
        }

        if (prefs.IsSpaceWeatherVisible)
        {
            IsSpaceWeatherVisible = true;
        }

        if (prefs.IsInspectorOpen)
        {
            IsInspectorOpen = true;
        }

        var loadedProfileId = string.IsNullOrWhiteSpace(prefs.EngineProfileId)
            ? null
            : prefs.EngineProfileId.Trim();
        var loadedEndpoint = string.IsNullOrWhiteSpace(prefs.EngineEndpoint)
            ? null
            : prefs.EngineEndpoint.Trim();
        _persistedEngineProfileId = loadedProfileId;
        _persistedEngineEndpoint = loadedEndpoint;

        if (HasEngineEnvironmentOverride())
        {
            _preferredEngineProfileId = null;
            _preferredEngineEndpoint = null;
        }
        else
        {
            _preferredEngineProfileId = loadedProfileId;
            _preferredEngineEndpoint = loadedEndpoint;
        }

        if (!string.IsNullOrWhiteSpace(prefs.CwDecoderDeviceOverride))
        {
            CwDecoderDeviceOverride = prefs.CwDecoderDeviceOverride.Trim();
        }

        IsCwDecoderLoopback = prefs.IsCwDecoderLoopback;
        IsCwWpmStatusBarVisible = prefs.IsCwWpmStatusBarVisible;
        IsCwDiagnosticsEnabled = prefs.IsCwDiagnosticsEnabled;

        if (prefs.IsCwDecoderEnabled)
        {
            ToggleCwDecoder();
        }

        UpdateDisabledCwStatusText();
    }

    /// <summary>
    /// Captures current UI toggle state for persistence across restarts.
    /// </summary>
    internal UiPreferences CapturePreferences()
    {
        var envOverride = HasEngineEnvironmentOverride();
        return new UiPreferences
        {
            IsRigEnabled = IsRigEnabled,
            IsSpaceWeatherVisible = IsSpaceWeatherVisible,
            IsInspectorOpen = IsInspectorOpen,
            EngineProfileId = envOverride
                ? _persistedEngineProfileId
                : (_switchableEngine?.CurrentProfile.ProfileId ?? _persistedEngineProfileId),
            EngineEndpoint = envOverride
                ? _persistedEngineEndpoint
                : (_switchableEngine?.CurrentEndpoint ?? _persistedEngineEndpoint),
            IsCwDecoderEnabled = IsCwDecoderEnabled,
            IsCwDecoderLoopback = IsCwDecoderLoopback,
            IsCwWpmStatusBarVisible = IsCwWpmStatusBarVisible,
            IsCwDiagnosticsEnabled = IsCwDiagnosticsEnabled,
            CwDecoderDeviceOverride = string.IsNullOrWhiteSpace(CwDecoderDeviceOverride)
                ? null
                : CwDecoderDeviceOverride.Trim(),
        };
    }

    private async Task ActivateDashboardAsync(bool focusSearch, Task? recentQsoRefreshTask = null)
    {
        GuiPerformanceTrace.Write(nameof(ActivateDashboardAsync) + ".start");
        StatusMessage = "Loading QSOs...";
        recentQsoRefreshTask ??= RecentQsos.RefreshAsync();
        await recentQsoRefreshTask;
        GuiPerformanceTrace.Write(nameof(ActivateDashboardAsync) + ".afterRecentQsosRefresh");
        StatusMessage = "Ready";
        _ = RefreshSyncStatusAsync();

        _ = FetchSpaceWeatherAsync();
        _spaceWeatherTimer.Start();
        await Task.Yield();

        if (IsWizardOpen)
        {
            return;
        }

        if (focusSearch)
        {
            SearchFocusRequested?.Invoke(this, EventArgs.Empty);
        }
        else
        {
            // Default: focus the QSO logger callsign field for immediate entry
            Logger.FocusLogger();
        }

        GuiPerformanceTrace.Write(nameof(ActivateDashboardAsync) + ".complete");
    }

    private void UtcTimerOnTick(object? sender, EventArgs e)
    {
        UpdateUtcClock();
    }

    private void UpdateUtcClock()
    {
        var utcNow = DateTimeOffset.UtcNow;
        CurrentUtcTime = utcNow.ToString("HH:mm:ss 'UTC'", CultureInfo.InvariantCulture);
        CurrentUtcDate = utcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
    }

    private DispatcherTimer CreateUtcTimer()
    {
        var timer = new DispatcherTimer
        {
            Interval = TimeSpan.FromSeconds(1)
        };
        timer.Tick += UtcTimerOnTick;
        timer.Start();
        return timer;
    }

    private async Task RefreshSetupContextAsync()
    {
        try
        {
            ApplySetupContext((await _engine.GetSetupStatusAsync()).Status);
            MarkEngineReachable();
        }
        catch (Grpc.Core.RpcException)
        {
            StatusMessage = "Engine unavailable";
            MarkEngineUnreachable();
        }
    }

    private void ApplySetupContext(QsoRipper.Services.SetupStatus status)
    {
        var activeProfile = status.StationProfile;
        _activeStationProfile = activeProfile?.Clone();
        ActiveLogText = BuildLogText(status);
        ActiveProfileText = BuildProfileText(activeProfile);
        ActiveStationText = BuildStationText(activeProfile);
        StationCallsign = activeProfile?.StationCallsign?.Trim() ?? string.Empty;
    }

    private static string BuildLogText(QsoRipper.Services.SetupStatus status)
    {
        var persistenceFields = PersistenceSetupFields.FromStatus(status, status.SuggestedLogFilePath ?? string.Empty);
        var pathValue = PersistenceSetupFields.GetPathValue(persistenceFields);
        if (string.IsNullOrWhiteSpace(pathValue))
        {
            return "Log: engine-managed";
        }

        return $"Log: {GetFileNameWithoutExtensionPortable(pathValue)}";
    }

    private static string GetFileNameWithoutExtensionPortable(string path)
    {
        var trimmed = path.Trim().TrimEnd(Path.DirectorySeparatorChar, Path.AltDirectorySeparatorChar, '\\', '/');
        var fileName = trimmed
            .Split(['\\', '/'], StringSplitOptions.RemoveEmptyEntries)
            .LastOrDefault();
        return string.IsNullOrWhiteSpace(fileName) ? trimmed : Path.GetFileNameWithoutExtension(fileName);
    }

    private static string BuildProfileText(QsoRipper.Domain.StationProfile? profile)
    {
        var profileName = profile?.ProfileName;
        return string.IsNullOrWhiteSpace(profileName)
            ? "Profile: Default"
            : $"Profile: {profileName.Trim()}";
    }

    private static string BuildStationText(QsoRipper.Domain.StationProfile? profile)
    {
        var stationCallsign = profile?.StationCallsign;
        return string.IsNullOrWhiteSpace(stationCallsign)
            ? "Station: -"
            : $"Station: {stationCallsign.Trim()}";
    }

    private void OnRecentQsosPropertyChanged(object? sender, System.ComponentModel.PropertyChangedEventArgs e)
    {
        if (e.PropertyName == nameof(RecentQsoListViewModel.SelectedQso))
        {
            OnPropertyChanged(nameof(InspectorQso));
            OnPropertyChanged(nameof(HasInspectorQso));
        }
    }

    private async Task RefreshSyncStatusAsync()
    {
        try
        {
            var status = await _engine.GetSyncStatusAsync();
            if (status.IsSyncing)
            {
                SyncStatusText = "Syncing\u2026";
            }
            else if (status.LastSync is not null)
            {
                var elapsed = DateTimeOffset.UtcNow - status.LastSync.ToDateTimeOffset();
                SyncStatusText = elapsed.TotalMinutes < 1
                    ? "Last sync: just now"
                    : elapsed.TotalHours < 1
                        ? $"Last sync: {(int)elapsed.TotalMinutes}m ago"
                        : elapsed.TotalDays < 1
                            ? $"Last sync: {(int)elapsed.TotalHours}h ago"
                            : $"Last sync: {(int)elapsed.TotalDays}d ago";
            }
            else
            {
                SyncStatusText = "Sync: never";
            }
        }
        catch (Grpc.Core.RpcException)
        {
            // Sync status unavailable — leave current text unchanged.
            MarkEngineUnreachable();
        }
    }

    private string BuildEngineUnreachableMessage()
    {
        var endpoint = _switchableEngine?.CurrentEndpoint;
        if (string.IsNullOrWhiteSpace(endpoint))
        {
            return "QsoRipper engine unreachable. Make sure the engine is running.";
        }

        return $"QsoRipper engine unreachable at {endpoint}. Make sure the engine is running.";
    }

    private void MarkEngineUnreachable()
    {
        EngineUnreachableMessage = BuildEngineUnreachableMessage();
        IsEngineUnreachable = true;
    }

    private void MarkEngineReachable()
    {
        if (IsEngineUnreachable)
        {
            IsEngineUnreachable = false;
        }
    }

    private void StartEngineHealthTimer()
    {
        if (IsWizardOpen || _engineHealthTimer.IsEnabled)
        {
            return;
        }

        _engineHealthTimer.Start();
    }

    private async void OnEngineHealthTimerTick(object? sender, EventArgs e)
    {
        if (_engineHealthProbeInFlight || IsWizardOpen)
        {
            return;
        }

        _engineHealthProbeInFlight = true;
        using var cts = new CancellationTokenSource(EngineHealthProbeTimeout);
        try
        {
            await _engine.GetSyncStatusAsync(cts.Token);
            MarkEngineReachable();
        }
        catch (RpcException)
        {
            MarkEngineUnreachable();
        }
        catch (HttpRequestException)
        {
            MarkEngineUnreachable();
        }
        catch (IOException)
        {
            MarkEngineUnreachable();
        }
        catch (OperationCanceledException)
        {
            MarkEngineUnreachable();
        }
        catch (ObjectDisposedException)
        {
            // Engine being torn down; leave state unchanged.
        }
        finally
        {
            _engineHealthProbeInFlight = false;
        }
    }
}
