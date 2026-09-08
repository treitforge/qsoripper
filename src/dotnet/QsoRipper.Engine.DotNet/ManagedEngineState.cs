using System.Text.RegularExpressions;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using QsoRipper.Domain;
using QsoRipper.Engine.ContestCalendar;
using QsoRipper.Engine.DotNet.Lotw;
using QsoRipper.Engine.DotNet.Wsjtx;
using QsoRipper.Engine.Lookup;
using QsoRipper.Engine.QrzLogbook;
using QsoRipper.Engine.RigControl;
using QsoRipper.Engine.SpaceWeather;
using QsoRipper.Engine.Storage;
using QsoRipper.Engine.Storage.Memory;
using QsoRipper.EngineSelection;
using QsoRipper.Services;

namespace QsoRipper.Engine.DotNet;

internal sealed record DeleteQsoOutcome(bool Found, bool RemoteDeleteQueued, bool MissingQrzLogid);

/// <summary>A QSO affected (inserted or refreshed) by a WSJT-X import.</summary>
internal sealed record WsjtxImportedQsoRef(string LocalId, string WorkedCallsign);

/// <summary>An import result paired with the QSOs it inserted or refreshed.</summary>
internal sealed record WsjtxImportDetail(ImportAdifResponse Response, IReadOnlyList<WsjtxImportedQsoRef> AffectedQsos);

/// <summary>Per-QSO outcome of a managed QRZ logbook sync triggered by WSJT-X ingestion.</summary>
internal sealed record WsjtxQrzSyncOutcome(string LocalId, string WorkedCallsign, bool Success, string? Error);

internal sealed record RestoreQsoOutcome(bool Found, QsoRecord? Restored);

internal sealed record PurgeDeletedQsosOutcome(
    int PurgedCount,
    int RemoteDeletesPushed,
    int RemoteDeletesFailed,
    string? ErrorSummary);

internal enum StationProfileDeleteOutcome
{
    Deleted,
    NotFound,
    ActiveProfile,
}

internal sealed class QsoSoftDeletedException : InvalidOperationException
{
    public QsoSoftDeletedException()
        : base("QSO is deleted; restore it before updating.")
    {
        LocalId = string.Empty;
    }

    public QsoSoftDeletedException(string localId)
        : base($"QSO '{localId}' is deleted; restore it before updating.")
    {
        LocalId = localId;
    }

    public QsoSoftDeletedException(string message, Exception innerException)
        : base(message, innerException)
    {
        LocalId = string.Empty;
    }

    public string LocalId { get; }
}

internal sealed class NoActiveStationProfileException : InvalidOperationException
{
    public NoActiveStationProfileException()
        : base("An active station profile is required before logging a QSO.")
    {
    }

    public NoActiveStationProfileException(string message)
        : base(message)
    {
    }

    public NoActiveStationProfileException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

internal sealed class QrzSyncUnavailableException : InvalidOperationException
{
    public QrzSyncUnavailableException()
        : base("QRZ sync is unavailable.")
    {
    }

    public QrzSyncUnavailableException(string message)
        : base(message)
    {
    }

    public QrzSyncUnavailableException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}

internal sealed partial class ManagedEngineState
{
    private const string PersistenceStepDescription = "The managed .NET engine keeps its logbook in memory. No persistence input is required during setup.";
    private const string PersistenceStepDescriptionSqlite = "The managed .NET engine stores its logbook in a local SQLite database backed by shared setup.";
    private const string PersistenceStepLabel = "Storage";
    private const string InMemoryPersistenceSummary = "In-memory logbook";
    private const string SqlitePersistenceSummary = "SQLite logbook";

    private const string StorageBackendKey = "QSORIPPER_STORAGE_BACKEND";
    private const string QrzXmlUsernameKey = "QSORIPPER_QRZ_XML_USERNAME";
    private const string QrzXmlPasswordKey = "QSORIPPER_QRZ_XML_PASSWORD";
    private const string QrzUserAgentKey = "QSORIPPER_QRZ_USER_AGENT";
    private const string QrzLogbookApiKeyKey = "QSORIPPER_QRZ_LOGBOOK_API_KEY";
    private const string LotwUsernameKey = "QSORIPPER_LOTW_USERNAME";
    private const string LotwPasswordKey = "QSORIPPER_LOTW_PASSWORD";
    private const string LotwTqslPathKey = "QSORIPPER_LOTW_TQSL_PATH";
    private const string LotwStationLocationKey = "QSORIPPER_LOTW_STATION_LOCATION";
    private const string LotwCertificatePasswordKey = "QSORIPPER_LOTW_CERTIFICATE_PASSWORD";
    private const string LotwReportUrlKey = "QSORIPPER_LOTW_REPORT_URL";
    private const string LotwTimeoutSecondsKey = "QSORIPPER_LOTW_TIMEOUT_SECONDS";
    private const string RigEnabledKey = "QSORIPPER_RIGCTLD_ENABLED";

    private const string ManagedLookupProviderSummary = "Managed sample provider";

    private static readonly JsonFormatter ProtoJsonFormatter = new(JsonFormatter.Settings.Default.WithFormatDefaultValues(true));
    private static readonly JsonParser ProtoJsonParser = new(JsonParser.Settings.Default.WithIgnoreUnknownFields(true));

    private readonly Lock _gate = new();
    private readonly IEngineStorage _storage;
    private ILookupCoordinator _lookupCoordinator;
    private readonly ContestCalendarMonitor? _contestCalendarMonitor;
    private readonly RigControlMonitor? _rigControlMonitor;
    private readonly SpaceWeatherMonitor? _spaceWeatherMonitor;
    private readonly IQrzCredentialTester _qrzCredentialTester;
    private readonly IQrzSecretStore _qrzSecretStore;
    private readonly string _configPath;
    private readonly SharedPersistedSetupConfig _persistedSetup;
    private readonly string? _currentPersistenceLocation;
    private string? _qrzXmlUsername;
    private string? _qrzXmlPassword;
    private bool _hasQrzXmlPassword;
    private string? _qrzLogbookApiKey;
    private bool _hasQrzLogbookApiKey;
    private string? _baseQrzXmlUsername;
    private string? _baseQrzXmlPassword;
    private string? _baseQrzLogbookApiKey;
    private RigControlSettings? _baseRigControl;
    private SyncConfig _syncConfig;
    private RigControlSettings? _rigControl;
    private readonly List<ManagedPersistedStationProfile> _stationProfiles;
    private string? _activeProfileId;
    private StationProfile? _sessionOverrideProfile;
    private readonly Dictionary<string, string> _runtimeOverrides;
    private readonly bool _ownsSyncEngine;
    private QrzLogbookClient? _ownedSyncClient;
    private QrzSyncEngine? _syncEngine;
    private readonly ILotwApi? _lotwApi;
    private readonly ManagedLotwSettings _lotwSettings;
    private bool _isSyncing;
    private bool _isLotwSyncing;
    private DateTimeOffset? _nextSync;
    private string? _lastSyncError;

    // Latest live WSJT-X ingest diagnostics published by the ingest supervisor. A null value means
    // the supervisor has not published a snapshot yet; BuildSetupStatusNoLock then leaves
    // SetupStatus.wsjtx_ingest_status unset. Stored as an already-cloned, immutable-by-convention
    // snapshot and swapped atomically via Volatile reads/writes.
    private WsjtxIngestStatus? _wsjtxIngestLiveStatus;

    public ManagedEngineState(string configPath)
        : this(configPath, new MemoryStorage(), null, null, null, null)
    {
    }

    public ManagedEngineState(string configPath, IEngineStorage storage)
        : this(configPath, storage, null, null, null, null)
    {
    }

    public ManagedEngineState(string configPath, IEngineStorage storage, ILookupCoordinator? lookupCoordinator)
        : this(configPath, storage, lookupCoordinator, null, null, null)
    {
    }

    public ManagedEngineState(string configPath, IEngineStorage storage, ILookupCoordinator? lookupCoordinator, RigControlMonitor? rigControlMonitor)
        : this(configPath, storage, lookupCoordinator, rigControlMonitor, null, null)
    {
    }

    public ManagedEngineState(string configPath, IEngineStorage storage, ILookupCoordinator? lookupCoordinator, RigControlMonitor? rigControlMonitor, SpaceWeatherMonitor? spaceWeatherMonitor)
        : this(configPath, storage, lookupCoordinator, null, rigControlMonitor, spaceWeatherMonitor, null, null, null)
    {
    }

    public ManagedEngineState(string configPath, IEngineStorage storage, ILookupCoordinator? lookupCoordinator, RigControlMonitor? rigControlMonitor, SpaceWeatherMonitor? spaceWeatherMonitor, QrzSyncEngine? syncEngine)
        : this(configPath, storage, lookupCoordinator, null, rigControlMonitor, spaceWeatherMonitor, syncEngine, null, null)
    {
    }

    public ManagedEngineState(string configPath, IEngineStorage storage, ILookupCoordinator? lookupCoordinator, ContestCalendarMonitor? contestCalendarMonitor, RigControlMonitor? rigControlMonitor, SpaceWeatherMonitor? spaceWeatherMonitor, QrzSyncEngine? syncEngine, string? currentPersistenceLocation, LoadedSharedSetupConfig? loadedPersistedSetup, IQrzCredentialTester? qrzCredentialTester = null, IQrzSecretStore? qrzSecretStore = null, ILotwApi? lotwApi = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(configPath);
        ArgumentNullException.ThrowIfNull(storage);

        _configPath = Path.GetFullPath(configPath.Trim());
        _storage = storage;
        _lookupCoordinator = lookupCoordinator ?? CreateDefaultCoordinator(storage);
        _contestCalendarMonitor = contestCalendarMonitor;
        _rigControlMonitor = rigControlMonitor;
        _spaceWeatherMonitor = spaceWeatherMonitor;
        _qrzCredentialTester = qrzCredentialTester ?? new QrzCredentialTester();
        _qrzSecretStore = qrzSecretStore ?? new NullQrzSecretStore();
        _lotwApi = lotwApi;
        _ownsSyncEngine = syncEngine is null;
        _syncEngine = syncEngine;
        var loadedSetup = loadedPersistedSetup ?? SharedSetupConfigPersistence.Load(_configPath);
        _persistedSetup = loadedSetup.Config;
        _currentPersistenceLocation = NormalizeOptional(currentPersistenceLocation)
            ?? (_storage.BackendName.Equals("sqlite", StringComparison.OrdinalIgnoreCase)
                ? NormalizeOptional(_persistedSetup.GetPersistedLogFilePath())
                : null);
        _qrzXmlUsername = NormalizeOptional(Environment.GetEnvironmentVariable(QrzXmlUsernameKey))
            ?? NormalizeOptional(_persistedSetup.QrzXmlUsername);
        var legacyQrzXmlPassword = NormalizeOptional(_persistedSetup.QrzXmlPassword);
        var legacyQrzLogbookApiKey = NormalizeOptional(_persistedSetup.QrzLogbookApiKey);
        MigrateLegacySecret(QrzSecret.XmlPassword, legacyQrzXmlPassword);
        MigrateLegacySecret(QrzSecret.LogbookApiKey, legacyQrzLogbookApiKey);
        _qrzXmlPassword = NormalizeOptional(Environment.GetEnvironmentVariable(QrzXmlPasswordKey))
            ?? legacyQrzXmlPassword
            ?? ReadStoredSecret(QrzSecret.XmlPassword);
        _hasQrzXmlPassword = _qrzXmlPassword is not null;
        _qrzLogbookApiKey = NormalizeOptional(Environment.GetEnvironmentVariable(QrzLogbookApiKeyKey))
            ?? legacyQrzLogbookApiKey
            ?? ReadStoredSecret(QrzSecret.LogbookApiKey);
        _hasQrzLogbookApiKey = _qrzLogbookApiKey is not null;
        var persistedLotw = _persistedSetup.Lotw ?? new ManagedLotwSettings();
        _lotwSettings = new ManagedLotwSettings
        {
            Username = NormalizeOptional(Environment.GetEnvironmentVariable(LotwUsernameKey))
                ?? NormalizeOptional(persistedLotw.Username),
            Password = NormalizeOptional(Environment.GetEnvironmentVariable(LotwPasswordKey))
                ?? NormalizeOptional(persistedLotw.Password),
            TqslPath = NormalizeOptional(Environment.GetEnvironmentVariable(LotwTqslPathKey))
                ?? NormalizeOptional(persistedLotw.TqslPath)
                ?? "tqsl",
            StationLocation = NormalizeOptional(Environment.GetEnvironmentVariable(LotwStationLocationKey))
                ?? NormalizeOptional(persistedLotw.StationLocation),
            CertificatePassword = NormalizeOptional(Environment.GetEnvironmentVariable(LotwCertificatePasswordKey))
                ?? NormalizeOptional(persistedLotw.CertificatePassword),
            ReportUrl = NormalizeOptional(Environment.GetEnvironmentVariable(LotwReportUrlKey))
                ?? NormalizeOptional(persistedLotw.ReportUrl)
                ?? LotwClient.DefaultReportUrl,
            TimeoutSeconds = ParsePositiveUInt32(Environment.GetEnvironmentVariable(LotwTimeoutSecondsKey))
                ?? persistedLotw.TimeoutSeconds
                ?? 60,
        };
        var hadPersistedSecrets = !string.IsNullOrWhiteSpace(_persistedSetup.QrzXmlPassword)
            || !string.IsNullOrWhiteSpace(_persistedSetup.QrzLogbookApiKey)
            || !string.IsNullOrWhiteSpace(persistedLotw.Password)
            || !string.IsNullOrWhiteSpace(persistedLotw.CertificatePassword);
        _persistedSetup.QrzXmlPassword = null;
        _persistedSetup.QrzLogbookApiKey = null;
        _persistedSetup.Lotw = new ManagedLotwSettings
        {
            Username = _lotwSettings.Username,
            TqslPath = _lotwSettings.TqslPath,
            StationLocation = _lotwSettings.StationLocation,
            ReportUrl = _lotwSettings.ReportUrl,
            TimeoutSeconds = _lotwSettings.TimeoutSeconds,
        };
        _syncConfig = NormalizeSyncConfig(_persistedSetup.SyncConfig);
        _persistedSetup.SyncConfig = _syncConfig.Clone();
        if (_persistedSetup.WsjtxIngest is not null)
        {
            _persistedSetup.WsjtxIngest = NormalizeWsjtxIngest(_persistedSetup.WsjtxIngest);
        }

        _rigControl = _persistedSetup.RigControl?.Clone();
        _baseQrzXmlUsername = _qrzXmlUsername;
        _baseQrzXmlPassword = _qrzXmlPassword;
        _baseQrzLogbookApiKey = _qrzLogbookApiKey;
        _baseRigControl = _rigControl?.Clone();
        _stationProfiles = _persistedSetup.StationProfiles
            .Select(static entry => new ManagedPersistedStationProfile
            {
                ProfileId = entry.ProfileId,
                ProfileJson = entry.ProfileJson,
            })
            .ToList();
        _activeProfileId = NormalizeOptional(_persistedSetup.ActiveProfileId);
        _sessionOverrideProfile = null;
        _runtimeOverrides = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (lookupCoordinator is null)
        {
            RebuildLookupCoordinatorNoLock();
        }

        if (_ownsSyncEngine)
        {
            RebuildSyncEngineNoLock();
        }

        if (hadPersistedSecrets)
        {
            PersistNoLock();
        }

        RepairLegacyQrzLogids();

        if (loadedSetup.LastSyncUtc is { } lastSync)
        {
            Sync(_storage.Logbook.UpsertSyncMetadataAsync(new SyncMetadata { LastSync = lastSync }));
        }
    }

    public static EngineInfo BuildEngineInfo()
    {
        return new EngineInfo
        {
            EngineId = EngineCatalog.DotNetProfile.EngineId,
            DisplayName = EngineCatalog.DotNetProfile.DisplayName,
            Version = typeof(ManagedEngineState).Assembly.GetName().Version?.ToString() ?? "0.0.0",
            Capabilities =
            {
                "engine-info",
                "logbook",
                "lookup-cache",
                "lookup-callsign",
                "lookup-stream",
                "setup",
                "station-profiles",
                "runtime-config",
                "rig-control",
                "contest-calendar",
                "space-weather",
                "cw-keying",
                "purge",
            }
        };
    }

    public SetupStatus GetSetupStatus()
    {
        lock (_gate)
        {
            return BuildSetupStatusNoLock();
        }
    }

    public GetSetupWizardStateResponse GetSetupWizardState()
    {
        lock (_gate)
        {
            var response = new GetSetupWizardStateResponse
            {
                Status = BuildSetupStatusNoLock(),
            };
            response.Steps.AddRange(BuildStepStatusesNoLock());
            response.StationProfiles.AddRange(BuildStationProfileRecordsNoLock());
            return response;
        }
    }

    public static ValidateSetupStepResponse ValidateSetupStep(ValidateSetupStepRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var response = new ValidateSetupStepResponse();
        switch (request.Step)
        {
            case SetupWizardStep.LogFile:
                response.Valid = true;
                break;
            case SetupWizardStep.StationProfiles:
                var profile = request.StationProfile ?? new StationProfile();
                AddValidation(response, "profile_name", !string.IsNullOrWhiteSpace(profile.ProfileName), "Profile name is required.");
                AddValidation(response, "callsign", !string.IsNullOrWhiteSpace(profile.StationCallsign), "Station callsign is required.");
                AddValidation(response, "operator_callsign", !string.IsNullOrWhiteSpace(profile.OperatorCallsign), "Operator callsign is required.");
                AddValidation(response, "grid_square", !string.IsNullOrWhiteSpace(profile.Grid), "Grid square is required.");
                break;
            case SetupWizardStep.QrzIntegration:
                var hasUsername = !string.IsNullOrWhiteSpace(request.QrzXmlUsername);
                var hasPassword = !string.IsNullOrWhiteSpace(request.QrzXmlPassword);
                AddValidation(response, "qrz_xml_username", hasUsername == hasPassword, "Provide both username and password, or leave both blank.");
                AddValidation(response, "qrz_xml_password", hasUsername == hasPassword, "Provide both username and password, or leave both blank.");
                break;
        }

        response.Valid = response.Fields.All(field => field.Valid);
        return response;
    }

    public Task<TestQrzCredentialsResponse> TestQrzCredentialsAsync(
        string username,
        string password,
        CancellationToken cancellationToken)
    {
        return _qrzCredentialTester.TestXmlCredentialsAsync(username, password, cancellationToken);
    }

    public Task<TestQrzLogbookCredentialsResponse> TestQrzLogbookCredentialsAsync(
        string apiKey,
        CancellationToken cancellationToken)
    {
        return _qrzCredentialTester.TestLogbookCredentialsAsync(apiKey, cancellationToken);
    }

    public SaveSetupResponse SaveSetup(SaveSetupRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        lock (_gate)
        {
            var requestedQrzXmlPassword = NormalizeOptional(request.QrzXmlPassword);
            var requestedQrzLogbookApiKey = NormalizeOptional(request.QrzLogbookApiKey);
            PersistRequestedSecret(QrzSecret.XmlPassword, requestedQrzXmlPassword);
            PersistRequestedSecret(QrzSecret.LogbookApiKey, requestedQrzLogbookApiKey);

            var normalizedWsjtxIngest = request.WsjtxIngest is null
                ? null
                : NormalizeWsjtxIngest(request.WsjtxIngest);

            if (request.StationProfile is not null)
            {
                SaveStationProfileNoLock(
                    NormalizeProfileIdOrDefault(request.StationProfile.ProfileName, request.StationProfile.StationCallsign),
                    request.StationProfile,
                    makeActive: true);
            }

            if (!string.IsNullOrWhiteSpace(request.QrzXmlUsername))
            {
                _qrzXmlUsername = request.QrzXmlUsername.Trim();
                _persistedSetup.QrzXmlUsername = _qrzXmlUsername;
                _runtimeOverrides.Remove(QrzXmlUsernameKey);
                RebuildLookupCoordinatorNoLock();
            }

            if (requestedQrzXmlPassword is not null)
            {
                _qrzXmlPassword = requestedQrzXmlPassword;
                _hasQrzXmlPassword = true;
                _runtimeOverrides.Remove(QrzXmlPasswordKey);
                RebuildLookupCoordinatorNoLock();
            }

            if (requestedQrzLogbookApiKey is not null)
            {
                _qrzLogbookApiKey = requestedQrzLogbookApiKey;
                _hasQrzLogbookApiKey = true;
                _runtimeOverrides.Remove(QrzLogbookApiKeyKey);
                if (_ownsSyncEngine)
                {
                    RebuildSyncEngineNoLock();
                }
            }

            if (request.SyncConfig is not null)
            {
                _syncConfig = NormalizeSyncConfig(request.SyncConfig);
                _persistedSetup.SyncConfig = _syncConfig.Clone();
            }

            if (request.RigControl is not null)
            {
                _rigControl = request.RigControl.Clone();
                _persistedSetup.RigControl = _rigControl.Clone();
            }

            if (request.WsjtxIngest is not null)
            {
                _persistedSetup.WsjtxIngest = normalizedWsjtxIngest;
                _persistedSetup.WsjtxIngestWriteOverride = normalizedWsjtxIngest?.Clone();
            }

            UpdatePersistedStorageSettingsNoLock(request);
            SyncPersistedProfilesNoLock();
            _runtimeOverrides.Remove(StorageBackendKey);

            _baseQrzXmlUsername = _qrzXmlUsername;
            _baseQrzXmlPassword = _qrzXmlPassword;
            _baseQrzLogbookApiKey = _qrzLogbookApiKey;
            _baseRigControl = _rigControl?.Clone();

            PersistNoLock();
            return new SaveSetupResponse
            {
                Status = BuildSetupStatusNoLock(),
            };
        }
    }

    public ListStationProfilesResponse ListStationProfiles()
    {
        lock (_gate)
        {
            var response = new ListStationProfilesResponse();
            response.Profiles.AddRange(BuildStationProfileRecordsNoLock());
            if (!string.IsNullOrWhiteSpace(_activeProfileId))
            {
                response.ActiveProfileId = _activeProfileId;
            }

            return response;
        }
    }

    public StationProfileRecord? GetStationProfile(string profileId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileId);

        lock (_gate)
        {
            var stored = _stationProfiles.FirstOrDefault(entry => string.Equals(entry.ProfileId, profileId.Trim(), StringComparison.Ordinal));
            if (stored is null)
            {
                return null;
            }

            return BuildStationProfileRecordNoLock(stored);
        }
    }

    public SaveStationProfileResponse SaveStationProfile(SaveStationProfileRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        lock (_gate)
        {
            var profile = request.Profile ?? throw new InvalidOperationException("profile is required.");
            var profileId = NormalizeProfileIdOrDefault(request.ProfileId, profile.ProfileName, profile.StationCallsign);
            var record = SaveStationProfileNoLock(profileId, profile, request.MakeActive);
            SyncPersistedProfilesNoLock();
            PersistNoLock();

            var response = new SaveStationProfileResponse
            {
                Profile = record,
            };
            if (!string.IsNullOrWhiteSpace(_activeProfileId))
            {
                response.ActiveProfileId = _activeProfileId;
            }

            return response;
        }
    }

    public StationProfileRecord? SetActiveStationProfile(string profileId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileId);

        lock (_gate)
        {
            var stored = _stationProfiles.FirstOrDefault(entry => string.Equals(entry.ProfileId, profileId.Trim(), StringComparison.Ordinal));
            if (stored is null)
            {
                return null;
            }

            _activeProfileId = stored.ProfileId;
            SyncPersistedProfilesNoLock();
            PersistNoLock();
            return BuildStationProfileRecordNoLock(stored);
        }
    }

    public StationProfileDeleteOutcome DeleteStationProfile(string profileId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profileId);

        lock (_gate)
        {
            if (string.Equals(_activeProfileId, profileId.Trim(), StringComparison.Ordinal))
            {
                return StationProfileDeleteOutcome.ActiveProfile;
            }

            var removed = _stationProfiles.RemoveAll(entry => string.Equals(entry.ProfileId, profileId.Trim(), StringComparison.Ordinal));
            if (removed > 0)
            {
                SyncPersistedProfilesNoLock();
                PersistNoLock();
                return StationProfileDeleteOutcome.Deleted;
            }

            return StationProfileDeleteOutcome.NotFound;
        }
    }

    public ActiveStationContext GetActiveStationContext()
    {
        lock (_gate)
        {
            return BuildActiveStationContextNoLock();
        }
    }

    public ActiveStationContext SetSessionStationProfileOverride(StationProfile profile)
    {
        ArgumentNullException.ThrowIfNull(profile);

        lock (_gate)
        {
            _sessionOverrideProfile = NormalizeStationProfile(profile);
            return BuildActiveStationContextNoLock();
        }
    }

    public ActiveStationContext ClearSessionStationProfileOverride()
    {
        lock (_gate)
        {
            _sessionOverrideProfile = null;
            return BuildActiveStationContextNoLock();
        }
    }

    public LogQsoResponse LogQso(LogQsoRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        lock (_gate)
        {
            var qso = request.Qso?.Clone() ?? throw new InvalidOperationException("qso is required.");
            if (GetEffectiveActiveProfileNoLock() is null)
            {
                throw new NoActiveStationProfileException();
            }

            // Identity, station context, timestamps, and QRZ state belong to
            // the engine. A LogQso caller cannot import or spoof them.
            qso.LocalId = Guid.NewGuid().ToString();
            qso.StationCallsign = string.Empty;
            qso.StationSnapshot = null;
            qso.SyncStatus = SyncStatus.LocalOnly;
            qso.ClearLotwSent();
            qso.ClearLotwReceived();
            qso.LotwSentStatus = QslStatus.Unspecified;
            qso.LotwReceivedStatus = QslStatus.Unspecified;
            qso.LotwSentDate = null;
            qso.LotwReceivedDate = null;
            qso.LotwSyncStatus = LotwSyncStatus.LocalOnly;
            qso.ClearQrzLogid();
            qso.ClearQrzBookid();
            qso.CreatedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
            qso.UpdatedAt = qso.CreatedAt.Clone();

            ApplyStationContextNoLock(qso);
            ManagedQsoParity.NormalizeQsoForPersistence(qso);
            ValidateQsoNoLock(qso);

            var response = new LogQsoResponse
            {
                LocalId = qso.LocalId,
            };

            Sync(_storage.Logbook.InsertQsoAsync(qso));
            ApplySyncFlagsNoLock(qso, request.SyncToQrz, response);
            if (response.SyncSuccess)
            {
                Sync(_storage.Logbook.UpdateQrzSyncMetadataAsync(qso.LocalId, qso.UpdatedAt, qso.QrzLogid));
            }
            ApplyLotwSyncFlagNoLock(qso, request.SyncToLotw, response);

            return response;
        }
    }

    public UpdateQsoResponse UpdateQso(UpdateQsoRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        lock (_gate)
        {
            var qso = request.Qso?.Clone() ?? throw new InvalidOperationException("qso is required.");
            var requestedLocalId = qso.LocalId?.Trim();
            if (string.IsNullOrWhiteSpace(requestedLocalId))
            {
                throw new ArgumentException("local_id is required.", nameof(request));
            }
            qso.LocalId = requestedLocalId;

            var existing = Sync(_storage.Logbook.GetQsoAsync(requestedLocalId));
            if (existing is null)
            {
                throw new KeyNotFoundException($"QSO '{requestedLocalId}' was not found.");
            }
            if (existing.DeletedAt is not null)
            {
                throw new QsoSoftDeletedException(qso.LocalId);
            }

            // UpdateQso is a full replacement of caller-owned fields. Preserve
            // only metadata whose source of truth is the engine.
            qso.LocalId = existing.LocalId;
            qso.StationCallsign = existing.StationCallsign;
            qso.StationSnapshot = existing.StationSnapshot?.Clone();
            qso.CreatedAt = existing.CreatedAt?.Clone();
            if (existing.HasQrzLogid)
            {
                qso.QrzLogid = existing.QrzLogid;
            }
            else
            {
                qso.ClearQrzLogid();
            }
            if (existing.HasQrzBookid)
            {
                qso.QrzBookid = existing.QrzBookid;
            }
            else
            {
                qso.ClearQrzBookid();
            }
            qso.DeletedAt = existing.DeletedAt?.Clone();
            qso.PendingRemoteDelete = existing.PendingRemoteDelete;
            if (existing.HasLotwSent)
            {
                qso.LotwSent = existing.LotwSent;
            }
            else
            {
                qso.ClearLotwSent();
            }
            if (existing.HasLotwReceived)
            {
                qso.LotwReceived = existing.LotwReceived;
            }
            else
            {
                qso.ClearLotwReceived();
            }
            qso.LotwSentStatus = existing.LotwSentStatus;
            qso.LotwReceivedStatus = existing.LotwReceivedStatus;
            qso.LotwSentDate = existing.LotwSentDate?.Clone();
            qso.LotwReceivedDate = existing.LotwReceivedDate?.Clone();
            qso.LotwSyncStatus = existing.LotwSyncStatus is LotwSyncStatus.Uploaded or LotwSyncStatus.Confirmed
                ? LotwSyncStatus.Modified
                : existing.LotwSyncStatus;
            qso.SyncStatus = existing.SyncStatus == SyncStatus.Synced
                ? SyncStatus.Modified
                : existing.SyncStatus;

            ManagedQsoParity.NormalizeQsoForPersistence(qso);
            ValidateQsoNoLock(qso);
            FinalizeQsoForWrite(qso, isNew: false);

            var response = new UpdateQsoResponse { Success = true };
            Sync(_storage.Logbook.UpdateQsoAsync(qso));
            ApplySyncFlagsNoLock(qso, request.SyncToQrz, response);
            if (response.SyncSuccess)
            {
                Sync(_storage.Logbook.UpdateQrzSyncMetadataAsync(qso.LocalId, qso.UpdatedAt, qso.QrzLogid));
            }
            ApplyLotwSyncFlagNoLock(qso, request.SyncToLotw, response);

            return response;
        }
    }

    public DeleteQsoOutcome DeleteQso(string localId, bool queueRemoteDelete)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localId);

        lock (_gate)
        {
            var trimmed = localId.Trim();
            var existing = Sync(_storage.Logbook.GetQsoAsync(trimmed));
            if (existing is null)
            {
                return new DeleteQsoOutcome(Found: false, RemoteDeleteQueued: false, MissingQrzLogid: false);
            }

            var hasLogid = !string.IsNullOrWhiteSpace(existing.QrzLogid);
            var pending = queueRemoteDelete && hasLogid;

            // Idempotent: re-deleting an already soft-deleted row is a no-op
            // success that may upgrade pending_remote_delete if asked.
            Sync(_storage.Logbook.SoftDeleteQsoAsync(trimmed, DateTimeOffset.UtcNow, pending));
            return new DeleteQsoOutcome(
                Found: true,
                RemoteDeleteQueued: pending,
                MissingQrzLogid: queueRemoteDelete && !hasLogid);
        }
    }

    public RestoreQsoOutcome RestoreQso(string localId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localId);

        lock (_gate)
        {
            var trimmed = localId.Trim();
            var existing = Sync(_storage.Logbook.GetQsoAsync(trimmed));
            if (existing is null)
            {
                return new RestoreQsoOutcome(Found: false, Restored: null);
            }

            // Track whether we need to demote sync_status post-restore so the
            // next sync re-uploads a row that has no remote logid yet.
            var demoteToLocalOnly = existing.DeletedAt is not null
                && string.IsNullOrWhiteSpace(existing.QrzLogid)
                && existing.SyncStatus == SyncStatus.Synced;

            Sync(_storage.Logbook.RestoreQsoAsync(trimmed));

            if (demoteToLocalOnly)
            {
                var afterRestore = Sync(_storage.Logbook.GetQsoAsync(trimmed));
                if (afterRestore is not null
                    && string.IsNullOrWhiteSpace(afterRestore.QrzLogid)
                    && afterRestore.SyncStatus == SyncStatus.Synced)
                {
                    afterRestore.SyncStatus = SyncStatus.LocalOnly;
                    afterRestore.UpdatedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
                    Sync(_storage.Logbook.UpdateQsoAsync(afterRestore));
                }
            }

            var restored = Sync(_storage.Logbook.GetQsoAsync(trimmed));
            return new RestoreQsoOutcome(Found: true, Restored: restored);
        }
    }

    public PurgeDeletedQsosOutcome PurgeDeletedQsos(
        IReadOnlyList<string>? localIds,
        DateTimeOffset? olderThan,
        bool includePendingRemoteDeletes)
    {
        lock (_gate)
        {
            if (!includePendingRemoteDeletes)
            {
                var purged = Sync(_storage.Logbook.PurgeDeletedQsosAsync(localIds, olderThan));
                return new PurgeDeletedQsosOutcome(purged, 0, 0, null);
            }

            var requestedIds = localIds is { Count: > 0 }
                ? new HashSet<string>(localIds.Select(static value => value.Trim()), StringComparer.Ordinal)
                : null;
            var candidates = Sync(_storage.Logbook.ListQsosAsync(new QsoListQuery
            {
                DeletedFilter = Storage.DeletedRecordsFilter.DeletedOnly,
            }))
            .Where(qso => requestedIds is null || requestedIds.Contains(qso.LocalId))
            .Where(qso => olderThan is null || qso.DeletedAt?.ToDateTimeOffset() <= olderThan)
            .ToArray();

            var purgeIds = new List<string>(candidates.Length);
            var errors = new List<string>();
            var pushed = 0;
            var failed = 0;

            foreach (var candidate in candidates)
            {
                if (!candidate.PendingRemoteDelete || string.IsNullOrWhiteSpace(candidate.QrzLogid))
                {
                    purgeIds.Add(candidate.LocalId);
                    continue;
                }

                if (!_hasQrzLogbookApiKey || _syncEngine is null)
                {
                    failed++;
                    errors.Add($"QRZ delete was not attempted for QSO '{candidate.LocalId}' because QRZ Logbook is not configured.");
                    continue;
                }

                try
                {
                    Sync(_syncEngine.DeleteRemoteQsoAsync(candidate.QrzLogid));
                    pushed++;
                    purgeIds.Add(candidate.LocalId);
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    failed++;
                    errors.Add($"QRZ delete failed for QSO '{candidate.LocalId}': {ex.Message}");
                }
            }

            var purgedCount = purgeIds.Count == 0
                ? 0
                : Sync(_storage.Logbook.PurgeDeletedQsosAsync(purgeIds, olderThan));
            return new PurgeDeletedQsosOutcome(
                purgedCount,
                pushed,
                failed,
                errors.Count == 0 ? null : string.Join(" ", errors));
        }
    }

    public QsoRecord? GetQso(string localId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(localId);

        lock (_gate)
        {
            return Sync(_storage.Logbook.GetQsoAsync(localId.Trim()));
        }
    }

    public IReadOnlyList<QsoRecord> ListQsos(ListQsosRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (request.HasBandFilter && !System.Enum.IsDefined(request.BandFilter))
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Invalid band_filter value.");
        }
        if (request.HasModeFilter && !System.Enum.IsDefined(request.ModeFilter))
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Invalid mode_filter value.");
        }
        if (!System.Enum.IsDefined(request.Sort))
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Invalid sort order.");
        }
        if (!System.Enum.IsDefined(request.DeletedFilter))
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Invalid deleted_filter value.");
        }

        lock (_gate)
        {
            var storageQuery = new QsoListQuery
            {
                After = request.After is not null ? request.After.ToDateTimeOffset() : null,
                Before = request.Before is not null ? request.Before.ToDateTimeOffset() : null,
                CallsignFilter = NormalizeOptional(request.CallsignFilter),
                BandFilter = request.HasBandFilter ? request.BandFilter : null,
                ModeFilter = request.HasModeFilter ? request.ModeFilter : null,
                ContestId = NormalizeOptional(request.ContestId),
                Offset = request.Offset > 0 ? (int)request.Offset : 0,
                Limit = request.Limit > 0 ? (int)request.Limit : null,
                Sort = request.Sort == QsoRipper.Services.QsoSortOrder.OldestFirst
                    ? Storage.QsoSortOrder.OldestFirst
                    : Storage.QsoSortOrder.NewestFirst,
                DeletedFilter = request.DeletedFilter switch
                {
                    QsoRipper.Services.DeletedRecordsFilter.DeletedOnly => Storage.DeletedRecordsFilter.DeletedOnly,
                    QsoRipper.Services.DeletedRecordsFilter.All => Storage.DeletedRecordsFilter.All,
                    _ => Storage.DeletedRecordsFilter.ActiveOnly,
                },
            };

            return Sync(_storage.Logbook.ListQsosAsync(storageQuery));
        }
    }

    public SyncWithQrzResponse SyncWithQrz(bool fullSync = false)
    {
        QrzSyncEngine syncEngine;
        ConflictPolicy conflictPolicy;
        lock (_gate)
        {
            if (_syncEngine is null)
            {
                throw new QrzSyncUnavailableException("QRZ logbook is not configured.");
            }
            if (_isSyncing)
            {
                throw new QrzSyncUnavailableException("A QRZ sync is already in progress.");
            }

            _isSyncing = true;
            _lastSyncError = null;
            syncEngine = _syncEngine;
            conflictPolicy = _syncConfig.ConflictPolicy;
        }

        try
        {
            var result = Sync(syncEngine.ExecuteSyncAsync(_storage.Logbook, fullSync, conflictPolicy));

            var syncResponse = new SyncWithQrzResponse
            {
                DownloadedRecords = result.DownloadedCount,
                UploadedRecords = result.UploadedCount,
                ConflictRecords = result.ConflictCount,
                TotalRecords = result.DownloadedCount + result.UploadedCount,
                ProcessedRecords = result.DownloadedCount + result.UploadedCount,
                CurrentAction = "Sync completed.",
                Complete = true,
                RemoteDeletesPushed = result.RemoteDeletesPushed,
                DeletesSkippedRemote = result.DeletesSkippedRemote,
                DuplicateReplaces = result.DuplicateReplaceCount,
            };

            if (result.ErrorSummary is not null)
            {
                syncResponse.Error = result.ErrorSummary;
                lock (_gate)
                {
                    _lastSyncError = result.ErrorSummary;
                }
            }

            return syncResponse;
        }
#pragma warning disable CA1031 // Do not catch general exception types — sync must not crash the engine
        catch (Exception ex)
#pragma warning restore CA1031
        {
            lock (_gate)
            {
                _lastSyncError = ex.Message;
            }

            return new SyncWithQrzResponse
            {
                Complete = true,
                Error = ex.Message,
            };
        }
        finally
        {
            lock (_gate)
            {
                _isSyncing = false;
            }
        }
    }

    public void EnsureQrzSyncAvailable()
    {
        lock (_gate)
        {
            if (_syncEngine is null)
            {
                throw new QrzSyncUnavailableException("QRZ logbook is not configured.");
            }
            if (_isSyncing)
            {
                throw new QrzSyncUnavailableException("A QRZ sync is already in progress.");
            }
        }
    }

    public void EnsureLotwSyncAvailable()
    {
        lock (_gate)
        {
            if (_isLotwSyncing)
            {
                throw new LotwConfigurationException("A LoTW sync is already in progress.");
            }

            var (_, ownedClient) = CreateLotwSyncEngineNoLock();
            ownedClient?.Dispose();
        }
    }

    public SyncWithLotwResponse SyncWithLotw(SyncWithLotwRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);

        LotwSyncEngine engine;
        IDisposable? ownedClient;
        lock (_gate)
        {
            if (_isLotwSyncing)
            {
                throw new LotwConfigurationException("A LoTW sync is already in progress.");
            }
            (engine, ownedClient) = CreateLotwSyncEngineNoLock();
            _isLotwSyncing = true;
        }

        try
        {
            var result = Sync(engine.SyncAsync(
                request.FullSync,
                request.HasUpload ? request.Upload : true,
                request.HasDownload ? request.Download : true,
                cancellationToken));
            return new SyncWithLotwResponse
            {
                TotalRecords = ToUInt32(result.TotalRecords),
                ProcessedRecords = ToUInt32(result.ProcessedRecords),
                UploadedRecords = ToUInt32(result.UploadedRecords),
                ConfirmedRecords = ToUInt32(result.ConfirmedRecords),
                UnmatchedRecords = ToUInt32(result.UnmatchedRecords),
                ConflictRecords = ToUInt32(result.ConflictRecords),
                ErrorRecords = ToUInt32(result.ErrorRecords),
                ConfirmationHighWater = result.ConfirmationHighWater ?? string.Empty,
                CurrentAction = "LoTW sync completed.",
                Complete = true,
                Error = result.ErrorSummary ?? string.Empty,
            };
        }
        finally
        {
            ownedClient?.Dispose();
            lock (_gate)
            {
                _isLotwSyncing = false;
            }
        }
    }

    public void SetNextAutomaticSync(DateTimeOffset? nextSync)
    {
        lock (_gate)
        {
            _nextSync = nextSync;
        }
    }

    public (bool Enabled, TimeSpan Interval) GetAutomaticSyncSettings()
    {
        lock (_gate)
        {
            return (
                _syncConfig.AutoSyncEnabled && _hasQrzLogbookApiKey,
                TimeSpan.FromSeconds(Math.Max(1, _syncConfig.SyncIntervalSeconds)));
        }
    }

    public GetSyncStatusResponse GetSyncStatus()
    {
        lock (_gate)
        {
            var counts = Sync(_storage.Logbook.GetCountsAsync());
            var syncMeta = Sync(_storage.Logbook.GetSyncMetadataAsync());

            var response = new GetSyncStatusResponse
            {
                LocalQsoCount = (uint)counts.LocalQsoCount,
                QrzQsoCount = (uint)Math.Max(0, syncMeta.QrzQsoCount),
                PendingUpload = (uint)counts.PendingUploadCount,
                IsSyncing = _isSyncing,
                AutoSyncEnabled = _syncConfig.AutoSyncEnabled && _hasQrzLogbookApiKey,
            };

            if (syncMeta.LastSync is { } lastSyncUtc)
            {
                response.LastSync = Timestamp.FromDateTimeOffset(lastSyncUtc);
            }

            if (!string.IsNullOrWhiteSpace(syncMeta.QrzLogbookOwner))
            {
                response.QrzLogbookOwner = syncMeta.QrzLogbookOwner;
            }

            if (_nextSync is { } nextSync)
            {
                response.NextSync = Timestamp.FromDateTimeOffset(nextSync);
            }

            if (!string.IsNullOrWhiteSpace(_lastSyncError))
            {
                response.LastSyncError = _lastSyncError;
            }

            return response;
        }
    }

    public ImportAdifResponse ImportAdif(byte[] adifBytes, bool refresh)
    {
        return ImportAdifDetailed(adifBytes, refresh).Response;
    }

    /// <summary>
    /// Imports ADIF bytes and reports the QSOs that were inserted or refreshed, so callers
    /// (such as the WSJT-X ingest supervisor) can drive per-import QRZ sync and live status.
    /// </summary>
    public WsjtxImportDetail ImportAdifDetailed(
        byte[] adifBytes,
        bool refresh,
        string? wsjtxImportSource = null)
    {
        ArgumentNullException.ThrowIfNull(adifBytes);

        var qsos = ManagedAdifCodec.ParseAdiQsos(adifBytes);
        var importedAt = DateTimeOffset.UtcNow;
        lock (_gate)
        {
            var response = new ImportAdifResponse();
            var affected = new List<WsjtxImportedQsoRef>();
            var activeStationProfile = GetEffectiveActiveProfileNoLock();
            var allExisting = Sync(_storage.Logbook.ListQsosAsync(new QsoListQuery())).ToList();

            for (var index = 0; index < qsos.Count; index++)
            {
                var recordNumber = index + 1;
                var qso = qsos[index].Clone();
                var hadImportedStationContext = ManagedQsoParity.QsoHasStationContext(qso);

                if (hadImportedStationContext)
                {
                    ManagedQsoParity.MaterializeStationSnapshotForCreate(qso, null);
                }
                else if (activeStationProfile is not null)
                {
                    ManagedQsoParity.MaterializeStationSnapshotForCreate(qso, activeStationProfile);
                    response.Warnings.Add(
                        $"Record {recordNumber}: local-station history was absent in ADIF; applied active station profile '{ManagedQsoParity.StationProfileLabel(activeStationProfile)}'.");
                }
                else
                {
                    response.RecordsSkipped++;
                    response.Warnings.Add(
                        $"Record {recordNumber}: local-station history was absent in ADIF and no active station profile is configured; skipped.");
                    continue;
                }

                ManagedQsoParity.NormalizeQsoForPersistence(qso);
                if (ManagedQsoParity.InvalidImportReason(qso) is { } reason)
                {
                    response.RecordsSkipped++;
                    response.Warnings.Add($"Record {recordNumber}: {reason} Skipped.");
                    continue;
                }

                var existingMatch = allExisting.FindIndex(existing => ManagedQsoParity.QsosMatchForDuplicate(existing, qso));
                if (existingMatch >= 0)
                {
                    if (refresh)
                    {
                        if (!string.IsNullOrWhiteSpace(wsjtxImportSource))
                        {
                            var current = WsjtxImportDiagnostic.Create(wsjtxImportSource, qso, importedAt);
                            var diagnostic = WsjtxImportDiagnostic.Resolve(
                                allExisting[existingMatch],
                                current);
                            WsjtxImportDiagnostic.Apply(qso, diagnostic);
                        }

                        var merged = ManagedQsoParity.MergeQsoForRefresh(allExisting[existingMatch], qso);
                        Sync(_storage.Logbook.UpdateQsoAsync(merged));
                        allExisting[existingMatch] = merged;
                        response.RecordsUpdated++;
                        affected.Add(new WsjtxImportedQsoRef(merged.LocalId, merged.WorkedCallsign));
                        response.Warnings.Add($"Record {recordNumber}: refreshed existing record '{merged.LocalId}'.");
                    }
                    else
                    {
                        response.RecordsSkipped++;
                        response.Warnings.Add(
                            $"Record {recordNumber}: duplicate skipped; matched an existing QSO on station_callsign, worked_callsign, compatible utc_timestamp, band, mode, and compatible submode/frequency.");
                    }

                    continue;
                }

                if (!string.IsNullOrWhiteSpace(wsjtxImportSource))
                {
                    var diagnostic = WsjtxImportDiagnostic.Create(wsjtxImportSource, qso, importedAt);
                    WsjtxImportDiagnostic.Apply(qso, diagnostic);
                }

                ValidateQsoNoLock(qso);
                FinalizeQsoForWrite(qso, isNew: true);
                qso.SyncStatus = SyncStatus.LocalOnly;
                Sync(_storage.Logbook.InsertQsoAsync(qso));
                allExisting.Add(qso);
                affected.Add(new WsjtxImportedQsoRef(qso.LocalId, qso.WorkedCallsign));
                response.RecordsImported++;
            }

            return new WsjtxImportDetail(response, affected);
        }
    }

    /// <summary>
    /// Publishes the latest live WSJT-X ingest diagnostics so a subsequent GetSetupStatus call
    /// surfaces them in SetupStatus.wsjtx_ingest_status. The supervisor owns the snapshot lifecycle.
    /// </summary>
    public void SetWsjtxIngestLiveStatus(WsjtxIngestStatus status)
    {
        ArgumentNullException.ThrowIfNull(status);
        Volatile.Write(ref _wsjtxIngestLiveStatus, status.Clone());
    }

    /// <summary>
    /// Returns a normalized snapshot of the effective WSJT-X ingest settings, applying the same
    /// defaults as the Rust runtime (enabled=false, udp_enabled=true, udp_bind=127.0.0.1:2237,
    /// adif_tail_enabled=false, poll_interval_ms=1000, sync_to_qrz=false). The supervisor polls this
    /// every loop iteration so configuration changes from SaveSetup take effect live.
    /// </summary>
    public WsjtxIngestSettings GetWsjtxIngestSettingsSnapshot()
    {
        lock (_gate)
        {
            if (_persistedSetup.WsjtxIngest is { } persisted)
            {
                return NormalizeWsjtxIngestSnapshot(persisted);
            }

            var defaults = new WsjtxIngestSettings
            {
                Enabled = false,
                UdpEnabled = true,
                UdpBind = "127.0.0.1:2237",
                AdifTailEnabled = false,
                PollIntervalMs = 1000,
                SyncToQrz = false,
            };
            return defaults;
        }
    }

    /// <summary>
    /// Uploads the supplied imported QSOs through the same QRZ API path used by
    /// LogQso(sync_to_qrz=true), then persists the QRZ-assigned logid.
    /// </summary>
    public IReadOnlyList<WsjtxQrzSyncOutcome> SyncImportedQsosToQrz(IEnumerable<string> localIds)
    {
        ArgumentNullException.ThrowIfNull(localIds);

        var outcomes = new List<WsjtxQrzSyncOutcome>();
        lock (_gate)
        {
            foreach (var rawLocalId in localIds)
            {
                var localId = rawLocalId?.Trim();
                if (string.IsNullOrWhiteSpace(localId))
                {
                    continue;
                }

                var existing = Sync(_storage.Logbook.GetQsoAsync(localId));
                if (existing is null)
                {
                    outcomes.Add(new WsjtxQrzSyncOutcome(localId, string.Empty, Success: false, $"QSO '{localId}' was not found."));
                    continue;
                }

                if (!_hasQrzLogbookApiKey)
                {
                    outcomes.Add(new WsjtxQrzSyncOutcome(localId, existing.WorkedCallsign, Success: false, "QRZ logbook is not configured."));
                    continue;
                }

                try
                {
                    var logid = Sync((_syncEngine ?? throw new InvalidOperationException("QRZ logbook sync is unavailable."))
                        .UploadSingleQsoAsync(_storage.Logbook, existing));
                    Sync(_storage.Logbook.UpdateQrzSyncMetadataAsync(existing.LocalId, existing.UpdatedAt, logid));
                    outcomes.Add(new WsjtxQrzSyncOutcome(localId, existing.WorkedCallsign, Success: true, Error: null));
                }
                catch (Exception ex) when (ex is not OutOfMemoryException)
                {
                    outcomes.Add(new WsjtxQrzSyncOutcome(localId, existing.WorkedCallsign, Success: false, ex.Message));
                }
            }
        }

        return outcomes;
    }

    public byte[] ExportAdif(ExportAdifRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        lock (_gate)
        {
            var storageQuery = new QsoListQuery
            {
                After = request.After is not null ? request.After.ToDateTimeOffset() : null,
                Before = request.Before is not null ? request.Before.ToDateTimeOffset() : null,
                ContestId = NormalizeOptional(request.ContestId),
                Sort = Storage.QsoSortOrder.OldestFirst,
            };

            var qsos = Sync(_storage.Logbook.ListQsosAsync(storageQuery));
            return ManagedAdifCodec.SerializeAdiQsos(qsos, request.IncludeHeader);
        }
    }

    public LookupResponse Lookup(string callsign, bool cacheOnly = false, bool skipCache = false)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(callsign);

        ILookupCoordinator coordinator;
        lock (_gate)
        {
            coordinator = _lookupCoordinator;
        }

        if (cacheOnly)
        {
            var cached = coordinator.GetCachedAsync(callsign).GetAwaiter().GetResult();
            return new LookupResponse { Result = cached };
        }

        var result = coordinator.LookupAsync(callsign, skipCache).GetAwaiter().GetResult();

        return new LookupResponse { Result = result };
    }

    public async IAsyncEnumerable<StreamLookupResponse> StreamLookup(
        string callsign,
        bool skipCache,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(callsign);

        ILookupCoordinator coordinator;
        lock (_gate)
        {
            coordinator = _lookupCoordinator;
        }

        await foreach (var result in coordinator
            .StreamLookupIncrementallyAsync(callsign, skipCache, cancellationToken)
            .ConfigureAwait(false))
        {
            yield return new StreamLookupResponse { Result = result };
        }
    }

    public async Task<LookupResult[]> BatchLookupAsync(
        IReadOnlyList<string> callsigns,
        CancellationToken cancellationToken)
    {
        ILookupCoordinator coordinator;
        lock (_gate)
        {
            coordinator = _lookupCoordinator;
        }

        return await BatchLookupOrchestrator.ExecuteAsync(
            coordinator,
            callsigns,
            ct: cancellationToken).ConfigureAwait(false);
    }

    public GetRigStatusResponse CreateRigStatusResponse()
    {
        lock (_gate)
        {
            if (_rigControl is not null)
            {
                if (!_rigControl.Enabled)
                {
                    return new GetRigStatusResponse
                    {
                        Status = RigConnectionStatus.Disabled,
                        ErrorMessage = "Rig control is disabled in the managed engine.",
                    };
                }

                var configuredSnapshot = BuildConfiguredRigSnapshotNoLock(_rigControl);
                return BuildRigStatusResponse(configuredSnapshot, _rigControl);
            }
        }

        if (_rigControlMonitor is null)
        {
            return new GetRigStatusResponse
            {
                Status = RigConnectionStatus.Disabled,
                ErrorMessage = "Rig control is disabled in the managed engine.",
            };
        }

        var snapshot = _rigControlMonitor.CurrentSnapshot();
        return BuildRigStatusResponse(snapshot, null);
    }

    public RigSnapshot BuildRigSnapshot()
    {
        lock (_gate)
        {
            if (_rigControl is not null)
            {
                if (!_rigControl.Enabled)
                {
                    return BuildRigSnapshot(
                        new GetRigStatusResponse
                        {
                            Status = RigConnectionStatus.Disabled,
                            ErrorMessage = "Rig control is disabled in the managed engine.",
                        });
                }

                return BuildConfiguredRigSnapshotNoLock(_rigControl);
            }
        }

        if (_rigControlMonitor is not null)
        {
            return _rigControlMonitor.CurrentSnapshot();
        }

        return BuildRigSnapshot(CreateRigStatusResponse());
    }

    public TestRigConnectionResponse TestRigConnection(TestRigConnectionRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        RigControlSettings? configured;
        lock (_gate)
        {
            configured = _rigControl?.Clone();
        }

        if (!request.HasHost && !request.HasPort && configured is null && _rigControlMonitor is not null)
        {
            var refreshed = _rigControlMonitor.RefreshSnapshot();
            return BuildTestRigConnectionResponse(refreshed);
        }

        var host = request.HasHost && !string.IsNullOrWhiteSpace(request.Host)
            ? request.Host.Trim()
            : configured is { HasHost: true } && !string.IsNullOrWhiteSpace(configured.Host)
                ? configured.Host.Trim()
                : RigctldProvider.DefaultHost;
        var requestedPort = request.HasPort
            ? request.Port
            : configured is { HasPort: true }
                ? configured.Port
                : (uint)RigctldProvider.DefaultPort;
        if (requestedPort is 0 or > 65535)
        {
            throw new ArgumentOutOfRangeException(nameof(request), "Rig control port must be between 1 and 65535.");
        }
        var port = (int)requestedPort;

        var readTimeoutMs = configured is { HasReadTimeoutMs: true } && configured.ReadTimeoutMs > 0
            ? checked((int)configured.ReadTimeoutMs)
            : RigctldProvider.DefaultReadTimeoutMs;
        var monitor = new RigControlMonitor(
            new RigctldProvider(host, port, TimeSpan.FromMilliseconds(readTimeoutMs)),
            TimeSpan.FromMilliseconds(RigControlMonitor.DefaultStaleThresholdMs));
        return BuildTestRigConnectionResponse(monitor.RefreshSnapshot());
    }

    private static RigSnapshot BuildRigSnapshot(GetRigStatusResponse status)
    {
        var snapshot = new RigSnapshot
        {
            FrequencyHz = status.Status == RigConnectionStatus.Connected ? 14_074_000UL : 0UL,
            Band = status.Status == RigConnectionStatus.Connected ? Band._20M : Band.Unspecified,
            Mode = status.Status == RigConnectionStatus.Connected ? Mode.Ft8 : Mode.Unspecified,
            Status = status.Status,
            SampledAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow),
        };

        if (status.HasErrorMessage)
        {
            snapshot.ErrorMessage = status.ErrorMessage;
        }

        return snapshot;
    }

    private static RigSnapshot BuildConfiguredRigSnapshotNoLock(RigControlSettings settings)
    {
        var host = settings.HasHost && !string.IsNullOrWhiteSpace(settings.Host)
            ? settings.Host
            : RigctldProvider.DefaultHost;
        var port = settings.HasPort && settings.Port > 0
            ? (int)settings.Port
            : RigctldProvider.DefaultPort;
        var readTimeoutMs = settings.HasReadTimeoutMs && settings.ReadTimeoutMs > 0
            ? (int)settings.ReadTimeoutMs
            : RigctldProvider.DefaultReadTimeoutMs;
        var staleThresholdMs = settings.HasStaleThresholdMs && settings.StaleThresholdMs > 0
            ? (int)settings.StaleThresholdMs
            : RigControlMonitor.DefaultStaleThresholdMs;
        var monitor = new RigControlMonitor(
            new RigctldProvider(host, port, TimeSpan.FromMilliseconds(readTimeoutMs)),
            TimeSpan.FromMilliseconds(staleThresholdMs));

        return monitor.CurrentSnapshot();
    }

    private static GetRigStatusResponse BuildRigStatusResponse(RigSnapshot snapshot, RigControlSettings? configuredSettings)
    {
        var result = new GetRigStatusResponse { Status = snapshot.Status };
        if (snapshot.HasErrorMessage)
        {
            result.ErrorMessage = snapshot.ErrorMessage;
        }

        if (configuredSettings is not null && configuredSettings.HasHost && configuredSettings.HasPort)
        {
            result.Endpoint = $"{configuredSettings.Host}:{configuredSettings.Port}";
        }

        return result;
    }

    private static TestRigConnectionResponse BuildTestRigConnectionResponse(RigSnapshot refreshed)
    {
        var response = new TestRigConnectionResponse
        {
            Success = refreshed.Status == RigConnectionStatus.Connected,
        };

        if (refreshed.HasErrorMessage)
        {
            response.ErrorMessage = refreshed.ErrorMessage;
        }

        if (refreshed.Status == RigConnectionStatus.Connected)
        {
            response.Snapshot = refreshed;
        }

        return response;
    }

    public SpaceWeatherSnapshot BuildSpaceWeatherSnapshot(bool refreshed)
    {
        if (_spaceWeatherMonitor is null)
        {
            return new SpaceWeatherSnapshot
            {
                Status = SpaceWeatherStatus.Error,
                ErrorMessage = "Space weather not configured",
                SourceName = "NOAA SWPC",
            };
        }

        return refreshed
            ? _spaceWeatherMonitor.RefreshSnapshot()
            : _spaceWeatherMonitor.CurrentSnapshot();
    }

    public ContestCalendarSnapshot BuildContestCalendarSnapshot(bool refreshed)
    {
        if (_contestCalendarMonitor is null)
        {
            return new ContestCalendarSnapshot
            {
                Status = ContestCalendarStatus.Disabled,
                ErrorMessage = "Contest calendar not configured",
            };
        }

        return refreshed
            ? _contestCalendarMonitor.RefreshSnapshot()
            : _contestCalendarMonitor.CurrentSnapshot();
    }

    public RuntimeConfigSnapshot GetRuntimeConfigSnapshot()
    {
        lock (_gate)
        {
            return BuildRuntimeConfigSnapshotNoLock();
        }
    }

    public RuntimeConfigSnapshot ApplyRuntimeConfig(IReadOnlyList<RuntimeConfigMutation> mutations)
    {
        ArgumentNullException.ThrowIfNull(mutations);

        lock (_gate)
        {
            foreach (var mutation in mutations)
            {
                switch (mutation.Key)
                {
                    case QrzXmlUsernameKey:
                        ApplyStringOverrideNoLock(QrzXmlUsernameKey, mutation, value => _qrzXmlUsername = NormalizeOptional(value));
                        RebuildLookupCoordinatorNoLock();
                        break;
                    case QrzXmlPasswordKey:
                        if (mutation.Kind == RuntimeConfigMutationKind.Clear)
                        {
                            _qrzXmlPassword = null;
                            _hasQrzXmlPassword = false;
                            _runtimeOverrides[QrzXmlPasswordKey] = string.Empty;
                            RebuildLookupCoordinatorNoLock();
                        }
                        else
                        {
                            var newPassword = string.IsNullOrWhiteSpace(mutation.Value) ? null : mutation.Value.Trim();
                            if (newPassword is not null && newPassword != "***")
                            {
                                _qrzXmlPassword = newPassword;
                                RebuildLookupCoordinatorNoLock();
                            }

                            _hasQrzXmlPassword = _qrzXmlPassword is not null;
                            _runtimeOverrides[QrzXmlPasswordKey] = "***";
                        }

                        break;
                    case QrzLogbookApiKeyKey:
                        if (mutation.Kind == RuntimeConfigMutationKind.Clear)
                        {
                            _qrzLogbookApiKey = null;
                            _hasQrzLogbookApiKey = false;
                            _runtimeOverrides[QrzLogbookApiKeyKey] = string.Empty;
                        }
                        else
                        {
                            _qrzLogbookApiKey = string.IsNullOrWhiteSpace(mutation.Value) ? null : mutation.Value.Trim();
                            _hasQrzLogbookApiKey = _qrzLogbookApiKey is not null;
                            _runtimeOverrides[QrzLogbookApiKeyKey] = _hasQrzLogbookApiKey ? "***" : string.Empty;
                        }

                        if (_ownsSyncEngine)
                        {
                            RebuildSyncEngineNoLock();
                        }
                        break;
                    case RigEnabledKey:
                        if (mutation.Kind == RuntimeConfigMutationKind.Clear)
                        {
                            _rigControl ??= new RigControlSettings();
                            _rigControl.Enabled = false;
                            _runtimeOverrides[RigEnabledKey] = string.Empty;
                        }
                        else if (bool.TryParse(mutation.Value, out var enabled))
                        {
                            _rigControl ??= new RigControlSettings();
                            _rigControl.Enabled = enabled;
                            _runtimeOverrides[RigEnabledKey] = enabled ? "TRUE" : "FALSE";
                        }
                        else
                        {
                            throw new InvalidOperationException("QSORIPPER_RIGCTLD_ENABLED expects true or false.");
                        }

                        break;
                    default:
                        throw new InvalidOperationException($"Unsupported runtime config key: {mutation.Key}");
                }
            }

            return BuildRuntimeConfigSnapshotNoLock();
        }
    }

    public RuntimeConfigSnapshot ResetRuntimeConfig(IReadOnlyList<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);

        lock (_gate)
        {
            var normalizedKeys = keys.Where(key => !string.IsNullOrWhiteSpace(key)).ToArray();
            if (normalizedKeys.Length == 0)
            {
                _runtimeOverrides.Clear();
                _qrzXmlUsername = _baseQrzXmlUsername;
                _qrzXmlPassword = _baseQrzXmlPassword;
                _hasQrzXmlPassword = _qrzXmlPassword is not null;
                _qrzLogbookApiKey = _baseQrzLogbookApiKey;
                _hasQrzLogbookApiKey = _qrzLogbookApiKey is not null;
                _rigControl = _baseRigControl?.Clone();
                RebuildLookupCoordinatorNoLock();
                if (_ownsSyncEngine)
                {
                    RebuildSyncEngineNoLock();
                }
            }
            else
            {
                foreach (var key in normalizedKeys)
                {
                    switch (key)
                    {
                        case QrzXmlUsernameKey:
                            _qrzXmlUsername = _baseQrzXmlUsername;
                            _runtimeOverrides.Remove(QrzXmlUsernameKey);
                            RebuildLookupCoordinatorNoLock();
                            break;
                        case QrzXmlPasswordKey:
                            _qrzXmlPassword = _baseQrzXmlPassword;
                            _hasQrzXmlPassword = _qrzXmlPassword is not null;
                            _runtimeOverrides.Remove(QrzXmlPasswordKey);
                            RebuildLookupCoordinatorNoLock();
                            break;
                        case QrzLogbookApiKeyKey:
                            _qrzLogbookApiKey = _baseQrzLogbookApiKey;
                            _hasQrzLogbookApiKey = _qrzLogbookApiKey is not null;
                            _runtimeOverrides.Remove(QrzLogbookApiKeyKey);
                            if (_ownsSyncEngine)
                            {
                                RebuildSyncEngineNoLock();
                            }
                            break;
                        case RigEnabledKey:
                            _rigControl = _baseRigControl?.Clone();
                            _runtimeOverrides.Remove(RigEnabledKey);
                            break;
                        default:
                            throw new InvalidOperationException($"Unsupported runtime config key: {key}");
                    }
                }
            }

            return BuildRuntimeConfigSnapshotNoLock();
        }
    }

    private void PersistNoLock()
    {
        SharedSetupConfigPersistence.Save(_configPath, _persistedSetup);

        // Clear the one-shot override after writing.
        _persistedSetup.WsjtxIngestWriteOverride = null;
    }

    private SetupStatus BuildSetupStatusNoLock()
    {
        var isSqlite = IsSqliteBackendNoLock();
        var persistedProfile = GetPersistedActiveProfileNoLock() ?? new StationProfile();
        var persistedPath = NormalizeOptional(_persistedSetup.GetPersistedLogFilePath())
            ?? (isSqlite ? NormalizeOptional(_currentPersistenceLocation) : null);
        var status = new SetupStatus
        {
            ConfigFileExists = File.Exists(_configPath),
            SetupComplete = IsSetupCompleteNoLock(),
            ConfigPath = _configPath,
            HasStationProfile = _stationProfiles.Count > 0,
            StationProfile = persistedProfile,
            StationProfileCount = (uint)_stationProfiles.Count,
            IsFirstRun = !File.Exists(_configPath),
            HasQrzXmlPassword = _hasQrzXmlPassword,
            HasQrzLogbookApiKey = _hasQrzLogbookApiKey,
            PersistenceDescription = isSqlite ? PersistenceStepDescriptionSqlite : PersistenceStepDescription,
            PersistenceLabel = PersistenceStepLabel,
            PersistenceContractExplicit = true,
            SyncConfig = _persistedSetup.SyncConfig.Clone(),
        };
#pragma warning disable CS0612
        status.StorageBackend = isSqlite ? StorageBackend.Sqlite : StorageBackend.Memory;
#pragma warning restore CS0612
        status.PersistenceStepEnabled = isSqlite;

        if (!string.IsNullOrWhiteSpace(_qrzXmlUsername))
        {
            status.QrzXmlUsername = _qrzXmlUsername;
        }

        if (!string.IsNullOrWhiteSpace(_persistedSetup.ActiveProfileId))
        {
            status.ActiveStationProfileId = _persistedSetup.ActiveProfileId;
        }

        if (_persistedSetup.RigControl is not null)
        {
            status.RigControl = _persistedSetup.RigControl.Clone();
        }

        if (_persistedSetup.WsjtxIngest is not null)
        {
            status.WsjtxIngest = _persistedSetup.WsjtxIngest.Clone();
        }

        if (Volatile.Read(ref _wsjtxIngestLiveStatus) is { } wsjtxLiveStatus)
        {
            status.WsjtxIngestStatus = wsjtxLiveStatus.Clone();
        }

        if (isSqlite)
        {
            status.PersistenceDefinitions.Add(
                new RuntimeConfigDefinition
                {
                    Key = PersistenceSetup.PathKey,
                    Label = PersistenceStepLabel,
                    Description = "SQLite logbook path for the shared durable setup.",
                    Kind = RuntimeConfigValueKind.Path,
                    Required = true,
                });

            if (!string.IsNullOrWhiteSpace(persistedPath))
            {
                status.LogFilePath = persistedPath;
                status.SuggestedLogFilePath = persistedPath;
                status.PersistenceValues.Add(
                    new RuntimeConfigValue
                    {
                        Key = PersistenceSetup.PathKey,
                        HasValue = true,
                        DisplayValue = persistedPath,
                    });
            }
        }

        if (!isSqlite)
        {
            status.Warnings.Add("Managed .NET engine currently uses an in-memory logbook.");
        }

        return status;
    }

    private SetupWizardStepStatus[] BuildStepStatusesNoLock()
    {
        return
        [
            new SetupWizardStepStatus
            {
                Step = SetupWizardStep.LogFile,
                Complete = !IsSqliteBackendNoLock() || !string.IsNullOrWhiteSpace(_persistedSetup.GetPersistedLogFilePath()) || !string.IsNullOrWhiteSpace(_currentPersistenceLocation),
            },
            new SetupWizardStepStatus
            {
                Step = SetupWizardStep.StationProfiles,
                Complete = GetPersistedActiveProfileNoLock() is { } active && IsStationProfileComplete(active),
            },
            new SetupWizardStepStatus
            {
                Step = SetupWizardStep.QrzIntegration,
                Complete = !string.IsNullOrWhiteSpace(_qrzXmlUsername)
                    && _hasQrzXmlPassword,
            },
            new SetupWizardStepStatus
            {
                Step = SetupWizardStep.Review,
                Complete = IsSetupCompleteNoLock(),
            }
        ];
    }

    private List<StationProfileRecord> BuildStationProfileRecordsNoLock()
    {
        return _stationProfiles
            .Select(BuildStationProfileRecordNoLock)
            .ToList();
    }

    private StationProfileRecord BuildStationProfileRecordNoLock(ManagedPersistedStationProfile entry)
    {
        return new StationProfileRecord
        {
            ProfileId = entry.ProfileId,
            Profile = ParseProtoOrDefault<StationProfile>(entry.ProfileJson),
            IsActive = string.Equals(_activeProfileId, entry.ProfileId, StringComparison.Ordinal),
        };
    }

    private ActiveStationContext BuildActiveStationContextNoLock()
    {
        var persistedActive = GetPersistedActiveProfileNoLock() ?? new StationProfile();
        var effectiveActive = GetEffectiveActiveProfileNoLock() ?? new StationProfile();
        var context = new ActiveStationContext
        {
            PersistedActiveProfile = persistedActive.Clone(),
            EffectiveActiveProfile = effectiveActive.Clone(),
            HasSessionOverride = _sessionOverrideProfile is not null,
            SessionOverrideProfile = _sessionOverrideProfile?.Clone() ?? new StationProfile(),
        };

        if (!string.IsNullOrWhiteSpace(_activeProfileId))
        {
            context.PersistedActiveProfileId = _activeProfileId;
        }

        if (!IsSetupCompleteNoLock())
        {
            context.Warnings.Add("Managed engine setup is incomplete.");
        }

        return context;
    }

    private StationProfileRecord SaveStationProfileNoLock(string profileId, StationProfile profile, bool makeActive)
    {
        var normalizedProfile = NormalizeStationProfile(profile);
        var normalizedId = NormalizeProfileIdOrDefault(profileId, normalizedProfile.ProfileName, normalizedProfile.StationCallsign);
        var serialized = ProtoJsonFormatter.Format(normalizedProfile);
        var existing = _stationProfiles.FirstOrDefault(entry => string.Equals(entry.ProfileId, normalizedId, StringComparison.Ordinal));
        if (existing is null)
        {
            _stationProfiles.Add(new ManagedPersistedStationProfile
            {
                ProfileId = normalizedId,
                ProfileJson = serialized,
            });
        }
        else
        {
            existing.ProfileJson = serialized;
        }

        if (makeActive || string.IsNullOrWhiteSpace(_activeProfileId))
        {
            _activeProfileId = normalizedId;
        }

        return BuildStationProfileRecordNoLock(_stationProfiles.First(entry => string.Equals(entry.ProfileId, normalizedId, StringComparison.Ordinal)));
    }

    private StationProfile? GetPersistedActiveProfileNoLock()
    {
        var stored = _stationProfiles.FirstOrDefault(entry => string.Equals(entry.ProfileId, _activeProfileId, StringComparison.Ordinal));
        return stored is null ? null : ParseProtoOrDefault<StationProfile>(stored.ProfileJson);
    }

    private StationProfile? GetEffectiveActiveProfileNoLock()
    {
        return _sessionOverrideProfile?.Clone() ?? GetPersistedActiveProfileNoLock();
    }

    private void SyncPersistedProfilesNoLock()
    {
        _persistedSetup.ActiveProfileId = _activeProfileId;
        _persistedSetup.StationProfiles.Clear();

        foreach (var entry in _stationProfiles)
        {
            _persistedSetup.StationProfiles.Add(
                new ManagedPersistedStationProfile
                {
                    ProfileId = entry.ProfileId,
                    ProfileJson = entry.ProfileJson,
                });
        }
    }

    private void UpdatePersistedStorageSettingsNoLock(SaveSetupRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);

        var requestedPath = request.PersistenceValues
            .FirstOrDefault(static value => PersistenceSetup.IsPathKey(value.Key))
            ?.Value;

        requestedPath = NormalizeOptional(requestedPath)
            ?? NormalizeOptional(request.LogFilePath);

#pragma warning disable CS0612
        requestedPath ??= NormalizeOptional(request.SqlitePath);
#pragma warning restore CS0612

        if (IsSqliteBackendNoLock())
        {
            _persistedSetup.LogbookFilePath = requestedPath
                ?? NormalizeOptional(_currentPersistenceLocation)
                ?? NormalizeOptional(_persistedSetup.GetPersistedLogFilePath());
            _persistedSetup.StorageBackend = null;
            _persistedSetup.StorageSqlitePath = null;
            return;
        }

        _persistedSetup.LogbookFilePath = null;
        _persistedSetup.StorageBackend = "memory";
        _persistedSetup.StorageSqlitePath = null;
    }

    private bool IsSqliteBackendNoLock()
    {
        return string.Equals(_storage.BackendName, "sqlite", StringComparison.OrdinalIgnoreCase);
    }

    private void ApplyStationContextNoLock(QsoRecord qso, QsoRecord? existing = null)
    {
        if (existing is null)
        {
            ManagedQsoParity.MaterializeStationSnapshotForCreate(qso, GetEffectiveActiveProfileNoLock());
            return;
        }

        ManagedQsoParity.MaterializeStationSnapshotForUpdate(qso, existing);
    }

    private static void ValidateQsoNoLock(QsoRecord qso)
    {
        ManagedQsoParity.ValidateQsoForPersistence(qso);
    }

    private static void FinalizeQsoForWrite(QsoRecord qso, bool isNew)
    {
        if (string.IsNullOrWhiteSpace(qso.LocalId))
        {
            qso.LocalId = Guid.NewGuid().ToString();
        }

        if (IsTimestampUnset(qso.UtcTimestamp))
        {
            qso.UtcTimestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
        }

        if (isNew && IsTimestampUnset(qso.CreatedAt))
        {
            qso.CreatedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
        }

        qso.UpdatedAt = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
    }

    private void ApplySyncFlagsNoLock(QsoRecord qso, bool syncToQrz, LogQsoResponse response)
    {
        if (!syncToQrz)
        {
            qso.SyncStatus = SyncStatus.LocalOnly;
            response.SyncSuccess = false;
            return;
        }

        if (!_hasQrzLogbookApiKey)
        {
            qso.SyncStatus = SyncStatus.LocalOnly;
            response.SyncSuccess = false;
            response.SyncError = "QRZ logbook is not configured.";
            return;
        }

        try
        {
            var logid = Sync((_syncEngine ?? throw new InvalidOperationException("QRZ logbook sync is unavailable."))
                .UploadSingleQsoAsync(_storage.Logbook, qso));
            qso.SyncStatus = SyncStatus.Synced;
            qso.QrzLogid = logid;
            response.QrzLogid = logid;
            response.SyncSuccess = true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            qso.SyncStatus = SyncStatus.LocalOnly;
            response.SyncSuccess = false;
            response.SyncError = ex.Message;
        }
    }

    private void ApplySyncFlagsNoLock(QsoRecord qso, bool syncToQrz, UpdateQsoResponse response)
    {
        if (!syncToQrz)
        {
            if (qso.SyncStatus == SyncStatus.Synced)
            {
                qso.SyncStatus = SyncStatus.Modified;
            }

            response.SyncSuccess = false;
            return;
        }

        if (!_hasQrzLogbookApiKey)
        {
            response.SyncSuccess = false;
            response.SyncError = "QRZ logbook is not configured.";
            return;
        }

        try
        {
            var logid = Sync((_syncEngine ?? throw new InvalidOperationException("QRZ logbook sync is unavailable."))
                .UploadSingleQsoAsync(_storage.Logbook, qso));
            qso.SyncStatus = SyncStatus.Synced;
            qso.QrzLogid = logid;
            response.SyncSuccess = true;
        }
        catch (Exception ex) when (ex is not OutOfMemoryException)
        {
            response.SyncSuccess = false;
            response.SyncError = ex.Message;
        }
    }

    private void ApplyLotwSyncFlagNoLock(QsoRecord qso, bool syncToLotw, LogQsoResponse response)
    {
        if (!syncToLotw)
        {
            response.LotwSyncSuccess = false;
            return;
        }

        try
        {
            var (engine, ownedClient) = CreateLotwSyncEngineNoLock();
            using (ownedClient)
            {
                Sync(engine.UploadSingleAsync(qso.Clone(), CancellationToken.None));
            }
            response.LotwSyncSuccess = true;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            response.LotwSyncSuccess = false;
            response.LotwSyncError = exception.Message;
        }
    }

    private void ApplyLotwSyncFlagNoLock(QsoRecord qso, bool syncToLotw, UpdateQsoResponse response)
    {
        if (!syncToLotw)
        {
            response.LotwSyncSuccess = false;
            return;
        }

        try
        {
            var (engine, ownedClient) = CreateLotwSyncEngineNoLock();
            using (ownedClient)
            {
                Sync(engine.UploadSingleAsync(qso.Clone(), CancellationToken.None));
            }
            response.LotwSyncSuccess = true;
        }
        catch (Exception exception) when (exception is not OutOfMemoryException)
        {
            response.LotwSyncSuccess = false;
            response.LotwSyncError = exception.Message;
        }
    }

    private (LotwSyncEngine Engine, IDisposable? OwnedClient) CreateLotwSyncEngineNoLock()
    {
        if (_lotwApi is not null)
        {
            return (new LotwSyncEngine(_lotwApi, _storage.Logbook), null);
        }

        var client = new LotwClient(_lotwSettings);
        return (new LotwSyncEngine(client, _storage.Logbook), client);
    }

    private void RebuildLookupCoordinatorNoLock()
    {
        var username = Environment.GetEnvironmentVariable(QrzXmlUsernameKey)?.Trim()
            ?? _qrzXmlUsername;
        var password = Environment.GetEnvironmentVariable(QrzXmlPasswordKey)?.Trim()
            ?? _qrzXmlPassword;
        var userAgent = Environment.GetEnvironmentVariable(QrzUserAgentKey)?.Trim()
            ?? NormalizeOptional(_persistedSetup.QrzXmlUserAgent);

        if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
        {
            // HttpClient is intentionally not disposed — it is a singleton owned by the provider for the app lifetime.
#pragma warning disable CA2000 // Dispose objects before losing scope
            var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(8) };
#pragma warning restore CA2000
            _lookupCoordinator = new LookupCoordinator(
                new Lookup.Qrz.QrzXmlProvider(httpClient, username, password, userAgent: userAgent),
                _storage.LookupSnapshots,
                logbookStore: _storage.Logbook);
        }
        else
        {
            _lookupCoordinator = new LookupCoordinator(
                new Lookup.Qrz.DisabledCallsignProvider(),
                _storage.LookupSnapshots,
                logbookStore: _storage.Logbook);
        }
    }

    private void RebuildSyncEngineNoLock()
    {
        var apiKey = Environment.GetEnvironmentVariable(QrzLogbookApiKeyKey)?.Trim()
            ?? _qrzLogbookApiKey;
        if (string.IsNullOrWhiteSpace(apiKey))
        {
            DisposeOwnedSyncResourcesNoLock();
            return;
        }

        var baseUrl = Environment.GetEnvironmentVariable("QSORIPPER_QRZ_LOGBOOK_BASE_URL")?.Trim()
            ?? NormalizeOptional(_persistedSetup.QrzLogbookBaseUrl);
        Uri? apiUri = null;
        if (!string.IsNullOrWhiteSpace(baseUrl))
        {
            apiUri = Uri.TryCreate(baseUrl, UriKind.Absolute, out var parsedUri)
                ? parsedUri
                : throw new InvalidOperationException($"Invalid QRZ logbook base URL '{baseUrl}'.");
        }

        var nextClient = apiUri is null
            ? new QrzLogbookClient(apiKey)
            : new QrzLogbookClient(apiKey, apiUri);
        var previousOwnedSyncClient = _ownedSyncClient;
        _ownedSyncClient = nextClient;
        _syncEngine = new QrzSyncEngine(nextClient);
        previousOwnedSyncClient?.Dispose();
    }

    private void DisposeOwnedSyncResourcesNoLock()
    {
        _ownedSyncClient?.Dispose();
        _ownedSyncClient = null;
        _syncEngine = null;
    }

    private static LookupCoordinator CreateDefaultCoordinator(IEngineStorage storage)
    {
        ICallsignProvider provider;
        var username = Environment.GetEnvironmentVariable("QSORIPPER_QRZ_XML_USERNAME")?.Trim();
        var password = Environment.GetEnvironmentVariable("QSORIPPER_QRZ_XML_PASSWORD")?.Trim();
        var userAgent = Environment.GetEnvironmentVariable(QrzUserAgentKey)?.Trim();

        if (!string.IsNullOrWhiteSpace(username) && !string.IsNullOrWhiteSpace(password))
        {
            // HttpClient is intentionally not disposed — it is a singleton owned by the provider for the app lifetime.
#pragma warning disable CA2000 // Dispose objects before losing scope
            var httpClient = new HttpClient { Timeout = TimeSpan.FromSeconds(8) };
#pragma warning restore CA2000
            provider = new Lookup.Qrz.QrzXmlProvider(httpClient, username, password, userAgent: userAgent);
        }
        else
        {
            provider = new Lookup.Qrz.DisabledCallsignProvider();
        }

        return new LookupCoordinator(provider, storage.LookupSnapshots, logbookStore: storage.Logbook);
    }

    private static List<RuntimeConfigDefinition> BuildRuntimeConfigDefinitionsNoLock()
    {
        return
        [
            new RuntimeConfigDefinition
            {
                Key = QrzXmlUsernameKey,
                Label = "QRZ XML username",
                Description = "Managed-engine sample QRZ XML username.",
                Kind = RuntimeConfigValueKind.String,
            },
            new RuntimeConfigDefinition
            {
                Key = QrzXmlPasswordKey,
                Label = "QRZ XML password",
                Description = "Managed-engine sample QRZ XML password.",
                Kind = RuntimeConfigValueKind.String,
                Secret = true,
            },
            new RuntimeConfigDefinition
            {
                Key = QrzLogbookApiKeyKey,
                Label = "QRZ logbook API key",
                Description = "Managed-engine sample QRZ logbook API key.",
                Kind = RuntimeConfigValueKind.String,
                Secret = true,
            },
            new RuntimeConfigDefinition
            {
                Key = RigEnabledKey,
                Label = "Rig control enabled",
                Description = "Enable the managed sample rig-control status responses.",
                Kind = RuntimeConfigValueKind.Boolean,
                AllowedValues = { "true", "false" },
                DefaultValue = "false",
            }
        ];
    }

    private RuntimeConfigSnapshot BuildRuntimeConfigSnapshotNoLock()
    {
        var isSqlite = IsSqliteBackendNoLock();
        var snapshot = new RuntimeConfigSnapshot
        {
            ActiveStorageBackend = _storage.BackendName,
            LookupProviderSummary = ManagedLookupProviderSummary,
            PersistenceSummary = isSqlite ? SqlitePersistenceSummary : InMemoryPersistenceSummary,
        };

        if (GetEffectiveActiveProfileNoLock() is { } activeProfile)
        {
            snapshot.ActiveStationProfile = activeProfile.Clone();
        }

        if (isSqlite && !string.IsNullOrWhiteSpace(_currentPersistenceLocation))
        {
            snapshot.PersistenceLocation = _currentPersistenceLocation;
        }

        if (!isSqlite)
        {
            snapshot.Warnings.Add("Managed .NET engine currently uses an in-memory logbook.");
        }

        snapshot.Definitions.AddRange(BuildRuntimeConfigDefinitionsNoLock());
        snapshot.Values.AddRange(BuildRuntimeConfigValuesNoLock());
        return snapshot;
    }

    private List<RuntimeConfigValue> BuildRuntimeConfigValuesNoLock()
    {
        return
        [
            BuildRuntimeValue(QrzXmlUsernameKey, _qrzXmlUsername, _runtimeOverrides.ContainsKey(QrzXmlUsernameKey), secret: false, redacted: false, hasDefault: false),
            BuildRuntimeValue(QrzXmlPasswordKey, _hasQrzXmlPassword ? "***" : null, _runtimeOverrides.ContainsKey(QrzXmlPasswordKey), secret: true, redacted: _hasQrzXmlPassword, hasDefault: false),
            BuildRuntimeValue(QrzLogbookApiKeyKey, _hasQrzLogbookApiKey ? "***" : null, _runtimeOverrides.ContainsKey(QrzLogbookApiKeyKey), secret: true, redacted: _hasQrzLogbookApiKey, hasDefault: false),
            BuildRuntimeValue(RigEnabledKey, (_rigControl?.Enabled ?? false) ? "TRUE" : "FALSE", _runtimeOverrides.ContainsKey(RigEnabledKey), secret: false, redacted: false, hasDefault: _rigControl is null),
        ];
    }

    private static RuntimeConfigValue BuildRuntimeValue(
        string key,
        string? value,
        bool overridden,
        bool secret,
        bool redacted,
        bool hasDefault)
    {
        return new RuntimeConfigValue
        {
            Key = key,
            HasValue = !string.IsNullOrWhiteSpace(value),
            DisplayValue = value ?? string.Empty,
            Overridden = overridden,
            Secret = secret,
            Redacted = redacted,
            Source = overridden
                ? RuntimeConfigValueSource.RuntimeOverride
                : hasDefault
                    ? RuntimeConfigValueSource.Default
                    : string.IsNullOrWhiteSpace(value)
                        ? RuntimeConfigValueSource.Unspecified
                        : RuntimeConfigValueSource.BaseConfig,
        };
    }

    private void ApplyStringOverrideNoLock(string key, RuntimeConfigMutation mutation, Action<string?> setter)
    {
        if (mutation.Kind == RuntimeConfigMutationKind.Clear)
        {
            setter(null);
            _runtimeOverrides[key] = string.Empty;
            return;
        }

        var value = NormalizeOptional(mutation.Value);
        setter(value);
        if (value is null)
        {
            _runtimeOverrides.Remove(key);
        }
        else
        {
            _runtimeOverrides[key] = value;
        }
    }

    private bool IsSetupCompleteNoLock()
    {
        var active = GetPersistedActiveProfileNoLock();
        return active is not null && IsStationProfileComplete(active);
    }

    private static void AddValidation(ValidateSetupStepResponse response, string field, bool valid, string message)
    {
        response.Fields.Add(new SetupFieldValidation
        {
            Field = field,
            Valid = valid,
            Message = valid ? string.Empty : message,
        });
    }

    private void RepairLegacyQrzLogids()
    {
        var qsos = Sync(_storage.Logbook.ListQsosAsync(new QsoListQuery
        {
            DeletedFilter = QsoRipper.Engine.Storage.DeletedRecordsFilter.All,
        })).Select(static qso => qso.Clone()).ToList();

        foreach (var qso in qsos)
        {
            if (!string.IsNullOrWhiteSpace(qso.QrzLogid))
            {
                continue;
            }

            var legacyLogid = GetLegacyQrzLogid(qso);
            if (legacyLogid is null)
            {
                continue;
            }

            qso.QrzLogid = legacyLogid;
            qso.ExtraFields.Remove("APP_QRZLOG_LOGID");
            qso.ExtraFields.Remove("APP_QRZ_LOGID");
            Sync(_storage.Logbook.UpdateQsoAsync(qso));
        }

        foreach (var group in qsos
                     .Where(static qso => !string.IsNullOrWhiteSpace(qso.QrzLogid))
                     .GroupBy(static qso => qso.QrzLogid, StringComparer.Ordinal)
                     .Where(static group => group.Count() > 1))
        {
            var rows = group
                .OrderBy(static qso => qso.CreatedAt?.Seconds ?? long.MaxValue)
                .ThenBy(static qso => qso.LocalId, StringComparer.Ordinal)
                .ToList();
            var keeper = rows[0].Clone();
            foreach (var victim in rows.Skip(1))
            {
                keeper = MergeRepairFields(keeper, victim);
                Sync(_storage.Logbook.DeleteQsoAsync(victim.LocalId));
            }

            Sync(_storage.Logbook.UpdateQsoAsync(keeper));
        }
    }

    private static string? GetLegacyQrzLogid(QsoRecord qso)
    {
        foreach (var key in new[] { "APP_QRZLOG_LOGID", "APP_QRZ_LOGID" })
        {
            if (qso.ExtraFields.TryGetValue(key, out var value) && NormalizeOptional(value) is { } logid)
            {
                return logid;
            }
        }

        return null;
    }

    private static QsoRecord MergeRepairFields(QsoRecord keeper, QsoRecord victim)
    {
        var merged = victim.Clone();
        merged.MergeFrom(keeper);
        merged.LocalId = keeper.LocalId;
        merged.QrzLogid = keeper.QrzLogid;
        merged.CreatedAt = keeper.CreatedAt?.Clone();
        merged.UpdatedAt = keeper.UpdatedAt?.Clone();
        merged.SyncStatus = keeper.SyncStatus;
        merged.DeletedAt = keeper.DeletedAt?.Clone();
        merged.PendingRemoteDelete = keeper.PendingRemoteDelete;
        merged.ExtraFields.Remove("APP_QRZLOG_LOGID");
        merged.ExtraFields.Remove("APP_QRZ_LOGID");
        return merged;
    }

    private static bool IsStationProfileComplete(StationProfile profile)
    {
        return !string.IsNullOrWhiteSpace(profile.ProfileName)
            && !string.IsNullOrWhiteSpace(profile.StationCallsign)
            && !string.IsNullOrWhiteSpace(profile.OperatorCallsign)
            && !string.IsNullOrWhiteSpace(profile.Grid);
    }

    private static StationProfile NormalizeStationProfile(StationProfile profile)
    {
        var normalized = profile.Clone();
        normalized.ProfileName = NormalizeOptional(normalized.ProfileName)
            ?? throw new InvalidOperationException("Station profile name is required.");
        normalized.StationCallsign = NormalizeOptional(normalized.StationCallsign)?.ToUpperInvariant()
            ?? throw new InvalidOperationException("Station callsign is required.");
        if (!string.IsNullOrWhiteSpace(normalized.OperatorCallsign))
        {
            normalized.OperatorCallsign = normalized.OperatorCallsign.Trim().ToUpperInvariant();
        }

        if (!string.IsNullOrWhiteSpace(normalized.Grid))
        {
            normalized.Grid = normalized.Grid.Trim().ToUpperInvariant();
        }

        return normalized;
    }

    private string? ReadStoredSecret(QrzSecret secret)
    {
        try
        {
            return NormalizeOptional(_qrzSecretStore.Get(secret));
        }
        catch (QrzSecretStoreException)
        {
            return null;
        }
    }

    private void MigrateLegacySecret(QrzSecret secret, string? value)
    {
        if (value is null)
        {
            return;
        }

        try
        {
            _qrzSecretStore.Set(secret, value);
        }
        catch (QrzSecretStoreException)
        {
            // Keep the legacy value active for this process. The TOML rewrite still removes it.
        }
    }

    private void PersistRequestedSecret(QrzSecret secret, string? value)
    {
        if (value is null)
        {
            return;
        }

        try
        {
            _qrzSecretStore.Set(secret, value);
        }
        catch (QrzSecretStoreException)
        {
            throw;
        }
        catch (Exception exception)
        {
            throw new QrzSecretStoreException(
                "The platform credential store did not save the QRZ secret.",
                exception);
        }
    }

    private static string? NormalizeOptional(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }

    private static uint? ParsePositiveUInt32(string? value)
    {
        return uint.TryParse(value, out var parsed) && parsed > 0 ? parsed : null;
    }

    private static uint ToUInt32(int value)
    {
        return value <= 0 ? 0 : (uint)value;
    }

    private static SyncConfig NormalizeSyncConfig(SyncConfig config)
    {
        var normalized = config.Clone();
        if (normalized.ConflictPolicy == ConflictPolicy.Unspecified)
        {
            normalized.ConflictPolicy = ConflictPolicy.FlagForReview;
        }

        return normalized;
    }

    private static WsjtxIngestSettings NormalizeWsjtxIngestSnapshot(WsjtxIngestSettings settings)
    {
        // Non-throwing variant used by the ingest supervisor's live polling. The persisted settings
        // were already validated at SaveSetup time, so here we only re-apply trivial defaults and
        // never throw (the supervisor must remain resilient to any transient config state).
        var normalized = settings.Clone();
        normalized.UdpEnabled = settings.HasUdpEnabled ? settings.UdpEnabled : true;
        if (string.IsNullOrWhiteSpace(normalized.UdpBind))
        {
            normalized.UdpBind = "127.0.0.1:2237";
        }

        if (string.IsNullOrWhiteSpace(normalized.AdifTailPath))
        {
            normalized.ClearAdifTailPath();
        }

        if (normalized.PollIntervalMs == 0)
        {
            normalized.PollIntervalMs = 1000;
        }

        return normalized;
    }

    private static WsjtxIngestSettings NormalizeWsjtxIngest(WsjtxIngestSettings settings)
    {
        var normalized = settings.Clone();
        normalized.UdpEnabled = settings.HasUdpEnabled ? settings.UdpEnabled : true;
        normalized.UdpBind = NormalizeOptional(settings.UdpBind) ?? "127.0.0.1:2237";
        ValidateHostPort(normalized.UdpBind, "WSJT-X UDP bind");

        normalized.AdifTailPath = NormalizeOptional(settings.AdifTailPath) ?? string.Empty;
        if (string.IsNullOrEmpty(normalized.AdifTailPath))
        {
            normalized.ClearAdifTailPath();
        }
        if (normalized.AdifTailEnabled && string.IsNullOrWhiteSpace(normalized.AdifTailPath))
        {
            throw new InvalidOperationException("WSJT-X ADIF tail path is required when ADIF tailing is enabled.");
        }

        if (normalized.PollIntervalMs == 0)
        {
            normalized.PollIntervalMs = 1000;
        }

        return normalized;
    }

    private static void ValidateHostPort(string bind, string label)
    {
        var separator = bind.LastIndexOf(':');
        if (separator <= 0 || separator == bind.Length - 1)
        {
            throw new InvalidOperationException($"{label} must be in host:port form.");
        }

        var portText = bind[(separator + 1)..];
        if (!ushort.TryParse(portText, out var port) || port == 0)
        {
            throw new InvalidOperationException($"{label} port must be between 1 and 65535.");
        }
    }

    private static bool IsTimestampUnset(Timestamp? value)
    {
        return value is null || (value.Seconds == 0 && value.Nanos == 0);
    }

    private static T ParseProtoOrDefault<T>(string? json)
        where T : class, IMessage<T>, new()
    {
        return string.IsNullOrWhiteSpace(json) ? new T() : ProtoJsonParser.Parse<T>(json);
    }

    private static T? ParseOptionalProto<T>(string? json)
        where T : class, IMessage<T>, new()
    {
        return string.IsNullOrWhiteSpace(json) ? null : ProtoJsonParser.Parse<T>(json);
    }

    private static string NormalizeProfileIdOrDefault(params string?[] candidates)
    {
        foreach (var candidate in candidates)
        {
            if (string.IsNullOrWhiteSpace(candidate))
            {
                continue;
            }

#pragma warning disable CA1308 // Profile IDs intentionally match Rust's lowercase normalization.
            var normalized = Regex.Replace(candidate.Trim().ToLowerInvariant(), "[^a-z0-9]+", "-").Trim('-');
#pragma warning restore CA1308
            if (!string.IsNullOrWhiteSpace(normalized))
            {
                return normalized;
            }
        }

        return "default";
    }

    /// <summary>Synchronously extracts the result of a completed <see cref="ValueTask{T}"/>.</summary>
    private static T Sync<T>(ValueTask<T> task) => task.GetAwaiter().GetResult();

    /// <summary>Synchronously awaits a completed <see cref="ValueTask"/>.</summary>
    private static void Sync(ValueTask task) => task.GetAwaiter().GetResult();

    /// <summary>Synchronously extracts the result of a <see cref="Task{T}"/>.</summary>
    private static T Sync<T>(Task<T> task) => task.GetAwaiter().GetResult();

    /// <summary>Synchronously awaits a <see cref="Task"/>.</summary>
    private static void Sync(Task task) => task.GetAwaiter().GetResult();
}
