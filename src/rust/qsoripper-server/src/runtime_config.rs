use std::collections::BTreeMap;
use std::sync::Arc;

use qsoripper_core::application::logbook::LogbookEngine;
use qsoripper_core::contest_calendar::{
    CatalogEnrichingContestCalendarProvider, ContestCalendarMonitor, ContestCalendarProvider,
    ContestDetailsCatalog, DisabledContestCalendarProvider, Wa7bnmContestCalendarConfig,
    Wa7bnmContestCalendarProvider, CONTEST_CALENDAR_DETAILS_PATH_ENV_VAR,
    CONTEST_CALENDAR_ENABLED_ENV_VAR, CONTEST_CALENDAR_HTTP_TIMEOUT_SECONDS_ENV_VAR,
    CONTEST_CALENDAR_REFRESH_INTERVAL_SECONDS_ENV_VAR, CONTEST_CALENDAR_RSS_URL_ENV_VAR,
    CONTEST_CALENDAR_STALE_AFTER_SECONDS_ENV_VAR, DEFAULT_CONTEST_CALENDAR_DETAILS_PATH,
    DEFAULT_CONTEST_CALENDAR_REFRESH_INTERVAL_SECONDS, DEFAULT_CONTEST_CALENDAR_RSS_URL,
    DEFAULT_CONTEST_CALENDAR_STALE_AFTER_SECONDS,
};
use qsoripper_core::domain::lookup::normalize_callsign;
use qsoripper_core::domain::station::station_profile_has_values;
use qsoripper_core::lookup::{
    CallsignProvider, DisabledCallsignProvider, LookupCoordinator, LookupCoordinatorConfig,
    QrzXmlConfig, QrzXmlProvider, DEFAULT_QRZ_XML_BASE_URL, QRZ_HTTP_TIMEOUT_SECONDS_ENV_VAR,
    QRZ_MAX_RETRIES_ENV_VAR, QRZ_USER_AGENT_ENV_VAR, QRZ_XML_BASE_URL_ENV_VAR,
    QRZ_XML_CAPTURE_ONLY_ENV_VAR, QRZ_XML_PASSWORD_ENV_VAR, QRZ_XML_USERNAME_ENV_VAR,
};
use qsoripper_core::proto::qsoripper::domain::StationProfile;
use qsoripper_core::proto::qsoripper::services::{
    ApplyRuntimeConfigRequest, ResetRuntimeConfigRequest, RuntimeConfigDefinition,
    RuntimeConfigMutation, RuntimeConfigMutationKind, RuntimeConfigSnapshot, RuntimeConfigValue,
    RuntimeConfigValueKind, RuntimeConfigValueSource,
};
use qsoripper_core::rig_control::{
    DisabledRigControlProvider, RigControlMonitor, RigControlProvider, RigctldConfig,
    RigctldProvider, DEFAULT_RIGCTLD_HOST, DEFAULT_RIGCTLD_STALE_THRESHOLD_MS,
    RIGCTLD_ENABLED_ENV_VAR, RIGCTLD_HOST_ENV_VAR, RIGCTLD_PORT_ENV_VAR,
    RIGCTLD_READ_TIMEOUT_MS_ENV_VAR, RIGCTLD_STALE_THRESHOLD_MS_ENV_VAR,
};
use qsoripper_core::space_weather::{
    DisabledSpaceWeatherProvider, NoaaSpaceWeatherConfig, NoaaSpaceWeatherProvider,
    SpaceWeatherMonitor, SpaceWeatherProvider, NOAA_HTTP_TIMEOUT_SECONDS_ENV_VAR,
    NOAA_KP_INDEX_URL_ENV_VAR, NOAA_REFRESH_INTERVAL_SECONDS_ENV_VAR,
    NOAA_SOLAR_INDICES_URL_ENV_VAR, NOAA_SPACE_WEATHER_ENABLED_ENV_VAR,
    NOAA_STALE_AFTER_SECONDS_ENV_VAR,
};
use tokio::sync::RwLock;

use crate::station_profile_support::{
    insert_station_profile_runtime_values, normalize_station_profile,
};
use crate::{build_storage, parse_storage_backend, StorageOptions};

pub(crate) const STORAGE_BACKEND_ENV_VAR: &str = "QSORIPPER_STORAGE_BACKEND";
pub(crate) const SQLITE_PATH_ENV_VAR: &str = "QSORIPPER_SQLITE_PATH";
pub(crate) const STATION_PROFILE_NAME_ENV_VAR: &str = "QSORIPPER_STATION_PROFILE_NAME";
pub(crate) const STATION_CALLSIGN_ENV_VAR: &str = "QSORIPPER_STATION_CALLSIGN";
pub(crate) const STATION_OPERATOR_CALLSIGN_ENV_VAR: &str = "QSORIPPER_STATION_OPERATOR_CALLSIGN";
pub(crate) const STATION_OPERATOR_NAME_ENV_VAR: &str = "QSORIPPER_STATION_OPERATOR_NAME";
pub(crate) const STATION_GRID_ENV_VAR: &str = "QSORIPPER_STATION_GRID";
pub(crate) const STATION_COUNTY_ENV_VAR: &str = "QSORIPPER_STATION_COUNTY";
pub(crate) const STATION_STATE_ENV_VAR: &str = "QSORIPPER_STATION_STATE";
pub(crate) const STATION_COUNTRY_ENV_VAR: &str = "QSORIPPER_STATION_COUNTRY";
pub(crate) const STATION_ARRL_SECTION_ENV_VAR: &str = "QSORIPPER_STATION_ARRL_SECTION";
pub(crate) const STATION_DXCC_ENV_VAR: &str = "QSORIPPER_STATION_DXCC";
pub(crate) const STATION_CQ_ZONE_ENV_VAR: &str = "QSORIPPER_STATION_CQ_ZONE";
pub(crate) const STATION_ITU_ZONE_ENV_VAR: &str = "QSORIPPER_STATION_ITU_ZONE";
pub(crate) const STATION_LATITUDE_ENV_VAR: &str = "QSORIPPER_STATION_LATITUDE";
pub(crate) const STATION_LONGITUDE_ENV_VAR: &str = "QSORIPPER_STATION_LONGITUDE";
pub(crate) const QRZ_LOGBOOK_API_KEY_ENV_VAR: &str = "QSORIPPER_QRZ_LOGBOOK_API_KEY";
pub(crate) const QRZ_LOGBOOK_BASE_URL_ENV_VAR: &str = "QSORIPPER_QRZ_LOGBOOK_BASE_URL";
pub(crate) const LOTW_USERNAME_ENV_VAR: &str = "QSORIPPER_LOTW_USERNAME";
pub(crate) const LOTW_PASSWORD_ENV_VAR: &str = "QSORIPPER_LOTW_PASSWORD";
pub(crate) const LOTW_TQSL_PATH_ENV_VAR: &str = "QSORIPPER_LOTW_TQSL_PATH";
pub(crate) const LOTW_STATION_LOCATION_ENV_VAR: &str = "QSORIPPER_LOTW_STATION_LOCATION";
pub(crate) const LOTW_CERTIFICATE_PASSWORD_ENV_VAR: &str = "QSORIPPER_LOTW_CERTIFICATE_PASSWORD";
pub(crate) const LOTW_REPORT_URL_ENV_VAR: &str = "QSORIPPER_LOTW_REPORT_URL";
pub(crate) const LOTW_TIMEOUT_SECONDS_ENV_VAR: &str = "QSORIPPER_LOTW_TIMEOUT_SECONDS";
pub(crate) const SYNC_AUTO_ENABLED_ENV_VAR: &str = "QSORIPPER_SYNC_AUTO_ENABLED";
pub(crate) const SYNC_INTERVAL_SECONDS_ENV_VAR: &str = "QSORIPPER_SYNC_INTERVAL_SECONDS";
pub(crate) const SYNC_CONFLICT_POLICY_ENV_VAR: &str = "QSORIPPER_SYNC_CONFLICT_POLICY";
pub(crate) const WSJTX_INGEST_ENABLED_ENV_VAR: &str = "QSORIPPER_WSJTX_INGEST_ENABLED";
pub(crate) const WSJTX_INGEST_UDP_ENABLED_ENV_VAR: &str = "QSORIPPER_WSJTX_INGEST_UDP_ENABLED";
pub(crate) const WSJTX_INGEST_UDP_BIND_ENV_VAR: &str = "QSORIPPER_WSJTX_INGEST_UDP_BIND";
pub(crate) const WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR: &str =
    "QSORIPPER_WSJTX_INGEST_ADIF_TAIL_ENABLED";
pub(crate) const WSJTX_INGEST_ADIF_TAIL_PATH_ENV_VAR: &str =
    "QSORIPPER_WSJTX_INGEST_ADIF_TAIL_PATH";
pub(crate) const WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR: &str =
    "QSORIPPER_WSJTX_INGEST_POLL_INTERVAL_MS";
pub(crate) const WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR: &str = "QSORIPPER_WSJTX_INGEST_SYNC_TO_QRZ";

pub(crate) const DEFAULT_QRZ_LOGBOOK_BASE_URL: &str = "https://logbook.qrz.com/api";
pub(crate) const DEFAULT_LOTW_REPORT_URL: &str = "https://lotw.arrl.org/lotwuser/lotwreport.adi";
pub(crate) const DEFAULT_LOTW_TQSL_PATH: &str = "tqsl";
pub(crate) const DEFAULT_LOTW_TIMEOUT_SECONDS: &str = "60";
const DEFAULT_SYNC_AUTO_ENABLED: &str = "false";
const DEFAULT_SYNC_INTERVAL_SECONDS: &str = "300";
const DEFAULT_SYNC_CONFLICT_POLICY: &str = "last_write_wins";
pub(crate) const DEFAULT_WSJTX_INGEST_UDP_BIND: &str = "127.0.0.1:2237";
pub(crate) const DEFAULT_WSJTX_INGEST_POLL_INTERVAL_MS: &str = "1000";

const DEFAULT_STORAGE_BACKEND: &str = "memory";
const DEFAULT_SQLITE_PATH: &str = "qsoripper.db";
const DEFAULT_QRZ_USER_AGENT: &str = "QsoRipper/1.0";
const REDACTED_VALUE: &str = "<redacted>";

const CONFLICT_POLICY_ALLOWED_VALUES: &[&str] = &["last_write_wins", "flag_for_review"];

#[derive(Clone)]
struct RuntimeBindings {
    logbook_engine: LogbookEngine,
    lookup_coordinator: Arc<LookupCoordinator>,
    contest_calendar_monitor: Arc<ContestCalendarMonitor>,
    space_weather_monitor: Arc<SpaceWeatherMonitor>,
    rig_control_monitor: Arc<RigControlMonitor>,
    active_storage_backend: String,
    lookup_provider_summary: String,
    active_station_profile: Option<StationProfile>,
}

pub(crate) struct RuntimeConfigManager {
    config_file_values: RwLock<BTreeMap<String, String>>,
    startup_values: BTreeMap<String, String>,
    session_station_profile_override: RwLock<Option<StationProfile>>,
    overrides: RwLock<BTreeMap<String, String>>,
    bindings: RwLock<RuntimeBindings>,
}

impl RuntimeConfigManager {
    pub(crate) fn new_with_config_file_values_and_cli_storage_overrides(
        config_file_values: BTreeMap<String, String>,
        cli_storage_overrides: &BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let startup_values = capture_supported_env();
        Self::new_with_config_file_values(
            merge_values(&startup_values, cli_storage_overrides),
            config_file_values,
        )
    }

    #[cfg(test)]
    pub(crate) fn new(startup_values: BTreeMap<String, String>) -> Result<Self, String> {
        Self::new_with_config_file_values(startup_values, BTreeMap::new())
    }

    pub(crate) fn new_with_config_file_values(
        startup_values: BTreeMap<String, String>,
        config_file_values: BTreeMap<String, String>,
    ) -> Result<Self, String> {
        let effective_values = merged_base_values(&config_file_values, &startup_values);
        let bindings = build_runtime_bindings(&effective_values)?;
        Ok(Self {
            config_file_values: RwLock::new(config_file_values),
            startup_values,
            session_station_profile_override: RwLock::new(None),
            overrides: RwLock::new(BTreeMap::new()),
            bindings: RwLock::new(bindings),
        })
    }

    pub(crate) async fn snapshot(&self) -> RuntimeConfigSnapshot {
        let config_file_values = self.config_file_values.read().await.clone();
        let session_station_profile_override =
            self.session_station_profile_override.read().await.clone();
        let overrides = self.overrides.read().await.clone();
        let bindings = self.bindings.read().await.clone();
        let base_values = merged_base_values(&config_file_values, &self.startup_values);
        build_snapshot(
            &base_values,
            session_station_profile_override.as_ref(),
            &overrides,
            &bindings,
        )
    }

    pub(crate) async fn apply_request(
        &self,
        request: ApplyRuntimeConfigRequest,
    ) -> Result<RuntimeConfigSnapshot, String> {
        let mut next_overrides = self.overrides.read().await.clone();

        for mutation in request.mutations {
            apply_mutation(&mut next_overrides, mutation)?;
        }

        self.swap_runtime(next_overrides).await
    }

    pub(crate) async fn reset_request(
        &self,
        request: ResetRuntimeConfigRequest,
    ) -> Result<RuntimeConfigSnapshot, String> {
        let mut next_overrides = self.overrides.read().await.clone();

        if request.keys.is_empty() {
            next_overrides.clear();
        } else {
            for raw_key in request.keys {
                let key = canonical_key(&raw_key)?;
                next_overrides.remove(key);
            }
        }

        self.swap_runtime(next_overrides).await
    }

    pub(crate) async fn logbook_engine(&self) -> LogbookEngine {
        self.bindings.read().await.logbook_engine.clone()
    }

    pub(crate) async fn logbook_context(&self) -> (LogbookEngine, Option<StationProfile>) {
        let bindings = self.bindings.read().await;
        (
            bindings.logbook_engine.clone(),
            bindings.active_station_profile.clone(),
        )
    }

    pub(crate) async fn lookup_coordinator(&self) -> Arc<LookupCoordinator> {
        self.bindings.read().await.lookup_coordinator.clone()
    }

    pub(crate) async fn enrichment_backfill_context(
        &self,
    ) -> (LogbookEngine, Arc<LookupCoordinator>) {
        let bindings = self.bindings.read().await;
        (
            bindings.logbook_engine.clone(),
            bindings.lookup_coordinator.clone(),
        )
    }

    pub(crate) async fn contest_calendar_monitor(&self) -> Arc<ContestCalendarMonitor> {
        self.bindings.read().await.contest_calendar_monitor.clone()
    }

    pub(crate) async fn space_weather_monitor(&self) -> Arc<SpaceWeatherMonitor> {
        self.bindings.read().await.space_weather_monitor.clone()
    }

    pub(crate) async fn rig_control_monitor(&self) -> Arc<RigControlMonitor> {
        self.bindings.read().await.rig_control_monitor.clone()
    }

    pub(crate) async fn active_storage_backend(&self) -> String {
        self.bindings.read().await.active_storage_backend.clone()
    }

    pub(crate) async fn effective_values(&self) -> BTreeMap<String, String> {
        let config_file_values = self.config_file_values.read().await.clone();
        let overrides = self.overrides.read().await.clone();
        let base = merged_base_values(&config_file_values, &self.startup_values);
        merge_values(&base, &overrides)
    }

    pub(crate) async fn preview_config_file_values(
        &self,
        next_config_file_values: BTreeMap<String, String>,
    ) -> Result<(), String> {
        let session_station_profile_override =
            self.session_station_profile_override.read().await.clone();
        let overrides = self.overrides.read().await.clone();
        let base_values = merged_base_values(&next_config_file_values, &self.startup_values);
        let effective_values = build_effective_values(
            &base_values,
            session_station_profile_override.as_ref(),
            &overrides,
        );
        build_runtime_bindings(&effective_values).map(|_| ())
    }

    pub(crate) async fn replace_config_file_values(
        &self,
        next_config_file_values: BTreeMap<String, String>,
    ) -> Result<RuntimeConfigSnapshot, String> {
        let session_station_profile_override =
            self.session_station_profile_override.read().await.clone();
        let overrides = self.overrides.read().await.clone();
        let base_values = merged_base_values(&next_config_file_values, &self.startup_values);
        let effective_values = build_effective_values(
            &base_values,
            session_station_profile_override.as_ref(),
            &overrides,
        );
        let next_bindings = build_runtime_bindings(&effective_values)?;

        {
            let mut bindings = self.bindings.write().await;
            *bindings = next_bindings;
        }

        {
            let mut config_file_values = self.config_file_values.write().await;
            *config_file_values = next_config_file_values;
        }

        Ok(self.snapshot().await)
    }

    pub(crate) async fn set_session_station_profile_override(
        &self,
        profile: Option<StationProfile>,
    ) -> Result<Option<StationProfile>, String> {
        let normalized = profile
            .map(|profile| {
                normalize_station_profile(
                    profile,
                    normalize_optional_station_callsign,
                    normalize_optional_runtime_string,
                )
            })
            .transpose()?;
        let config_file_values = self.config_file_values.read().await.clone();
        let overrides = self.overrides.read().await.clone();
        let base_values = merged_base_values(&config_file_values, &self.startup_values);
        let effective_values =
            build_effective_values(&base_values, normalized.as_ref(), &overrides);
        let next_bindings = build_runtime_bindings(&effective_values)?;

        {
            let mut bindings = self.bindings.write().await;
            *bindings = next_bindings;
        }

        {
            let mut session_override = self.session_station_profile_override.write().await;
            session_override.clone_from(&normalized);
        }

        Ok(normalized)
    }

    pub(crate) async fn session_station_profile_override(&self) -> Option<StationProfile> {
        self.session_station_profile_override.read().await.clone()
    }

    pub(crate) async fn effective_station_profile(&self) -> Option<StationProfile> {
        self.bindings.read().await.active_station_profile.clone()
    }

    async fn swap_runtime(
        &self,
        next_overrides: BTreeMap<String, String>,
    ) -> Result<RuntimeConfigSnapshot, String> {
        let config_file_values = self.config_file_values.read().await.clone();
        let session_station_profile_override =
            self.session_station_profile_override.read().await.clone();
        let base_values = merged_base_values(&config_file_values, &self.startup_values);
        let effective_values = build_effective_values(
            &base_values,
            session_station_profile_override.as_ref(),
            &next_overrides,
        );
        let next_bindings = build_runtime_bindings(&effective_values)?;

        {
            let mut bindings = self.bindings.write().await;
            *bindings = next_bindings;
        }

        {
            let mut overrides = self.overrides.write().await;
            *overrides = next_overrides;
        }

        Ok(self.snapshot().await)
    }
}

struct ConfigFieldSpec {
    key: &'static str,
    label: &'static str,
    description: &'static str,
    kind: RuntimeConfigValueKind,
    secret: bool,
    allowed_values: &'static [&'static str],
    default_value: Option<&'static str>,
}

const STORAGE_ALLOWED_VALUES: &[&str] = &["memory", "sqlite"];
const BOOLEAN_ALLOWED_VALUES: &[&str] = &["true", "false"];

const SUPPORTED_FIELDS: &[ConfigFieldSpec] = &[
    ConfigFieldSpec {
        key: STORAGE_BACKEND_ENV_VAR,
        label: "Storage backend",
        description: "Hot-swap the active logbook storage implementation for new requests.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: STORAGE_ALLOWED_VALUES,
        default_value: Some(DEFAULT_STORAGE_BACKEND),
    },
    ConfigFieldSpec {
        key: SQLITE_PATH_ENV_VAR,
        label: "SQLite path",
        description: "SQLite database path used when the active storage backend is sqlite.",
        kind: RuntimeConfigValueKind::Path,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_SQLITE_PATH),
    },
    ConfigFieldSpec {
        key: STATION_PROFILE_NAME_ENV_VAR,
        label: "Station profile name",
        description: "Friendly label shown for the active local-station profile.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_CALLSIGN_ENV_VAR,
        label: "Station callsign",
        description: "Default local station callsign used when logging new QSOs.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_OPERATOR_CALLSIGN_ENV_VAR,
        label: "Operator callsign",
        description: "Operator callsign captured in the saved station snapshot.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_OPERATOR_NAME_ENV_VAR,
        label: "Operator name",
        description: "Human-readable operator name captured in the saved station snapshot.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_GRID_ENV_VAR,
        label: "Station grid",
        description: "Default local station Maidenhead grid square.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_COUNTY_ENV_VAR,
        label: "Station county",
        description: "Default local station county for saved QSOs.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_STATE_ENV_VAR,
        label: "Station state",
        description: "Default local station state or province.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_COUNTRY_ENV_VAR,
        label: "Station country",
        description: "Default local station country name.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_ARRL_SECTION_ENV_VAR,
        label: "Station ARRL section",
        description: "Default local station ARRL section.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_DXCC_ENV_VAR,
        label: "Station DXCC",
        description: "Default local station DXCC entity code.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_CQ_ZONE_ENV_VAR,
        label: "Station CQ zone",
        description: "Default local station CQ zone.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_ITU_ZONE_ENV_VAR,
        label: "Station ITU zone",
        description: "Default local station ITU zone.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_LATITUDE_ENV_VAR,
        label: "Station latitude",
        description: "Default local station latitude in decimal degrees.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: STATION_LONGITUDE_ENV_VAR,
        label: "Station longitude",
        description: "Default local station longitude in decimal degrees.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: QRZ_XML_USERNAME_ENV_VAR,
        label: "QRZ XML username",
        description: "QRZ XML login username for live callsign lookups.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: QRZ_XML_PASSWORD_ENV_VAR,
        label: "QRZ XML password",
        description: "QRZ XML login password. The live snapshot always redacts this value.",
        kind: RuntimeConfigValueKind::String,
        secret: true,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: QRZ_USER_AGENT_ENV_VAR,
        label: "QRZ user agent",
        description: "User agent string supplied to QRZ XML requests.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_QRZ_USER_AGENT),
    },
    ConfigFieldSpec {
        key: QRZ_XML_BASE_URL_ENV_VAR,
        label: "QRZ XML base URL",
        description: "QRZ XML endpoint used by the live lookup provider.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_QRZ_XML_BASE_URL),
    },
    ConfigFieldSpec {
        key: QRZ_HTTP_TIMEOUT_SECONDS_ENV_VAR,
        label: "QRZ HTTP timeout seconds",
        description: "HTTP timeout used by live QRZ XML requests.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("8"),
    },
    ConfigFieldSpec {
        key: QRZ_MAX_RETRIES_ENV_VAR,
        label: "QRZ max retries",
        description: "Retry count for retryable QRZ XML transport failures.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("2"),
    },
    ConfigFieldSpec {
        key: QRZ_XML_CAPTURE_ONLY_ENV_VAR,
        label: "QRZ capture-only mode",
        description:
            "When true, capture and redact the outbound QRZ request instead of sending it.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("false"),
    },
    ConfigFieldSpec {
        key: QRZ_LOGBOOK_API_KEY_ENV_VAR,
        label: "QRZ logbook API key",
        description: "API key for bidirectional sync with the QRZ logbook.",
        kind: RuntimeConfigValueKind::String,
        secret: true,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: QRZ_LOGBOOK_BASE_URL_ENV_VAR,
        label: "QRZ logbook base URL",
        description: "QRZ logbook API endpoint URL.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_QRZ_LOGBOOK_BASE_URL),
    },
    ConfigFieldSpec {
        key: LOTW_USERNAME_ENV_VAR,
        label: "LoTW username",
        description: "LoTW website account name used for confirmation downloads.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: LOTW_PASSWORD_ENV_VAR,
        label: "LoTW password",
        description: "LoTW website password. The live snapshot always redacts this value.",
        kind: RuntimeConfigValueKind::String,
        secret: true,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: LOTW_TQSL_PATH_ENV_VAR,
        label: "TQSL executable",
        description: "Path or command name for the local TQSL executable.",
        kind: RuntimeConfigValueKind::Path,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_LOTW_TQSL_PATH),
    },
    ConfigFieldSpec {
        key: LOTW_STATION_LOCATION_ENV_VAR,
        label: "TQSL station location",
        description: "Existing TQSL station location used to sign QSO records.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: LOTW_CERTIFICATE_PASSWORD_ENV_VAR,
        label: "TQSL certificate password",
        description: "Optional TQSL certificate password. The live snapshot always redacts this value.",
        kind: RuntimeConfigValueKind::String,
        secret: true,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: LOTW_REPORT_URL_ENV_VAR,
        label: "LoTW report URL",
        description: "HTTPS endpoint used to download LoTW confirmation reports.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_LOTW_REPORT_URL),
    },
    ConfigFieldSpec {
        key: LOTW_TIMEOUT_SECONDS_ENV_VAR,
        label: "LoTW timeout seconds",
        description: "Timeout for LoTW report requests and TQSL execution.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_LOTW_TIMEOUT_SECONDS),
    },
    ConfigFieldSpec {
        key: SYNC_AUTO_ENABLED_ENV_VAR,
        label: "Sync auto-enabled",
        description: "Whether the engine should automatically sync with the QRZ logbook on a periodic schedule.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some(DEFAULT_SYNC_AUTO_ENABLED),
    },
    ConfigFieldSpec {
        key: SYNC_INTERVAL_SECONDS_ENV_VAR,
        label: "Sync interval seconds",
        description: "Interval between automatic QRZ logbook syncs, in seconds.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_SYNC_INTERVAL_SECONDS),
    },
    ConfigFieldSpec {
        key: SYNC_CONFLICT_POLICY_ENV_VAR,
        label: "Sync conflict policy",
        description: "How to handle conflicting records during QRZ logbook sync.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: CONFLICT_POLICY_ALLOWED_VALUES,
        default_value: Some(DEFAULT_SYNC_CONFLICT_POLICY),
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_ENABLED_ENV_VAR,
        label: "WSJT-X ingest enabled",
        description: "Enable automatic ingestion of QSOs logged by WSJT-X.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("false"),
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_UDP_ENABLED_ENV_VAR,
        label: "WSJT-X UDP ingest enabled",
        description: "Listen for WSJT-X real-time Logged ADIF UDP messages.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("true"),
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_UDP_BIND_ENV_VAR,
        label: "WSJT-X UDP bind",
        description: "Local host:port for the WSJT-X UDP listener.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_WSJTX_INGEST_UDP_BIND),
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR,
        label: "WSJT-X ADIF tail enabled",
        description: "Poll WSJT-X's wsjtx_log.adi file for recovery ingestion.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("false"),
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_ADIF_TAIL_PATH_ENV_VAR,
        label: "WSJT-X ADIF tail path",
        description: "Path to WSJT-X's wsjtx_log.adi file.",
        kind: RuntimeConfigValueKind::Path,
        secret: false,
        allowed_values: &[],
        default_value: None,
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR,
        label: "WSJT-X ADIF poll interval",
        description: "Polling interval for WSJT-X ADIF tail recovery, in milliseconds.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_WSJTX_INGEST_POLL_INTERVAL_MS),
    },
    ConfigFieldSpec {
        key: WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR,
        label: "WSJT-X sync to QRZ",
        description: "Immediately upload imported WSJT-X QSOs to QRZ Logbook.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("false"),
    },
    ConfigFieldSpec {
        key: CONTEST_CALENDAR_ENABLED_ENV_VAR,
        label: "Contest calendar enabled",
        description: "Enable live contest calendar fetching.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("true"),
    },
    ConfigFieldSpec {
        key: CONTEST_CALENDAR_RSS_URL_ENV_VAR,
        label: "Contest calendar RSS URL",
        description: "RSS endpoint used for contest calendar metadata.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_CONTEST_CALENDAR_RSS_URL),
    },
    ConfigFieldSpec {
        key: CONTEST_CALENDAR_HTTP_TIMEOUT_SECONDS_ENV_VAR,
        label: "Contest calendar HTTP timeout seconds",
        description: "HTTP timeout used by contest calendar requests.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("8"),
    },
    ConfigFieldSpec {
        key: CONTEST_CALENDAR_REFRESH_INTERVAL_SECONDS_ENV_VAR,
        label: "Contest calendar refresh interval seconds",
        description: "How long the engine caches contest calendar metadata before refreshing.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("3600"),
    },
    ConfigFieldSpec {
        key: CONTEST_CALENDAR_STALE_AFTER_SECONDS_ENV_VAR,
        label: "Contest calendar stale after seconds",
        description: "When cached contest calendar metadata should be marked stale.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("86400"),
    },
    ConfigFieldSpec {
        key: CONTEST_CALENDAR_DETAILS_PATH_ENV_VAR,
        label: "Contest calendar details path",
        description: "Optional reviewed local JSON catalog that enriches contest calendar entries.",
        kind: RuntimeConfigValueKind::Path,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_CONTEST_CALENDAR_DETAILS_PATH),
    },
    ConfigFieldSpec {
        key: NOAA_SPACE_WEATHER_ENABLED_ENV_VAR,
        label: "NOAA space weather enabled",
        description: "Enable live NOAA SWPC current space weather fetching.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("true"),
    },
    ConfigFieldSpec {
        key: NOAA_KP_INDEX_URL_ENV_VAR,
        label: "NOAA K-index URL",
        description: "NOAA SWPC endpoint used for planetary K-index and running A-index data.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(qsoripper_core::space_weather::DEFAULT_NOAA_KP_INDEX_URL),
    },
    ConfigFieldSpec {
        key: NOAA_SOLAR_INDICES_URL_ENV_VAR,
        label: "NOAA solar indices URL",
        description: "NOAA SWPC endpoint used for daily solar flux and sunspot data.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(qsoripper_core::space_weather::DEFAULT_NOAA_SOLAR_INDICES_URL),
    },
    ConfigFieldSpec {
        key: NOAA_HTTP_TIMEOUT_SECONDS_ENV_VAR,
        label: "NOAA HTTP timeout seconds",
        description: "HTTP timeout used by live NOAA SWPC requests.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("8"),
    },
    ConfigFieldSpec {
        key: NOAA_REFRESH_INTERVAL_SECONDS_ENV_VAR,
        label: "NOAA refresh interval seconds",
        description:
            "How long the engine caches a current space weather snapshot before refreshing.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("900"),
    },
    ConfigFieldSpec {
        key: NOAA_STALE_AFTER_SECONDS_ENV_VAR,
        label: "NOAA stale after seconds",
        description:
            "When cached space weather data should be marked stale if refreshes fail or lag.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("3600"),
    },
    ConfigFieldSpec {
        key: RIGCTLD_ENABLED_ENV_VAR,
        label: "Rig control enabled",
        description: "Enable live rig control via rigctld.",
        kind: RuntimeConfigValueKind::Boolean,
        secret: false,
        allowed_values: BOOLEAN_ALLOWED_VALUES,
        default_value: Some("false"),
    },
    ConfigFieldSpec {
        key: RIGCTLD_HOST_ENV_VAR,
        label: "Rig control host",
        description: "Host address of the running rigctld daemon.",
        kind: RuntimeConfigValueKind::String,
        secret: false,
        allowed_values: &[],
        default_value: Some(DEFAULT_RIGCTLD_HOST),
    },
    ConfigFieldSpec {
        key: RIGCTLD_PORT_ENV_VAR,
        label: "Rig control port",
        description: "TCP port used to connect to the rigctld daemon.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("4532"),
    },
    ConfigFieldSpec {
        key: RIGCTLD_READ_TIMEOUT_MS_ENV_VAR,
        label: "Rig control read timeout ms",
        description: "Read timeout in milliseconds for rigctld TCP operations.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("2000"),
    },
    ConfigFieldSpec {
        key: RIGCTLD_STALE_THRESHOLD_MS_ENV_VAR,
        label: "Rig control stale threshold ms",
        description: "How long a cached rig snapshot is considered fresh before re-polling.",
        kind: RuntimeConfigValueKind::Integer,
        secret: false,
        allowed_values: &[],
        default_value: Some("200"),
    },
];

fn capture_supported_env() -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();

    for field in SUPPORTED_FIELDS {
        if let Ok(value) = std::env::var(field.key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                values.insert(field.key.to_string(), trimmed.to_string());
            }
        }
    }

    values
}

fn apply_mutation(
    overrides: &mut BTreeMap<String, String>,
    mutation: RuntimeConfigMutation,
) -> Result<(), String> {
    let key = canonical_key(&mutation.key)?;
    let action = RuntimeConfigMutationKind::try_from(mutation.kind)
        .map_err(|_| format!("Unsupported mutation kind for '{key}'."))?;

    match action {
        RuntimeConfigMutationKind::Set => {
            let value = mutation
                .value
                .ok_or_else(|| format!("A value is required when setting '{key}'."))?;
            let normalized = normalize_value(key, &value)?;
            overrides.insert(key.to_string(), normalized);
        }
        RuntimeConfigMutationKind::Clear => {
            overrides.remove(key);
        }
        RuntimeConfigMutationKind::Unspecified => {
            return Err(format!("A mutation kind is required for '{key}'."));
        }
    }

    Ok(())
}

fn canonical_key(raw_key: &str) -> Result<&'static str, String> {
    let trimmed = raw_key.trim();
    SUPPORTED_FIELDS
        .iter()
        .find(|field| field.key.eq_ignore_ascii_case(trimmed))
        .map(|field| field.key)
        .ok_or_else(|| format!("Unsupported runtime config key '{trimmed}'."))
}

fn normalize_value(key: &'static str, raw_value: &str) -> Result<String, String> {
    let trimmed = raw_value.trim();
    if trimmed.is_empty() {
        return Err(format!("A non-empty value is required for '{key}'."));
    }

    if key == STORAGE_BACKEND_ENV_VAR {
        parse_storage_backend(trimmed)
            .map_err(|error| error.to_string())
            .map(|_| trimmed.to_ascii_lowercase())
    } else if is_boolean_key(key) {
        normalize_bool_value(trimmed)
    } else if key == QRZ_HTTP_TIMEOUT_SECONDS_ENV_VAR {
        trimmed
            .parse::<u64>()
            .map(|value| value.to_string())
            .map_err(|_| format!("'{key}' expects an integer value."))
    } else if key == QRZ_MAX_RETRIES_ENV_VAR {
        trimmed
            .parse::<u32>()
            .map(|value| value.to_string())
            .map_err(|_| format!("'{key}' expects an integer value."))
    } else if matches!(
        key,
        NOAA_HTTP_TIMEOUT_SECONDS_ENV_VAR
            | NOAA_REFRESH_INTERVAL_SECONDS_ENV_VAR
            | NOAA_STALE_AFTER_SECONDS_ENV_VAR
    ) {
        trimmed
            .parse::<u64>()
            .map(|value| value.to_string())
            .map_err(|_| format!("'{key}' expects an integer value."))
    } else if is_station_positive_integer_key(key) {
        parse_positive_integer(key, trimmed).map(|value| value.to_string())
    } else if key == STATION_LATITUDE_ENV_VAR {
        parse_bounded_f64(key, trimmed, -90.0, 90.0).map(|value| value.to_string())
    } else if key == STATION_LONGITUDE_ENV_VAR {
        parse_bounded_f64(key, trimmed, -180.0, 180.0).map(|value| value.to_string())
    } else if key == SYNC_INTERVAL_SECONDS_ENV_VAR || key == LOTW_TIMEOUT_SECONDS_ENV_VAR {
        trimmed
            .parse::<u32>()
            .map(|value| value.to_string())
            .map_err(|_| format!("'{key}' expects an integer value."))
    } else if key == SYNC_CONFLICT_POLICY_ENV_VAR {
        let lower = trimmed.to_ascii_lowercase();
        if CONFLICT_POLICY_ALLOWED_VALUES.contains(&lower.as_str()) {
            Ok(lower)
        } else {
            Err(format!(
                "'{key}' must be one of: {}.",
                CONFLICT_POLICY_ALLOWED_VALUES.join(", ")
            ))
        }
    } else if matches!(
        key,
        RIGCTLD_PORT_ENV_VAR | RIGCTLD_READ_TIMEOUT_MS_ENV_VAR | RIGCTLD_STALE_THRESHOLD_MS_ENV_VAR
    ) {
        trimmed
            .parse::<u64>()
            .map(|value| value.to_string())
            .map_err(|_| format!("'{key}' expects an integer value."))
    } else if key == WSJTX_INGEST_UDP_BIND_ENV_VAR {
        validate_host_port(trimmed, key).map(|()| trimmed.to_string())
    } else if key == WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR {
        parse_positive_integer(key, trimmed).map(|value| value.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn is_boolean_key(key: &str) -> bool {
    matches!(
        key,
        QRZ_XML_CAPTURE_ONLY_ENV_VAR
            | NOAA_SPACE_WEATHER_ENABLED_ENV_VAR
            | SYNC_AUTO_ENABLED_ENV_VAR
            | RIGCTLD_ENABLED_ENV_VAR
            | WSJTX_INGEST_ENABLED_ENV_VAR
            | WSJTX_INGEST_UDP_ENABLED_ENV_VAR
            | WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR
            | WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR
    )
}

fn normalize_bool_value(raw_value: &str) -> Result<String, String> {
    parse_bool(raw_value).map(|value| {
        if value {
            "true".to_string()
        } else {
            "false".to_string()
        }
    })
}

fn parse_bool(raw_value: &str) -> Result<bool, String> {
    match raw_value.trim().to_ascii_lowercase().as_str() {
        "1" | "true" | "yes" | "y" | "on" => Ok(true),
        "0" | "false" | "no" | "n" | "off" => Ok(false),
        _ => Err(format!(
            "'{raw_value}' is not a valid boolean. Use true/false, yes/no, 1/0, y/n, or on/off."
        )),
    }
}

fn parse_positive_integer(key: &str, raw_value: &str) -> Result<u32, String> {
    let value = raw_value
        .parse::<u32>()
        .map_err(|_| format!("'{key}' expects an integer value."))?;
    if value == 0 {
        return Err(format!("'{key}' expects an integer greater than 0."));
    }
    Ok(value)
}

fn parse_bounded_f64(key: &str, raw_value: &str, min: f64, max: f64) -> Result<f64, String> {
    let value = raw_value
        .parse::<f64>()
        .map_err(|_| format!("'{key}' expects a decimal value."))?;
    if !value.is_finite() {
        return Err(format!("'{key}' expects a finite decimal value."));
    }
    if value < min || value > max {
        return Err(format!("'{key}' must be between {min} and {max}."));
    }
    Ok(value)
}

fn validate_host_port(bind: &str, label: &str) -> Result<(), String> {
    let (host, port) = bind
        .rsplit_once(':')
        .ok_or_else(|| format!("'{label}' must be in host:port form."))?;
    if host.trim().is_empty() {
        return Err(format!("'{label}' must include a host."));
    }
    let port: u32 = port
        .parse()
        .map_err(|_| format!("'{label}' port is not a number."))?;
    if port == 0 || port > u32::from(u16::MAX) {
        return Err(format!("'{label}' port must be between 1 and 65535."));
    }
    Ok(())
}

fn is_station_positive_integer_key(key: &str) -> bool {
    matches!(
        key,
        STATION_DXCC_ENV_VAR | STATION_CQ_ZONE_ENV_VAR | STATION_ITU_ZONE_ENV_VAR
    )
}

fn merge_values(
    base_values: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut merged = base_values.clone();
    merged.extend(overrides.clone());
    merged
}

fn merged_base_values(
    config_file_values: &BTreeMap<String, String>,
    startup_values: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    merge_values(config_file_values, startup_values)
}

fn build_effective_values(
    base_values: &BTreeMap<String, String>,
    session_station_profile_override: Option<&StationProfile>,
    overrides: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let session_values = station_profile_override_values(session_station_profile_override);
    let mut with_session_override = base_values.clone();
    if session_station_profile_override.is_some() {
        for key in [
            STATION_PROFILE_NAME_ENV_VAR,
            STATION_CALLSIGN_ENV_VAR,
            STATION_OPERATOR_CALLSIGN_ENV_VAR,
            STATION_OPERATOR_NAME_ENV_VAR,
            STATION_GRID_ENV_VAR,
            STATION_COUNTY_ENV_VAR,
            STATION_STATE_ENV_VAR,
            STATION_COUNTRY_ENV_VAR,
            STATION_ARRL_SECTION_ENV_VAR,
            STATION_DXCC_ENV_VAR,
            STATION_CQ_ZONE_ENV_VAR,
            STATION_ITU_ZONE_ENV_VAR,
            STATION_LATITUDE_ENV_VAR,
            STATION_LONGITUDE_ENV_VAR,
        ] {
            with_session_override.remove(key);
        }
    }
    with_session_override.extend(session_values);
    merge_values(&with_session_override, overrides)
}

fn station_profile_override_values(profile: Option<&StationProfile>) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    if let Some(profile) = profile {
        insert_station_profile_runtime_values(&mut values, profile);
    }
    values
}

fn build_runtime_bindings(values: &BTreeMap<String, String>) -> Result<RuntimeBindings, String> {
    validate_wsjtx_ingest_values(values)?;
    let storage = build_storage(
        &parse_storage_options_from_values(values).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    let logbook_engine = LogbookEngine::new(Arc::clone(&storage));
    let active_storage_backend = logbook_engine.storage_backend_name().to_string();
    let (provider, lookup_provider_summary) = build_lookup_provider(values);
    let lookup_coordinator = Arc::new(LookupCoordinator::with_snapshot_store(
        provider,
        LookupCoordinatorConfig::default(),
        storage,
    ));
    let contest_calendar_monitor = build_contest_calendar_monitor(values);
    let space_weather_monitor = build_space_weather_monitor(values);
    let rig_control_monitor = build_rig_control_monitor(values);
    let active_station_profile = build_active_station_profile(values)?;

    Ok(RuntimeBindings {
        logbook_engine,
        lookup_coordinator,
        contest_calendar_monitor,
        space_weather_monitor,
        rig_control_monitor,
        active_storage_backend,
        lookup_provider_summary,
        active_station_profile,
    })
}

fn validate_wsjtx_ingest_values(values: &BTreeMap<String, String>) -> Result<(), String> {
    for key in [
        WSJTX_INGEST_ENABLED_ENV_VAR,
        WSJTX_INGEST_UDP_ENABLED_ENV_VAR,
        WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR,
        WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR,
    ] {
        if let Some(value) = values.get(key) {
            parse_bool(value)?;
        }
    }

    if let Some(bind) = values.get(WSJTX_INGEST_UDP_BIND_ENV_VAR) {
        validate_host_port(bind, WSJTX_INGEST_UDP_BIND_ENV_VAR)?;
    }

    if let Some(interval) = values.get(WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR) {
        parse_positive_integer(WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR, interval)?;
    }

    Ok(())
}

fn build_active_station_profile(
    values: &BTreeMap<String, String>,
) -> Result<Option<StationProfile>, String> {
    let profile = StationProfile {
        profile_name: values.get(STATION_PROFILE_NAME_ENV_VAR).cloned(),
        station_callsign: values
            .get(STATION_CALLSIGN_ENV_VAR)
            .cloned()
            .unwrap_or_default(),
        operator_callsign: values.get(STATION_OPERATOR_CALLSIGN_ENV_VAR).cloned(),
        operator_name: values.get(STATION_OPERATOR_NAME_ENV_VAR).cloned(),
        grid: values.get(STATION_GRID_ENV_VAR).cloned(),
        county: values.get(STATION_COUNTY_ENV_VAR).cloned(),
        state: values.get(STATION_STATE_ENV_VAR).cloned(),
        country: values.get(STATION_COUNTRY_ENV_VAR).cloned(),
        arrl_section: values.get(STATION_ARRL_SECTION_ENV_VAR).cloned(),
        dxcc: parse_optional_positive_integer(values, STATION_DXCC_ENV_VAR)?,
        cq_zone: parse_optional_positive_integer(values, STATION_CQ_ZONE_ENV_VAR)?,
        itu_zone: parse_optional_positive_integer(values, STATION_ITU_ZONE_ENV_VAR)?,
        latitude: parse_optional_bounded_f64(values, STATION_LATITUDE_ENV_VAR, -90.0, 90.0)?,
        longitude: parse_optional_bounded_f64(values, STATION_LONGITUDE_ENV_VAR, -180.0, 180.0)?,
    };

    Ok(station_profile_has_values(&profile).then_some(profile))
}

fn parse_optional_positive_integer(
    values: &BTreeMap<String, String>,
    key: &str,
) -> Result<Option<u32>, String> {
    values
        .get(key)
        .map(|value| parse_positive_integer(key, value))
        .transpose()
}

fn parse_optional_bounded_f64(
    values: &BTreeMap<String, String>,
    key: &str,
    min: f64,
    max: f64,
) -> Result<Option<f64>, String> {
    values
        .get(key)
        .map(|value| parse_bounded_f64(key, value, min, max))
        .transpose()
}

fn parse_storage_options_from_values(
    values: &BTreeMap<String, String>,
) -> Result<StorageOptions, Box<dyn std::error::Error>> {
    let backend = parse_storage_backend(
        values
            .get(STORAGE_BACKEND_ENV_VAR)
            .map_or(DEFAULT_STORAGE_BACKEND, String::as_str),
    )?;
    let sqlite_path = values
        .get(SQLITE_PATH_ENV_VAR)
        .cloned()
        .unwrap_or_else(|| DEFAULT_SQLITE_PATH.to_string())
        .into();

    Ok(StorageOptions {
        backend,
        sqlite_path,
    })
}

fn build_lookup_provider(values: &BTreeMap<String, String>) -> (Arc<dyn CallsignProvider>, String) {
    // Derive a user agent fallback from the username when no explicit value is
    // configured, matching the pattern used by TestQrzCredentials.
    let derived_user_agent = values
        .get(QRZ_XML_USERNAME_ENV_VAR)
        .filter(|u| !u.trim().is_empty())
        .map(|u| format!("{DEFAULT_QRZ_USER_AGENT} ({u})"));

    match QrzXmlConfig::from_value_provider(|name| {
        values.get(name).cloned().or_else(|| {
            if name == QRZ_USER_AGENT_ENV_VAR {
                derived_user_agent.clone()
            } else {
                None
            }
        })
    }) {
        Ok(config) => match QrzXmlProvider::new(config.clone()) {
            Ok(provider) => {
                let summary = if config.capture_only() {
                    format!("QRZ XML capture-only via {}", config.base_url())
                } else {
                    format!("QRZ XML live via {}", config.base_url())
                };
                (Arc::new(provider), summary)
            }
            Err(error) => {
                let reason = error.to_string();
                (
                    Arc::new(DisabledCallsignProvider::new(reason.clone())),
                    format!("Disabled: {reason}"),
                )
            }
        },
        Err(error) => {
            let reason = error.to_string();
            (
                Arc::new(DisabledCallsignProvider::new(reason.clone())),
                format!("Disabled: {reason}"),
            )
        }
    }
}

fn build_contest_calendar_monitor(
    values: &BTreeMap<String, String>,
) -> Arc<ContestCalendarMonitor> {
    match Wa7bnmContestCalendarConfig::from_value_provider(|name| values.get(name).cloned()) {
        Ok(config) => {
            let provider: Arc<dyn ContestCalendarProvider> = if config.enabled() {
                match Wa7bnmContestCalendarProvider::new(config.clone()) {
                    Ok(provider) => {
                        let provider: Arc<dyn ContestCalendarProvider> = Arc::new(provider);
                        if let Some(path) = config.details_path().filter(|path| path.exists()) {
                            match ContestDetailsCatalog::load(path) {
                                Ok(catalog) => Arc::new(
                                    CatalogEnrichingContestCalendarProvider::new(provider, catalog),
                                ),
                                Err(error) => Arc::new(DisabledContestCalendarProvider::new(
                                    error.to_string(),
                                )),
                            }
                        } else if config.details_path_is_explicit() {
                            Arc::new(DisabledContestCalendarProvider::new(format!(
                                "Contest calendar details catalog does not exist: {}",
                                config
                                    .details_path()
                                    .map_or_else(String::new, |path| path.display().to_string())
                            )))
                        } else {
                            provider
                        }
                    }
                    Err(error) => Arc::new(DisabledContestCalendarProvider::new(error.to_string())),
                }
            } else {
                Arc::new(DisabledContestCalendarProvider::new(
                    "Contest calendar fetching is disabled.",
                ))
            };
            Arc::new(ContestCalendarMonitor::new(
                provider,
                config.refresh_interval(),
                config.stale_after(),
            ))
        }
        Err(error) => Arc::new(ContestCalendarMonitor::new(
            Arc::new(DisabledContestCalendarProvider::new(error)),
            std::time::Duration::from_secs(DEFAULT_CONTEST_CALENDAR_REFRESH_INTERVAL_SECONDS),
            std::time::Duration::from_secs(DEFAULT_CONTEST_CALENDAR_STALE_AFTER_SECONDS),
        )),
    }
}

fn build_space_weather_monitor(values: &BTreeMap<String, String>) -> Arc<SpaceWeatherMonitor> {
    match NoaaSpaceWeatherConfig::from_value_provider(|name| values.get(name).cloned()) {
        Ok(config) => {
            let provider: Arc<dyn SpaceWeatherProvider> = if config.enabled() {
                match NoaaSpaceWeatherProvider::new(config.clone()) {
                    Ok(provider) => Arc::new(provider),
                    Err(error) => Arc::new(DisabledSpaceWeatherProvider::new(error.to_string())),
                }
            } else {
                Arc::new(DisabledSpaceWeatherProvider::new(
                    "NOAA space weather fetching is disabled.",
                ))
            };
            Arc::new(SpaceWeatherMonitor::new(
                provider,
                config.refresh_interval(),
                config.stale_after(),
            ))
        }
        Err(error) => Arc::new(SpaceWeatherMonitor::new(
            Arc::new(DisabledSpaceWeatherProvider::new(error.to_string())),
            std::time::Duration::from_secs(900),
            std::time::Duration::from_secs(3600),
        )),
    }
}

fn build_rig_control_monitor(values: &BTreeMap<String, String>) -> Arc<RigControlMonitor> {
    let stale_threshold_ms = values
        .get(RIGCTLD_STALE_THRESHOLD_MS_ENV_VAR)
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RIGCTLD_STALE_THRESHOLD_MS);
    let stale_threshold = std::time::Duration::from_millis(stale_threshold_ms);

    if let Some(config) = RigctldConfig::from_value_provider(|name| values.get(name).cloned()) {
        let provider: Arc<dyn RigControlProvider> = Arc::new(RigctldProvider::new(config));
        Arc::new(RigControlMonitor::new(provider, stale_threshold))
    } else {
        let provider: Arc<dyn RigControlProvider> = Arc::new(DisabledRigControlProvider::new(
            "Rig control is not enabled.",
        ));
        Arc::new(RigControlMonitor::new(provider, stale_threshold))
    }
}

fn build_snapshot(
    base_values: &BTreeMap<String, String>,
    session_station_profile_override: Option<&StationProfile>,
    overrides: &BTreeMap<String, String>,
    bindings: &RuntimeBindings,
) -> RuntimeConfigSnapshot {
    let merged = build_effective_values(base_values, session_station_profile_override, overrides);
    let session_values = station_profile_override_values(session_station_profile_override);
    let definitions = SUPPORTED_FIELDS
        .iter()
        .map(|field| RuntimeConfigDefinition {
            key: field.key.to_string(),
            label: field.label.to_string(),
            description: field.description.to_string(),
            kind: field.kind as i32,
            secret: field.secret,
            allowed_values: field
                .allowed_values
                .iter()
                .map(|value| (*value).to_string())
                .collect(),
            required: false,
            default_value: field.default_value.map(str::to_string),
        })
        .collect();
    let values = SUPPORTED_FIELDS
        .iter()
        .map(|field| {
            build_value(
                field,
                &merged,
                overrides,
                session_values.contains_key(field.key),
            )
        })
        .collect();
    let mut warnings = Vec::new();

    if overrides.contains_key(STORAGE_BACKEND_ENV_VAR)
        || overrides.contains_key(SQLITE_PATH_ENV_VAR)
    {
        warnings.push(
            "Switching storage backends swaps the active engine state; records are not migrated automatically."
                .to_string(),
        );
    }
    if session_station_profile_override.is_some() {
        warnings.push(
            "A process-session station override is active; new QSOs use it until the override is cleared."
                .to_string(),
        );
    }

    let persistence_summary = match bindings.active_storage_backend.as_str() {
        "memory" => "In-memory logbook".to_string(),
        "sqlite" => "SQLite logbook".to_string(),
        other if !other.is_empty() => other.to_string(),
        _ => "Unspecified persistence".to_string(),
    };
    let persistence_location = if bindings.active_storage_backend == "sqlite" {
        merged.get(SQLITE_PATH_ENV_VAR).cloned()
    } else {
        None
    };

    RuntimeConfigSnapshot {
        definitions,
        values,
        active_storage_backend: bindings.active_storage_backend.clone(),
        lookup_provider_summary: bindings.lookup_provider_summary.clone(),
        warnings,
        active_station_profile: bindings.active_station_profile.clone(),
        persistence_summary,
        persistence_location,
    }
}

fn normalize_optional_runtime_string(value: Option<&str>) -> Option<String> {
    let trimmed = value?.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

fn normalize_optional_station_callsign(value: Option<&str>) -> Option<String> {
    normalize_optional_runtime_string(value).map(|value| normalize_callsign(&value))
}

fn build_value(
    field: &ConfigFieldSpec,
    merged: &BTreeMap<String, String>,
    overrides: &BTreeMap<String, String>,
    session_override: bool,
) -> RuntimeConfigValue {
    let effective_value = merged
        .get(field.key)
        .cloned()
        .or_else(|| field.default_value.map(str::to_string));
    let has_value = effective_value.is_some();
    let display_value = if field.secret && has_value {
        REDACTED_VALUE.to_string()
    } else {
        effective_value.unwrap_or_default()
    };

    RuntimeConfigValue {
        key: field.key.to_string(),
        has_value,
        display_value,
        overridden: overrides.contains_key(field.key),
        secret: field.secret,
        redacted: field.secret && has_value,
        source: if overrides.contains_key(field.key) {
            RuntimeConfigValueSource::RuntimeOverride as i32
        } else if session_override {
            RuntimeConfigValueSource::SessionOverride as i32
        } else if merged.contains_key(field.key) {
            RuntimeConfigValueSource::BaseConfig as i32
        } else if field.default_value.is_some() {
            RuntimeConfigValueSource::Default as i32
        } else {
            RuntimeConfigValueSource::Unspecified as i32
        },
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::time::{SystemTime, UNIX_EPOCH};

    use qsoripper_core::proto::qsoripper::domain::{Band, Mode, QsoRecord, StationProfile};
    use qsoripper_core::proto::qsoripper::services::RuntimeConfigValueSource;

    use super::{
        ApplyRuntimeConfigRequest, ResetRuntimeConfigRequest, RuntimeConfigManager,
        RuntimeConfigMutation, RuntimeConfigMutationKind, QRZ_USER_AGENT_ENV_VAR,
        QRZ_XML_CAPTURE_ONLY_ENV_VAR, QRZ_XML_PASSWORD_ENV_VAR, QRZ_XML_USERNAME_ENV_VAR,
        SQLITE_PATH_ENV_VAR, STATION_ARRL_SECTION_ENV_VAR, STATION_CALLSIGN_ENV_VAR,
        STATION_GRID_ENV_VAR, STATION_LATITUDE_ENV_VAR, STATION_OPERATOR_CALLSIGN_ENV_VAR,
        STORAGE_BACKEND_ENV_VAR,
    };

    fn sample_qso(local_id: &str) -> QsoRecord {
        QsoRecord {
            local_id: local_id.to_string(),
            station_callsign: "K7DBG".to_string(),
            worked_callsign: "W1AW".to_string(),
            utc_timestamp: Some(prost_types::Timestamp {
                seconds: 1_731_600_000,
                nanos: 0,
            }),
            band: Band::Band20m as i32,
            mode: Mode::Ssb as i32,
            ..QsoRecord::default()
        }
    }

    fn unique_sqlite_path() -> String {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        std::env::temp_dir()
            .join(format!(
                "qsoripper-runtime-config-{}-{suffix}.db",
                std::process::id()
            ))
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn runtime_snapshot_reports_defaults_and_value_sources() {
        let manager = RuntimeConfigManager::new(BTreeMap::new()).expect("manager");
        let snapshot = manager.snapshot().await;
        let storage_definition = snapshot
            .definitions
            .iter()
            .find(|definition| definition.key == STORAGE_BACKEND_ENV_VAR)
            .expect("storage definition");
        let storage_value = snapshot
            .values
            .iter()
            .find(|value| value.key == STORAGE_BACKEND_ENV_VAR)
            .expect("storage value");

        assert_eq!(Some("memory"), storage_definition.default_value.as_deref());
        assert_eq!("memory", storage_value.display_value);
        assert_eq!(
            RuntimeConfigValueSource::Default as i32,
            storage_value.source
        );

        let overridden = manager
            .apply_request(ApplyRuntimeConfigRequest {
                mutations: vec![RuntimeConfigMutation {
                    key: STORAGE_BACKEND_ENV_VAR.to_string(),
                    kind: RuntimeConfigMutationKind::Set as i32,
                    value: Some("memory".to_string()),
                }],
            })
            .await
            .expect("runtime override");
        let overridden_value = overridden
            .values
            .iter()
            .find(|value| value.key == STORAGE_BACKEND_ENV_VAR)
            .expect("overridden storage value");
        assert_eq!(
            RuntimeConfigValueSource::RuntimeOverride as i32,
            overridden_value.source
        );
    }

    #[tokio::test]
    async fn runtime_manager_hot_swaps_storage_backends() {
        let manager = RuntimeConfigManager::new(BTreeMap::new()).expect("manager");
        let initial_snapshot = manager.snapshot().await;
        assert_eq!("memory", initial_snapshot.active_storage_backend);

        let sqlite_path = unique_sqlite_path();
        let sqlite_snapshot = manager
            .apply_request(ApplyRuntimeConfigRequest {
                mutations: vec![
                    RuntimeConfigMutation {
                        key: STORAGE_BACKEND_ENV_VAR.to_string(),
                        kind: RuntimeConfigMutationKind::Set as i32,
                        value: Some("sqlite".to_string()),
                    },
                    RuntimeConfigMutation {
                        key: SQLITE_PATH_ENV_VAR.to_string(),
                        kind: RuntimeConfigMutationKind::Set as i32,
                        value: Some(sqlite_path.clone()),
                    },
                ],
            })
            .await
            .expect("sqlite apply");
        assert_eq!("sqlite", sqlite_snapshot.active_storage_backend);

        let sqlite_engine = manager.logbook_engine().await;
        sqlite_engine
            .log_qso(sample_qso("sqlite-record"))
            .await
            .expect("sqlite write");
        drop(sqlite_engine);

        let memory_snapshot = manager
            .apply_request(ApplyRuntimeConfigRequest {
                mutations: vec![RuntimeConfigMutation {
                    key: STORAGE_BACKEND_ENV_VAR.to_string(),
                    kind: RuntimeConfigMutationKind::Set as i32,
                    value: Some("memory".to_string()),
                }],
            })
            .await
            .expect("memory apply");
        assert_eq!("memory", memory_snapshot.active_storage_backend);

        let memory_status = manager
            .logbook_engine()
            .await
            .get_sync_status()
            .await
            .expect("memory status");
        assert_eq!(0, memory_status.local_qso_count);

        let reset_snapshot = manager
            .reset_request(ResetRuntimeConfigRequest {
                keys: vec![
                    STORAGE_BACKEND_ENV_VAR.to_string(),
                    SQLITE_PATH_ENV_VAR.to_string(),
                ],
            })
            .await
            .expect("reset");
        assert_eq!("memory", reset_snapshot.active_storage_backend);
        drop(manager);

        let sqlite_file = std::path::PathBuf::from(sqlite_path);
        if sqlite_file.exists() {
            std::fs::remove_file(sqlite_file).expect("remove sqlite file");
        }
    }

    #[tokio::test]
    async fn runtime_manager_updates_lookup_provider_summary_when_capture_only_changes() {
        let mut base_values = BTreeMap::new();
        base_values.insert(QRZ_XML_USERNAME_ENV_VAR.to_string(), "KC7AVA".to_string());
        base_values.insert(
            QRZ_XML_PASSWORD_ENV_VAR.to_string(),
            "super-secret-password".to_string(),
        );
        base_values.insert(
            QRZ_USER_AGENT_ENV_VAR.to_string(),
            "QsoRipper/0.1.0 (KC7AVA)".to_string(),
        );

        let manager = RuntimeConfigManager::new(base_values).expect("manager");
        let initial_summary = manager.snapshot().await.lookup_provider_summary;
        assert_contains(&initial_summary, "QRZ XML live");

        let capture_only_summary = manager
            .apply_request(ApplyRuntimeConfigRequest {
                mutations: vec![RuntimeConfigMutation {
                    key: QRZ_XML_CAPTURE_ONLY_ENV_VAR.to_string(),
                    kind: RuntimeConfigMutationKind::Set as i32,
                    value: Some("true".to_string()),
                }],
            })
            .await
            .expect("capture-only apply")
            .lookup_provider_summary;
        assert_contains(&capture_only_summary, "capture-only");
    }

    #[tokio::test]
    async fn runtime_manager_exposes_active_station_profile_in_snapshot() {
        let mut base_values = BTreeMap::new();
        base_values.insert(STATION_CALLSIGN_ENV_VAR.to_string(), "K7RND".to_string());
        base_values.insert(
            STATION_OPERATOR_CALLSIGN_ENV_VAR.to_string(),
            "N7OPS".to_string(),
        );
        base_values.insert(STATION_GRID_ENV_VAR.to_string(), "CN87".to_string());
        base_values.insert(STATION_ARRL_SECTION_ENV_VAR.to_string(), "WWA".to_string());

        let manager = RuntimeConfigManager::new(base_values).expect("manager");
        let snapshot = manager.snapshot().await;
        let profile = snapshot.active_station_profile.expect("active profile");

        assert_eq!("K7RND", profile.station_callsign);
        assert_eq!(Some("N7OPS"), profile.operator_callsign.as_deref());
        assert_eq!(Some("CN87"), profile.grid.as_deref());
        assert_eq!(Some("WWA"), profile.arrl_section.as_deref());
    }

    #[tokio::test]
    async fn runtime_manager_prefers_config_file_storage_when_no_cli_override_is_present() {
        let sqlite_path = unique_sqlite_path();
        let mut config_values = BTreeMap::new();
        config_values.insert(STORAGE_BACKEND_ENV_VAR.to_string(), "sqlite".to_string());
        config_values.insert(SQLITE_PATH_ENV_VAR.to_string(), sqlite_path.clone());

        let manager = RuntimeConfigManager::new_with_config_file_values_and_cli_storage_overrides(
            config_values,
            &BTreeMap::new(),
        )
        .expect("manager");

        let snapshot = manager.snapshot().await;
        assert_eq!("sqlite", snapshot.active_storage_backend);
        assert_eq!(
            sqlite_path,
            snapshot
                .values
                .iter()
                .find(|value| value.key == SQLITE_PATH_ENV_VAR)
                .expect("sqlite path value")
                .display_value
        );
    }

    #[tokio::test]
    async fn runtime_manager_prefers_cli_storage_override_over_config_file_storage() {
        let mut config_values = BTreeMap::new();
        config_values.insert(STORAGE_BACKEND_ENV_VAR.to_string(), "sqlite".to_string());
        config_values.insert(SQLITE_PATH_ENV_VAR.to_string(), unique_sqlite_path());

        let mut cli_overrides = BTreeMap::new();
        cli_overrides.insert(STORAGE_BACKEND_ENV_VAR.to_string(), "memory".to_string());

        let manager = RuntimeConfigManager::new_with_config_file_values_and_cli_storage_overrides(
            config_values,
            &cli_overrides,
        )
        .expect("manager");

        let snapshot = manager.snapshot().await;
        assert_eq!("memory", snapshot.active_storage_backend);
    }

    #[tokio::test]
    async fn runtime_manager_preserves_config_file_sqlite_path_when_cli_only_selects_backend() {
        let sqlite_path = unique_sqlite_path();
        let mut config_values = BTreeMap::new();
        config_values.insert(STORAGE_BACKEND_ENV_VAR.to_string(), "sqlite".to_string());
        config_values.insert(SQLITE_PATH_ENV_VAR.to_string(), sqlite_path.clone());

        // CLI says --storage sqlite but does NOT pass --sqlite-path.
        let mut cli_overrides = BTreeMap::new();
        cli_overrides.insert(STORAGE_BACKEND_ENV_VAR.to_string(), "sqlite".to_string());

        let manager = RuntimeConfigManager::new_with_config_file_values_and_cli_storage_overrides(
            config_values,
            &cli_overrides,
        )
        .expect("manager");

        let snapshot = manager.snapshot().await;
        assert_eq!("sqlite", snapshot.active_storage_backend);
        assert_eq!(
            sqlite_path,
            snapshot
                .values
                .iter()
                .find(|value| value.key == SQLITE_PATH_ENV_VAR)
                .expect("sqlite path value")
                .display_value
        );
    }

    #[tokio::test]
    async fn runtime_manager_applies_station_profile_overrides() {
        let manager = RuntimeConfigManager::new(BTreeMap::new()).expect("manager");

        let snapshot = manager
            .apply_request(ApplyRuntimeConfigRequest {
                mutations: vec![
                    RuntimeConfigMutation {
                        key: STATION_CALLSIGN_ENV_VAR.to_string(),
                        kind: RuntimeConfigMutationKind::Set as i32,
                        value: Some("K7RND".to_string()),
                    },
                    RuntimeConfigMutation {
                        key: STATION_LATITUDE_ENV_VAR.to_string(),
                        kind: RuntimeConfigMutationKind::Set as i32,
                        value: Some("47.6205".to_string()),
                    },
                    RuntimeConfigMutation {
                        key: STATION_ARRL_SECTION_ENV_VAR.to_string(),
                        kind: RuntimeConfigMutationKind::Set as i32,
                        value: Some("WWA".to_string()),
                    },
                ],
            })
            .await
            .expect("station overrides");
        let profile = snapshot.active_station_profile.expect("active profile");

        assert_eq!("K7RND", profile.station_callsign);
        assert_eq!(Some(47.6205), profile.latitude);
        assert_eq!(Some("WWA"), profile.arrl_section.as_deref());
    }

    fn assert_contains(actual: &str, expected: &str) {
        assert!(
            actual.contains(expected),
            "expected '{actual}' to contain '{expected}'"
        );
    }

    #[tokio::test]
    async fn runtime_manager_applies_process_session_station_override() {
        let mut config_values = BTreeMap::new();
        config_values.insert(STATION_CALLSIGN_ENV_VAR.to_string(), "K7RND".to_string());
        config_values.insert(STATION_GRID_ENV_VAR.to_string(), "CN87".to_string());
        config_values.insert(STATION_ARRL_SECTION_ENV_VAR.to_string(), "WWA".to_string());
        let manager =
            RuntimeConfigManager::new_with_config_file_values(BTreeMap::new(), config_values)
                .expect("manager");

        let override_profile = manager
            .set_session_station_profile_override(Some(StationProfile {
                profile_name: Some("POTA".to_string()),
                station_callsign: "K7RND/P".to_string(),
                grid: Some("CN88".to_string()),
                ..StationProfile::default()
            }))
            .await
            .expect("session override")
            .expect("profile");
        assert_eq!("K7RND/P", override_profile.station_callsign);

        let snapshot = manager.snapshot().await;
        assert_eq!(
            Some("K7RND/P"),
            snapshot
                .active_station_profile
                .as_ref()
                .map(|profile| profile.station_callsign.as_str())
        );
        assert_eq!(
            None,
            snapshot
                .active_station_profile
                .as_ref()
                .and_then(|profile| profile.arrl_section.as_deref()),
            "a session profile is a full replacement, not a partial overlay"
        );
        assert!(snapshot
            .warnings
            .iter()
            .any(|warning| warning.contains("process-session")));

        manager
            .set_session_station_profile_override(None)
            .await
            .expect("clear override");
        let cleared = manager.snapshot().await;
        assert_eq!(
            Some("K7RND"),
            cleared
                .active_station_profile
                .as_ref()
                .map(|profile| profile.station_callsign.as_str())
        );
    }
}
