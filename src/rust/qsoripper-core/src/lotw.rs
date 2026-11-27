//! ARRL Logbook of the World adapters and synchronization workflow.

use std::path::{Path, PathBuf};
use std::time::Duration;

use prost_types::Timestamp;
use reqwest::{Client, Url};
use tempfile::TempDir;
use time::OffsetDateTime;
use tokio::process::Command;

use crate::adif::{parse_adi_qsos, serialize_adi_qsos};
use crate::proto::qsoripper::domain::{LotwSyncStatus, QslStatus, QsoRecord};
use crate::storage::{
    DeletedRecordsFilter, LogbookStore, QsoListQuery, QsoSortOrder, StorageError,
};

const DEFAULT_REPORT_URL: &str = "https://lotw.arrl.org/lotwuser/lotwreport.adi";
const MATCH_TOLERANCE_SECONDS: i64 = 30 * 60;

/// Configuration for TQSL upload and `LoTW` confirmation download.
#[derive(Debug, Clone)]
pub struct LotwConfig {
    /// `LoTW` account name used by the report endpoint.
    pub username: String,
    /// `LoTW` website password used by the report endpoint.
    pub password: String,
    /// TQSL executable path or command name.
    pub tqsl_path: PathBuf,
    /// Existing TQSL station location name.
    pub station_location: String,
    /// Optional TQSL certificate passphrase.
    pub certificate_password: Option<String>,
    /// Confirmation report endpoint.
    pub report_url: Url,
    /// Network and process timeout.
    pub timeout: Duration,
}

impl LotwConfig {
    /// Create a validated `LoTW` configuration with the production report endpoint.
    ///
    /// # Errors
    ///
    /// Returns an error when a required value is empty or the report URL is invalid.
    pub fn new(
        username: impl Into<String>,
        password: impl Into<String>,
        tqsl_path: impl Into<PathBuf>,
        station_location: impl Into<String>,
    ) -> Result<Self, LotwError> {
        let config = Self {
            username: username.into(),
            password: password.into(),
            tqsl_path: tqsl_path.into(),
            station_location: station_location.into(),
            certificate_password: None,
            report_url: Url::parse(DEFAULT_REPORT_URL)
                .map_err(|error| LotwError::Configuration(error.to_string()))?,
            timeout: Duration::from_secs(60),
        };
        config.validate()?;
        Ok(config)
    }

    /// Set an alternate report URL, primarily for local integration tests.
    ///
    /// # Errors
    ///
    /// Returns an error when the supplied URL is invalid.
    pub fn with_report_url(mut self, report_url: &str) -> Result<Self, LotwError> {
        self.report_url =
            Url::parse(report_url).map_err(|error| LotwError::Configuration(error.to_string()))?;
        Ok(self)
    }

    /// Set the optional TQSL certificate passphrase.
    #[must_use]
    pub fn with_certificate_password(mut self, password: Option<String>) -> Self {
        self.certificate_password = password.filter(|value| !value.is_empty());
        self
    }

    /// Set the timeout used for report requests and TQSL execution.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    fn validate(&self) -> Result<(), LotwError> {
        if self.username.trim().is_empty() {
            return Err(LotwError::Configuration(
                "LoTW username is not configured.".to_string(),
            ));
        }
        if self.password.is_empty() {
            return Err(LotwError::Configuration(
                "LoTW password is not configured.".to_string(),
            ));
        }
        if self.tqsl_path.as_os_str().is_empty() {
            return Err(LotwError::Configuration(
                "TQSL executable is not configured.".to_string(),
            ));
        }
        if self.station_location.trim().is_empty() {
            return Err(LotwError::Configuration(
                "TQSL station location is not configured.".to_string(),
            ));
        }
        Ok(())
    }
}

/// A parsed `LoTW` confirmation report and its incremental high-water value.
#[derive(Debug, Clone, Default)]
pub struct LotwReport {
    /// Confirmation QSO records returned by `LoTW`.
    pub confirmations: Vec<QsoRecord>,
    /// `APP_LoTW_LASTQSL` header value, when present.
    pub high_water: Option<String>,
}

/// Result of one TQSL upload process.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LotwUploadResult {
    /// Number of QSO records submitted to TQSL.
    pub submitted: u32,
}

/// Aggregate outcome from one `LoTW` synchronization cycle.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LotwSyncResult {
    /// Number of records considered by the sync.
    pub total_records: u32,
    /// Number of records processed by upload or download.
    pub processed_records: u32,
    /// Number of local records submitted successfully through TQSL.
    pub uploaded_records: u32,
    /// Number of local records updated from `LoTW` confirmations.
    pub confirmed_records: u32,
    /// Number of downloaded confirmations without a local match.
    pub unmatched_records: u32,
    /// Number of downloaded confirmations with ambiguous local matches.
    pub conflict_records: u32,
    /// Number of records affected by an error.
    pub error_records: u32,
    /// Confirmation high-water value saved after the run.
    pub confirmation_high_water: Option<String>,
    /// Safe error summary that does not include credentials.
    pub error_summary: Option<String>,
}

/// Errors produced by `LoTW` adapters and synchronization.
#[derive(Debug, thiserror::Error)]
pub enum LotwError {
    /// A required configuration value is invalid or missing.
    #[error("{0}")]
    Configuration(String),
    /// A network operation failed.
    #[error("LoTW network request failed: {0}")]
    Network(String),
    /// The report endpoint rejected the configured credentials.
    #[error("LoTW authentication failed.")]
    Authentication,
    /// The report endpoint returned an unsuccessful status.
    #[error("LoTW report request returned HTTP {0}.")]
    HttpStatus(u16),
    /// The report ADIF could not be parsed.
    #[error("LoTW report ADIF is invalid: {0}")]
    Adif(String),
    /// A local temporary-file operation failed.
    #[error("LoTW staging failed: {0}")]
    Io(#[from] std::io::Error),
    /// TQSL did not complete before the configured timeout.
    #[error("TQSL timed out.")]
    TqslTimeout,
    /// TQSL returned a failure status.
    #[error("TQSL upload failed: {0}")]
    Tqsl(String),
    /// Local QSO persistence failed.
    #[error("LoTW storage operation failed: {0}")]
    Storage(#[from] StorageError),
}

/// Testable boundary for `LoTW` upload and confirmation report operations.
#[tonic::async_trait]
pub trait LotwApi: Send + Sync {
    /// Sign and upload the supplied QSO records with TQSL.
    async fn upload_qsos(&self, qsos: &[QsoRecord]) -> Result<LotwUploadResult, LotwError>;

    /// Download confirmed QSOs after the optional `LoTW` high-water value.
    async fn fetch_confirmations(&self, since: Option<&str>) -> Result<LotwReport, LotwError>;
}

/// Production `LoTW` adapter that invokes TQSL and calls the `LoTW` report endpoint.
pub struct LotwClient {
    config: LotwConfig,
    http_client: Client,
}

impl LotwClient {
    /// Build a production `LoTW` adapter.
    ///
    /// # Errors
    ///
    /// Returns an error when the HTTP client cannot be configured.
    pub fn new(config: LotwConfig) -> Result<Self, LotwError> {
        config.validate()?;
        let http_client = Client::builder()
            .timeout(config.timeout)
            .build()
            .map_err(|error| LotwError::Network(error.without_url().to_string()))?;
        Ok(Self {
            config,
            http_client,
        })
    }

    async fn run_tqsl(&self, input_path: &Path) -> Result<(), LotwError> {
        let mut command = Command::new(&self.config.tqsl_path);
        command
            .arg("-a")
            .arg("compliant")
            .arg("-d")
            .arg("-l")
            .arg(&self.config.station_location);
        if let Some(password) = self.config.certificate_password.as_deref() {
            command.arg("-p").arg(password);
        }
        command
            .arg("-u")
            .arg(input_path)
            .arg("-x")
            .kill_on_drop(true);

        let output = tokio::time::timeout(self.config.timeout, command.output())
            .await
            .map_err(|_| LotwError::TqslTimeout)??;
        if output.status.success() {
            return Ok(());
        }

        let detail = sanitized_process_detail(
            &output.stderr,
            &output.stdout,
            &[
                self.config.password.as_str(),
                self.config
                    .certificate_password
                    .as_deref()
                    .unwrap_or_default(),
            ],
        );
        Err(LotwError::Tqsl(detail))
    }
}

#[tonic::async_trait]
impl LotwApi for LotwClient {
    async fn upload_qsos(&self, qsos: &[QsoRecord]) -> Result<LotwUploadResult, LotwError> {
        if qsos.is_empty() {
            return Ok(LotwUploadResult::default());
        }

        let staging = TempDir::new()?;
        let input_path = staging.path().join("qsoripper-lotw-upload.adi");
        std::fs::write(&input_path, serialize_adi_qsos(qsos, true))?;
        self.run_tqsl(&input_path).await?;

        Ok(LotwUploadResult {
            submitted: saturating_u32(qsos.len()),
        })
    }

    async fn fetch_confirmations(&self, since: Option<&str>) -> Result<LotwReport, LotwError> {
        let mut query = vec![
            ("login", self.config.username.as_str()),
            ("password", self.config.password.as_str()),
            ("qso_query", "1"),
            ("qso_qsl", "yes"),
            ("qso_qsldetail", "yes"),
            ("qso_mydetail", "yes"),
            ("qso_withown", "yes"),
        ];
        if let Some(value) = since.filter(|value| !value.trim().is_empty()) {
            query.push(("qso_qslsince", value));
        }

        let response = self
            .http_client
            .get(self.config.report_url.clone())
            .query(&query)
            .send()
            .await
            .map_err(|error| LotwError::Network(error.without_url().to_string()))?;
        if !response.status().is_success() {
            return Err(LotwError::HttpStatus(response.status().as_u16()));
        }

        let payload = response
            .bytes()
            .await
            .map_err(|error| LotwError::Network(error.without_url().to_string()))?;
        let text = String::from_utf8_lossy(&payload);
        if looks_like_authentication_failure(&text) {
            return Err(LotwError::Authentication);
        }

        let high_water = extract_adif_field(&text, "APP_LOTW_LASTQSL");
        let confirmations = parse_adi_qsos(&payload).await.map_err(LotwError::Adif)?;
        Ok(LotwReport {
            confirmations,
            high_water,
        })
    }
}

/// Execute one `LoTW` upload and confirmation-download cycle.
///
/// # Errors
///
/// Returns an error only when local storage cannot be loaded. Adapter failures are
/// returned in the result so one failed phase does not prevent the other phase.
pub async fn execute_sync(
    api: &dyn LotwApi,
    store: &dyn LogbookStore,
    full_sync: bool,
    upload: bool,
    download: bool,
) -> Result<LotwSyncResult, LotwError> {
    let local_qsos = store
        .list_qsos(&QsoListQuery {
            sort: QsoSortOrder::OldestFirst,
            deleted_filter: DeletedRecordsFilter::ActiveOnly,
            ..QsoListQuery::default()
        })
        .await?;
    let mut result = LotwSyncResult {
        total_records: saturating_u32(local_qsos.len()),
        ..LotwSyncResult::default()
    };
    let mut errors = Vec::new();

    if upload {
        let pending: Vec<QsoRecord> = local_qsos
            .iter()
            .filter(|qso| lotw_upload_is_pending(qso))
            .cloned()
            .collect();
        if !pending.is_empty() {
            match api.upload_qsos(&pending).await {
                Ok(upload_result) => {
                    result.uploaded_records = upload_result.submitted;
                    result.processed_records = result
                        .processed_records
                        .saturating_add(upload_result.submitted);
                    let now = timestamp_now();
                    for snapshot in &pending {
                        patch_upload_result(store, snapshot, &now, true).await?;
                    }
                }
                Err(error) => {
                    result.error_records = result
                        .error_records
                        .saturating_add(saturating_u32(pending.len()));
                    for snapshot in &pending {
                        patch_upload_result(store, snapshot, &timestamp_now(), false).await?;
                    }
                    errors.push(error.to_string());
                }
            }
        }
    }

    if download {
        let mut metadata = store.get_sync_metadata().await?;
        let since = if full_sync {
            None
        } else {
            metadata.lotw_last_qsl.as_deref()
        };
        match api.fetch_confirmations(since).await {
            Ok(report) => {
                result.confirmation_high_water = report
                    .high_water
                    .clone()
                    .or_else(|| metadata.lotw_last_qsl.clone());
                result.processed_records = result
                    .processed_records
                    .saturating_add(saturating_u32(report.confirmations.len()));
                for confirmation in &report.confirmations {
                    let matches = find_confirmation_matches(confirmation, &local_qsos);
                    match matches.as_slice() {
                        [] => {
                            result.unmatched_records = result.unmatched_records.saturating_add(1);
                        }
                        [local] => {
                            if apply_confirmation(store, local, confirmation).await? {
                                result.confirmed_records =
                                    result.confirmed_records.saturating_add(1);
                            }
                        }
                        many => {
                            result.conflict_records = result.conflict_records.saturating_add(1);
                            for local in many {
                                patch_lotw_status(store, local, LotwSyncStatus::Conflict).await?;
                            }
                        }
                    }
                }

                metadata.lotw_last_sync = Some(timestamp_now());
                metadata
                    .lotw_last_qsl
                    .clone_from(&result.confirmation_high_water);
                store.upsert_sync_metadata(&metadata).await?;
            }
            Err(error) => {
                result.error_records = result.error_records.saturating_add(1);
                errors.push(error.to_string());
            }
        }
    }

    if !errors.is_empty() {
        result.error_summary = Some(errors.join(" "));
    }
    Ok(result)
}

/// Upload one QSO and persist its `LoTW` state for a per-operation sync request.
///
/// # Errors
///
/// Returns an adapter or storage error. The local QSO remains available for retry.
pub async fn upload_single_qso(
    api: &dyn LotwApi,
    store: &dyn LogbookStore,
    snapshot: &QsoRecord,
) -> Result<QsoRecord, LotwError> {
    match api.upload_qsos(std::slice::from_ref(snapshot)).await {
        Ok(_) => patch_upload_result(store, snapshot, &timestamp_now(), true).await?,
        Err(error) => {
            patch_upload_result(store, snapshot, &timestamp_now(), false).await?;
            return Err(error);
        }
    }

    store.get_qso(&snapshot.local_id).await?.ok_or_else(|| {
        LotwError::Storage(StorageError::backend(format!(
            "QSO '{}' disappeared after LoTW upload.",
            snapshot.local_id
        )))
    })
}

fn lotw_upload_is_pending(qso: &QsoRecord) -> bool {
    matches!(
        LotwSyncStatus::try_from(qso.lotw_sync_status).unwrap_or(LotwSyncStatus::LocalOnly),
        LotwSyncStatus::LocalOnly
            | LotwSyncStatus::Queued
            | LotwSyncStatus::Modified
            | LotwSyncStatus::Failed
    )
}

async fn patch_upload_result(
    store: &dyn LogbookStore,
    upload_snapshot: &QsoRecord,
    now: &Timestamp,
    success: bool,
) -> Result<(), LotwError> {
    for _ in 0..3 {
        let Some(current) = store.get_qso(&upload_snapshot.local_id).await? else {
            return Ok(());
        };
        if current.deleted_at.is_some() {
            return Ok(());
        }
        let mut replacement = current.clone();
        if success {
            replacement.lotw_sent = Some(true);
            replacement.lotw_sent_status = QslStatus::Yes.into();
            replacement.lotw_sent_date = Some(*now);
            replacement.lotw_sync_status = if current.updated_at == upload_snapshot.updated_at {
                LotwSyncStatus::Uploaded.into()
            } else {
                LotwSyncStatus::Modified.into()
            };
        } else {
            replacement.lotw_sync_status = LotwSyncStatus::Failed.into();
        }
        if store
            .update_qso_if_unchanged(&current, &replacement)
            .await?
        {
            return Ok(());
        }
    }
    Ok(())
}

async fn patch_lotw_status(
    store: &dyn LogbookStore,
    snapshot: &QsoRecord,
    status: LotwSyncStatus,
) -> Result<(), LotwError> {
    for _ in 0..3 {
        let Some(current) = store.get_qso(&snapshot.local_id).await? else {
            return Ok(());
        };
        let mut replacement = current.clone();
        replacement.lotw_sync_status = status.into();
        if store
            .update_qso_if_unchanged(&current, &replacement)
            .await?
        {
            return Ok(());
        }
    }
    Ok(())
}

async fn apply_confirmation(
    store: &dyn LogbookStore,
    snapshot: &QsoRecord,
    confirmation: &QsoRecord,
) -> Result<bool, LotwError> {
    for _ in 0..3 {
        let Some(current) = store.get_qso(&snapshot.local_id).await? else {
            return Ok(false);
        };
        if current.deleted_at.is_some() {
            return Ok(false);
        }

        let mut replacement = current.clone();
        replacement.lotw_received = Some(true);
        replacement.lotw_received_status = QslStatus::Yes.into();
        replacement.lotw_received_date = confirmation
            .lotw_received_date
            .or(confirmation.qsl_received_date)
            .or_else(|| Some(timestamp_now()));
        replacement.lotw_sync_status = LotwSyncStatus::Confirmed.into();
        merge_confirmation_enrichment(&mut replacement, confirmation);

        if store
            .update_qso_if_unchanged(&current, &replacement)
            .await?
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn find_confirmation_matches<'a>(
    confirmation: &QsoRecord,
    local_qsos: &'a [QsoRecord],
) -> Vec<&'a QsoRecord> {
    let Some(remote_time) = confirmation.utc_timestamp.as_ref() else {
        return Vec::new();
    };
    let station_callsign = report_station_callsign(confirmation);
    if station_callsign.is_empty() || confirmation.worked_callsign.trim().is_empty() {
        return Vec::new();
    }

    local_qsos
        .iter()
        .filter(|local| {
            local.deleted_at.is_none()
                && local
                    .station_callsign
                    .eq_ignore_ascii_case(station_callsign)
                && local
                    .worked_callsign
                    .eq_ignore_ascii_case(&confirmation.worked_callsign)
                && local.band == confirmation.band
                && local.mode == confirmation.mode
                && local.utc_timestamp.as_ref().is_some_and(|local_time| {
                    local_time.seconds.abs_diff(remote_time.seconds)
                        <= u64::try_from(MATCH_TOLERANCE_SECONDS).unwrap_or(u64::MAX)
                })
        })
        .collect()
}

fn report_station_callsign(qso: &QsoRecord) -> &str {
    if !qso.station_callsign.trim().is_empty() {
        return qso.station_callsign.trim();
    }
    if let Some(owner) = qso
        .owner_callsign
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        return owner.trim();
    }
    for key in ["APP_LOTW_OWNCALL", "OWNER_CALLSIGN"] {
        if let Some(value) = qso
            .extra_fields
            .get(key)
            .filter(|value| !value.trim().is_empty())
        {
            return value.trim();
        }
    }
    ""
}

fn merge_confirmation_enrichment(target: &mut QsoRecord, source: &QsoRecord) {
    copy_string(&mut target.worked_grid, source.worked_grid.as_deref());
    copy_string(&mut target.worked_state, source.worked_state.as_deref());
    copy_string(&mut target.worked_county, source.worked_county.as_deref());
    copy_string(&mut target.worked_country, source.worked_country.as_deref());
    if source.worked_dxcc.is_some() {
        target.worked_dxcc = source.worked_dxcc;
    }
    if source.worked_cq_zone.is_some() {
        target.worked_cq_zone = source.worked_cq_zone;
    }
    if source.worked_itu_zone.is_some() {
        target.worked_itu_zone = source.worked_itu_zone;
    }
}

fn copy_string(target: &mut Option<String>, source: Option<&str>) {
    if let Some(value) = source.filter(|value| !value.trim().is_empty()) {
        *target = Some(value.trim().to_string());
    }
}

fn extract_adif_field(text: &str, field_name: &str) -> Option<String> {
    let upper = text.to_ascii_uppercase();
    let marker = format!("<{}:", field_name.to_ascii_uppercase());
    let start = upper.find(&marker)? + marker.len();
    let suffix = text.get(start..)?;
    let length_end = suffix.find(['>', ':'])?;
    let length = suffix.get(..length_end)?.parse::<usize>().ok()?;
    let value_start = start + suffix.find('>')? + 1;
    let value_end = value_start.checked_add(length)?;
    text.get(value_start..value_end)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

fn looks_like_authentication_failure(text: &str) -> bool {
    let normalized = text.trim_start().to_ascii_lowercase();
    normalized.starts_with("<!doctype html")
        || normalized.starts_with("<html")
        || (normalized.contains("password") && normalized.contains("invalid"))
        || (normalized.contains("login") && normalized.contains("failed"))
}

fn sanitized_process_detail(stderr: &[u8], stdout: &[u8], secrets: &[&str]) -> String {
    let raw = if stderr.is_empty() { stdout } else { stderr };
    let detail = String::from_utf8_lossy(raw)
        .trim()
        .replace(['\r', '\n'], " ");
    if detail.is_empty() {
        "process returned a failure status".to_string()
    } else {
        let mut redacted = detail;
        for secret in secrets.iter().filter(|value| !value.is_empty()) {
            redacted = redacted.replace(secret, "[REDACTED]");
        }
        redacted.chars().take(500).collect()
    }
}

fn timestamp_now() -> Timestamp {
    crate::utc::to_timestamp(OffsetDateTime::now_utc())
}

fn saturating_u32(value: usize) -> u32 {
    u32::try_from(value).unwrap_or(u32::MAX)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::thread;
    use std::time::Duration;

    use super::{
        extract_adif_field, find_confirmation_matches, looks_like_authentication_failure,
        merge_confirmation_enrichment, report_station_callsign, sanitized_process_detail, LotwApi,
        LotwClient, LotwConfig, LotwError,
    };
    use crate::proto::qsoripper::domain::{Band, Mode, QsoRecord};
    use prost_types::Timestamp;

    fn qso(station: &str, worked: &str, seconds: i64) -> QsoRecord {
        QsoRecord {
            station_callsign: station.to_string(),
            worked_callsign: worked.to_string(),
            utc_timestamp: Some(Timestamp { seconds, nanos: 0 }),
            band: Band::Band20m.into(),
            mode: Mode::Ft8.into(),
            ..QsoRecord::default()
        }
    }

    #[test]
    fn extracts_confirmation_high_water_from_adif_header() {
        let text = "<APP_LoTW_LASTQSL:19>2026-08-09 12:34:56<EOH>";
        assert_eq!(
            extract_adif_field(text, "APP_LOTW_LASTQSL").as_deref(),
            Some("2026-08-09 12:34:56")
        );
    }

    #[test]
    fn confirmation_match_uses_station_call_and_thirty_minute_tolerance() {
        let local = vec![
            qso("KC7AVA", "W1AW", 1_000),
            qso("KC7AVA", "W1AW", 5_000),
            qso("N0CALL", "W1AW", 1_000),
        ];
        let remote = qso("kc7ava", "w1aw", 2_799);

        let matches = find_confirmation_matches(&remote, &local);

        assert_eq!(matches.len(), 1);
        assert_eq!(
            matches
                .first()
                .and_then(|qso| qso.utc_timestamp.as_ref())
                .map(|timestamp| timestamp.seconds),
            Some(1_000)
        );
    }

    #[test]
    fn html_login_page_is_not_treated_as_adif() {
        assert!(looks_like_authentication_failure(
            "<!DOCTYPE html><html>Login failed</html>"
        ));
    }

    #[test]
    fn tqsl_error_detail_redacts_passwords() {
        let detail = sanitized_process_detail(
            b"certificate passphrase was secret-value",
            b"",
            &["secret-value"],
        );

        assert!(!detail.contains("secret-value"));
        assert!(detail.contains("[REDACTED]"));
    }

    #[test]
    fn configuration_rejects_each_missing_required_value() {
        let cases = [
            (" ", "password", "tqsl", "Home", "username"),
            ("KC7AVA", "", "tqsl", "Home", "password"),
            ("KC7AVA", "password", "", "Home", "executable"),
            ("KC7AVA", "password", "tqsl", " ", "station location"),
        ];

        for (username, password, tqsl, station, expected) in cases {
            let error = LotwConfig::new(username, password, tqsl, station)
                .expect_err("configuration must fail");
            assert!(error.to_string().to_ascii_lowercase().contains(expected));
        }
    }

    #[test]
    fn configuration_builders_apply_safe_defaults_and_overrides() {
        let config = LotwConfig::new("KC7AVA", "password", "tqsl", "Home")
            .expect("valid configuration")
            .with_report_url("http://127.0.0.1:45321/report")
            .expect("valid report URL")
            .with_certificate_password(Some(String::new()))
            .with_timeout(Duration::from_secs(7));

        assert_eq!(config.report_url.as_str(), "http://127.0.0.1:45321/report");
        assert!(config.certificate_password.is_none());
        assert_eq!(config.timeout, Duration::from_secs(7));
        assert!(LotwClient::new(config).is_ok());

        let config = LotwConfig::new("KC7AVA", "password", "tqsl", "Home")
            .expect("valid configuration")
            .with_certificate_password(Some("certificate-password".to_string()));
        assert_eq!(
            config.certificate_password.as_deref(),
            Some("certificate-password")
        );
        assert!(matches!(
            config.with_report_url("not a URL"),
            Err(LotwError::Configuration(_))
        ));
    }

    #[tokio::test]
    async fn report_client_sends_incremental_query_and_parses_adif() {
        let body = concat!(
            "Generated by ARRL Logbook of the World\n",
            "<ADIF_VER:5>3.1.4\n",
            "<APP_LoTW_LASTQSL:19>2026-08-09 12:34:56<EOH>\n",
            "<CALL:5>N7XAK<STATION_CALLSIGN:6>KC7AVA",
            "<QSO_DATE:8>20260808<TIME_ON:6>031000",
            "<BAND:3>20M<MODE:3>FT8<EOR>"
        );
        let (url, request_rx, server) = start_http_server(200, body);
        let client = report_client(&url);

        let report = client
            .fetch_confirmations(Some("2026-08-01 01:02:03"))
            .await
            .expect("confirmation report");

        assert_eq!(report.confirmations.len(), 1);
        assert_eq!(report.high_water.as_deref(), Some("2026-08-09 12:34:56"));
        let request = request_rx.recv().expect("HTTP request");
        assert!(request.contains("qso_qslsince=2026-08-01+01%3A02%3A03"));
        assert!(request.contains("qso_qsl=yes"));
        server.join().expect("HTTP server");
    }

    #[tokio::test]
    async fn report_client_maps_http_and_authentication_failures() {
        let (url, _request_rx, server) = start_http_server(503, "unavailable");
        let error = report_client(&url)
            .fetch_confirmations(None)
            .await
            .expect_err("HTTP status failure");
        assert!(matches!(error, LotwError::HttpStatus(503)));
        server.join().expect("HTTP server");

        let (url, _request_rx, server) =
            start_http_server(200, "<!DOCTYPE html><html>Login failed</html>");
        let error = report_client(&url)
            .fetch_confirmations(Some(" "))
            .await
            .expect_err("authentication failure");
        assert!(matches!(error, LotwError::Authentication));
        server.join().expect("HTTP server");
    }

    #[tokio::test]
    async fn empty_upload_does_not_start_tqsl() {
        let client = report_client("http://127.0.0.1:1/report");
        let result = client.upload_qsos(&[]).await.expect("empty upload");
        assert_eq!(result.submitted, 0);
    }

    #[test]
    fn matching_and_enrichment_handle_report_fallbacks() {
        let local = vec![qso("KC7AVA", "W1AW", 1_000)];
        let mut remote = qso("", "W1AW", 1_000);
        assert!(find_confirmation_matches(&remote, &local).is_empty());

        remote.owner_callsign = Some(" kc7ava ".to_string());
        assert_eq!(report_station_callsign(&remote), "kc7ava");
        assert_eq!(find_confirmation_matches(&remote, &local).len(), 1);

        remote.owner_callsign = None;
        remote
            .extra_fields
            .insert("APP_LOTW_OWNCALL".to_string(), " KC7AVA ".to_string());
        assert_eq!(report_station_callsign(&remote), "KC7AVA");

        remote.extra_fields.clear();
        remote
            .extra_fields
            .insert("OWNER_CALLSIGN".to_string(), "KC7AVA".to_string());
        assert_eq!(report_station_callsign(&remote), "KC7AVA");

        remote.utc_timestamp = None;
        assert!(find_confirmation_matches(&remote, &local).is_empty());

        let mut target = QsoRecord::default();
        remote.worked_grid = Some(" DM34 ".to_string());
        remote.worked_state = Some("AZ".to_string());
        remote.worked_county = Some("MARICOPA".to_string());
        remote.worked_country = Some("United States".to_string());
        remote.worked_dxcc = Some(291);
        remote.worked_cq_zone = Some(3);
        remote.worked_itu_zone = Some(6);
        merge_confirmation_enrichment(&mut target, &remote);
        assert_eq!(target.worked_grid.as_deref(), Some("DM34"));
        assert_eq!(target.worked_dxcc, Some(291));
        assert_eq!(target.worked_cq_zone, Some(3));
        assert_eq!(target.worked_itu_zone, Some(6));
    }

    #[test]
    fn adif_and_process_helpers_reject_malformed_values() {
        assert_eq!(extract_adif_field("missing", "FIELD"), None);
        assert_eq!(extract_adif_field("<FIELD:x>value", "FIELD"), None);
        assert_eq!(extract_adif_field("<FIELD:20>short", "FIELD"), None);
        assert!(looks_like_authentication_failure("The password is invalid"));
        assert!(looks_like_authentication_failure("Login failed"));
        assert!(!looks_like_authentication_failure("<EOH><EOR>"));
        assert_eq!(
            sanitized_process_detail(b"", b"", &[]),
            "process returned a failure status"
        );
        assert_eq!(
            sanitized_process_detail(b"", b"line one\r\nline two", &[]),
            "line one  line two"
        );
    }

    fn report_client(url: &str) -> LotwClient {
        let config = LotwConfig::new("KC7AVA", "password", "tqsl", "Home")
            .expect("valid configuration")
            .with_report_url(url)
            .expect("valid report URL")
            .with_timeout(Duration::from_secs(5));
        LotwClient::new(config).expect("report client")
    }

    fn start_http_server(
        status: u16,
        body: &'static str,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind HTTP server");
        let address = listener.local_addr().expect("HTTP server address");
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept HTTP request");
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .expect("HTTP read timeout");
            let mut request = vec![0_u8; 8_192];
            let read = stream.read(&mut request).expect("read HTTP request");
            request.truncate(read);
            request_tx
                .send(String::from_utf8_lossy(&request).into_owned())
                .expect("send HTTP request");
            let reason = if status == 200 {
                "OK"
            } else {
                "Service Unavailable"
            };
            let response = format!(
                "HTTP/1.1 {status} {reason}\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write HTTP response");
        });
        (format!("http://{address}/report"), request_rx, server)
    }
}
