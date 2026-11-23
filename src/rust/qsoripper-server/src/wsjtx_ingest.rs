//! Background WSJT-X ingestion supervisor.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use prost_types::Timestamp;
use qsoripper_core::application::logbook::AdifImportSummary;
use qsoripper_core::proto::qsoripper::services::WsjtxIngestStatus;
use qsoripper_core::qrz_logbook::{QrzLogbookClient, QrzLogbookConfig};
use qsoripper_core::wsjtx::{ingest_wsjtx_adif_tail, ingest_wsjtx_logged_adif_datagram};
use tokio::net::UdpSocket;
use tokio::sync::{watch, Mutex};
use tokio::time::{timeout, Duration};

use crate::runtime_config::{
    RuntimeConfigManager, DEFAULT_QRZ_LOGBOOK_BASE_URL, DEFAULT_WSJTX_INGEST_POLL_INTERVAL_MS,
    DEFAULT_WSJTX_INGEST_UDP_BIND, QRZ_LOGBOOK_API_KEY_ENV_VAR, QRZ_LOGBOOK_BASE_URL_ENV_VAR,
    WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR, WSJTX_INGEST_ADIF_TAIL_PATH_ENV_VAR,
    WSJTX_INGEST_ENABLED_ENV_VAR, WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR,
    WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR, WSJTX_INGEST_UDP_BIND_ENV_VAR,
    WSJTX_INGEST_UDP_ENABLED_ENV_VAR,
};
use crate::sync;

#[derive(Clone)]
pub(crate) struct WsjtxIngestSupervisor {
    status: Arc<Mutex<WsjtxIngestStatus>>,
    import_lock: Arc<Mutex<()>>,
    qrz_sync_lock: Arc<Mutex<()>>,
    cancel_tx: watch::Sender<bool>,
}

impl WsjtxIngestSupervisor {
    #[cfg(test)]
    pub(crate) fn new() -> Self {
        Self::with_qrz_sync_lock(Arc::new(Mutex::new(())))
    }

    pub(crate) fn with_qrz_sync_lock(qrz_sync_lock: Arc<Mutex<()>>) -> Self {
        let (cancel_tx, _) = watch::channel(false);
        Self {
            status: Arc::new(Mutex::new(WsjtxIngestStatus::default())),
            import_lock: Arc::new(Mutex::new(())),
            qrz_sync_lock,
            cancel_tx,
        }
    }

    pub(crate) fn status_handle(&self) -> Arc<Mutex<WsjtxIngestStatus>> {
        self.status.clone()
    }

    pub(crate) fn start(&self, runtime_config: Arc<RuntimeConfigManager>) {
        let udp_status = self.status.clone();
        let udp_import_lock = self.import_lock.clone();
        let udp_qrz_sync_lock = self.qrz_sync_lock.clone();
        let udp_runtime = runtime_config.clone();
        let mut udp_cancel = self.cancel_tx.subscribe();
        tokio::spawn(async move {
            udp_loop(
                udp_runtime,
                udp_status,
                udp_import_lock,
                udp_qrz_sync_lock,
                &mut udp_cancel,
            )
            .await;
        });

        let tail_status = self.status.clone();
        let tail_import_lock = self.import_lock.clone();
        let tail_qrz_sync_lock = self.qrz_sync_lock.clone();
        let mut tail_cancel = self.cancel_tx.subscribe();
        tokio::spawn(async move {
            adif_tail_loop(
                runtime_config,
                tail_status,
                tail_import_lock,
                tail_qrz_sync_lock,
                &mut tail_cancel,
            )
            .await;
        });
    }

    pub(crate) fn stop(&self) {
        let _ = self.cancel_tx.send(true);
    }
}

async fn udp_loop(
    runtime_config: Arc<RuntimeConfigManager>,
    status: Arc<Mutex<WsjtxIngestStatus>>,
    import_lock: Arc<Mutex<()>>,
    qrz_sync_lock: Arc<Mutex<()>>,
    cancel_rx: &mut watch::Receiver<bool>,
) {
    let mut active_bind = String::new();
    let mut socket: Option<UdpSocket> = None;
    let mut buffer = vec![0_u8; 65_535];

    loop {
        if *cancel_rx.borrow() {
            mark_udp_stopped(&status).await;
            return;
        }

        let settings = WsjtxRuntimeSettings::from_values(&runtime_config.effective_values().await);
        update_config_status(&status, &settings).await;

        if !settings.enabled || !settings.udp_enabled {
            socket = None;
            active_bind.clear();
            mark_udp_stopped(&status).await;
            if wait_or_cancel(cancel_rx, Duration::from_millis(500)).await {
                return;
            }
            continue;
        }

        if socket.is_none() || active_bind != settings.udp_bind {
            match UdpSocket::bind(&settings.udp_bind).await {
                Ok(bound) => {
                    let local = bound
                        .local_addr()
                        .map_or_else(|_| settings.udp_bind.clone(), |addr| addr.to_string());
                    active_bind.clone_from(&settings.udp_bind);
                    socket = Some(bound);
                    let mut guard = status.lock().await;
                    guard.enabled = true;
                    guard.running = true;
                    guard.udp_running = true;
                    guard.udp_bind = local;
                    guard.last_error = None;
                }
                Err(error) => {
                    let mut guard = status.lock().await;
                    guard.enabled = true;
                    guard.running = settings.adif_tail_enabled;
                    guard.udp_running = false;
                    guard.udp_bind.clone_from(&settings.udp_bind);
                    guard.last_error = Some(format!(
                        "Failed to bind WSJT-X UDP listener on {}: {error}",
                        settings.udp_bind
                    ));
                    socket = None;
                    active_bind.clear();
                    if wait_or_cancel(cancel_rx, Duration::from_millis(1_000)).await {
                        return;
                    }
                    continue;
                }
            }
        }

        let Some(bound) = socket.as_ref() else {
            continue;
        };
        match timeout(Duration::from_millis(500), bound.recv_from(&mut buffer)).await {
            Ok(Ok((len, _addr))) => {
                if let Some(datagram) = buffer.get(..len) {
                    process_datagram(
                        &runtime_config,
                        &status,
                        &import_lock,
                        &qrz_sync_lock,
                        datagram,
                        settings.sync_to_qrz,
                    )
                    .await;
                }
            }
            Ok(Err(error)) => {
                status.lock().await.last_error =
                    Some(format!("WSJT-X UDP receive failed: {error}"));
                socket = None;
                active_bind.clear();
            }
            Err(_) => {}
        }
    }
}

async fn adif_tail_loop(
    runtime_config: Arc<RuntimeConfigManager>,
    status: Arc<Mutex<WsjtxIngestStatus>>,
    import_lock: Arc<Mutex<()>>,
    qrz_sync_lock: Arc<Mutex<()>>,
    cancel_rx: &mut watch::Receiver<bool>,
) {
    let mut cursor = 0_usize;
    let mut active_path = PathBuf::new();

    loop {
        if *cancel_rx.borrow() {
            mark_tail_stopped(&status).await;
            return;
        }

        let settings = WsjtxRuntimeSettings::from_values(&runtime_config.effective_values().await);
        update_config_status(&status, &settings).await;

        if !settings.enabled || !settings.adif_tail_enabled {
            cursor = 0;
            active_path = PathBuf::new();
            mark_tail_stopped(&status).await;
            if wait_or_cancel(cancel_rx, Duration::from_millis(500)).await {
                return;
            }
            continue;
        }

        let Some(path) = settings.adif_tail_path.clone() else {
            status.lock().await.last_error =
                Some("WSJT-X ADIF tail is enabled but no path is configured.".to_string());
            if wait_or_cancel(cancel_rx, settings.poll_interval).await {
                return;
            }
            continue;
        };

        if active_path != path {
            cursor = 0;
            active_path.clone_from(&path);
        }

        {
            let mut guard = status.lock().await;
            guard.enabled = true;
            guard.running = true;
            guard.adif_tail_running = true;
            guard.adif_tail_path = Some(path.display().to_string());
        }

        let summary = {
            let _guard = import_lock.lock().await;
            let (engine, active_station_profile) = runtime_config.logbook_context().await;
            ingest_wsjtx_adif_tail(
                &engine,
                &path,
                active_station_profile.as_ref(),
                false,
                &mut cursor,
            )
            .await
        };
        match summary {
            Ok(summary) => {
                record_import_summary(&status, &summary).await;
                queue_qrz_sync(
                    &runtime_config,
                    &status,
                    &qrz_sync_lock,
                    settings.sync_to_qrz,
                    summary,
                );
            }
            Err(error) => {
                let mut guard = status.lock().await;
                guard.parse_errors = guard.parse_errors.saturating_add(1);
                guard.last_error = Some(error);
            }
        }

        if wait_or_cancel(cancel_rx, settings.poll_interval).await {
            return;
        }
    }
}

async fn process_datagram(
    runtime_config: &Arc<RuntimeConfigManager>,
    status: &Arc<Mutex<WsjtxIngestStatus>>,
    import_lock: &Arc<Mutex<()>>,
    qrz_sync_lock: &Arc<Mutex<()>>,
    datagram: &[u8],
    sync_to_qrz: bool,
) {
    let summary = {
        let _guard = import_lock.lock().await;
        let (engine, active_station_profile) = runtime_config.logbook_context().await;
        ingest_wsjtx_logged_adif_datagram(&engine, datagram, active_station_profile.as_ref(), true)
            .await
    };
    match summary {
        Ok(summary) => {
            record_import_summary(status, &summary).await;
            queue_qrz_sync(runtime_config, status, qrz_sync_lock, sync_to_qrz, summary);
        }
        Err(error) => {
            let mut guard = status.lock().await;
            guard.parse_errors = guard.parse_errors.saturating_add(1);
            guard.last_error = Some(error);
        }
    }
}

fn queue_qrz_sync(
    runtime_config: &Arc<RuntimeConfigManager>,
    status: &Arc<Mutex<WsjtxIngestStatus>>,
    qrz_sync_lock: &Arc<Mutex<()>>,
    sync_to_qrz: bool,
    summary: AdifImportSummary,
) {
    if !sync_to_qrz || summary.affected_qsos.is_empty() {
        return;
    }

    let runtime_config = runtime_config.clone();
    let status = status.clone();
    let qrz_sync_lock = qrz_sync_lock.clone();
    tokio::spawn(async move {
        let _guard = qrz_sync_lock.lock().await;
        maybe_sync_to_qrz(&runtime_config, &status, true, &summary).await;
    });
}

async fn maybe_sync_to_qrz(
    runtime_config: &Arc<RuntimeConfigManager>,
    status: &Arc<Mutex<WsjtxIngestStatus>>,
    sync_to_qrz: bool,
    summary: &AdifImportSummary,
) {
    if !sync_to_qrz || summary.affected_qsos.is_empty() {
        return;
    }

    let values = runtime_config.effective_values().await;
    let api_key = values
        .get(QRZ_LOGBOOK_API_KEY_ENV_VAR)
        .cloned()
        .unwrap_or_default();
    if api_key.trim().is_empty() {
        let mut guard = status.lock().await;
        guard.last_qrz_sync_success = false;
        guard.last_qrz_sync_error = Some("QRZ Logbook API key is not configured.".to_string());
        return;
    }

    let base_url = values
        .get(QRZ_LOGBOOK_BASE_URL_ENV_VAR)
        .cloned()
        .unwrap_or_else(|| DEFAULT_QRZ_LOGBOOK_BASE_URL.to_string());
    let client = match QrzLogbookClient::new(QrzLogbookConfig::new(
        api_key,
        base_url,
        "QsoRipper/1.0".to_string(),
    )) {
        Ok(client) => client,
        Err(error) => {
            let mut guard = status.lock().await;
            guard.last_qrz_sync_success = false;
            guard.last_qrz_sync_error =
                Some(format!("Failed to create QRZ logbook client: {error}"));
            return;
        }
    };

    let engine = runtime_config.logbook_engine().await;
    let cached_metadata = engine
        .logbook_store()
        .get_sync_metadata()
        .await
        .unwrap_or_default();
    let book_owner = sync::resolve_book_owner_for_upload(&client, &cached_metadata).await;

    for qso in &summary.affected_qsos {
        match sync::sync_single_qso_metadata_only(
            &client,
            engine.logbook_store(),
            qso,
            book_owner.as_deref(),
        )
        .await
        {
            Ok(outcome) => {
                let mut guard = status.lock().await;
                guard.last_qrz_sync_success = true;
                guard.last_qrz_sync_error = None;
                guard.last_imported_callsign = Some(outcome.qso.worked_callsign);
                guard.last_imported_local_id = Some(outcome.qso.local_id);
            }
            Err(error) => {
                let mut guard = status.lock().await;
                guard.last_qrz_sync_success = false;
                guard.last_qrz_sync_error = Some(error);
            }
        }
    }
}

async fn record_import_summary(
    status: &Arc<Mutex<WsjtxIngestStatus>>,
    summary: &AdifImportSummary,
) {
    let mut guard = status.lock().await;
    guard.last_event_at = Some(now_timestamp());
    guard.records_imported = guard
        .records_imported
        .saturating_add(summary.records_imported);
    guard.records_updated = guard
        .records_updated
        .saturating_add(summary.records_updated);
    guard.records_skipped = guard
        .records_skipped
        .saturating_add(summary.records_skipped);
    guard.duplicates_skipped = guard
        .duplicates_skipped
        .saturating_add(count_duplicate_warnings(summary));
    if let Some(qso) = summary.affected_qsos.last() {
        guard.last_imported_callsign = Some(qso.worked_callsign.clone());
        guard.last_imported_local_id = Some(qso.local_id.clone());
    }
    guard.last_error = None;
}

fn count_duplicate_warnings(summary: &AdifImportSummary) -> u32 {
    u32::try_from(
        summary
            .warnings
            .iter()
            .filter(|warning| warning.contains("duplicate skipped"))
            .count(),
    )
    .unwrap_or(u32::MAX)
}

async fn update_config_status(
    status: &Arc<Mutex<WsjtxIngestStatus>>,
    settings: &WsjtxRuntimeSettings,
) {
    let mut guard = status.lock().await;
    guard.enabled = settings.enabled;
    guard.udp_bind.clone_from(&settings.udp_bind);
    guard.adif_tail_path = settings
        .adif_tail_path
        .as_ref()
        .map(|path| path.display().to_string());
    guard.running = settings.enabled && (guard.udp_running || guard.adif_tail_running);
}

async fn mark_udp_stopped(status: &Arc<Mutex<WsjtxIngestStatus>>) {
    let mut guard = status.lock().await;
    guard.udp_running = false;
    guard.running = guard.enabled && guard.adif_tail_running;
}

async fn mark_tail_stopped(status: &Arc<Mutex<WsjtxIngestStatus>>) {
    let mut guard = status.lock().await;
    guard.adif_tail_running = false;
    guard.running = guard.enabled && guard.udp_running;
}

async fn wait_or_cancel(cancel_rx: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        result = cancel_rx.changed() => result.is_ok() && *cancel_rx.borrow(),
    }
}

fn now_timestamp() -> Timestamp {
    qsoripper_core::utc::to_timestamp(time::OffsetDateTime::now_utc())
}

#[derive(Clone, Debug, PartialEq, Eq)]
#[expect(
    clippy::struct_excessive_bools,
    reason = "WSJT-X settings mirror protobuf/TOML boolean toggles."
)]
struct WsjtxRuntimeSettings {
    enabled: bool,
    udp_enabled: bool,
    udp_bind: String,
    adif_tail_enabled: bool,
    adif_tail_path: Option<PathBuf>,
    poll_interval: Duration,
    sync_to_qrz: bool,
}

impl WsjtxRuntimeSettings {
    fn from_values(values: &BTreeMap<String, String>) -> Self {
        let enabled = bool_value(values, WSJTX_INGEST_ENABLED_ENV_VAR, false);
        Self {
            enabled,
            udp_enabled: bool_value(values, WSJTX_INGEST_UDP_ENABLED_ENV_VAR, true),
            udp_bind: values
                .get(WSJTX_INGEST_UDP_BIND_ENV_VAR)
                .filter(|value| !value.trim().is_empty())
                .cloned()
                .unwrap_or_else(|| DEFAULT_WSJTX_INGEST_UDP_BIND.to_string()),
            adif_tail_enabled: bool_value(values, WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR, false),
            adif_tail_path: values
                .get(WSJTX_INGEST_ADIF_TAIL_PATH_ENV_VAR)
                .filter(|value| !value.trim().is_empty())
                .map(PathBuf::from),
            poll_interval: Duration::from_millis(
                values
                    .get(WSJTX_INGEST_POLL_INTERVAL_MS_ENV_VAR)
                    .and_then(|value| value.parse::<u64>().ok())
                    .filter(|value| *value > 0)
                    .unwrap_or_else(|| {
                        DEFAULT_WSJTX_INGEST_POLL_INTERVAL_MS
                            .parse()
                            .unwrap_or(1_000)
                    }),
            ),
            sync_to_qrz: bool_value(values, WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR, false),
        }
    }
}

fn bool_value(values: &BTreeMap<String, String>, key: &str, default: bool) -> bool {
    values.get(key).map_or(default, |value| {
        match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" | "yes" | "y" | "on" => true,
            "0" | "false" | "no" | "n" | "off" => false,
            _ => default,
        }
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{
        process_datagram, record_import_summary, WsjtxIngestSupervisor, WsjtxRuntimeSettings,
    };
    use crate::runtime_config::{
        RuntimeConfigManager, WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR,
        WSJTX_INGEST_ADIF_TAIL_PATH_ENV_VAR, WSJTX_INGEST_ENABLED_ENV_VAR,
        WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR, WSJTX_INGEST_UDP_BIND_ENV_VAR,
    };
    use qsoripper_core::application::logbook::AdifImportSummary;
    use qsoripper_core::proto::qsoripper::services::WsjtxIngestStatus;
    use qsoripper_core::storage::QsoListQuery;
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::net::UdpSocket;
    use tokio::time::{sleep, Duration};

    fn append_be_u32(target: &mut Vec<u8>, value: u32) {
        target.extend_from_slice(&value.to_be_bytes());
    }

    fn append_wsjtx_utf8(target: &mut Vec<u8>, value: &str) {
        append_be_u32(
            target,
            u32::try_from(value.len()).expect("test string length"),
        );
        target.extend_from_slice(value.as_bytes());
    }

    fn wsjtx_logged_adif_frame(adif: &str) -> Vec<u8> {
        let mut frame = Vec::new();
        append_be_u32(&mut frame, 0xadbc_cbda);
        append_be_u32(&mut frame, 2);
        append_be_u32(&mut frame, 12);
        append_wsjtx_utf8(&mut frame, "WSJT-X");
        append_wsjtx_utf8(&mut frame, adif);
        frame
    }

    fn sample_adif(callsign: &str) -> String {
        format!(
            "<STATION_CALLSIGN:4>W1AW <CALL:{}>{callsign} <QSO_DATE:8>20250101 <TIME_ON:4>1200 <BAND:3>20M <MODE:3>FT8 <RST_SENT:3>-10 <RST_RCVD:3>-12 <EOR>",
            callsign.len()
        )
    }

    fn free_udp_bind() -> String {
        let socket = std::net::UdpSocket::bind("127.0.0.1:0").expect("bind probe");
        socket.local_addr().expect("local addr").to_string()
    }

    #[tokio::test]
    async fn wsjtx_udp_supervisor_imports_logged_adif_datagram() {
        let bind = free_udp_bind();
        let mut values = BTreeMap::new();
        values.insert(WSJTX_INGEST_ENABLED_ENV_VAR.to_string(), "true".to_string());
        values.insert(WSJTX_INGEST_UDP_BIND_ENV_VAR.to_string(), bind.clone());
        let runtime = Arc::new(RuntimeConfigManager::new(values).expect("runtime"));
        let supervisor = WsjtxIngestSupervisor::new();
        supervisor.start(runtime.clone());

        sleep(Duration::from_millis(100)).await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("sender");
        sender
            .send_to(&wsjtx_logged_adif_frame(&sample_adif("K7UDP")), &bind)
            .await
            .expect("send datagram");

        let mut imported = false;
        for _ in 0..20 {
            let qsos = runtime
                .logbook_engine()
                .await
                .list_qsos(&QsoListQuery::default())
                .await
                .expect("list qsos");
            if qsos.iter().any(|qso| qso.worked_callsign == "K7UDP") {
                imported = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        supervisor.stop();
        assert!(imported, "WSJT-X UDP datagram was not imported");
    }

    #[tokio::test]
    async fn record_import_summary_does_not_treat_warnings_as_errors() {
        let status = Arc::new(tokio::sync::Mutex::new(WsjtxIngestStatus::default()));
        let summary = AdifImportSummary {
            records_skipped: 1,
            warnings: vec!["Record 1 duplicate skipped".to_string()],
            ..AdifImportSummary::default()
        };

        record_import_summary(&status, &summary).await;

        let guard = status.lock().await;
        assert_eq!(guard.records_skipped, 1);
        assert_eq!(guard.duplicates_skipped, 1);
        assert!(guard.last_error.is_none());
    }

    #[tokio::test]
    async fn process_datagram_rejects_raw_adif_on_production_udp_path() {
        let mut values = BTreeMap::new();
        values.insert(WSJTX_INGEST_ENABLED_ENV_VAR.to_string(), "true".to_string());
        let runtime = Arc::new(RuntimeConfigManager::new(values).expect("runtime"));
        let status = Arc::new(tokio::sync::Mutex::new(WsjtxIngestStatus::default()));
        let import_lock = Arc::new(tokio::sync::Mutex::new(()));
        let qrz_sync_lock = Arc::new(tokio::sync::Mutex::new(()));

        process_datagram(
            &runtime,
            &status,
            &import_lock,
            &qrz_sync_lock,
            sample_adif("K7RAW").as_bytes(),
            false,
        )
        .await;

        let qsos = runtime
            .logbook_engine()
            .await
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("list qsos");
        let guard = status.lock().await;
        assert!(qsos.is_empty());
        assert_eq!(guard.records_skipped, 1);
        assert_eq!(guard.parse_errors, 0);
    }

    #[tokio::test]
    async fn wsjtx_adif_tail_supervisor_imports_existing_log_file() {
        let file = NamedTempFile::new().expect("temp file");
        fs::write(file.path(), sample_adif("K7TAIL")).expect("write adif");

        let mut values = BTreeMap::new();
        values.insert(WSJTX_INGEST_ENABLED_ENV_VAR.to_string(), "true".to_string());
        values.insert(
            WSJTX_INGEST_ADIF_TAIL_ENABLED_ENV_VAR.to_string(),
            "true".to_string(),
        );
        values.insert(
            WSJTX_INGEST_ADIF_TAIL_PATH_ENV_VAR.to_string(),
            file.path().display().to_string(),
        );
        let runtime = Arc::new(RuntimeConfigManager::new(values).expect("runtime"));
        let supervisor = WsjtxIngestSupervisor::new();
        supervisor.start(runtime.clone());

        let mut imported = false;
        for _ in 0..20 {
            let qsos = runtime
                .logbook_engine()
                .await
                .list_qsos(&QsoListQuery::default())
                .await
                .expect("list qsos");
            if qsos.iter().any(|qso| qso.worked_callsign == "K7TAIL") {
                imported = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        supervisor.stop();
        assert!(imported, "WSJT-X ADIF tail file was not imported");
    }

    #[tokio::test]
    async fn wsjtx_sync_to_qrz_records_missing_key_without_losing_local_qso() {
        let bind = free_udp_bind();
        let mut values = BTreeMap::new();
        values.insert(WSJTX_INGEST_ENABLED_ENV_VAR.to_string(), "true".to_string());
        values.insert(WSJTX_INGEST_UDP_BIND_ENV_VAR.to_string(), bind.clone());
        values.insert(
            WSJTX_INGEST_SYNC_TO_QRZ_ENV_VAR.to_string(),
            "true".to_string(),
        );
        let runtime = Arc::new(RuntimeConfigManager::new(values).expect("runtime"));
        let supervisor = WsjtxIngestSupervisor::new();
        let status = supervisor.status_handle();
        supervisor.start(runtime.clone());

        sleep(Duration::from_millis(100)).await;
        let sender = UdpSocket::bind("127.0.0.1:0").await.expect("sender");
        sender
            .send_to(&wsjtx_logged_adif_frame(&sample_adif("K7QRZ")), &bind)
            .await
            .expect("send datagram");

        let mut recorded = false;
        for _ in 0..20 {
            let qsos = runtime
                .logbook_engine()
                .await
                .list_qsos(&QsoListQuery::default())
                .await
                .expect("list qsos");
            let snapshot = status.lock().await.clone();
            if qsos.iter().any(|qso| qso.worked_callsign == "K7QRZ")
                && snapshot.last_qrz_sync_error.is_some()
            {
                assert!(!snapshot.last_qrz_sync_success);
                assert_eq!(
                    Some("QRZ Logbook API key is not configured."),
                    snapshot.last_qrz_sync_error.as_deref()
                );
                recorded = true;
                break;
            }
            sleep(Duration::from_millis(100)).await;
        }

        supervisor.stop();
        assert!(
            recorded,
            "WSJT-X QRZ missing-key diagnostic was not recorded"
        );
    }

    #[test]
    fn wsjtx_runtime_settings_default_udp_enabled_when_ingest_enabled() {
        let mut values = BTreeMap::new();
        values.insert(WSJTX_INGEST_ENABLED_ENV_VAR.to_string(), "true".to_string());

        let settings = WsjtxRuntimeSettings::from_values(&values);

        assert!(settings.enabled);
        assert!(settings.udp_enabled);
    }
}
