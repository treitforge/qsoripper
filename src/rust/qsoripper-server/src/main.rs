//! Runnable tonic gRPC host for the `QsoRipper` Rust engine.

mod qrz_secret_store;
mod repair;
mod runtime_config;
mod setup;
mod station_profile_support;
mod sync;
mod sync_scheduler;
mod wsjtx_ingest;

use std::{
    fs,
    future::Future,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use qsoripper_core::adif::{parse_adi_qsos, serialize_adi_qsos};
use qsoripper_core::application::enrichment_backfill::{
    run_enrichment_backfill, EnrichmentBackfillProgress,
};
use qsoripper_core::application::logbook::LogbookError;
use qsoripper_core::cw::{CwController, CwError, CwKeyerConfig};
use qsoripper_core::lookup::QRZ_USER_AGENT_ENV_VAR;
use qsoripper_core::lotw::{self, LotwConfig};
use qsoripper_core::storage::{
    DeletedRecordsFilter, EngineStorage, QsoListQuery, QsoSortOrder, StorageError,
};
use qsoripper_storage_memory::MemoryStorage;
use qsoripper_storage_sqlite::SqliteStorageBuilder;
use tokio::sync::Mutex;
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    Stream, StreamExt,
};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use prost_types::Timestamp;
use qsoripper_core::proto::qsoripper::domain::{
    Band, ConflictPolicy, ContestCalendarEntry, Mode, StationProfile,
};
use qsoripper_core::proto::qsoripper::services::{
    contest_calendar_service_server::{ContestCalendarService, ContestCalendarServiceServer},
    cw_service_server::{CwService, CwServiceServer},
    developer_control_service_server::{DeveloperControlService, DeveloperControlServiceServer},
    engine_service_server::{EngineService, EngineServiceServer},
    great_circle_service_server::{GreatCircleService, GreatCircleServiceServer},
    logbook_service_server::{LogbookService, LogbookServiceServer},
    lookup_service_server::{LookupService, LookupServiceServer},
    rig_control_service_server::{RigControlService, RigControlServiceServer},
    setup_service_server::SetupServiceServer,
    space_weather_service_server::{SpaceWeatherService, SpaceWeatherServiceServer},
    station_profile_service_server::StationProfileServiceServer,
    AbortCwRequest, AbortCwResponse, AdifChunk, ApplyRuntimeConfigRequest,
    ApplyRuntimeConfigResponse, BackfillQsoEnrichmentMode, BackfillQsoEnrichmentRequest,
    BackfillQsoEnrichmentResponse, BatchLookupRequest, BatchLookupResponse,
    ComputeGreatCircleRequest, ComputeGreatCircleResponse, CwSendState, DeleteQsoRequest,
    DeleteQsoResponse, DeletedRecordsFilter as ProtoDeletedRecordsFilter, EngineInfo,
    ExportAdifRequest, ExportAdifResponse, GetActiveContestsRequest, GetActiveContestsResponse,
    GetCachedCallsignRequest, GetCachedCallsignResponse, GetCurrentSpaceWeatherRequest,
    GetCurrentSpaceWeatherResponse, GetCwKeyerStatusRequest, GetCwKeyerStatusResponse,
    GetDxccEntityRequest, GetDxccEntityResponse, GetEngineInfoRequest, GetEngineInfoResponse,
    GetQsoRequest, GetQsoResponse, GetRigSnapshotRequest, GetRigSnapshotResponse,
    GetRigStatusRequest, GetRigStatusResponse, GetRuntimeConfigRequest, GetRuntimeConfigResponse,
    GetSyncStatusRequest, GetSyncStatusResponse, ImportAdifRequest, ImportAdifResponse,
    ListCwMacrosRequest, ListCwMacrosResponse, ListQsosRequest, ListQsosResponse, LogQsoRequest,
    LogQsoResponse, LookupRequest, LookupResponse, PurgeDeletedQsosRequest,
    PurgeDeletedQsosResponse, QsoSortOrder as ProtoQsoSortOrder, RefreshContestCalendarRequest,
    RefreshContestCalendarResponse, RefreshSpaceWeatherRequest, RefreshSpaceWeatherResponse,
    ResetRuntimeConfigRequest, ResetRuntimeConfigResponse, RestoreQsoRequest, RestoreQsoResponse,
    SendCwMacroRequest, SendCwMacroResponse, SendCwTextRequest, SendCwTextResponse,
    SetCwSpeedRequest, SetCwSpeedResponse, StreamLookupRequest, StreamLookupResponse,
    SyncWithLotwRequest, SyncWithLotwResponse, SyncWithQrzRequest, SyncWithQrzResponse,
    TestRigConnectionRequest, TestRigConnectionResponse, UpdateQsoRequest, UpdateQsoResponse,
};
use qsoripper_core::rig_control::{
    RigControlProvider, RigctldConfig, RigctldProvider, DEFAULT_RIGCTLD_HOST, DEFAULT_RIGCTLD_PORT,
    DEFAULT_RIGCTLD_READ_TIMEOUT_MS,
};
use runtime_config::RuntimeConfigManager;
use setup::{
    default_config_path, SetupControlSurface, SetupState, StationProfileControlSurface,
    CONFIG_PATH_ENV_VAR,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_dotenv_if_present();
    let options = ServerOptions::from_env_and_args(std::env::args().skip(1))?;
    run_server(options, tokio::signal::ctrl_c()).await
}

async fn run_server<ShutdownSignal>(
    options: ServerOptions,
    shutdown_signal: ShutdownSignal,
) -> Result<(), Box<dyn std::error::Error>>
where
    ShutdownSignal: Future<Output = std::io::Result<()>>,
{
    let address = options.listen_address;
    let setup_state = Arc::new(SetupState::load(options.config_path.clone())?);
    let config_file_values = setup_state.runtime_config_values().await;
    let cw_config = CwKeyerConfig::from_config_values(&config_file_values)?;
    let runtime_config = Arc::new(
        RuntimeConfigManager::new_with_config_file_values_and_cli_storage_overrides(
            config_file_values,
            &options.runtime_config_cli_storage_overrides(),
        )?,
    );
    let background_services = start_background_services(runtime_config.clone());

    run_startup_repairs(&runtime_config).await;
    let logbook_service = DeveloperLogbookService::new(
        runtime_config.clone(),
        background_services.sync_scheduler.clone(),
        background_services.qrz_upload_lock.clone(),
        background_services.lotw_upload_lock.clone(),
    );
    let lookup_service = DeveloperLookupService::new(runtime_config.clone());
    let engine_service = EngineControlSurface;
    let developer_control_service = DeveloperControlSurface::new(runtime_config.clone());
    let setup_service = SetupControlSurface::new(setup_state.clone(), runtime_config.clone())
        .with_wsjtx_ingest_status(background_services.wsjtx_ingest.status_handle());
    let station_profile_service =
        StationProfileControlSurface::new(setup_state.clone(), runtime_config.clone());
    let contest_calendar_service = ContestCalendarControlSurface::new(runtime_config.clone());
    let space_weather_service = SpaceWeatherControlSurface::new(runtime_config.clone());
    let rig_control_service = RigControlControlSurface::new(runtime_config.clone());
    let great_circle_service = GreatCircleControlSurface::new();
    let cw_service = CwControlSurface::new(runtime_config.clone(), CwController::new(cw_config));
    let active_storage_backend = runtime_config.active_storage_backend().await;
    let setup_status = setup_state.status().await;
    let setup_completion = setup_completion_label(setup_status.setup_complete);
    let config_path = setup_status.config_path.clone();

    println!(
        "{}",
        server_starting_message(
            address,
            active_storage_backend.as_str(),
            setup_completion,
            config_path.as_str(),
        )
    );

    let listener = tokio::net::TcpListener::bind(address).await?;
    let bound_address = listener.local_addr()?;

    println!(
        "{}",
        server_ready_message(
            bound_address,
            active_storage_backend.as_str(),
            setup_completion,
            config_path.as_str(),
        )
    );

    let (shutdown_sender, shutdown_receiver) = tokio::sync::oneshot::channel::<()>();
    let server = Server::builder()
        .add_service(EngineServiceServer::new(engine_service))
        .add_service(LogbookServiceServer::new(logbook_service))
        .add_service(LookupServiceServer::new(lookup_service))
        .add_service(SetupServiceServer::new(setup_service))
        .add_service(StationProfileServiceServer::new(station_profile_service))
        .add_service(ContestCalendarServiceServer::new(contest_calendar_service))
        .add_service(SpaceWeatherServiceServer::new(space_weather_service))
        .add_service(RigControlServiceServer::new(rig_control_service))
        .add_service(GreatCircleServiceServer::new(great_circle_service))
        .add_service(CwServiceServer::new(cw_service))
        .add_service(DeveloperControlServiceServer::new(
            developer_control_service,
        ))
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let _ = shutdown_receiver.await;
        });
    tokio::pin!(server);
    tokio::pin!(shutdown_signal);

    tokio::select! {
        result = &mut server => {
            result?;
        }
        signal_result = &mut shutdown_signal => {
            signal_result?;
            println!("Shutting down.");
            background_services.stop();
            let _ = shutdown_sender.send(());
            server.await?;
        }
    }

    background_services.stop();

    Ok(())
}

async fn run_startup_repairs(runtime_config: &Arc<RuntimeConfigManager>) {
    // One-shot QRZ logid backfill + duplicate collapse. Older builds never
    // mapped APP_QRZLOG_LOGID into qrz_logid, so QRZ pulls produced rows
    // that subsequent syncs duplicated whenever fuzzy matching missed.
    // Repair is idempotent — clean stores are a no-op.
    let logbook_engine = runtime_config.logbook_engine().await;
    match repair::backfill_qrz_logids(logbook_engine.logbook_store()).await {
        Ok(report) => {
            if !report.is_no_op() {
                eprintln!(
                    "[repair] QRZ logid backfill: backfilled={}, duplicates_removed={}, merged_groups={}",
                    report.backfilled, report.duplicates_removed, report.merged_groups,
                );
            }
        }
        Err(err) => {
            eprintln!("[repair] QRZ logid backfill failed (continuing startup): {err}");
        }
    }
}

struct BackgroundServices {
    sync_scheduler: Arc<sync_scheduler::SyncScheduler>,
    wsjtx_ingest: Arc<wsjtx_ingest::WsjtxIngestSupervisor>,
    qrz_upload_lock: Arc<Mutex<()>>,
    lotw_upload_lock: Arc<Mutex<()>>,
}

impl BackgroundServices {
    fn stop(&self) {
        self.sync_scheduler.stop();
        self.wsjtx_ingest.stop();
    }
}

fn start_background_services(runtime_config: Arc<RuntimeConfigManager>) -> BackgroundServices {
    let qrz_upload_lock = Arc::new(Mutex::new(()));
    let lotw_upload_lock = Arc::new(Mutex::new(()));
    let sync_scheduler = Arc::new(sync_scheduler::SyncScheduler::new(qrz_upload_lock.clone()));
    sync_scheduler.start(runtime_config.clone());
    let wsjtx_ingest = Arc::new(wsjtx_ingest::WsjtxIngestSupervisor::with_qrz_sync_lock(
        qrz_upload_lock.clone(),
    ));
    wsjtx_ingest.start(runtime_config);
    BackgroundServices {
        sync_scheduler,
        wsjtx_ingest,
        qrz_upload_lock,
        lotw_upload_lock,
    }
}

fn setup_completion_label(setup_complete: bool) -> &'static str {
    if setup_complete {
        "complete"
    } else {
        "incomplete"
    }
}

fn server_starting_message(
    address: SocketAddr,
    active_storage_backend: &str,
    setup_completion: &str,
    config_path: &str,
) -> String {
    format!(
        "Starting QsoRipper gRPC server on {address} using {active_storage_backend} storage (setup: {setup_completion}, config: {config_path})"
    )
}

fn server_ready_message(
    address: SocketAddr,
    active_storage_backend: &str,
    setup_completion: &str,
    config_path: &str,
) -> String {
    format!(
        "QsoRipper gRPC server ready on {address} using {active_storage_backend} storage (setup: {setup_completion}, config: {config_path})"
    )
}

fn load_dotenv_if_present() {
    match dotenvy::dotenv() {
        Ok(path) => println!("Loaded config from {}", path.display()),
        Err(dotenvy::Error::Io(error)) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            if let Some(path) = load_dotenv_with_legacy_qrz_user_agent_compatibility(&error) {
                eprintln!(
                    "Warning: loaded {} after auto-correcting a legacy unquoted {} value; quote that value in .env to remove this warning.",
                    path.display(),
                    QRZ_USER_AGENT_ENV_VAR
                );
                println!("Loaded config from {}", path.display());
            } else {
                eprintln!("Warning: failed to parse .env file: {error}");
            }
        }
    }
}

fn load_dotenv_with_legacy_qrz_user_agent_compatibility(error: &dotenvy::Error) -> Option<PathBuf> {
    let dotenvy::Error::LineParse(_, _) = error else {
        return None;
    };
    let path = find_dotenv_path().ok()??;
    let contents = fs::read_to_string(&path).ok()?;
    let compatible_contents = sanitize_legacy_qrz_user_agent_contents(&contents)?;
    dotenvy::from_read(std::io::Cursor::new(compatible_contents)).ok()?;
    Some(path)
}

fn legacy_qrz_user_agent_line_compatibility(line: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let (key, raw_value) = trimmed.split_once('=')?;

    if key.trim() != QRZ_USER_AGENT_ENV_VAR {
        return None;
    }

    let value = raw_value.trim();
    if value.is_empty()
        || value.contains('#')
        || matches!(value.chars().next(), Some('"' | '\''))
        || !(value.chars().any(char::is_whitespace) || value.contains('(') || value.contains(')'))
    {
        return None;
    }

    let leading_whitespace = &line[..line.len() - trimmed.len()];
    Some(format!(
        r#"{leading_whitespace}{QRZ_USER_AGENT_ENV_VAR}="{}""#,
        value.replace('\\', "\\\\").replace('"', "\\\"")
    ))
}

fn sanitize_legacy_qrz_user_agent_contents(contents: &str) -> Option<String> {
    let mut changed = false;
    let mut compatible_contents = String::with_capacity(contents.len());

    for segment in contents.split_inclusive('\n') {
        let (line, newline) = if let Some(line) = segment.strip_suffix("\r\n") {
            (line, "\r\n")
        } else if let Some(line) = segment.strip_suffix('\n') {
            (line, "\n")
        } else {
            (segment, "")
        };

        if let Some(compatible_line) = legacy_qrz_user_agent_line_compatibility(line) {
            compatible_contents.push_str(&compatible_line);
            changed = true;
        } else {
            compatible_contents.push_str(line);
        }

        compatible_contents.push_str(newline);
    }

    changed.then_some(compatible_contents)
}

fn find_dotenv_path() -> io::Result<Option<PathBuf>> {
    let current_dir = std::env::current_dir()?;
    Ok(current_dir
        .ancestors()
        .map(|directory| directory.join(".env"))
        .find(|candidate| candidate.is_file()))
}

#[derive(Clone)]
struct DeveloperLogbookService {
    runtime_config: Arc<RuntimeConfigManager>,
    sync_scheduler: Arc<sync_scheduler::SyncScheduler>,
    qrz_upload_lock: Arc<Mutex<()>>,
    lotw_upload_lock: Arc<Mutex<()>>,
    enrichment_backfill_lock: Arc<Mutex<()>>,
}

impl DeveloperLogbookService {
    fn new(
        runtime_config: Arc<RuntimeConfigManager>,
        sync_scheduler: Arc<sync_scheduler::SyncScheduler>,
        qrz_upload_lock: Arc<Mutex<()>>,
        lotw_upload_lock: Arc<Mutex<()>>,
    ) -> Self {
        Self {
            runtime_config,
            sync_scheduler,
            qrz_upload_lock,
            lotw_upload_lock,
            enrichment_backfill_lock: Arc::new(Mutex::new(())),
        }
    }

    /// Build a QRZ logbook client from the current runtime configuration.
    /// Returns a human-readable error string on failure (suitable for gRPC
    /// response fields, not `Status`).
    async fn build_qrz_logbook_client(
        &self,
    ) -> Result<qsoripper_core::qrz_logbook::QrzLogbookClient, String> {
        let effective = self.runtime_config.effective_values().await;

        let api_key = effective
            .get(runtime_config::QRZ_LOGBOOK_API_KEY_ENV_VAR)
            .cloned()
            .unwrap_or_default();
        if api_key.trim().is_empty() {
            return Err("QRZ Logbook API key is not configured.".into());
        }

        let base_url = effective
            .get(runtime_config::QRZ_LOGBOOK_BASE_URL_ENV_VAR)
            .cloned()
            .unwrap_or_else(|| runtime_config::DEFAULT_QRZ_LOGBOOK_BASE_URL.to_string());

        let config = qsoripper_core::qrz_logbook::QrzLogbookConfig::new(
            api_key,
            base_url,
            "QsoRipper/1.0".to_string(),
        );

        qsoripper_core::qrz_logbook::QrzLogbookClient::new(config)
            .map_err(|err| format!("Failed to create QRZ logbook client: {err}"))
    }

    async fn build_lotw_client(&self) -> Result<lotw::LotwClient, String> {
        let effective = self.runtime_config.effective_values().await;
        let value = |key: &str| effective.get(key).cloned().unwrap_or_default();

        let timeout_seconds = effective
            .get(runtime_config::LOTW_TIMEOUT_SECONDS_ENV_VAR)
            .map_or(Ok(60), |value| value.parse::<u64>())
            .map_err(|_| "LoTW timeout must be an integer.".to_string())?;
        let config = LotwConfig::new(
            value(runtime_config::LOTW_USERNAME_ENV_VAR),
            value(runtime_config::LOTW_PASSWORD_ENV_VAR),
            effective
                .get(runtime_config::LOTW_TQSL_PATH_ENV_VAR)
                .cloned()
                .unwrap_or_else(|| runtime_config::DEFAULT_LOTW_TQSL_PATH.to_string()),
            value(runtime_config::LOTW_STATION_LOCATION_ENV_VAR),
        )
        .and_then(|config| {
            config.with_report_url(
                effective
                    .get(runtime_config::LOTW_REPORT_URL_ENV_VAR)
                    .map_or(runtime_config::DEFAULT_LOTW_REPORT_URL, String::as_str),
            )
        })
        .map_err(|error| error.to_string())?
        .with_certificate_password(
            effective
                .get(runtime_config::LOTW_CERTIFICATE_PASSWORD_ENV_VAR)
                .cloned(),
        )
        .with_timeout(Duration::from_secs(timeout_seconds.max(1)));

        lotw::LotwClient::new(config).map_err(|error| error.to_string())
    }

    /// Push a single QSO to QRZ for the per-RPC `sync_to_qrz=true` paths on
    /// `LogQso` / `UpdateQso`. On success, mutates `stored` in-place to carry
    /// the QRZ-assigned logid and `Synced` status, mirroring what the bulk
    /// sync Phase 2 writes back to local storage.
    ///
    /// Returns `(sync_success, sync_error)` shaped for the gRPC response.
    /// Failure leaves `stored` untouched; the local row keeps its current
    /// status (`LocalOnly` or `Modified`) and the next bulk sync will retry.
    async fn run_per_op_qrz_sync(
        &self,
        engine: &qsoripper_core::application::logbook::LogbookEngine,
        stored: &mut qsoripper_core::proto::qsoripper::domain::QsoRecord,
    ) -> (bool, Option<String>) {
        let client = match self.build_qrz_logbook_client().await {
            Ok(client) => client,
            Err(err) => return (false, Some(err)),
        };

        let cached_metadata = engine
            .logbook_store()
            .get_sync_metadata()
            .await
            .unwrap_or_default();
        let book_owner = sync::resolve_book_owner_for_upload(&client, &cached_metadata).await;

        let _upload_guard = self.qrz_upload_lock.lock().await;
        match sync::sync_single_qso(
            &client,
            engine.logbook_store(),
            stored,
            book_owner.as_deref(),
        )
        .await
        {
            Ok(outcome) => {
                *stored = outcome.qso;
                (true, None)
            }
            Err(err) => (false, Some(err)),
        }
    }

    async fn run_per_op_lotw_sync(
        &self,
        engine: &qsoripper_core::application::logbook::LogbookEngine,
        stored: &mut qsoripper_core::proto::qsoripper::domain::QsoRecord,
    ) -> (bool, Option<String>) {
        let client = match self.build_lotw_client().await {
            Ok(client) => client,
            Err(error) => return (false, Some(error)),
        };
        let _upload_guard = self.lotw_upload_lock.lock().await;
        match lotw::upload_single_qso(&client, engine.logbook_store(), stored).await {
            Ok(updated) => {
                *stored = updated;
                (true, None)
            }
            Err(error) => (false, Some(error.to_string())),
        }
    }
}

#[tonic::async_trait]
impl LogbookService for DeveloperLogbookService {
    type ListQsosStream = ReceiverStream<Result<ListQsosResponse, Status>>;
    type BackfillQsoEnrichmentStream =
        Pin<Box<dyn Stream<Item = Result<BackfillQsoEnrichmentResponse, Status>> + Send>>;
    type SyncWithQrzStream = ReceiverStream<Result<SyncWithQrzResponse, Status>>;
    type SyncWithLotwStream = ReceiverStream<Result<SyncWithLotwResponse, Status>>;
    type ExportAdifStream = ReceiverStream<Result<ExportAdifResponse, Status>>;

    async fn log_qso(
        &self,
        request: Request<LogQsoRequest>,
    ) -> Result<Response<LogQsoResponse>, Status> {
        let (engine, active_station_profile) = self.runtime_config.logbook_context().await;
        let request = request.into_inner();
        let qso = request
            .qso
            .ok_or_else(|| Status::invalid_argument("LogQso requires a qso payload."))?;
        let mut stored = engine
            .log_qso_with_station_profile(qso, active_station_profile.as_ref())
            .await
            .map_err(map_logbook_error)?;
        let (sync_success, sync_error) = if request.sync_to_qrz {
            self.run_per_op_qrz_sync(&engine, &mut stored).await
        } else {
            (false, None)
        };
        let (lotw_sync_success, lotw_sync_error) = if request.sync_to_lotw {
            self.run_per_op_lotw_sync(&engine, &mut stored).await
        } else {
            (false, None)
        };

        Ok(Response::new(LogQsoResponse {
            local_id: stored.local_id,
            qrz_logid: stored.qrz_logid,
            sync_success,
            sync_error,
            lotw_sync_success,
            lotw_sync_error,
        }))
    }

    async fn update_qso(
        &self,
        request: Request<UpdateQsoRequest>,
    ) -> Result<Response<UpdateQsoResponse>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();
        let qso = request
            .qso
            .ok_or_else(|| Status::invalid_argument("UpdateQso requires a qso payload."))?;
        let mut stored = engine.update_qso(qso).await.map_err(map_logbook_error)?;
        let (sync_success, sync_error) = if request.sync_to_qrz {
            self.run_per_op_qrz_sync(&engine, &mut stored).await
        } else {
            (false, None)
        };
        let (lotw_sync_success, lotw_sync_error) = if request.sync_to_lotw {
            self.run_per_op_lotw_sync(&engine, &mut stored).await
        } else {
            (false, None)
        };

        Ok(Response::new(UpdateQsoResponse {
            success: true,
            error: None,
            sync_success,
            sync_error,
            lotw_sync_success,
            lotw_sync_error,
        }))
    }

    async fn delete_qso(
        &self,
        request: Request<DeleteQsoRequest>,
    ) -> Result<Response<DeleteQsoResponse>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();

        // Soft-delete the local row. When the caller asked for QRZ deletion
        // we mark pending_remote_delete so the next SyncWithQrz removes it
        // from QRZ during sync Phase 2. Restore before sync cancels it.
        let deleted = engine
            .delete_qso(&request.local_id, request.delete_from_qrz)
            .await
            .map_err(map_logbook_error)?;

        let has_logid = deleted.qrz_logid.as_deref().is_some_and(|s| !s.is_empty());
        let queued = request.delete_from_qrz && has_logid;
        let qrz_delete_error = if request.delete_from_qrz && !has_logid {
            Some("QSO has no QRZ logid — it may not have been synced yet.".into())
        } else {
            None
        };

        Ok(Response::new(DeleteQsoResponse {
            success: true,
            error: None,
            // Legacy fields: synchronous QRZ delete is no longer performed.
            qrz_delete_success: false,
            qrz_delete_error,
            remote_delete_queued: queued,
        }))
    }

    async fn restore_qso(
        &self,
        request: Request<RestoreQsoRequest>,
    ) -> Result<Response<RestoreQsoResponse>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();
        if request.local_id.trim().is_empty() {
            return Err(Status::invalid_argument(
                "RestoreQso requires a non-empty local_id.",
            ));
        }
        let restored = engine
            .restore_qso(&request.local_id)
            .await
            .map_err(map_logbook_error)?;
        Ok(Response::new(RestoreQsoResponse {
            success: true,
            error: None,
            restored: Some(restored),
        }))
    }

    async fn purge_deleted_qsos(
        &self,
        request: Request<PurgeDeletedQsosRequest>,
    ) -> Result<Response<PurgeDeletedQsosResponse>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();

        // Safety latch: require explicit confirmation.
        if !request.confirm {
            return Err(Status::invalid_argument(
                "PurgeDeletedQsos requires confirm=true to proceed.",
            ));
        }

        // Sync gating: hold the sync lock across the entire purge so that
        // no sync can start between the check and the storage DELETE.
        let sync_guard = self.sync_scheduler.sync_guard().await;
        if *sync_guard {
            return Err(Status::failed_precondition(
                "Cannot purge while a sync is in progress.",
            ));
        }

        let older_than_ms = request.older_than.as_ref().map(|timestamp| {
            timestamp
                .seconds
                .saturating_mul(1_000)
                .saturating_add(i64::from(timestamp.nanos) / 1_000_000)
        });
        let client = if request.include_pending_remote_deletes {
            self.build_qrz_logbook_client().await.ok()
        } else {
            None
        };
        let _upload_guard = if client.is_some() {
            Some(self.qrz_upload_lock.lock().await)
        } else {
            None
        };
        let outcome = sync::purge_deleted_qsos(
            client
                .as_ref()
                .map(|client| client as &dyn sync::QrzLogbookApi),
            engine.logbook_store(),
            &request.local_ids,
            older_than_ms,
            request.include_pending_remote_deletes,
        )
        .await
        .map_err(|error| Status::internal(error.to_string()))?;

        // Release sync guard after purge completes.
        drop(sync_guard);

        Ok(Response::new(PurgeDeletedQsosResponse {
            purged_count: outcome.purged_count,
            remote_deletes_pushed: outcome.remote_deletes_pushed,
            remote_deletes_failed: outcome.remote_deletes_failed,
            error_summary: outcome.errors.join(" "),
        }))
    }

    async fn get_qso(
        &self,
        request: Request<GetQsoRequest>,
    ) -> Result<Response<GetQsoResponse>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();
        let qso = engine
            .get_qso(&request.local_id)
            .await
            .map_err(map_logbook_error)?;

        Ok(Response::new(GetQsoResponse { qso: Some(qso) }))
    }

    async fn list_qsos(
        &self,
        request: Request<ListQsosRequest>,
    ) -> Result<Response<Self::ListQsosStream>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();
        let query = qso_list_query_from_request(&request).map_err(|status| *status)?;
        let records = engine.list_qsos(&query).await.map_err(map_logbook_error)?;
        let (sender, receiver) = tokio::sync::mpsc::channel(records.len().max(1));

        for record in records {
            if sender
                .send(Ok(ListQsosResponse { qso: Some(record) }))
                .await
                .is_err()
            {
                break;
            }
        }

        Ok(Response::new(ReceiverStream::new(receiver)))
    }

    async fn backfill_qso_enrichment(
        &self,
        request: Request<BackfillQsoEnrichmentRequest>,
    ) -> Result<Response<Self::BackfillQsoEnrichmentStream>, Status> {
        let request = request.into_inner();
        let mode = BackfillQsoEnrichmentMode::try_from(request.mode)
            .map_err(|_| Status::invalid_argument("Invalid backfill mode."))?;
        if !is_valid_backfill_timestamp(request.after.as_ref()) {
            return Err(Status::invalid_argument(
                "Backfill after must be a valid protobuf Timestamp.",
            ));
        }
        if !is_valid_backfill_timestamp(request.before.as_ref()) {
            return Err(Status::invalid_argument(
                "Backfill before must be a valid protobuf Timestamp.",
            ));
        }
        if matches!(
            (&request.after, &request.before),
            (Some(after), Some(before))
                if (after.seconds, after.nanos) > (before.seconds, before.nanos)
        ) {
            return Err(Status::invalid_argument(
                "Backfill after must not be later than before.",
            ));
        }
        let guard = self
            .enrichment_backfill_lock
            .clone()
            .try_lock_owned()
            .map_err(|_| Status::resource_exhausted("A QSO enrichment backfill is active."))?;
        let apply = mode == BackfillQsoEnrichmentMode::Apply;
        let query = QsoListQuery {
            after: request.after,
            before: request.before,
            sort: QsoSortOrder::OldestFirst,
            deleted_filter: DeletedRecordsFilter::ActiveOnly,
            ..QsoListQuery::default()
        };
        let (engine, coordinator) = self.runtime_config.enrichment_backfill_context().await;
        let (progress_tx, progress_rx) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            let _guard = guard;
            run_enrichment_backfill(
                engine.logbook_store(),
                &coordinator,
                &query,
                apply,
                &progress_tx,
            )
            .await;
        });
        let stream =
            ReceiverStream::new(progress_rx).map(|progress| Ok(backfill_response(progress)));

        Ok(Response::new(Box::pin(stream)))
    }

    async fn sync_with_qrz(
        &self,
        request: Request<SyncWithQrzRequest>,
    ) -> Result<Response<Self::SyncWithQrzStream>, Status> {
        let request = request.into_inner();

        let client = self
            .build_qrz_logbook_client()
            .await
            .map_err(Status::failed_precondition)?;

        let effective = self.runtime_config.effective_values().await;
        let conflict_policy = match effective
            .get(runtime_config::SYNC_CONFLICT_POLICY_ENV_VAR)
            .map(String::as_str)
        {
            Some("flag_for_review") => ConflictPolicy::FlagForReview,
            _ => ConflictPolicy::LastWriteWins,
        };

        let engine = self.runtime_config.logbook_engine().await;
        let (tx, rx) = tokio::sync::mpsc::channel(16);
        let qrz_upload_lock = self.qrz_upload_lock.clone();

        tokio::spawn(async move {
            let _upload_guard = qrz_upload_lock.lock().await;
            let store = engine.logbook_store();
            sync::execute_sync(&client, store, request.full_sync, conflict_policy, &tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn sync_with_lotw(
        &self,
        request: Request<SyncWithLotwRequest>,
    ) -> Result<Response<Self::SyncWithLotwStream>, Status> {
        let request = request.into_inner();
        let upload = request.upload.unwrap_or(true);
        let download = request.download.unwrap_or(true);
        if !upload && !download {
            return Err(Status::invalid_argument(
                "SyncWithLotw requires upload, download, or both.",
            ));
        }

        let client = self
            .build_lotw_client()
            .await
            .map_err(Status::failed_precondition)?;
        let engine = self.runtime_config.logbook_engine().await;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let lotw_upload_lock = self.lotw_upload_lock.clone();

        tokio::spawn(async move {
            let _upload_guard = lotw_upload_lock.lock().await;
            let _ = tx
                .send(Ok(SyncWithLotwResponse {
                    current_action: Some("Starting LoTW sync".to_string()),
                    ..SyncWithLotwResponse::default()
                }))
                .await;
            let response = match lotw::execute_sync(
                &client,
                engine.logbook_store(),
                request.full_sync,
                upload,
                download,
            )
            .await
            {
                Ok(result) => SyncWithLotwResponse {
                    total_records: result.total_records,
                    processed_records: result.processed_records,
                    uploaded_records: result.uploaded_records,
                    confirmed_records: result.confirmed_records,
                    unmatched_records: result.unmatched_records,
                    conflict_records: result.conflict_records,
                    error_records: result.error_records,
                    current_action: Some("LoTW sync complete".to_string()),
                    complete: true,
                    error: result.error_summary,
                    confirmation_high_water: result.confirmation_high_water,
                },
                Err(error) => SyncWithLotwResponse {
                    current_action: Some("LoTW sync failed".to_string()),
                    complete: true,
                    error_records: 1,
                    error: Some(error.to_string()),
                    ..SyncWithLotwResponse::default()
                },
            };
            let _ = tx.send(Ok(response)).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_sync_status(
        &self,
        _request: Request<GetSyncStatusRequest>,
    ) -> Result<Response<GetSyncStatusResponse>, Status> {
        let sync_status = self
            .runtime_config
            .logbook_engine()
            .await
            .get_sync_status()
            .await
            .map_err(map_logbook_error)?;

        let effective = self.runtime_config.effective_values().await;
        let auto_sync_enabled = effective
            .get(runtime_config::SYNC_AUTO_ENABLED_ENV_VAR)
            .is_some_and(|v| v == "true");

        Ok(Response::new(GetSyncStatusResponse {
            local_qso_count: sync_status.local_qso_count,
            qrz_qso_count: sync_status.qrz_qso_count,
            pending_upload: sync_status.pending_upload,
            last_sync: sync_status.last_sync,
            qrz_logbook_owner: sync_status.qrz_logbook_owner,
            is_syncing: self.sync_scheduler.is_syncing().await,
            next_sync: self.sync_scheduler.next_sync().await,
            auto_sync_enabled,
            last_sync_error: self.sync_scheduler.last_error().await,
        }))
    }

    async fn import_adif(
        &self,
        request: Request<tonic::Streaming<ImportAdifRequest>>,
    ) -> Result<Response<ImportAdifResponse>, Status> {
        let (engine, active_station_profile) = self.runtime_config.logbook_context().await;
        let mut stream = request.into_inner();
        let mut adif_bytes = Vec::new();
        let mut refresh = false;

        while let Some(msg) = stream.message().await? {
            refresh = refresh || msg.refresh;
            let chunk = msg
                .chunk
                .ok_or_else(|| Status::invalid_argument("ImportAdifRequest requires a chunk."))?;
            adif_bytes.extend_from_slice(&chunk.data);
        }

        let qsos = parse_adi_qsos(&adif_bytes)
            .await
            .map_err(Status::invalid_argument)?;
        let summary =
            Box::pin(engine.import_adif_qsos(qsos, active_station_profile.as_ref(), refresh))
                .await
                .map_err(map_logbook_error)?;

        Ok(Response::new(ImportAdifResponse {
            records_imported: summary.records_imported,
            records_skipped: summary.records_skipped,
            warnings: summary.warnings,
            records_updated: summary.records_updated,
        }))
    }

    async fn export_adif(
        &self,
        request: Request<ExportAdifRequest>,
    ) -> Result<Response<Self::ExportAdifStream>, Status> {
        let engine = self.runtime_config.logbook_engine().await;
        let request = request.into_inner();
        let query = export_qso_list_query_from_request(&request);
        let qsos = engine.list_qsos(&query).await.map_err(map_logbook_error)?;
        let adif_bytes = serialize_adi_qsos(&qsos, request.include_header);
        let chunk_count = adif_bytes.len().div_ceil(ADIF_CHUNK_SIZE).max(1);
        let (sender, receiver) = tokio::sync::mpsc::channel(chunk_count);

        for chunk in adif_bytes.chunks(ADIF_CHUNK_SIZE) {
            if sender
                .send(Ok(ExportAdifResponse {
                    chunk: Some(AdifChunk {
                        data: chunk.to_vec(),
                    }),
                }))
                .await
                .is_err()
            {
                break;
            }
        }

        Ok(Response::new(ReceiverStream::new(receiver)))
    }
}

#[derive(Clone)]
struct DeveloperLookupService {
    runtime_config: Arc<RuntimeConfigManager>,
}

impl DeveloperLookupService {
    fn new(runtime_config: Arc<RuntimeConfigManager>) -> Self {
        Self { runtime_config }
    }
}

#[tonic::async_trait]
impl LookupService for DeveloperLookupService {
    type StreamLookupStream = ReceiverStream<Result<StreamLookupResponse, Status>>;

    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let coordinator = self.runtime_config.lookup_coordinator().await;
        let request = request.into_inner();
        let result = coordinator
            .lookup(&request.callsign, request.skip_cache)
            .await;
        Ok(Response::new(LookupResponse {
            result: Some(result),
        }))
    }

    async fn stream_lookup(
        &self,
        request: Request<StreamLookupRequest>,
    ) -> Result<Response<Self::StreamLookupStream>, Status> {
        let coordinator = self.runtime_config.lookup_coordinator().await;
        let request = request.into_inner();
        let (transport_tx, transport_rx) = tokio::sync::mpsc::channel(8);

        tokio::spawn(async move {
            let (update_tx, mut update_rx) = tokio::sync::mpsc::unbounded_channel();
            let producer = async move {
                coordinator
                    .stream_lookup_into(&request.callsign, request.skip_cache, &update_tx)
                    .await;
                drop(update_tx);
            };

            let forwarder = async {
                while let Some(update) = update_rx.recv().await {
                    if transport_tx
                        .send(Ok(StreamLookupResponse {
                            result: Some(update),
                        }))
                        .await
                        .is_err()
                    {
                        break;
                    }
                }
            };

            tokio::join!(producer, forwarder);
        });

        Ok(Response::new(ReceiverStream::new(transport_rx)))
    }

    async fn get_cached_callsign(
        &self,
        request: Request<GetCachedCallsignRequest>,
    ) -> Result<Response<GetCachedCallsignResponse>, Status> {
        let coordinator = self.runtime_config.lookup_coordinator().await;
        let request = request.into_inner();
        let result = coordinator.get_cached_callsign(&request.callsign).await;
        Ok(Response::new(GetCachedCallsignResponse {
            result: Some(result),
        }))
    }

    async fn get_dxcc_entity(
        &self,
        request: Request<GetDxccEntityRequest>,
    ) -> Result<Response<GetDxccEntityResponse>, Status> {
        use qsoripper_core::proto::qsoripper::services::get_dxcc_entity_request::Query;

        let request = request.into_inner();
        match request.query {
            Some(Query::DxccCode(code)) => {
                match qsoripper_core::adif::lookup_dxcc_entity_by_code(code) {
                    Some(entity) => Ok(Response::new(GetDxccEntityResponse {
                        entity: Some(entity),
                    })),
                    None => Err(Status::not_found(format!("DXCC entity {code} not found."))),
                }
            }
            Some(Query::Prefix(_)) => Err(Status::unimplemented(
                "Prefix-based DXCC lookup is not yet supported.",
            )),
            None => Err(Status::invalid_argument(
                "Either dxcc_code or prefix must be specified.",
            )),
        }
    }

    async fn batch_lookup(
        &self,
        request: Request<BatchLookupRequest>,
    ) -> Result<Response<BatchLookupResponse>, Status> {
        use std::sync::Arc;
        use tokio::sync::Semaphore;

        const MAX_CONCURRENCY: usize = 5;

        let coordinator = self.runtime_config.lookup_coordinator().await;
        let request = request.into_inner();
        let callsigns = request.callsigns;

        if callsigns.is_empty() {
            return Ok(Response::new(BatchLookupResponse {
                results: Vec::new(),
            }));
        }

        let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENCY));
        let mut handles = Vec::with_capacity(callsigns.len());

        for callsign in callsigns {
            let coordinator = coordinator.clone();
            let semaphore = semaphore.clone();
            handles.push(tokio::spawn(async move {
                let permit = semaphore.acquire().await.map_err(|_| {
                    Status::internal("batch lookup semaphore was closed unexpectedly")
                })?;
                let result = coordinator.lookup(&callsign, false).await;
                drop(permit);
                Ok::<_, Status>(result)
            }));
        }

        let mut results = Vec::with_capacity(handles.len());
        for handle in handles {
            let result = handle
                .await
                .map_err(|err| Status::internal(format!("batch lookup task failed: {err}")))??;
            results.push(result);
        }

        Ok(Response::new(BatchLookupResponse { results }))
    }
}

#[derive(Clone)]
struct DeveloperControlSurface {
    runtime_config: Arc<RuntimeConfigManager>,
}

impl DeveloperControlSurface {
    fn new(runtime_config: Arc<RuntimeConfigManager>) -> Self {
        Self { runtime_config }
    }
}

const RUST_ENGINE_ID: &str = "rust-tonic";
const RUST_ENGINE_DISPLAY_NAME: &str = "QsoRipper Rust Engine";
const RUST_ENGINE_CAPABILITIES: &[&str] = &[
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
];

#[derive(Clone)]
struct CwControlSurface {
    runtime_config: Arc<RuntimeConfigManager>,
    controller: CwController,
}

impl CwControlSurface {
    fn new(runtime_config: Arc<RuntimeConfigManager>, controller: CwController) -> Self {
        Self {
            runtime_config,
            controller,
        }
    }

    async fn active_station_profile(&self) -> Option<StationProfile> {
        self.runtime_config.effective_station_profile().await
    }
}

#[tonic::async_trait]
impl CwService for CwControlSurface {
    async fn list_cw_macros(
        &self,
        _request: Request<ListCwMacrosRequest>,
    ) -> Result<Response<ListCwMacrosResponse>, Status> {
        Ok(Response::new(ListCwMacrosResponse {
            macros: self.controller.built_in_macros(),
        }))
    }

    async fn send_cw_macro(
        &self,
        request: Request<SendCwMacroRequest>,
    ) -> Result<Response<SendCwMacroResponse>, Status> {
        let request = request.into_inner();
        let station_profile = self.active_station_profile().await;
        let expanded_text = self
            .controller
            .expand_macro(
                request.name.as_str(),
                request.context.as_ref(),
                station_profile.as_ref(),
            )
            .map_err(cw_status)?;
        let speed_wpm = request
            .context
            .as_ref()
            .and_then(|context| context.speed_wpm);
        let controller = self.controller.clone();
        let text_to_send = expanded_text.clone();
        cw_blocking(move || controller.send_text(&text_to_send, speed_wpm)).await?;
        Ok(Response::new(SendCwMacroResponse {
            state: CwSendState::Accepted as i32,
            expanded_text,
            error_message: None,
        }))
    }

    async fn send_cw_text(
        &self,
        request: Request<SendCwTextRequest>,
    ) -> Result<Response<SendCwTextResponse>, Status> {
        let request = request.into_inner();
        let station_profile = self.active_station_profile().await;
        let expanded_text = self
            .controller
            .expand_text(
                request.text.as_str(),
                request.context.as_ref(),
                station_profile.as_ref(),
            )
            .map_err(cw_status)?;
        let speed_wpm = request
            .context
            .as_ref()
            .and_then(|context| context.speed_wpm);
        let controller = self.controller.clone();
        let text_to_send = expanded_text.clone();
        cw_blocking(move || controller.send_text(&text_to_send, speed_wpm)).await?;
        Ok(Response::new(SendCwTextResponse {
            state: CwSendState::Accepted as i32,
            expanded_text,
            error_message: None,
        }))
    }

    async fn abort_cw(
        &self,
        _request: Request<AbortCwRequest>,
    ) -> Result<Response<AbortCwResponse>, Status> {
        let controller = self.controller.clone();
        cw_blocking(move || controller.abort()).await?;
        Ok(Response::new(AbortCwResponse {
            state: CwSendState::AbortRequested as i32,
            error_message: None,
        }))
    }

    async fn set_cw_speed(
        &self,
        request: Request<SetCwSpeedRequest>,
    ) -> Result<Response<SetCwSpeedResponse>, Status> {
        let speed_wpm = request.into_inner().speed_wpm;
        let controller = self.controller.clone();
        cw_blocking(move || controller.set_speed(speed_wpm)).await?;
        let controller = self.controller.clone();
        let status = cw_blocking(move || Ok(controller.status())).await?;
        Ok(Response::new(SetCwSpeedResponse {
            status: Some(status),
        }))
    }

    async fn get_cw_keyer_status(
        &self,
        _request: Request<GetCwKeyerStatusRequest>,
    ) -> Result<Response<GetCwKeyerStatusResponse>, Status> {
        let controller = self.controller.clone();
        let status = cw_blocking(move || Ok(controller.status())).await?;
        Ok(Response::new(GetCwKeyerStatusResponse {
            status: Some(status),
        }))
    }
}

async fn cw_blocking<T>(
    operation: impl FnOnce() -> Result<T, CwError> + Send + 'static,
) -> Result<T, Status>
where
    T: Send + 'static,
{
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|error| Status::internal(format!("CW backend task failed: {error}")))?
        .map_err(cw_status)
}

fn cw_status(error: CwError) -> Status {
    match error {
        CwError::UnknownMacro(message) => Status::not_found(message),
        CwError::MissingTokenValue("MYCALL", _) => Status::failed_precondition(error.to_string()),
        CwError::UnknownToken(_)
        | CwError::UnmatchedOpenBrace
        | CwError::UnmatchedCloseBrace
        | CwError::MissingTokenValue(_, _)
        | CwError::InvalidSpeed(_)
        | CwError::InvalidText(_) => Status::invalid_argument(error.to_string()),
        CwError::BackendUnavailable(_) | CwError::TransmitDisabled => {
            Status::failed_precondition(error.to_string())
        }
        CwError::Io(_) => Status::unavailable(error.to_string()),
    }
}

#[derive(Debug, Default)]
struct EngineControlSurface;

#[tonic::async_trait]
impl EngineService for EngineControlSurface {
    async fn get_engine_info(
        &self,
        _request: Request<GetEngineInfoRequest>,
    ) -> Result<Response<GetEngineInfoResponse>, Status> {
        Ok(Response::new(GetEngineInfoResponse {
            engine: Some(EngineInfo {
                engine_id: RUST_ENGINE_ID.to_string(),
                display_name: RUST_ENGINE_DISPLAY_NAME.to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
                capabilities: RUST_ENGINE_CAPABILITIES
                    .iter()
                    .map(|value| (*value).to_string())
                    .collect(),
            }),
        }))
    }
}

#[tonic::async_trait]
impl DeveloperControlService for DeveloperControlSurface {
    async fn get_runtime_config(
        &self,
        _request: Request<GetRuntimeConfigRequest>,
    ) -> Result<Response<GetRuntimeConfigResponse>, Status> {
        Ok(Response::new(GetRuntimeConfigResponse {
            snapshot: Some(self.runtime_config.snapshot().await),
        }))
    }

    async fn apply_runtime_config(
        &self,
        request: Request<ApplyRuntimeConfigRequest>,
    ) -> Result<Response<ApplyRuntimeConfigResponse>, Status> {
        let snapshot = self
            .runtime_config
            .apply_request(request.into_inner())
            .await
            .map_err(Status::invalid_argument)?;
        Ok(Response::new(ApplyRuntimeConfigResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn reset_runtime_config(
        &self,
        request: Request<ResetRuntimeConfigRequest>,
    ) -> Result<Response<ResetRuntimeConfigResponse>, Status> {
        let snapshot = self
            .runtime_config
            .reset_request(request.into_inner())
            .await
            .map_err(Status::invalid_argument)?;
        Ok(Response::new(ResetRuntimeConfigResponse {
            snapshot: Some(snapshot),
        }))
    }
}

#[derive(Clone)]
struct ContestCalendarControlSurface {
    runtime_config: Arc<RuntimeConfigManager>,
}

impl ContestCalendarControlSurface {
    fn new(runtime_config: Arc<RuntimeConfigManager>) -> Self {
        Self { runtime_config }
    }
}

#[tonic::async_trait]
impl ContestCalendarService for ContestCalendarControlSurface {
    async fn get_active_contests(
        &self,
        request: Request<GetActiveContestsRequest>,
    ) -> Result<Response<GetActiveContestsResponse>, Status> {
        let request = request.into_inner();
        if let Some(value) = request.band {
            Band::try_from(value)
                .map_err(|_| Status::invalid_argument("Invalid band filter value."))?;
        }
        if let Some(value) = request.mode {
            Mode::try_from(value)
                .map_err(|_| Status::invalid_argument("Invalid mode filter value."))?;
        }
        let snapshot = self
            .runtime_config
            .contest_calendar_monitor()
            .await
            .current_snapshot()
            .await;
        let contests = filter_active_contests(snapshot.contests, &request);
        Ok(Response::new(GetActiveContestsResponse {
            contests,
            status: snapshot.status as i32,
            fetched_at: snapshot.fetched_at,
            valid_until: snapshot.valid_until,
            error_message: snapshot.error_message,
        }))
    }

    async fn refresh_contest_calendar(
        &self,
        _request: Request<RefreshContestCalendarRequest>,
    ) -> Result<Response<RefreshContestCalendarResponse>, Status> {
        let snapshot = self
            .runtime_config
            .contest_calendar_monitor()
            .await
            .refresh_snapshot()
            .await;
        Ok(Response::new(RefreshContestCalendarResponse {
            contests: snapshot.contests,
            status: snapshot.status as i32,
            fetched_at: snapshot.fetched_at,
            valid_until: snapshot.valid_until,
            error_message: snapshot.error_message,
        }))
    }
}

fn filter_active_contests(
    contests: Vec<ContestCalendarEntry>,
    request: &GetActiveContestsRequest,
) -> Vec<ContestCalendarEntry> {
    let at = request.at_utc.unwrap_or_else(now_timestamp);
    let lookahead_seconds = i64::from(request.lookahead_minutes).saturating_mul(60);
    let through = at.seconds.saturating_add(lookahead_seconds);
    contests
        .into_iter()
        .filter(|contest| contest_is_active(contest, at.seconds, through))
        .filter(|contest| {
            enum_filter_matches(
                request.band,
                &contest.bands,
                request.include_partial_matches,
            )
        })
        .filter(|contest| {
            enum_filter_matches(
                request.mode,
                &contest.modes,
                request.include_partial_matches,
            )
        })
        .collect()
}

fn contest_is_active(
    contest: &ContestCalendarEntry,
    at_seconds: i64,
    through_seconds: i64,
) -> bool {
    match (&contest.start_time_utc, &contest.end_time_utc) {
        (Some(start), Some(end)) => start.seconds <= through_seconds && end.seconds >= at_seconds,
        _ => false,
    }
}

fn enum_filter_matches(filter: Option<i32>, values: &[i32], include_partial_matches: bool) -> bool {
    match filter {
        Some(value) if value > 0 => {
            values.contains(&value) || values.is_empty() && include_partial_matches
        }
        _ => true,
    }
}

fn now_timestamp() -> Timestamp {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    Timestamp {
        seconds: i64::try_from(now.as_secs()).unwrap_or(i64::MAX),
        nanos: i32::try_from(now.subsec_nanos()).unwrap_or(i32::MAX),
    }
}

#[derive(Clone)]
struct SpaceWeatherControlSurface {
    runtime_config: Arc<RuntimeConfigManager>,
}

impl SpaceWeatherControlSurface {
    fn new(runtime_config: Arc<RuntimeConfigManager>) -> Self {
        Self { runtime_config }
    }
}

#[tonic::async_trait]
impl SpaceWeatherService for SpaceWeatherControlSurface {
    async fn get_current_space_weather(
        &self,
        _request: Request<GetCurrentSpaceWeatherRequest>,
    ) -> Result<Response<GetCurrentSpaceWeatherResponse>, Status> {
        let snapshot = self
            .runtime_config
            .space_weather_monitor()
            .await
            .current_snapshot()
            .await;
        Ok(Response::new(GetCurrentSpaceWeatherResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn refresh_space_weather(
        &self,
        _request: Request<RefreshSpaceWeatherRequest>,
    ) -> Result<Response<RefreshSpaceWeatherResponse>, Status> {
        let snapshot = self
            .runtime_config
            .space_weather_monitor()
            .await
            .refresh_snapshot()
            .await;
        Ok(Response::new(RefreshSpaceWeatherResponse {
            snapshot: Some(snapshot),
        }))
    }
}

#[derive(Clone, Default)]
struct GreatCircleControlSurface;

impl GreatCircleControlSurface {
    fn new() -> Self {
        Self
    }
}

#[tonic::async_trait]
impl GreatCircleService for GreatCircleControlSurface {
    async fn compute_great_circle(
        &self,
        request: Request<ComputeGreatCircleRequest>,
    ) -> Result<Response<ComputeGreatCircleResponse>, Status> {
        use qsoripper_core::geodesy::{
            distance_km, final_bearing_deg, initial_bearing_deg, resolve_sample_count,
            sample_great_circle, validate_point,
        };
        use qsoripper_core::proto::qsoripper::domain::GreatCirclePath;

        let req = request.into_inner();
        let origin = resolve_geo_reference(req.origin.as_ref(), "origin")?;
        let target = resolve_geo_reference(req.target.as_ref(), "target")?;
        validate_point(&origin)
            .map_err(|err| Status::invalid_argument(format!("origin: {err}")))?;
        validate_point(&target)
            .map_err(|err| Status::invalid_argument(format!("target: {err}")))?;
        let count = resolve_sample_count(req.sample_count)
            .map_err(|err| Status::invalid_argument(err.to_string()))?;
        let samples = sample_great_circle(&origin, &target, count);
        let path = GreatCirclePath {
            origin: Some(origin),
            target: Some(target),
            distance_km: distance_km(&origin, &target),
            initial_bearing_deg: initial_bearing_deg(&origin, &target),
            final_bearing_deg: final_bearing_deg(&origin, &target),
            samples,
        };
        Ok(Response::new(ComputeGreatCircleResponse {
            path: Some(path),
        }))
    }
}

#[allow(clippy::result_large_err)]
fn resolve_geo_reference(
    reference: Option<&qsoripper_core::proto::qsoripper::domain::GeoReference>,
    label: &str,
) -> Result<qsoripper_core::proto::qsoripper::domain::GeoPoint, Status> {
    let reference = reference
        .ok_or_else(|| Status::invalid_argument(format!("{label} reference is required")))?;
    if let Some(coords) = reference.coordinates {
        return Ok(coords);
    }
    if let Some(grid) = reference.maidenhead.as_deref() {
        return qsoripper_core::geodesy::maidenhead_to_geopoint(grid)
            .map_err(|err| Status::invalid_argument(format!("{label}: {err}")));
    }
    Err(Status::invalid_argument(format!(
        "{label}: must supply coordinates or maidenhead"
    )))
}

#[derive(Clone)]
struct RigControlControlSurface {
    runtime_config: Arc<RuntimeConfigManager>,
}

impl RigControlControlSurface {
    fn new(runtime_config: Arc<RuntimeConfigManager>) -> Self {
        Self { runtime_config }
    }
}

#[tonic::async_trait]
impl RigControlService for RigControlControlSurface {
    async fn get_rig_status(
        &self,
        _request: Request<GetRigStatusRequest>,
    ) -> Result<Response<GetRigStatusResponse>, Status> {
        let snapshot = self
            .runtime_config
            .rig_control_monitor()
            .await
            .current_snapshot()
            .await;
        Ok(Response::new(GetRigStatusResponse {
            status: snapshot.status,
            error_message: snapshot.error_message,
            endpoint: None,
        }))
    }

    async fn get_rig_snapshot(
        &self,
        _request: Request<GetRigSnapshotRequest>,
    ) -> Result<Response<GetRigSnapshotResponse>, Status> {
        let snapshot = self
            .runtime_config
            .rig_control_monitor()
            .await
            .current_snapshot()
            .await;
        Ok(Response::new(GetRigSnapshotResponse {
            snapshot: Some(snapshot),
        }))
    }

    async fn test_rig_connection(
        &self,
        request: Request<TestRigConnectionRequest>,
    ) -> Result<Response<TestRigConnectionResponse>, Status> {
        let inner = request.into_inner();
        let effective_values = self.runtime_config.effective_values().await;

        let host = inner.host.unwrap_or_else(|| {
            effective_values
                .get(qsoripper_core::rig_control::RIGCTLD_HOST_ENV_VAR)
                .cloned()
                .unwrap_or_else(|| DEFAULT_RIGCTLD_HOST.to_string())
        });

        let port = match inner.port {
            Some(port) => u16::try_from(port)
                .ok()
                .filter(|port| *port > 0)
                .ok_or_else(|| {
                    Status::invalid_argument("Rig control port must be between 1 and 65535.")
                })?,
            None => effective_values
                .get(qsoripper_core::rig_control::RIGCTLD_PORT_ENV_VAR)
                .and_then(|v| v.parse().ok())
                .unwrap_or(DEFAULT_RIGCTLD_PORT),
        };

        let read_timeout_ms = effective_values
            .get(qsoripper_core::rig_control::RIGCTLD_READ_TIMEOUT_MS_ENV_VAR)
            .and_then(|v| v.parse().ok())
            .unwrap_or(DEFAULT_RIGCTLD_READ_TIMEOUT_MS);

        let config = RigctldConfig {
            host,
            port,
            read_timeout: std::time::Duration::from_millis(read_timeout_ms),
        };

        let provider = RigctldProvider::new(config);
        match provider.get_snapshot().await {
            Ok(snapshot) => Ok(Response::new(TestRigConnectionResponse {
                success: true,
                error_message: None,
                snapshot: Some(snapshot),
            })),
            Err(error) => Ok(Response::new(TestRigConnectionResponse {
                success: false,
                error_message: Some(error.to_string()),
                snapshot: None,
            })),
        }
    }
}

#[derive(Debug, Clone)]
struct ServerOptions {
    listen_address: SocketAddr,
    config_path: PathBuf,
    #[cfg(test)]
    storage: StorageOptions,
    storage_cli_overrides: std::collections::BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StorageOptions {
    backend: StorageBackendKind,
    sqlite_path: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StorageBackendKind {
    Memory,
    Sqlite,
}

impl ServerOptions {
    fn from_env_and_args<I>(args: I) -> Result<Self, Box<dyn std::error::Error>>
    where
        I: IntoIterator<Item = String>,
    {
        let mut listen = std::env::var("QSORIPPER_SERVER_ADDR")
            .unwrap_or_else(|_| "127.0.0.1:50051".to_string());
        let mut config_path = std::env::var(CONFIG_PATH_ENV_VAR)
            .map(PathBuf::from)
            .unwrap_or(default_config_path()?);
        #[cfg(test)]
        let mut storage_backend = parse_storage_backend(
            &std::env::var("QSORIPPER_STORAGE_BACKEND").unwrap_or_else(|_| "memory".to_string()),
        )?;
        #[cfg(test)]
        let mut sqlite_path = PathBuf::from(
            std::env::var("QSORIPPER_SQLITE_PATH").unwrap_or_else(|_| "qsoripper.db".to_string()),
        );
        let mut storage_cli_overrides = std::collections::BTreeMap::new();
        let mut args = args.into_iter();

        while let Some(arg) = args.next() {
            match arg.as_str() {
                "--listen" => {
                    let value = args.next().ok_or("Missing value for --listen")?;
                    listen = value;
                }
                "--config" => {
                    let value = args.next().ok_or("Missing value for --config")?;
                    config_path = PathBuf::from(value);
                }
                "--storage" => {
                    let value = args.next().ok_or("Missing value for --storage")?;
                    let backend = parse_storage_backend(&value)?;
                    storage_cli_overrides.insert(
                        runtime_config::STORAGE_BACKEND_ENV_VAR.to_string(),
                        match backend {
                            StorageBackendKind::Memory => "memory".to_string(),
                            StorageBackendKind::Sqlite => "sqlite".to_string(),
                        },
                    );
                    #[cfg(test)]
                    {
                        storage_backend = backend;
                    }
                }
                "--sqlite-path" => {
                    let value = args.next().ok_or("Missing value for --sqlite-path")?;
                    let path = PathBuf::from(value);
                    storage_cli_overrides.insert(
                        runtime_config::SQLITE_PATH_ENV_VAR.to_string(),
                        path.display().to_string(),
                    );
                    #[cfg(test)]
                    {
                        sqlite_path = path;
                    }
                }
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                _ => return Err(format!("Unknown argument: {arg}").into()),
            }
        }

        Ok(Self {
            listen_address: listen.parse()?,
            config_path,
            #[cfg(test)]
            storage: StorageOptions {
                backend: storage_backend,
                sqlite_path,
            },
            storage_cli_overrides,
        })
    }

    fn runtime_config_cli_storage_overrides(&self) -> std::collections::BTreeMap<String, String> {
        self.storage_cli_overrides.clone()
    }
}

fn print_help() {
    println!(
        "QsoRipper gRPC server\n\nUsage:\n  cargo run -p qsoripper-server -- [--listen 127.0.0.1:50051] [--config path\\to\\config.toml] [--storage memory|sqlite] [--sqlite-path path\\to\\qsoripper.db]\n\nEnvironment:\n  QSORIPPER_SERVER_ADDR       Overrides the bind address\n  QSORIPPER_CONFIG_PATH       Overrides the persisted setup config path\n  QSORIPPER_STORAGE_BACKEND   Selects memory or sqlite storage (default: memory)\n  QSORIPPER_SQLITE_PATH       SQLite path when sqlite storage is selected (default: qsoripper.db)"
    );
}

fn build_storage(
    options: &StorageOptions,
) -> Result<Arc<dyn EngineStorage>, Box<dyn std::error::Error>> {
    let storage: Arc<dyn EngineStorage> = match options.backend {
        StorageBackendKind::Memory => Arc::new(MemoryStorage::new()),
        StorageBackendKind::Sqlite => Arc::new(
            SqliteStorageBuilder::new()
                .path(options.sqlite_path.clone())
                .build()?,
        ),
    };

    Ok(storage)
}

fn parse_storage_backend(value: &str) -> Result<StorageBackendKind, Box<dyn std::error::Error>> {
    match value.trim().to_ascii_lowercase().as_str() {
        "memory" => Ok(StorageBackendKind::Memory),
        "sqlite" => Ok(StorageBackendKind::Sqlite),
        other => Err(format!("Unsupported storage backend: {other}").into()),
    }
}

fn map_logbook_error(error: LogbookError) -> Status {
    match error {
        LogbookError::NoActiveStationProfile => Status::failed_precondition(error.to_string()),
        LogbookError::Validation(message) => Status::invalid_argument(message),
        LogbookError::NotFound(local_id) => {
            Status::not_found(format!("QSO '{local_id}' was not found."))
        }
        LogbookError::AlreadyDeleted(local_id) => Status::failed_precondition(format!(
            "QSO '{local_id}' is deleted; restore it before updating."
        )),
        LogbookError::Storage(StorageError::Duplicate { entity, key }) => {
            Status::already_exists(format!("{entity} '{key}' already exists."))
        }
        LogbookError::Storage(other) => Status::internal(other.to_string()),
    }
}

fn qso_list_query_from_request(request: &ListQsosRequest) -> Result<QsoListQuery, Box<Status>> {
    let band_filter = request
        .band_filter
        .map(|value| {
            Band::try_from(value)
                .map_err(|_| Box::new(Status::invalid_argument("Invalid band_filter value.")))
        })
        .transpose()?;
    let mode_filter = request
        .mode_filter
        .map(|value| {
            Mode::try_from(value)
                .map_err(|_| Box::new(Status::invalid_argument("Invalid mode_filter value.")))
        })
        .transpose()?;
    let sort = match ProtoQsoSortOrder::try_from(request.sort) {
        Ok(ProtoQsoSortOrder::NewestFirst) => QsoSortOrder::NewestFirst,
        Ok(ProtoQsoSortOrder::OldestFirst) => QsoSortOrder::OldestFirst,
        Err(_) => return Err(Box::new(Status::invalid_argument("Invalid sort order."))),
    };

    let deleted_filter = match ProtoDeletedRecordsFilter::try_from(request.deleted_filter) {
        Ok(ProtoDeletedRecordsFilter::Unspecified | ProtoDeletedRecordsFilter::ActiveOnly) => {
            DeletedRecordsFilter::ActiveOnly
        }
        Ok(ProtoDeletedRecordsFilter::DeletedOnly) => DeletedRecordsFilter::DeletedOnly,
        Ok(ProtoDeletedRecordsFilter::All) => DeletedRecordsFilter::All,
        Err(_) => {
            return Err(Box::new(Status::invalid_argument(
                "Invalid deleted_filter value.",
            )))
        }
    };

    Ok(QsoListQuery {
        after: request.after,
        before: request.before,
        callsign_filter: request
            .callsign_filter
            .as_deref()
            .and_then(non_empty_string),
        band_filter,
        mode_filter,
        contest_id: request.contest_id.as_deref().and_then(non_empty_string),
        limit: (request.limit > 0).then_some(request.limit),
        offset: request.offset,
        sort,
        deleted_filter,
    })
}

fn is_valid_backfill_timestamp(timestamp: Option<&Timestamp>) -> bool {
    const MIN_SECONDS: i64 = -62_135_596_800;
    const MAX_SECONDS: i64 = 253_402_300_799;

    if let Some(timestamp) = timestamp {
        if !(MIN_SECONDS..=MAX_SECONDS).contains(&timestamp.seconds)
            || !(0..1_000_000_000).contains(&timestamp.nanos)
        {
            return false;
        }
    }
    true
}

fn backfill_response(progress: EnrichmentBackfillProgress) -> BackfillQsoEnrichmentResponse {
    BackfillQsoEnrichmentResponse {
        scanned: progress.scanned,
        candidates: progress.candidates,
        unique_callsigns: progress.unique_callsigns,
        found: progress.found,
        not_found: progress.not_found,
        errors: progress.errors,
        changed: progress.changed,
        unchanged: progress.unchanged,
        concurrent_edits: progress.concurrent_edits,
        storage_errors: progress.storage_errors,
        complete: progress.complete,
        current_callsign: progress.current_callsign,
    }
}

const ADIF_CHUNK_SIZE: usize = 16 * 1024;

fn export_qso_list_query_from_request(request: &ExportAdifRequest) -> QsoListQuery {
    QsoListQuery {
        after: request.after,
        before: request.before,
        contest_id: request.contest_id.as_deref().and_then(non_empty_string),
        sort: QsoSortOrder::OldestFirst,
        ..QsoListQuery::default()
    }
}

fn non_empty_string(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::time::Duration;
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        build_storage, legacy_qrz_user_agent_line_compatibility, load_dotenv_if_present,
        parse_storage_backend, run_server, sanitize_legacy_qrz_user_agent_contents,
        server_ready_message, server_starting_message, sync_scheduler, DeveloperLogbookService,
        DeveloperLookupService, GreatCircleControlSurface, Server, ServerOptions,
        SpaceWeatherControlSurface, StorageBackendKind, StorageOptions,
    };
    use crate::runtime_config::{
        self, RuntimeConfigManager, SQLITE_PATH_ENV_VAR, STATION_CALLSIGN_ENV_VAR,
        STATION_GRID_ENV_VAR, STATION_OPERATOR_CALLSIGN_ENV_VAR, STATION_PROFILE_NAME_ENV_VAR,
        STORAGE_BACKEND_ENV_VAR,
    };
    use prost_types::Timestamp;
    use qsoripper_core::lookup::{
        QRZ_USER_AGENT_ENV_VAR, QRZ_XML_PASSWORD_ENV_VAR, QRZ_XML_USERNAME_ENV_VAR,
    };
    use qsoripper_core::proto::qsoripper::domain::SpaceWeatherStatus;
    use qsoripper_core::proto::qsoripper::domain::{
        Band, LookupResult, LookupState, Mode, QsoRecord, StationSnapshot, SyncStatus,
    };
    use qsoripper_core::proto::qsoripper::services::{
        get_dxcc_entity_request,
        great_circle_service_server::GreatCircleService,
        logbook_service_client::LogbookServiceClient,
        logbook_service_server::{LogbookService, LogbookServiceServer},
        lookup_service_server::LookupService,
        space_weather_service_server::SpaceWeatherService,
        AdifChunk, BackfillQsoEnrichmentRequest, BatchLookupRequest, ComputeGreatCircleRequest,
        DeleteQsoRequest, ExportAdifRequest, GetCachedCallsignRequest,
        GetCurrentSpaceWeatherRequest, GetDxccEntityRequest, GetQsoRequest, GetSyncStatusRequest,
        ImportAdifRequest, ImportAdifResponse, ListQsosRequest, LogQsoRequest, LookupRequest,
        QsoSortOrder, RefreshSpaceWeatherRequest, RestoreQsoRequest, StreamLookupRequest,
        SyncWithLotwRequest, UpdateQsoRequest,
    };
    use tokio_stream::StreamExt;
    use tonic::transport::Channel;
    use tonic::{Code, Request};

    static PROCESS_STATE_LOCK: Mutex<()> = Mutex::new(());

    const PROCESS_ENV_KEYS: [&str; 10] = [
        "QSORIPPER_SERVER_ADDR",
        "QSORIPPER_CONFIG_PATH",
        "QSORIPPER_STORAGE_BACKEND",
        "QSORIPPER_SQLITE_PATH",
        "QSORIPPER_NOAA_SPACE_WEATHER_ENABLED",
        "QSORIPPER_NOAA_KP_INDEX_URL",
        "QSORIPPER_NOAA_SOLAR_INDICES_URL",
        "QSORIPPER_NOAA_HTTP_TIMEOUT_SECONDS",
        "QSORIPPER_NOAA_REFRESH_INTERVAL_SECONDS",
        "QSORIPPER_NOAA_STALE_AFTER_SECONDS",
    ];

    struct ProcessStateGuard {
        original_dir: PathBuf,
        original_env: Vec<(&'static str, Option<String>)>,
    }

    impl ProcessStateGuard {
        fn capture() -> Self {
            Self {
                original_dir: std::env::current_dir().expect("current working directory"),
                original_env: PROCESS_ENV_KEYS
                    .into_iter()
                    .map(|key| (key, std::env::var(key).ok()))
                    .collect(),
            }
        }

        fn restore_current_dir(&self) {
            std::env::set_current_dir(&self.original_dir).expect("restore current directory");
        }
    }

    impl Drop for ProcessStateGuard {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.original_dir);
            for (key, value) in &self.original_env {
                match value {
                    Some(value) => std::env::set_var(key, value),
                    None => std::env::remove_var(key),
                }
            }
        }
    }

    fn capture_clean_process_state() -> (std::sync::MutexGuard<'static, ()>, ProcessStateGuard) {
        let process_state_lock = PROCESS_STATE_LOCK.lock().expect("lock process state");
        let process_state = ProcessStateGuard::capture();

        for key in PROCESS_ENV_KEYS {
            std::env::remove_var(key);
        }

        (process_state_lock, process_state)
    }

    fn test_lookup_service() -> DeveloperLookupService {
        DeveloperLookupService::new(test_runtime_config())
    }

    fn test_sync_scheduler() -> Arc<sync_scheduler::SyncScheduler> {
        Arc::new(sync_scheduler::SyncScheduler::new(Arc::new(
            tokio::sync::Mutex::new(()),
        )))
    }

    fn test_logbook_service(config: Arc<RuntimeConfigManager>) -> DeveloperLogbookService {
        DeveloperLogbookService::new(
            config,
            test_sync_scheduler(),
            Arc::new(tokio::sync::Mutex::new(())),
            Arc::new(tokio::sync::Mutex::new(())),
        )
    }

    fn test_runtime_config() -> Arc<RuntimeConfigManager> {
        Arc::new(RuntimeConfigManager::new(BTreeMap::new()).expect("runtime config"))
    }

    #[tokio::test]
    async fn second_enrichment_backfill_returns_resource_exhausted() {
        let service = test_logbook_service(test_runtime_config());
        let _guard = service.enrichment_backfill_lock.lock().await;

        let error = LogbookService::backfill_qso_enrichment(
            &service,
            Request::new(BackfillQsoEnrichmentRequest::default()),
        )
        .await
        .err()
        .expect("second backfill must fail");

        assert_eq!(error.code(), Code::ResourceExhausted);
    }

    #[tokio::test]
    async fn enrichment_backfill_rejects_invalid_timestamp_components() {
        let service = test_logbook_service(test_runtime_config());
        for timestamp in [
            Timestamp {
                seconds: 0,
                nanos: -1,
            },
            Timestamp {
                seconds: 0,
                nanos: 1_000_000_000,
            },
            Timestamp {
                seconds: 253_402_300_800,
                nanos: 0,
            },
        ] {
            let error = LogbookService::backfill_qso_enrichment(
                &service,
                Request::new(BackfillQsoEnrichmentRequest {
                    after: Some(timestamp),
                    ..Default::default()
                }),
            )
            .await
            .err()
            .expect("invalid timestamp must fail");

            assert_eq!(error.code(), Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn enrichment_backfill_rejects_reversed_timestamp_range() {
        let service = test_logbook_service(test_runtime_config());
        let error = LogbookService::backfill_qso_enrichment(
            &service,
            Request::new(BackfillQsoEnrichmentRequest {
                after: Some(Timestamp {
                    seconds: 10,
                    nanos: 1,
                }),
                before: Some(Timestamp {
                    seconds: 10,
                    nanos: 0,
                }),
                ..Default::default()
            }),
        )
        .await
        .err()
        .expect("reversed range must fail");

        assert_eq!(error.code(), Code::InvalidArgument);
    }

    fn test_runtime_config_with_logbook(
        backend: StorageBackendKind,
        sqlite_path: Option<&std::path::Path>,
        include_active_station: bool,
    ) -> Arc<RuntimeConfigManager> {
        let mut startup_values = BTreeMap::new();
        startup_values.insert(
            STORAGE_BACKEND_ENV_VAR.to_string(),
            match backend {
                StorageBackendKind::Memory => "memory".to_string(),
                StorageBackendKind::Sqlite => "sqlite".to_string(),
            },
        );

        if let Some(path) = sqlite_path {
            startup_values.insert(
                SQLITE_PATH_ENV_VAR.to_string(),
                path.to_string_lossy().into_owned(),
            );
        }

        if include_active_station {
            startup_values.insert(STATION_PROFILE_NAME_ENV_VAR.to_string(), "Home".to_string());
            startup_values.insert(STATION_CALLSIGN_ENV_VAR.to_string(), "K7RND".to_string());
            startup_values.insert(
                STATION_OPERATOR_CALLSIGN_ENV_VAR.to_string(),
                "K7RND".to_string(),
            );
            startup_values.insert(STATION_GRID_ENV_VAR.to_string(), "CN87".to_string());
        }

        Arc::new(RuntimeConfigManager::new(startup_values).expect("runtime config"))
    }

    fn test_runtime_config_with_space_weather_enabled(enabled: bool) -> Arc<RuntimeConfigManager> {
        let mut startup_values = BTreeMap::new();
        startup_values.insert(
            qsoripper_core::space_weather::NOAA_SPACE_WEATHER_ENABLED_ENV_VAR.to_string(),
            enabled.to_string(),
        );

        Arc::new(RuntimeConfigManager::new(startup_values).expect("runtime config"))
    }

    fn test_runtime_config_with_lotw(report_url: &str) -> Arc<RuntimeConfigManager> {
        let mut startup_values = BTreeMap::new();
        startup_values.insert(
            runtime_config::STORAGE_BACKEND_ENV_VAR.to_string(),
            "memory".to_string(),
        );
        startup_values.insert(
            runtime_config::LOTW_USERNAME_ENV_VAR.to_string(),
            "KC7AVA".to_string(),
        );
        startup_values.insert(
            runtime_config::LOTW_PASSWORD_ENV_VAR.to_string(),
            "test-password".to_string(),
        );
        startup_values.insert(
            runtime_config::LOTW_STATION_LOCATION_ENV_VAR.to_string(),
            "Home".to_string(),
        );
        startup_values.insert(
            runtime_config::LOTW_REPORT_URL_ENV_VAR.to_string(),
            report_url.to_string(),
        );
        startup_values.insert(
            runtime_config::LOTW_TIMEOUT_SECONDS_ENV_VAR.to_string(),
            "1".to_string(),
        );
        Arc::new(RuntimeConfigManager::new(startup_values).expect("runtime config"))
    }

    #[tokio::test]
    async fn get_current_space_weather_returns_disabled_snapshot_when_provider_is_disabled() {
        let service =
            SpaceWeatherControlSurface::new(test_runtime_config_with_space_weather_enabled(false));

        let response = service
            .get_current_space_weather(Request::new(GetCurrentSpaceWeatherRequest {}))
            .await
            .expect("space weather response")
            .into_inner();
        let snapshot = response.snapshot.expect("space weather snapshot");
        let error_message = snapshot.error_message.expect("error message");

        assert_eq!(SpaceWeatherStatus::Error as i32, snapshot.status);
        assert!(
            error_message.contains("disabled"),
            "unexpected error message: {error_message}"
        );
    }

    #[tokio::test]
    async fn refresh_space_weather_returns_disabled_snapshot_when_provider_is_disabled() {
        let service =
            SpaceWeatherControlSurface::new(test_runtime_config_with_space_weather_enabled(false));

        let response = service
            .refresh_space_weather(Request::new(RefreshSpaceWeatherRequest {}))
            .await
            .expect("space weather response")
            .into_inner();
        let snapshot = response.snapshot.expect("space weather snapshot");
        let error_message = snapshot.error_message.expect("error message");

        assert_eq!(SpaceWeatherStatus::Error as i32, snapshot.status);
        assert!(
            error_message.contains("disabled"),
            "unexpected error message: {error_message}"
        );
    }

    #[tokio::test]
    async fn lotw_client_reports_missing_and_invalid_configuration() {
        let service = test_logbook_service(test_runtime_config());
        let error = service
            .build_lotw_client()
            .await
            .err()
            .expect("missing LoTW configuration");
        assert!(error.contains("username"));

        let mut startup_values = BTreeMap::new();
        startup_values.insert(
            runtime_config::LOTW_TIMEOUT_SECONDS_ENV_VAR.to_string(),
            "not-an-integer".to_string(),
        );
        let config = Arc::new(RuntimeConfigManager::new(startup_values).expect("runtime config"));
        let service = test_logbook_service(config);
        let error = service
            .build_lotw_client()
            .await
            .err()
            .expect("invalid LoTW timeout");
        assert!(error.contains("integer"));
    }

    #[tokio::test]
    async fn lotw_sync_rejects_request_with_no_enabled_phase() {
        let service = test_logbook_service(test_runtime_config());

        let error = LogbookService::sync_with_lotw(
            &service,
            Request::new(SyncWithLotwRequest {
                upload: Some(false),
                download: Some(false),
                ..SyncWithLotwRequest::default()
            }),
        )
        .await
        .expect_err("invalid LoTW request");

        assert_eq!(error.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn lotw_upload_only_sync_completes_when_logbook_is_empty() {
        let service =
            test_logbook_service(test_runtime_config_with_lotw("http://127.0.0.1:1/report"));

        let mut stream = LogbookService::sync_with_lotw(
            &service,
            Request::new(SyncWithLotwRequest {
                upload: Some(true),
                download: Some(false),
                ..SyncWithLotwRequest::default()
            }),
        )
        .await
        .expect("LoTW sync stream")
        .into_inner();

        let starting = stream
            .next()
            .await
            .expect("starting result")
            .expect("starting response");
        assert_eq!(
            starting.current_action.as_deref(),
            Some("Starting LoTW sync")
        );
        let completed = stream
            .next()
            .await
            .expect("completion result")
            .expect("completion response");
        assert!(completed.complete);
        assert_eq!(completed.error_records, 0);
        assert_eq!(
            completed.current_action.as_deref(),
            Some("LoTW sync complete")
        );
    }

    #[tokio::test]
    async fn lotw_download_failure_is_returned_in_completion_response() {
        let service =
            test_logbook_service(test_runtime_config_with_lotw("http://127.0.0.1:1/report"));

        let mut stream = LogbookService::sync_with_lotw(
            &service,
            Request::new(SyncWithLotwRequest {
                upload: Some(false),
                download: Some(true),
                ..SyncWithLotwRequest::default()
            }),
        )
        .await
        .expect("LoTW sync stream")
        .into_inner();

        let _starting = stream.next().await.expect("starting result");
        let completed = stream
            .next()
            .await
            .expect("completion result")
            .expect("completion response");
        assert!(completed.complete);
        assert_eq!(completed.error_records, 1);
        assert!(completed
            .error
            .as_deref()
            .is_some_and(|error| error.contains("network request failed")));
    }

    #[tokio::test]
    async fn per_operation_lotw_sync_returns_configuration_error_without_losing_qso() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));

        let logged = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(sample_qso_without_station_callsign("W1AW")),
                sync_to_qrz: false,
                sync_to_lotw: true,
            }),
        )
        .await
        .expect("log response")
        .into_inner();

        assert!(!logged.lotw_sync_success);
        assert!(logged
            .lotw_sync_error
            .as_deref()
            .is_some_and(|error| error.contains("username")));
        let stored = LogbookService::get_qso(
            &service,
            Request::new(GetQsoRequest {
                local_id: logged.local_id,
            }),
        )
        .await
        .expect("get response")
        .into_inner()
        .qso;
        assert!(stored.is_some());
    }

    fn unique_sqlite_test_path(label: &str) -> PathBuf {
        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        std::env::temp_dir().join(format!(
            "qsoripper-{label}-{}-{unique_suffix}.db",
            std::process::id()
        ))
    }

    fn sample_qso_without_station_callsign(worked_callsign: &str) -> QsoRecord {
        QsoRecord {
            worked_callsign: worked_callsign.to_string(),
            utc_timestamp: Some(Timestamp {
                seconds: 1_731_600_000,
                nanos: 0,
            }),
            band: Band::Band20m as i32,
            mode: Mode::Ssb as i32,
            notes: Some("Logged from gRPC test".to_string()),
            ..QsoRecord::default()
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "This test helper exercises the full CRUD flow end-to-end in one place."
    )]
    async fn exercise_logbook_crud_flow(service: &DeveloperLogbookService) {
        let log_response = LogbookService::log_qso(
            service,
            Request::new(LogQsoRequest {
                qso: Some(sample_qso_without_station_callsign("W1AW")),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect("log response")
        .into_inner();

        assert!(!log_response.sync_success);
        assert!(log_response.sync_error.is_none());

        let loaded = LogbookService::get_qso(
            service,
            Request::new(GetQsoRequest {
                local_id: log_response.local_id.clone(),
            }),
        )
        .await
        .expect("get response")
        .into_inner()
        .qso
        .expect("loaded qso");

        assert_eq!("K7RND", loaded.station_callsign);
        let loaded_snapshot = loaded
            .station_snapshot
            .as_ref()
            .expect("station snapshot should be materialized");
        assert_eq!("K7RND", loaded_snapshot.station_callsign);
        assert_eq!(Some("Home"), loaded_snapshot.profile_name.as_deref());
        assert_eq!(Some("CN87"), loaded_snapshot.grid.as_deref());

        let listed = LogbookService::list_qsos(
            service,
            Request::new(ListQsosRequest {
                callsign_filter: Some("W1AW".to_string()),
                limit: 10,
                sort: QsoSortOrder::NewestFirst as i32,
                ..ListQsosRequest::default()
            }),
        )
        .await
        .expect("list response")
        .into_inner()
        .map(|result| result.expect("list item").qso.expect("listed qso payload"))
        .collect::<Vec<_>>()
        .await;

        assert_eq!(1, listed.len());
        assert_eq!(
            log_response.local_id,
            listed.first().expect("expected listed QSO").local_id
        );

        let mut updated = loaded.clone();
        updated.station_callsign.clear();
        updated.station_snapshot = None;
        updated.notes = Some("Updated through gRPC".to_string());
        let update_response = LogbookService::update_qso(
            service,
            Request::new(UpdateQsoRequest {
                qso: Some(updated),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect("update response")
        .into_inner();

        assert!(update_response.success);
        assert!(!update_response.sync_success);
        assert!(update_response.sync_error.is_none());

        let reloaded = LogbookService::get_qso(
            service,
            Request::new(GetQsoRequest {
                local_id: log_response.local_id.clone(),
            }),
        )
        .await
        .expect("reload response")
        .into_inner()
        .qso
        .expect("reloaded qso");

        assert_eq!("K7RND", reloaded.station_callsign);
        assert_eq!(Some("Updated through gRPC"), reloaded.notes.as_deref());
        assert_eq!(
            Some("Home"),
            reloaded
                .station_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.profile_name.as_deref())
        );

        let sync_status =
            LogbookService::get_sync_status(service, Request::new(GetSyncStatusRequest {}))
                .await
                .expect("sync status response")
                .into_inner();

        assert_eq!(1, sync_status.local_qso_count);
        assert_eq!(1, sync_status.pending_upload);
        assert_eq!(0, sync_status.qrz_qso_count);
        assert!(sync_status.last_sync.is_none());
        assert!(sync_status.qrz_logbook_owner.is_none());

        let delete_response = LogbookService::delete_qso(
            service,
            Request::new(DeleteQsoRequest {
                local_id: log_response.local_id.clone(),
                delete_from_qrz: false,
            }),
        )
        .await
        .expect("delete response")
        .into_inner();

        assert!(delete_response.success);
        assert!(!delete_response.qrz_delete_success);
        assert!(delete_response.qrz_delete_error.is_none());
        assert!(!delete_response.remote_delete_queued);

        // After soft-delete the record is still loadable by id but carries
        // a deleted_at tombstone.
        let get_after_delete = LogbookService::get_qso(
            service,
            Request::new(GetQsoRequest {
                local_id: log_response.local_id.clone(),
            }),
        )
        .await
        .expect("get after soft-delete")
        .into_inner();
        let after_delete_qso = get_after_delete.qso.expect("soft-deleted record");
        assert!(after_delete_qso.deleted_at.is_some());
        assert!(!after_delete_qso.pending_remote_delete);

        // Restoring clears the tombstone and pending flag.
        let restore_response = LogbookService::restore_qso(
            service,
            Request::new(RestoreQsoRequest {
                local_id: log_response.local_id.clone(),
            }),
        )
        .await
        .expect("restore response")
        .into_inner();
        assert!(restore_response.success);
        let restored = restore_response.restored.expect("restored record present");
        assert!(restored.deleted_at.is_none());
        assert!(!restored.pending_remote_delete);

        // Final hard-delete via storage is not exposed; soft-delete it again
        // and assert ListQsos default filter hides it.
        let _ = LogbookService::delete_qso(
            service,
            Request::new(DeleteQsoRequest {
                local_id: log_response.local_id,
                delete_from_qrz: false,
            }),
        )
        .await
        .expect("re-delete");
    }

    async fn grpc_logbook_client(
        service: DeveloperLogbookService,
    ) -> (LogbookServiceClient<Channel>, tokio::task::JoinHandle<()>) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("listener address");
        drop(listener);

        let handle = tokio::spawn(async move {
            Server::builder()
                .add_service(LogbookServiceServer::new(service))
                .serve(address)
                .await
                .expect("serve test gRPC server");
        });

        let endpoint = format!("http://{address}");
        for attempt in 0..20 {
            match LogbookServiceClient::connect(endpoint.clone()).await {
                Ok(client) => return (client, handle),
                Err(error) => {
                    assert!(
                        attempt < 19,
                        "failed to connect to test gRPC server at {endpoint}: {error}"
                    );
                    std::thread::sleep(Duration::from_millis(25));
                }
            }
        }
        unreachable!("connection loop should have returned or asserted");
    }

    async fn import_adif_payload(
        client: &mut LogbookServiceClient<Channel>,
        chunks: Vec<Vec<u8>>,
    ) -> ImportAdifResponse {
        import_adif_payload_with_refresh(client, chunks, false).await
    }

    async fn import_adif_payload_with_refresh(
        client: &mut LogbookServiceClient<Channel>,
        chunks: Vec<Vec<u8>>,
        refresh: bool,
    ) -> ImportAdifResponse {
        let stream =
            tokio_stream::iter(chunks.into_iter().enumerate().map(move |(index, chunk)| {
                ImportAdifRequest {
                    chunk: Some(AdifChunk { data: chunk }),
                    refresh: refresh && index == 0,
                }
            }));

        client
            .import_adif(Request::new(stream))
            .await
            .expect("import response")
            .into_inner()
    }

    #[test]
    fn load_dotenv_if_present_reads_env_from_current_directory() {
        let (_process_state_lock, process_state) = capture_clean_process_state();

        let temp_dir = std::env::temp_dir().join(format!(
            "qsoripper-dotenv-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("system clock")
                .as_nanos()
        ));
        fs::create_dir_all(&temp_dir).expect("create temp dir");
        let env_path = temp_dir.join(".env");
        fs::write(
            &env_path,
            concat!(
                "QSORIPPER_SERVER_ADDR=127.0.0.1:61051\n",
                "QSORIPPER_STORAGE_BACKEND=sqlite\n",
                "QSORIPPER_SQLITE_PATH=data/test-qsoripper.db\n"
            ),
        )
        .expect("write temp .env");

        std::env::set_current_dir(&temp_dir).expect("switch to temp dir");
        load_dotenv_if_present();

        let options = ServerOptions::from_env_and_args(Vec::<String>::new()).unwrap();

        assert_eq!("127.0.0.1:61051", options.listen_address.to_string());
        assert_eq!(StorageBackendKind::Sqlite, options.storage.backend);
        assert_eq!(
            PathBuf::from("data/test-qsoripper.db"),
            options.storage.sqlite_path
        );

        process_state.restore_current_dir();
        fs::remove_file(env_path).expect("remove temp .env");
        fs::remove_dir_all(temp_dir).expect("remove temp dir");
    }

    #[test]
    fn legacy_qrz_user_agent_compatibility_rewrites_and_preserves_later_values() {
        let legacy_line = "QSORIPPER_QRZ_USER_AGENT=QsoRipper/0.1.0 (AE7XI)";
        assert_eq!(
            Some("QSORIPPER_QRZ_USER_AGENT=\"QsoRipper/0.1.0 (AE7XI)\"".to_string()),
            legacy_qrz_user_agent_line_compatibility(legacy_line)
        );

        let sanitized = sanitize_legacy_qrz_user_agent_contents(concat!(
            "QSORIPPER_QRZ_USER_AGENT=QsoRipper/0.1.0 (AE7XI)\n",
            "QSORIPPER_QRZ_XML_USERNAME=KC7AVA\n",
            "QSORIPPER_QRZ_XML_PASSWORD=test-password\n"
        ))
        .expect("compatibility rewrite");

        let entries: Vec<(String, String)> =
            dotenvy::from_read_iter(std::io::Cursor::new(sanitized))
                .collect::<Result<_, _>>()
                .expect("sanitized dotenv entries");

        assert_eq!(
            vec![
                (
                    QRZ_USER_AGENT_ENV_VAR.to_string(),
                    "QsoRipper/0.1.0 (AE7XI)".to_string()
                ),
                (QRZ_XML_USERNAME_ENV_VAR.to_string(), "KC7AVA".to_string()),
                (
                    QRZ_XML_PASSWORD_ENV_VAR.to_string(),
                    "test-password".to_string()
                ),
            ],
            entries
        );
    }

    #[test]
    fn server_starting_message_reports_pending_startup() {
        let message = server_starting_message(
            "127.0.0.1:50051".parse().expect("address"),
            "memory",
            "complete",
            "config.toml",
        );

        assert_eq!(
            "Starting QsoRipper gRPC server on 127.0.0.1:50051 using memory storage (setup: complete, config: config.toml)",
            message
        );
    }

    #[test]
    fn server_ready_message_confirms_bound_listener() {
        let message = server_ready_message(
            "127.0.0.1:50051".parse().expect("address"),
            "sqlite",
            "incomplete",
            "config.toml",
        );

        assert_eq!(
            "QsoRipper gRPC server ready on 127.0.0.1:50051 using sqlite storage (setup: incomplete, config: config.toml)",
            message
        );
    }

    #[test]
    fn server_options_default_to_localhost_port_50051() {
        let (_process_state_lock, _process_state) = capture_clean_process_state();

        let options = ServerOptions::from_env_and_args(Vec::<String>::new()).unwrap();

        assert_eq!("127.0.0.1:50051", options.listen_address.to_string());
        assert_eq!(options.storage.backend, StorageBackendKind::Memory);
        assert!(options.runtime_config_cli_storage_overrides().is_empty());
    }

    #[test]
    fn server_options_allow_explicit_listen_override() {
        let (_process_state_lock, _process_state) = capture_clean_process_state();

        let options = ServerOptions::from_env_and_args([
            "--listen".to_string(),
            "127.0.0.1:60051".to_string(),
        ])
        .unwrap();

        assert_eq!("127.0.0.1:60051", options.listen_address.to_string());
    }

    #[test]
    fn lookup_result_defaults_to_unspecified_state() {
        let result = LookupResult::default();

        assert_eq!(LookupState::Unspecified as i32, result.state);
    }

    #[test]
    fn server_options_allow_sqlite_storage_override() {
        let (_process_state_lock, _process_state) = capture_clean_process_state();

        let options = ServerOptions::from_env_and_args([
            "--storage".to_string(),
            "sqlite".to_string(),
            "--sqlite-path".to_string(),
            "data\\qsoripper.db".to_string(),
        ])
        .unwrap();

        assert_eq!(options.storage.backend, StorageBackendKind::Sqlite);
        assert_eq!(
            options.storage.sqlite_path,
            std::path::PathBuf::from("data\\qsoripper.db")
        );
        assert_eq!(
            Some("sqlite"),
            options
                .runtime_config_cli_storage_overrides()
                .get(STORAGE_BACKEND_ENV_VAR)
                .map(String::as_str)
        );
        assert_eq!(
            Some("data\\qsoripper.db"),
            options
                .runtime_config_cli_storage_overrides()
                .get(SQLITE_PATH_ENV_VAR)
                .map(String::as_str)
        );
    }

    #[test]
    fn server_options_do_not_override_sqlite_path_when_cli_selects_sqlite_without_path() {
        let (_process_state_lock, _process_state) = capture_clean_process_state();

        let options =
            ServerOptions::from_env_and_args(["--storage".to_string(), "sqlite".to_string()])
                .unwrap();

        assert_eq!(options.storage.backend, StorageBackendKind::Sqlite);
        assert_eq!(
            Some("sqlite"),
            options
                .runtime_config_cli_storage_overrides()
                .get(STORAGE_BACKEND_ENV_VAR)
                .map(String::as_str)
        );
        // When --sqlite-path is not explicitly passed, the CLI must NOT inject a
        // default path override so that config-file values (e.g. setup wizard
        // log_file_path) are not silently overwritten.
        assert_eq!(
            None,
            options
                .runtime_config_cli_storage_overrides()
                .get(SQLITE_PATH_ENV_VAR)
                .map(String::as_str)
        );
    }

    #[test]
    fn parse_storage_backend_rejects_unknown_values() {
        let error = parse_storage_backend("rocksdb").unwrap_err();

        assert!(error.to_string().contains("Unsupported storage backend"));
    }

    #[test]
    fn build_storage_uses_requested_backend() {
        let memory_storage = build_storage(&StorageOptions {
            backend: StorageBackendKind::Memory,
            sqlite_path: std::path::PathBuf::from("ignored.db"),
        })
        .expect("memory storage");
        assert_eq!(memory_storage.backend_name(), "memory");

        let unique_suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock")
            .as_nanos();
        let sqlite_path = std::env::temp_dir().join(format!(
            "qsoripper-storage-{}-{unique_suffix}.db",
            std::process::id()
        ));
        let sqlite_storage = build_storage(&StorageOptions {
            backend: StorageBackendKind::Sqlite,
            sqlite_path: sqlite_path.clone(),
        })
        .expect("sqlite storage");
        assert_eq!(sqlite_storage.backend_name(), "sqlite");
        drop(sqlite_storage);

        if sqlite_path.exists() {
            fs::remove_file(sqlite_path).expect("remove sqlite test database");
        }
    }

    #[tokio::test]
    async fn server_exits_cleanly_when_shutdown_signal_resolves() {
        let config_path = unique_sqlite_test_path("ctrl-c-config").with_extension("toml");
        let options = ServerOptions {
            listen_address: "127.0.0.1:0".parse().expect("listen address"),
            config_path: config_path.clone(),
            storage: StorageOptions {
                backend: StorageBackendKind::Memory,
                sqlite_path: PathBuf::from("ignored.db"),
            },
            storage_cli_overrides: BTreeMap::from([(
                STORAGE_BACKEND_ENV_VAR.to_string(),
                "memory".to_string(),
            )]),
        };

        tokio::time::timeout(
            Duration::from_secs(1),
            run_server(options, std::future::ready(Ok(()))),
        )
        .await
        .expect("shutdown timeout")
        .expect("clean server shutdown");

        if config_path.exists() {
            fs::remove_file(config_path).expect("remove config test file");
        }
    }

    #[tokio::test]
    async fn lookup_service_lookup_returns_error_state_when_provider_is_disabled() {
        let service = test_lookup_service();

        let response = LookupService::lookup(
            &service,
            Request::new(LookupRequest {
                callsign: "W1AW".to_string(),
                skip_cache: false,
            }),
        )
        .await
        .expect("lookup response")
        .into_inner()
        .result
        .expect("lookup result payload");

        assert_eq!(LookupState::Error as i32, response.state);
        assert_eq!(
            Some(
                "Provider configuration error: Required environment variable 'QSORIPPER_QRZ_XML_USERNAME' is missing or blank."
            ),
            response.error_message.as_deref()
        );
    }

    #[tokio::test]
    async fn stream_lookup_emits_loading_then_error_when_provider_is_disabled() {
        let service = test_lookup_service();

        let response = LookupService::stream_lookup(
            &service,
            Request::new(StreamLookupRequest {
                callsign: "W1AW".to_string(),
                skip_cache: false,
            }),
        )
        .await
        .expect("stream response")
        .into_inner();
        let updates = response
            .map(|result| {
                result
                    .expect("stream item")
                    .result
                    .expect("stream result payload")
            })
            .collect::<Vec<_>>()
            .await;

        assert_eq!(2, updates.len());
        assert_eq!(
            LookupState::Loading as i32,
            updates.first().expect("loading update").state
        );
        assert_eq!(
            LookupState::Error as i32,
            updates.get(1).expect("error update").state
        );
    }

    #[tokio::test]
    async fn cache_lookup_defaults_to_unspecified_without_cached_value() {
        let service = test_lookup_service();

        let response = LookupService::get_cached_callsign(
            &service,
            Request::new(GetCachedCallsignRequest {
                callsign: "W1AW".to_string(),
            }),
        )
        .await
        .expect("cache response")
        .into_inner()
        .result
        .expect("cached lookup result payload");

        assert_eq!(LookupState::NotFound as i32, response.state);
        assert!(!response.cache_hit);
    }

    #[tokio::test]
    async fn get_dxcc_entity_returns_known_us_entity_by_code() {
        let service = test_lookup_service();

        let response = LookupService::get_dxcc_entity(
            &service,
            Request::new(GetDxccEntityRequest {
                query: Some(get_dxcc_entity_request::Query::DxccCode(291)),
            }),
        )
        .await
        .expect("dxcc lookup should succeed");

        let entity = response
            .into_inner()
            .entity
            .expect("dxcc entity payload present");
        assert_eq!(291, entity.dxcc_code);
        assert_eq!("UNITED STATES OF AMERICA", entity.country_name.as_str());
        assert_eq!("NA", entity.continent.as_str());
    }

    #[tokio::test]
    async fn get_dxcc_entity_returns_not_found_for_unknown_code() {
        let service = test_lookup_service();

        let error = LookupService::get_dxcc_entity(
            &service,
            Request::new(GetDxccEntityRequest {
                query: Some(get_dxcc_entity_request::Query::DxccCode(9_999)),
            }),
        )
        .await
        .expect_err("unknown dxcc should not be found");

        assert_eq!(Code::NotFound, error.code());
    }

    #[tokio::test]
    async fn get_dxcc_entity_prefix_query_remains_unimplemented() {
        let service = test_lookup_service();

        let error = LookupService::get_dxcc_entity(
            &service,
            Request::new(GetDxccEntityRequest {
                query: Some(get_dxcc_entity_request::Query::Prefix("W1AW".to_string())),
            }),
        )
        .await
        .expect_err("prefix lookup should be unimplemented");

        assert_eq!(Code::Unimplemented, error.code());
    }

    #[tokio::test]
    async fn get_dxcc_entity_rejects_missing_query() {
        let service = test_lookup_service();

        let error =
            LookupService::get_dxcc_entity(&service, Request::new(GetDxccEntityRequest::default()))
                .await
                .expect_err("missing query should be rejected");

        assert_eq!(Code::InvalidArgument, error.code());
    }

    #[tokio::test]
    async fn batch_lookup_returns_empty_results_for_empty_input() {
        let service = test_lookup_service();

        let response = LookupService::batch_lookup(
            &service,
            Request::new(BatchLookupRequest { callsigns: vec![] }),
        )
        .await
        .expect("empty batch lookup should succeed");

        assert!(response.into_inner().results.is_empty());
    }

    #[tokio::test]
    async fn batch_lookup_returns_one_result_per_callsign_in_order() {
        let service = test_lookup_service();

        let response = LookupService::batch_lookup(
            &service,
            Request::new(BatchLookupRequest {
                callsigns: vec!["W1AW".to_string(), "K7DBG".to_string(), "AE7XI".to_string()],
            }),
        )
        .await
        .expect("batch lookup should succeed");

        let results = response.into_inner().results;
        assert_eq!(3, results.len());
        let actual: Vec<&str> = results
            .iter()
            .map(|result| result.queried_callsign.as_str())
            .collect();
        assert_eq!(vec!["W1AW", "K7DBG", "AE7XI"], actual);
    }

    #[tokio::test]
    async fn logbook_crud_flow_works_through_memory_grpc_surface() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));

        exercise_logbook_crud_flow(&service).await;
    }

    #[tokio::test]
    async fn log_qso_replaces_caller_owned_identity_station_and_sync_metadata() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let caller_created_at = Timestamp {
            seconds: 946_684_800,
            nanos: 0,
        };

        let response = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(QsoRecord {
                    local_id: "caller-owned-id".to_string(),
                    station_callsign: "N0FAKE".to_string(),
                    worked_callsign: " w1aw ".to_string(),
                    utc_timestamp: Some(Timestamp {
                        seconds: 1_731_600_000,
                        nanos: 0,
                    }),
                    band: Band::Band20m as i32,
                    mode: Mode::Cw as i32,
                    created_at: Some(caller_created_at),
                    sync_status: SyncStatus::Synced as i32,
                    qrz_logid: Some("caller-logid".to_string()),
                    qrz_bookid: Some("caller-bookid".to_string()),
                    station_snapshot: Some(StationSnapshot {
                        station_callsign: "N0FAKE".to_string(),
                        ..StationSnapshot::default()
                    }),
                    ..QsoRecord::default()
                }),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect("log response")
        .into_inner();

        let stored = LogbookService::get_qso(
            &service,
            Request::new(GetQsoRequest {
                local_id: response.local_id,
            }),
        )
        .await
        .expect("get response")
        .into_inner()
        .qso
        .expect("stored qso");

        assert_ne!("caller-owned-id", stored.local_id);
        assert_eq!("W1AW", stored.worked_callsign);
        assert_eq!("K7RND", stored.station_callsign);
        assert_eq!(
            "K7RND",
            stored
                .station_snapshot
                .as_ref()
                .expect("station snapshot")
                .station_callsign
        );
        assert_ne!(Some(caller_created_at), stored.created_at);
        assert_eq!(SyncStatus::LocalOnly as i32, stored.sync_status);
        assert!(stored.qrz_logid.is_none());
        assert!(stored.qrz_bookid.is_none());
    }

    #[tokio::test]
    async fn logbook_crud_flow_works_through_sqlite_grpc_surface() {
        let sqlite_path = unique_sqlite_test_path("logbook-grpc");
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Sqlite,
            Some(&sqlite_path),
            true,
        ));

        exercise_logbook_crud_flow(&service).await;

        drop(service);
        if sqlite_path.exists() {
            fs::remove_file(sqlite_path).expect("remove sqlite test database");
        }
    }

    #[tokio::test]
    async fn logbook_sync_status_reports_live_local_counts() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));

        let logged = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(QsoRecord {
                    station_callsign: "K7RND".to_string(),
                    worked_callsign: "W1AW".to_string(),
                    utc_timestamp: Some(Timestamp {
                        seconds: 1_731_600_000,
                        nanos: 0,
                    }),
                    band: Band::Band20m as i32,
                    mode: Mode::Ssb as i32,
                    ..QsoRecord::default()
                }),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect("log response")
        .into_inner();

        let mut second_qso = QsoRecord {
            station_callsign: "K7RND".to_string(),
            worked_callsign: "K7XYZ".to_string(),
            utc_timestamp: Some(Timestamp {
                seconds: 1_731_600_100,
                nanos: 0,
            }),
            band: Band::Band40m as i32,
            mode: Mode::Cw as i32,
            ..QsoRecord::default()
        };
        second_qso.sync_status =
            qsoripper_core::proto::qsoripper::domain::SyncStatus::Synced as i32;
        let _ = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(second_qso),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect("second log response");

        let response =
            LogbookService::get_sync_status(&service, Request::new(GetSyncStatusRequest {}))
                .await
                .expect("sync status")
                .into_inner();

        assert_eq!(2, response.local_qso_count);
        assert_eq!(0, response.qrz_qso_count);
        assert_eq!(2, response.pending_upload);
        assert!(response.last_sync.is_none());
        assert!(response.qrz_logbook_owner.is_none());

        let _ = LogbookService::delete_qso(
            &service,
            Request::new(DeleteQsoRequest {
                local_id: logged.local_id,
                delete_from_qrz: false,
            }),
        )
        .await
        .expect("delete first qso");
    }

    #[tokio::test]
    async fn adif_import_preserves_station_history_and_reports_duplicates() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service).await;
        let payload = concat!(
            "<ADIF_VER:5>3.1.7\n<EOH>\n",
            "<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<OPERATOR:5>N1OPS<MY_GRIDSQUARE:6>FN42aa<QSO_DATE:8>20250102<TIME_ON:6>010203<BAND:3>20M<MODE:3>SSB<EOR>\n",
            "<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<OPERATOR:5>N1OPS<MY_GRIDSQUARE:6>FN42aa<QSO_DATE:8>20250102<TIME_ON:6>010203<BAND:3>20M<MODE:3>SSB<EOR>\n"
        )
        .as_bytes();

        let (first_chunk, second_chunk) = payload.split_at(40);
        let result = import_adif_payload(
            &mut client,
            vec![first_chunk.to_vec(), second_chunk.to_vec()],
        )
        .await;

        assert_eq!(1, result.records_imported);
        assert_eq!(1, result.records_skipped);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("duplicate skipped")));

        let imported = client
            .list_qsos(Request::new(ListQsosRequest {
                limit: 10,
                sort: QsoSortOrder::OldestFirst as i32,
                ..ListQsosRequest::default()
            }))
            .await
            .expect("list response")
            .into_inner()
            .map(|result| result.expect("list item").qso.expect("listed qso payload"))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(1, imported.len());
        let imported = imported.first().expect("imported qso");
        assert_eq!("K1ABC", imported.station_callsign);
        let snapshot = imported
            .station_snapshot
            .as_ref()
            .expect("station snapshot");
        assert_eq!("K1ABC", snapshot.station_callsign);
        assert_eq!(Some("N1OPS"), snapshot.operator_callsign.as_deref());
        assert_eq!(Some("FN42aa"), snapshot.grid.as_deref());
        assert_eq!(
            None,
            snapshot.profile_name.as_deref(),
            "imported station history should not be overwritten by the active profile"
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_import_skips_minute_precision_duplicate_with_small_frequency_drift() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service).await;
        let existing_timestamp = chrono::NaiveDate::from_ymd_opt(2025, 1, 2)
            .expect("date")
            .and_hms_opt(1, 2, 32)
            .expect("time")
            .and_utc()
            .timestamp();
        let existing = QsoRecord {
            station_callsign: "K1ABC".to_string(),
            worked_callsign: "W1AW".to_string(),
            utc_timestamp: Some(Timestamp {
                seconds: existing_timestamp,
                nanos: 0,
            }),
            band: Band::Band15m as i32,
            mode: Mode::Cw as i32,
            frequency_hz: Some(21_028_340),
            worked_country: Some("United States".to_string()),
            worked_grid: Some("FN31pr".to_string()),
            ..QsoRecord::default()
        };
        let _ = client
            .log_qso(Request::new(LogQsoRequest {
                qso: Some(existing),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }))
            .await
            .expect("log response");

        let payload = b"<CALL:4>W1AW<STATION_CALLSIGN:5>K7RND<QSO_DATE:8>20250102<TIME_ON:4>0102<BAND:3>15M<MODE:2>CW<FREQ:8>21.02830<EOR>\n";
        let result = import_adif_payload(&mut client, vec![payload.to_vec()]).await;

        assert_eq!(0, result.records_imported);
        assert_eq!(1, result.records_skipped);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("duplicate skipped")));

        let stored = client
            .list_qsos(Request::new(ListQsosRequest {
                limit: 10,
                sort: QsoSortOrder::OldestFirst as i32,
                ..ListQsosRequest::default()
            }))
            .await
            .expect("list response")
            .into_inner()
            .map(|result| result.expect("list item").qso.expect("listed qso payload"))
            .collect::<Vec<_>>()
            .await;

        assert_eq!(1, stored.len());
        let stored = stored.first().expect("stored qso");
        assert_eq!(Some("United States"), stored.worked_country.as_deref());
        assert_eq!(Some("FN31pr"), stored.worked_grid.as_deref());

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_import_uses_active_station_profile_only_as_explicit_fallback() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service).await;
        let payload =
            b"<CALL:5>DL1AA<QSO_DATE:8>20250103<TIME_ON:6>030405<BAND:3>20M<MODE:2>CW<EOR>\n";

        let result = import_adif_payload(&mut client, vec![payload.to_vec()]).await;

        assert_eq!(1, result.records_imported);
        assert_eq!(0, result.records_skipped);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("applied active station profile")));

        let imported = client
            .list_qsos(Request::new(ListQsosRequest {
                limit: 1,
                sort: QsoSortOrder::OldestFirst as i32,
                ..ListQsosRequest::default()
            }))
            .await
            .expect("list response")
            .into_inner()
            .next()
            .await
            .expect("list item")
            .expect("qso")
            .qso
            .expect("listed qso payload");

        assert_eq!("K7RND", imported.station_callsign);
        assert_eq!(
            Some("Home"),
            imported
                .station_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.profile_name.as_deref())
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_import_reports_invalid_records_without_active_station_fallback() {
        let service = test_logbook_service(test_runtime_config());
        let (mut client, server_handle) = grpc_logbook_client(service).await;
        let payload = b"<CALL:5>DL1AA<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250103<TIME_ON:6>030405<BAND:3>11M<MODE:2>CW<EOR>\n";

        let result = import_adif_payload(&mut client, vec![payload.to_vec()]).await;

        assert_eq!(0, result.records_imported);
        assert_eq!(1, result.records_skipped);
        assert!(result
            .warnings
            .iter()
            .any(|warning| warning.contains("unrecognized ADIF band '11M'")));

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_import_refresh_updates_existing_records() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service).await;

        // First import: record with original RST.
        let original = b"<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250102<TIME_ON:6>010203<BAND:3>20M<MODE:3>SSB<RST_SENT:2>55<RST_RCVD:2>44<EOR>\n";
        let first = import_adif_payload(&mut client, vec![original.to_vec()]).await;
        assert_eq!(1, first.records_imported);

        // Capture the local_id assigned by the engine.
        let original_qso = client
            .list_qsos(Request::new(ListQsosRequest {
                limit: 1,
                sort: QsoSortOrder::NewestFirst as i32,
                ..ListQsosRequest::default()
            }))
            .await
            .expect("list")
            .into_inner()
            .next()
            .await
            .expect("item")
            .expect("msg")
            .qso
            .expect("qso");

        // Re-import same file without refresh: should skip.
        let no_refresh = import_adif_payload(&mut client, vec![original.to_vec()]).await;
        assert_eq!(0, no_refresh.records_imported);
        assert_eq!(1, no_refresh.records_skipped);
        assert_eq!(0, no_refresh.records_updated);

        // Re-import with corrected RST and refresh=true.
        let corrected = b"<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250102<TIME_ON:6>010203<BAND:3>20M<MODE:3>SSB<RST_SENT:2>59<RST_RCVD:2>57<EOR>\n";
        let refreshed =
            import_adif_payload_with_refresh(&mut client, vec![corrected.to_vec()], true).await;
        assert_eq!(0, refreshed.records_imported);
        assert_eq!(0, refreshed.records_skipped);
        assert_eq!(1, refreshed.records_updated);
        assert!(refreshed
            .warnings
            .iter()
            .any(|w| w.contains("refreshed existing record")));

        // Verify the record was updated in-place (same local_id, new RST).
        let updated_qso = client
            .get_qso(Request::new(GetQsoRequest {
                local_id: original_qso.local_id.clone(),
            }))
            .await
            .expect("get")
            .into_inner()
            .qso
            .expect("qso");

        assert_eq!(original_qso.local_id, updated_qso.local_id);
        assert_eq!("59", updated_qso.rst_sent.as_ref().expect("rst_sent").raw);
        assert_eq!(
            "57",
            updated_qso.rst_received.as_ref().expect("rst_received").raw
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_import_refresh_preserves_fields_absent_from_import() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service).await;

        // Import a record with notes via the log RPC (richer than ADIF).
        let original = b"<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250105<TIME_ON:6>120000<BAND:3>40M<MODE:2>CW<RST_SENT:3>559<COMMENT:14>My field notes<EOR>\n";
        let first = import_adif_payload(&mut client, vec![original.to_vec()]).await;
        assert_eq!(1, first.records_imported);

        // Re-import with corrected RST but no COMMENT field — notes should be preserved.
        let corrected = b"<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250105<TIME_ON:6>120000<BAND:3>40M<MODE:2>CW<RST_SENT:3>599<EOR>\n";
        let refreshed =
            import_adif_payload_with_refresh(&mut client, vec![corrected.to_vec()], true).await;
        assert_eq!(1, refreshed.records_updated);

        let updated_qso = client
            .list_qsos(Request::new(ListQsosRequest {
                limit: 1,
                sort: QsoSortOrder::NewestFirst as i32,
                ..ListQsosRequest::default()
            }))
            .await
            .expect("list")
            .into_inner()
            .next()
            .await
            .expect("item")
            .expect("msg")
            .qso
            .expect("qso");

        assert_eq!(
            "599",
            updated_qso.rst_sent.as_ref().expect("rst_sent").raw,
            "RST should be refreshed from import"
        );
        assert_eq!(
            Some("My field notes"),
            updated_qso.comment.as_deref(),
            "COMMENT from original should be preserved when absent from refresh import"
        );

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_import_refresh_handles_mixed_new_and_existing() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service).await;

        // Import one record.
        let original = b"<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250110<TIME_ON:6>080000<BAND:3>20M<MODE:3>SSB<EOR>\n";
        let first = import_adif_payload(&mut client, vec![original.to_vec()]).await;
        assert_eq!(1, first.records_imported);

        // Re-import with the same record plus a new one, with refresh.
        let mixed = concat!(
            "<CALL:4>W1AW<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250110<TIME_ON:6>080000<BAND:3>20M<MODE:3>SSB<RST_SENT:2>59<EOR>\n",
            "<CALL:5>DL1AA<STATION_CALLSIGN:5>K1ABC<QSO_DATE:8>20250110<TIME_ON:6>090000<BAND:3>15M<MODE:2>CW<EOR>\n"
        );
        let refreshed =
            import_adif_payload_with_refresh(&mut client, vec![mixed.as_bytes().to_vec()], true)
                .await;
        assert_eq!(
            1, refreshed.records_imported,
            "new record should be imported"
        );
        assert_eq!(
            1, refreshed.records_updated,
            "existing record should be updated"
        );
        assert_eq!(0, refreshed.records_skipped);

        server_handle.abort();
    }

    #[tokio::test]
    async fn adif_export_streams_filtered_qsos_in_adif_order() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));
        let (mut client, server_handle) = grpc_logbook_client(service.clone()).await;

        let first = QsoRecord {
            station_callsign: "K7RND".to_string(),
            worked_callsign: "W1AW".to_string(),
            utc_timestamp: Some(Timestamp {
                seconds: 1_735_689_600,
                nanos: 0,
            }),
            band: Band::Band20m as i32,
            mode: Mode::Ssb as i32,
            contest_id: Some("FIELD-DAY".to_string()),
            ..QsoRecord::default()
        };
        let second = QsoRecord {
            station_callsign: "K7RND".to_string(),
            worked_callsign: "K3LR".to_string(),
            utc_timestamp: Some(Timestamp {
                seconds: 1_735_689_900,
                nanos: 0,
            }),
            band: Band::Band40m as i32,
            mode: Mode::Cw as i32,
            ..QsoRecord::default()
        };

        let _ = client
            .log_qso(Request::new(LogQsoRequest {
                qso: Some(first),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }))
            .await
            .expect("log first qso");
        let _ = client
            .log_qso(Request::new(LogQsoRequest {
                qso: Some(second),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }))
            .await
            .expect("log second qso");

        let response = client
            .export_adif(Request::new(ExportAdifRequest {
                contest_id: Some("FIELD-DAY".to_string()),
                include_header: true,
                ..ExportAdifRequest::default()
            }))
            .await
            .expect("export response")
            .into_inner();
        let chunks = response
            .map(|result| result.expect("chunk"))
            .collect::<Vec<_>>()
            .await;
        let bytes = chunks.into_iter().fold(Vec::new(), |mut output, chunk| {
            output.extend_from_slice(&chunk.chunk.expect("export chunk payload").data);
            output
        });
        let text = String::from_utf8(bytes).expect("utf8 payload");

        assert!(text.contains("<ADIF_VER:5>3.1.7"));
        assert!(text.contains("<PROGRAMID:9>QsoRipper"));
        assert!(text.contains("<CALL:4>W1AW"));
        assert!(!text.contains("<CALL:4>K3LR"));
        assert!(text.contains("<CONTEST_ID:9>FIELD-DAY"));

        server_handle.abort();
    }

    #[tokio::test]
    async fn logbook_requires_an_active_station_profile() {
        let service = test_logbook_service(test_runtime_config());

        let error = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(sample_qso_without_station_callsign("W1AW")),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect_err("missing station context should fail");

        assert_eq!(Code::FailedPrecondition, error.code());
        assert_eq!(
            "An active station profile is required before logging a QSO.",
            error.message()
        );
    }

    #[tokio::test]
    async fn logbook_requires_timestamp_band_and_mode() {
        let service = test_logbook_service(test_runtime_config_with_logbook(
            StorageBackendKind::Memory,
            None,
            true,
        ));

        let error = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(QsoRecord {
                    worked_callsign: "W1AW".to_string(),
                    ..QsoRecord::default()
                }),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect_err("missing timestamp/band/mode should fail");

        assert_eq!(Code::InvalidArgument, error.code());
        assert_eq!("utc_timestamp is required.", error.message());

        let band_error = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(QsoRecord {
                    worked_callsign: "W1AW".to_string(),
                    utc_timestamp: Some(Timestamp {
                        seconds: 1_731_600_000,
                        nanos: 0,
                    }),
                    ..QsoRecord::default()
                }),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect_err("missing band should fail");

        assert_eq!(Code::InvalidArgument, band_error.code());
        assert_eq!("band is required.", band_error.message());

        let mode_error = LogbookService::log_qso(
            &service,
            Request::new(LogQsoRequest {
                qso: Some(QsoRecord {
                    worked_callsign: "W1AW".to_string(),
                    utc_timestamp: Some(Timestamp {
                        seconds: 1_731_600_000,
                        nanos: 0,
                    }),
                    band: Band::Band20m as i32,
                    ..QsoRecord::default()
                }),
                sync_to_qrz: false,
                sync_to_lotw: false,
            }),
        )
        .await
        .expect_err("missing mode should fail");

        assert_eq!(Code::InvalidArgument, mode_error.code());
        assert_eq!("mode is required.", mode_error.message());
    }

    #[tokio::test]
    async fn great_circle_resolves_coordinates_and_returns_path() {
        use qsoripper_core::proto::qsoripper::domain::{GeoPoint, GeoReference};
        let service = GreatCircleControlSurface::new();
        let response = service
            .compute_great_circle(Request::new(ComputeGreatCircleRequest {
                origin: Some(GeoReference {
                    coordinates: Some(GeoPoint {
                        latitude: 47.45,
                        longitude: -122.31,
                    }),
                    maidenhead: None,
                }),
                target: Some(GeoReference {
                    coordinates: Some(GeoPoint {
                        latitude: 51.47,
                        longitude: -0.46,
                    }),
                    maidenhead: None,
                }),
                sample_count: 0,
            }))
            .await
            .expect("compute great circle")
            .into_inner();
        let path = response.path.expect("path");
        assert_eq!(64, path.samples.len());
        assert!((path.distance_km - 7720.0).abs() < 30.0);
        assert!(path.initial_bearing_deg.is_some());
        assert!(path.final_bearing_deg.is_some());
    }

    #[tokio::test]
    async fn great_circle_resolves_maidenhead_grid() {
        use qsoripper_core::proto::qsoripper::domain::{GeoPoint, GeoReference};
        let service = GreatCircleControlSurface::new();
        let response = service
            .compute_great_circle(Request::new(ComputeGreatCircleRequest {
                origin: Some(GeoReference {
                    coordinates: None,
                    maidenhead: Some("CN87wn".to_string()),
                }),
                target: Some(GeoReference {
                    coordinates: Some(GeoPoint {
                        latitude: 0.0,
                        longitude: 0.0,
                    }),
                    maidenhead: None,
                }),
                sample_count: 8,
            }))
            .await
            .expect("compute great circle")
            .into_inner();
        let path = response.path.expect("path");
        assert_eq!(8, path.samples.len());
        // CN87wn is around the Seattle area; great-circle distance to (0,0)
        // is a long way (>10000 km).
        assert!(path.distance_km > 10000.0);
    }

    #[tokio::test]
    async fn great_circle_rejects_missing_origin() {
        let service = GreatCircleControlSurface::new();
        let err = service
            .compute_great_circle(Request::new(ComputeGreatCircleRequest {
                origin: None,
                target: None,
                sample_count: 0,
            }))
            .await
            .expect_err("must reject missing origin");
        assert_eq!(Code::InvalidArgument, err.code());
    }

    #[tokio::test]
    async fn great_circle_rejects_invalid_sample_count() {
        use qsoripper_core::proto::qsoripper::domain::{GeoPoint, GeoReference};
        let service = GreatCircleControlSurface::new();
        let err = service
            .compute_great_circle(Request::new(ComputeGreatCircleRequest {
                origin: Some(GeoReference {
                    coordinates: Some(GeoPoint {
                        latitude: 0.0,
                        longitude: 0.0,
                    }),
                    maidenhead: None,
                }),
                target: Some(GeoReference {
                    coordinates: Some(GeoPoint {
                        latitude: 1.0,
                        longitude: 1.0,
                    }),
                    maidenhead: None,
                }),
                sample_count: 1024,
            }))
            .await
            .expect_err("must reject high sample count");
        assert_eq!(Code::InvalidArgument, err.code());
    }
}
