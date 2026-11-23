//! Logbook workflows built on top of the engine-owned storage ports.

use crate::domain::qso::new_local_id;
use crate::domain::station::{
    materialize_station_snapshot_for_create, station_snapshot_has_values,
};
use crate::proto::qsoripper::domain::{
    Band, LotwSyncStatus, Mode, QslStatus, QsoRecord, StationProfile, SyncStatus,
};
use crate::storage::{
    DeletedRecordsFilter, EngineStorage, LogbookCounts, QsoListQuery, StorageError, SyncMetadata,
};
use prost_types::Timestamp;
use std::sync::Arc;
use thiserror::Error;
use time::OffsetDateTime;

const ADIF_DUPLICATE_TIME_WINDOW_SECONDS: i64 = 59;
const ADIF_DUPLICATE_FREQUENCY_TOLERANCE_HZ: u64 = 100;
const ADIF_IMPORT_FINGERPRINT_FIELD: &str = "APP_QSORIPPER_IMPORT_FINGERPRINT";

/// Coordinates QSO CRUD and sync-status reads through a storage backend.
#[derive(Clone)]
pub struct LogbookEngine {
    storage: Arc<dyn EngineStorage>,
}

impl LogbookEngine {
    /// Create a logbook engine backed by the provided storage implementation.
    #[must_use]
    pub fn new(storage: Arc<dyn EngineStorage>) -> Self {
        Self { storage }
    }

    /// Return the active storage backend name for diagnostics and startup logs.
    #[must_use]
    pub fn storage_backend_name(&self) -> &'static str {
        self.storage.backend_name()
    }

    /// Return the logbook-oriented storage surface for direct store access.
    ///
    /// Used by the sync workflow which needs fine-grained control over
    /// individual QSO reads and writes without the validation/normalization
    /// pipeline applied by the higher-level CRUD methods on this engine.
    #[must_use]
    pub fn logbook_store(&self) -> &dyn crate::storage::LogbookStore {
        self.storage.logbook()
    }

    /// Persist a new QSO and return the normalized stored record.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Validation`] when required fields are missing and
    /// [`LogbookError::Storage`] when the configured backend rejects the write.
    pub async fn log_qso(&self, mut qso: QsoRecord) -> Result<QsoRecord, LogbookError> {
        materialize_station_snapshot_for_create(&mut qso, None);
        normalize_qso_for_persistence(&mut qso);
        validate_qso_for_persistence(&qso)?;

        if qso.local_id.trim().is_empty() {
            qso.local_id = new_local_id();
        }

        let now = now_timestamp();
        if qso.created_at.is_none() {
            qso.created_at = Some(now);
        }
        qso.updated_at = Some(now);

        self.storage.logbook().insert_qso(&qso).await?;
        Ok(qso)
    }

    /// Persist a new QSO using the supplied active station profile as the base snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Validation`] when required fields are missing and
    /// [`LogbookError::Storage`] when the configured backend rejects the write.
    pub async fn log_qso_with_station_profile(
        &self,
        mut qso: QsoRecord,
        active_station_profile: Option<&crate::proto::qsoripper::domain::StationProfile>,
    ) -> Result<QsoRecord, LogbookError> {
        let active_station_profile =
            active_station_profile.ok_or(LogbookError::NoActiveStationProfile)?;

        // Identity, station context, timestamps, deletion state, and QRZ
        // linkage are engine-owned on the public LogQso path.
        qso.local_id = new_local_id();
        qso.station_callsign.clear();
        qso.station_snapshot = None;
        qso.sync_status = SyncStatus::LocalOnly as i32;
        qso.qrz_logid = None;
        qso.qrz_bookid = None;
        qso.deleted_at = None;
        qso.pending_remote_delete = false;
        qso.lotw_sent = None;
        qso.lotw_received = None;
        qso.lotw_sent_status = QslStatus::Unspecified as i32;
        qso.lotw_received_status = QslStatus::Unspecified as i32;
        qso.lotw_sent_date = None;
        qso.lotw_received_date = None;
        qso.lotw_sync_status = LotwSyncStatus::LocalOnly as i32;

        materialize_station_snapshot_for_create(&mut qso, Some(active_station_profile));
        normalize_qso_for_persistence(&mut qso);
        validate_qso_for_persistence(&qso)?;

        let now = now_timestamp();
        qso.created_at = Some(now);
        qso.updated_at = Some(now);

        self.storage.logbook().insert_qso(&qso).await?;
        Ok(qso)
    }

    /// Update an existing QSO and return the normalized stored record.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Validation`] when the record is missing required
    /// fields, [`LogbookError::NotFound`] when the local ID does not exist,
    /// [`LogbookError::AlreadyDeleted`] when the existing record is soft-deleted,
    /// and [`LogbookError::Storage`] when the backend write fails.
    pub async fn update_qso(&self, mut qso: QsoRecord) -> Result<QsoRecord, LogbookError> {
        let local_id = qso.local_id.trim().to_string();
        if local_id.is_empty() {
            return Err(LogbookError::Validation(
                "local_id is required when updating a QSO.".into(),
            ));
        }
        qso.local_id.clone_from(&local_id);

        let existing = self.storage.logbook().get_qso(&local_id).await?;
        let existing = existing.ok_or_else(|| LogbookError::NotFound(qso.local_id.clone()))?;
        if existing.deleted_at.is_some() {
            return Err(LogbookError::AlreadyDeleted(qso.local_id));
        }
        qso.local_id.clone_from(&existing.local_id);
        qso.station_callsign.clone_from(&existing.station_callsign);
        qso.station_snapshot.clone_from(&existing.station_snapshot);
        qso.deleted_at.clone_from(&existing.deleted_at);
        qso.pending_remote_delete = existing.pending_remote_delete;
        qso.lotw_sent = existing.lotw_sent;
        qso.lotw_received = existing.lotw_received;
        qso.lotw_sent_status = existing.lotw_sent_status;
        qso.lotw_received_status = existing.lotw_received_status;
        qso.lotw_sent_date.clone_from(&existing.lotw_sent_date);
        qso.lotw_received_date
            .clone_from(&existing.lotw_received_date);
        qso.lotw_sync_status = if matches!(
            LotwSyncStatus::try_from(existing.lotw_sync_status),
            Ok(LotwSyncStatus::Uploaded | LotwSyncStatus::Confirmed)
        ) {
            LotwSyncStatus::Modified as i32
        } else {
            existing.lotw_sync_status
        };
        normalize_qso_for_persistence(&mut qso);
        validate_qso_for_persistence(&qso)?;

        qso.created_at = existing.created_at.or_else(|| Some(now_timestamp()));
        qso.updated_at = Some(now_timestamp());

        // The engine — not the client — is the source of truth for `sync_status`.
        // Per the engine specification (docs/architecture/engine-specification.md
        // §UpdateQso step 4): "If the QSO was previously synced, set
        // `sync_status` to `SYNC_STATUS_MODIFIED`." This keeps the QRZ logid
        // attached to the row so the next bulk sync can issue a REPLACE instead
        // of stranding the local correction.
        //
        // For any other prior state (LocalOnly, Modified, Conflict) we preserve
        // `existing.sync_status` rather than trust the inbound record, since
        // clients typically round-trip whatever value they read and have no
        // authority to advance the sync state on their own. A per-operation
        // sync attempted by the server layer is what flips Modified → Synced
        // after a successful REPLACE.
        qso.sync_status = if existing.sync_status == SyncStatus::Synced as i32 {
            SyncStatus::Modified as i32
        } else {
            existing.sync_status
        };
        qso.qrz_logid.clone_from(&existing.qrz_logid);
        qso.qrz_bookid.clone_from(&existing.qrz_bookid);

        let updated = self.storage.logbook().update_qso(&qso).await?;
        if updated {
            Ok(qso)
        } else {
            Err(LogbookError::NotFound(qso.local_id))
        }
    }

    /// Soft-delete a QSO by local identifier, optionally queuing a remote delete
    /// for the next QRZ sync. Returns the soft-deleted record.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Validation`] when the local ID is blank,
    /// [`LogbookError::NotFound`] when the record does not exist, and
    /// [`LogbookError::Storage`] when the backend write fails.
    pub async fn delete_qso(
        &self,
        local_id: &str,
        queue_remote_delete: bool,
    ) -> Result<QsoRecord, LogbookError> {
        if local_id.trim().is_empty() {
            return Err(LogbookError::Validation(
                "local_id is required when deleting a QSO.".into(),
            ));
        }

        let existing = self.storage.logbook().get_qso(local_id).await?;
        let mut existing = existing.ok_or_else(|| LogbookError::NotFound(local_id.to_string()))?;

        // Idempotent: re-deleting an already soft-deleted row is a no-op
        // success that may upgrade pending_remote_delete if the caller asks.
        let already_deleted = existing.deleted_at.is_some();
        let now = now_timestamp();
        let pending =
            queue_remote_delete && !existing.qrz_logid.as_deref().unwrap_or("").is_empty();

        if !already_deleted {
            existing.deleted_at = Some(now);
        }
        if pending {
            existing.pending_remote_delete = true;
        }
        existing.updated_at = Some(now);

        let updated = self.storage.logbook().update_qso(&existing).await?;
        if !updated {
            return Err(LogbookError::NotFound(local_id.to_string()));
        }
        Ok(existing)
    }

    /// Restore a soft-deleted QSO by clearing `deleted_at` and
    /// `pending_remote_delete`. Returns the restored record.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Validation`] when the local ID is blank,
    /// [`LogbookError::NotFound`] when the record does not exist, and
    /// [`LogbookError::Storage`] when the backend write fails. Restoring a
    /// row that is not soft-deleted is a no-op success.
    pub async fn restore_qso(&self, local_id: &str) -> Result<QsoRecord, LogbookError> {
        if local_id.trim().is_empty() {
            return Err(LogbookError::Validation(
                "local_id is required when restoring a QSO.".into(),
            ));
        }

        let existing = self.storage.logbook().get_qso(local_id).await?;
        let mut existing = existing.ok_or_else(|| LogbookError::NotFound(local_id.to_string()))?;

        let was_deleted = existing.deleted_at.is_some();
        existing.deleted_at = None;
        existing.pending_remote_delete = false;

        // If the row was never synced (no qrz_logid), restoring should mark it
        // for upload again so a future sync picks it up.
        if was_deleted
            && existing.sync_status == SyncStatus::Synced as i32
            && existing.qrz_logid.as_deref().unwrap_or("").is_empty()
        {
            existing.sync_status = SyncStatus::LocalOnly as i32;
        }
        existing.updated_at = Some(now_timestamp());

        let updated = self.storage.logbook().update_qso(&existing).await?;
        if !updated {
            return Err(LogbookError::NotFound(local_id.to_string()));
        }
        Ok(existing)
    }

    /// Retrieve a persisted QSO by local identifier.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Validation`] when the local ID is blank,
    /// [`LogbookError::NotFound`] when the record does not exist, and
    /// [`LogbookError::Storage`] when the backend read fails.
    pub async fn get_qso(&self, local_id: &str) -> Result<QsoRecord, LogbookError> {
        if local_id.trim().is_empty() {
            return Err(LogbookError::Validation(
                "local_id is required when loading a QSO.".into(),
            ));
        }

        self.storage
            .logbook()
            .get_qso(local_id)
            .await?
            .ok_or_else(|| LogbookError::NotFound(local_id.to_string()))
    }

    /// List QSOs using the provided filter and pagination options.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Storage`] when the backend query fails.
    pub async fn list_qsos(&self, query: &QsoListQuery) -> Result<Vec<QsoRecord>, LogbookError> {
        self.storage
            .logbook()
            .list_qsos(query)
            .await
            .map_err(Into::into)
    }

    /// Return the current aggregate sync status derived from local and remote metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Storage`] when the backend count or metadata reads fail.
    pub async fn get_sync_status(&self) -> Result<LogbookSyncStatus, LogbookError> {
        let counts = self.storage.logbook().qso_counts().await?;
        let metadata = self.storage.logbook().get_sync_metadata().await?;

        Ok(LogbookSyncStatus::from_parts(counts, metadata))
    }

    /// Import ADIF-mapped QSOs into the logbook with duplicate detection and explicit fallback rules.
    ///
    /// Imported station history wins. The active station profile is only applied when a record carries
    /// no local-station context at all, and that fallback is reported in the returned warnings.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Storage`] when the backend fails while checking duplicates or
    /// persisting an imported record.
    pub async fn import_adif_qsos(
        &self,
        qsos: Vec<QsoRecord>,
        active_station_profile: Option<&StationProfile>,
        refresh: bool,
    ) -> Result<AdifImportSummary, LogbookError> {
        Box::pin(self.import_adif_qsos_inner(qsos, active_station_profile, refresh, None)).await
    }

    /// Import ADIF-mapped QSOs and add a durable diagnostic to affected records.
    ///
    /// Each incoming record must contain its diagnostic in the configured extra field.
    /// The first diagnostic stored for a duplicate identity remains authoritative.
    /// This behavior lets concurrent recovery inputs identify the path that won.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Storage`] when the backend fails while checking duplicates or
    /// persisting an imported record.
    pub async fn import_adif_qsos_with_diagnostic(
        &self,
        qsos: Vec<QsoRecord>,
        active_station_profile: Option<&StationProfile>,
        refresh: bool,
        diagnostic: &AdifImportDiagnostic,
    ) -> Result<AdifImportSummary, LogbookError> {
        Box::pin(self.import_adif_qsos_inner(
            qsos,
            active_station_profile,
            refresh,
            Some(diagnostic),
        ))
        .await
    }

    async fn import_adif_qsos_inner(
        &self,
        qsos: Vec<QsoRecord>,
        active_station_profile: Option<&StationProfile>,
        refresh: bool,
        diagnostic: Option<&AdifImportDiagnostic>,
    ) -> Result<AdifImportSummary, LogbookError> {
        let mut summary = AdifImportSummary::default();

        for (index, qso) in qsos.into_iter().enumerate() {
            let outcome = self
                .import_single_adif_qso(index + 1, qso, active_station_profile, refresh, diagnostic)
                .await?;

            match outcome.kind {
                AdifImportOutcomeKind::Imported => {
                    summary.records_imported = summary.records_imported.saturating_add(1);
                }
                AdifImportOutcomeKind::Updated => {
                    summary.records_updated = summary.records_updated.saturating_add(1);
                }
                AdifImportOutcomeKind::Skipped => {
                    summary.records_skipped = summary.records_skipped.saturating_add(1);
                }
            }

            if let Some(qso) = outcome.affected_qso {
                summary.affected_qsos.push(qso);
            }
            summary.warnings.extend(outcome.warnings);
        }

        Ok(summary)
    }

    /// Update the stored remote sync metadata.
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Storage`] when the backend write fails.
    pub async fn update_sync_metadata(&self, metadata: &SyncMetadata) -> Result<(), LogbookError> {
        self.storage
            .logbook()
            .upsert_sync_metadata(metadata)
            .await
            .map_err(Into::into)
    }

    /// Permanently remove soft-deleted QSOs from storage.
    ///
    /// Only rows with `deleted_at` set are eligible. When `local_ids` is
    /// non-empty, only those IDs are considered (inclusion filter).
    ///
    /// # Errors
    ///
    /// Returns [`LogbookError::Storage`] when the backend delete fails.
    pub async fn purge_deleted_qsos(
        &self,
        local_ids: &[String],
        older_than: Option<Timestamp>,
    ) -> Result<u32, LogbookError> {
        let older_than_ms = older_than.as_ref().map(|ts| {
            ts.seconds
                .saturating_mul(1_000)
                .saturating_add(i64::from(ts.nanos) / 1_000_000)
        });

        let purged = self
            .storage
            .logbook()
            .purge_deleted_qsos(local_ids, older_than_ms)
            .await?;

        Ok(purged)
    }

    async fn import_single_adif_qso(
        &self,
        record_number: usize,
        mut qso: QsoRecord,
        active_station_profile: Option<&StationProfile>,
        refresh: bool,
        diagnostic: Option<&AdifImportDiagnostic>,
    ) -> Result<AdifImportOutcome, LogbookError> {
        let had_imported_station_context = qso_has_station_context(&qso);
        let mut warnings = Vec::new();

        if had_imported_station_context {
            materialize_station_snapshot_for_create(&mut qso, None);
        } else if let Some(active_station_profile) = active_station_profile {
            materialize_station_snapshot_for_create(&mut qso, Some(active_station_profile));
            warnings.push(format!(
                "Record {record_number}: local-station history was absent in ADIF; applied active station profile '{}'.",
                station_profile_label(active_station_profile)
            ));
        } else {
            return Ok(AdifImportOutcome::skipped(format!(
                "Record {record_number}: local-station history was absent in ADIF and no active station profile is configured; skipped."
            )));
        }

        normalize_qso_for_persistence(&mut qso);
        stamp_adif_import_fingerprint(&mut qso);

        if let Some(reason) = invalid_import_reason(&qso) {
            return Ok(AdifImportOutcome::skipped(format!(
                "Record {record_number}: {reason} Skipped."
            )));
        }

        if let Some(existing) = self.find_duplicate_import(&qso).await? {
            if existing.deleted_at.is_some() {
                warnings.push(format!(
                    "Record {record_number}: duplicate skipped; matched a deleted existing QSO and was not resurrected."
                ));
                return Ok(AdifImportOutcome {
                    kind: AdifImportOutcomeKind::Skipped,
                    affected_qso: None,
                    warnings,
                });
            }

            if refresh {
                if let Some(diagnostic) = diagnostic {
                    if let Some(current_note) =
                        qso.extra_fields.get(&diagnostic.extra_field_key).cloned()
                    {
                        let effective_note = existing
                            .extra_fields
                            .get(&diagnostic.extra_field_key)
                            .map_or(current_note.as_str(), String::as_str);
                        set_import_diagnostic(&mut qso, diagnostic, effective_note);
                    }
                }
                let merged = merge_qso_for_refresh(existing.clone(), &qso);
                let mut comparable_existing = existing;
                comparable_existing.updated_at = merged.updated_at;
                if comparable_existing == merged {
                    warnings.push(format!(
                        "Record {record_number}: duplicate skipped; matched an existing QSO and import data did not change it."
                    ));
                    return Ok(AdifImportOutcome {
                        kind: AdifImportOutcomeKind::Skipped,
                        affected_qso: None,
                        warnings,
                    });
                }
                let updated = self.update_qso(merged).await?;
                warnings.push(format!(
                    "Record {record_number}: refreshed existing record '{}'.",
                    updated.local_id
                ));
                return Ok(AdifImportOutcome {
                    kind: AdifImportOutcomeKind::Updated,
                    affected_qso: Some(updated),
                    warnings,
                });
            }

            warnings.push(format!(
                "Record {record_number}: duplicate skipped; matched an existing QSO on station_callsign, worked_callsign, compatible utc_timestamp, band, mode, and compatible submode/frequency."
            ));
            return Ok(AdifImportOutcome {
                kind: AdifImportOutcomeKind::Skipped,
                affected_qso: None,
                warnings,
            });
        }

        if let Some(diagnostic) = diagnostic {
            if let Some(note) = qso.extra_fields.get(&diagnostic.extra_field_key).cloned() {
                set_import_diagnostic(&mut qso, diagnostic, &note);
            }
        }
        let stored = self.log_qso(qso).await?;
        Ok(AdifImportOutcome {
            kind: AdifImportOutcomeKind::Imported,
            affected_qso: Some(stored),
            warnings,
        })
    }

    async fn find_duplicate_import(
        &self,
        candidate: &QsoRecord,
    ) -> Result<Option<QsoRecord>, LogbookError> {
        if let Some(import_fingerprint) = candidate.extra_fields.get(ADIF_IMPORT_FINGERPRINT_FIELD)
        {
            let all_qsos = self
                .storage
                .logbook()
                .list_qsos(&QsoListQuery {
                    deleted_filter: DeletedRecordsFilter::All,
                    ..QsoListQuery::default()
                })
                .await?;
            if let Some(existing) = all_qsos.into_iter().find(|existing| {
                existing.extra_fields.get(ADIF_IMPORT_FINGERPRINT_FIELD) == Some(import_fingerprint)
            }) {
                return Ok(Some(existing));
            }
        }

        let Some(timestamp) = candidate.utc_timestamp else {
            return Ok(None);
        };

        let callsign_filter = non_empty_trimmed(&candidate.worked_callsign)
            .or_else(|| non_empty_trimmed(&candidate.station_callsign));
        let existing = self
            .storage
            .logbook()
            .list_qsos(&QsoListQuery {
                after: Some(offset_timestamp_seconds(
                    timestamp,
                    -ADIF_DUPLICATE_TIME_WINDOW_SECONDS,
                )),
                before: Some(offset_timestamp_seconds(
                    timestamp,
                    ADIF_DUPLICATE_TIME_WINDOW_SECONDS,
                )),
                callsign_filter,
                deleted_filter: DeletedRecordsFilter::All,
                ..QsoListQuery::default()
            })
            .await?;

        Ok(existing
            .into_iter()
            .find(|existing_qso| qsos_match_for_duplicate(existing_qso, candidate)))
    }
}

/// Aggregated sync status returned by the logbook engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogbookSyncStatus {
    /// Total number of locally persisted QSOs.
    pub local_qso_count: u32,
    /// Number of local QSOs that are not yet fully synced.
    pub pending_upload: u32,
    /// Latest remote QSO count reported by QRZ metadata.
    pub qrz_qso_count: u32,
    /// Timestamp of the last completed QRZ sync, if known.
    pub last_sync: Option<Timestamp>,
    /// Remote logbook owner reported by QRZ, if known.
    pub qrz_logbook_owner: Option<String>,
}

/// Summary of an ADIF import run.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AdifImportSummary {
    /// Number of records inserted into storage.
    pub records_imported: u32,
    /// Number of records skipped due to duplicates or validation problems.
    pub records_skipped: u32,
    /// Number of existing records updated during a refresh import.
    pub records_updated: u32,
    /// Human-readable warnings collected during import.
    pub warnings: Vec<String>,
    /// Records inserted or updated by this import.
    pub affected_qsos: Vec<QsoRecord>,
}

/// Diagnostic data that an integration adds to an ADIF-imported QSO.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdifImportDiagnostic {
    /// Extra-field key that keeps the first diagnostic stable across refreshes.
    pub extra_field_key: String,
    /// Prefix that identifies diagnostic lines in operator notes.
    pub note_prefix: String,
}

impl LogbookSyncStatus {
    fn from_parts(counts: LogbookCounts, metadata: SyncMetadata) -> Self {
        Self {
            local_qso_count: counts.local_qso_count,
            pending_upload: counts.pending_upload_count,
            qrz_qso_count: metadata.qrz_qso_count,
            last_sync: metadata.last_sync,
            qrz_logbook_owner: metadata.qrz_logbook_owner,
        }
    }
}

/// Errors returned by logbook workflows before they are translated to transport status codes.
#[derive(Debug, Error)]
pub enum LogbookError {
    /// The public logging surface requires an active station profile.
    #[error("An active station profile is required before logging a QSO.")]
    NoActiveStationProfile,
    /// The incoming request is missing required fields or contains invalid values.
    #[error("Validation failed: {0}")]
    Validation(String),
    /// The requested QSO could not be found in persistent storage.
    #[error("QSO '{0}' was not found.")]
    NotFound(String),
    /// The QSO is soft-deleted and must be restored before it can be updated.
    #[error("QSO '{0}' is deleted; restore it before updating.")]
    AlreadyDeleted(String),
    /// The underlying storage layer failed to complete the requested operation.
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum AdifImportOutcomeKind {
    Imported,
    Updated,
    Skipped,
}

#[derive(Debug, Clone, PartialEq)]
struct AdifImportOutcome {
    kind: AdifImportOutcomeKind,
    affected_qso: Option<QsoRecord>,
    warnings: Vec<String>,
}

impl AdifImportOutcome {
    fn skipped(warning: String) -> Self {
        Self {
            kind: AdifImportOutcomeKind::Skipped,
            affected_qso: None,
            warnings: vec![warning],
        }
    }
}

fn normalize_qso_for_persistence(qso: &mut QsoRecord) {
    qso.station_callsign = qso.station_callsign.trim().to_uppercase();
    qso.worked_callsign = qso.worked_callsign.trim().to_uppercase();
}

fn set_import_diagnostic(
    qso: &mut QsoRecord,
    diagnostic: &AdifImportDiagnostic,
    effective_note: &str,
) {
    let mut notes = qso
        .notes
        .as_deref()
        .unwrap_or_default()
        .lines()
        .filter(|line| !line.starts_with(&diagnostic.note_prefix))
        .collect::<Vec<_>>()
        .join("\n");
    if !notes.is_empty() {
        notes.push('\n');
    }
    notes.push_str(effective_note);
    qso.notes = Some(notes);
    qso.extra_fields.insert(
        diagnostic.extra_field_key.clone(),
        effective_note.to_string(),
    );
}

fn validate_qso_for_persistence(qso: &QsoRecord) -> Result<(), LogbookError> {
    if qso.station_callsign.trim().is_empty() {
        return Err(LogbookError::Validation(
            "station_callsign is required.".into(),
        ));
    }

    if qso.worked_callsign.trim().is_empty() {
        return Err(LogbookError::Validation(
            "worked_callsign is required.".into(),
        ));
    }

    if qso.utc_timestamp.is_none() {
        return Err(LogbookError::Validation(
            "utc_timestamp is required.".into(),
        ));
    }

    match Band::try_from(qso.band) {
        Ok(Band::Unspecified) => {
            return Err(LogbookError::Validation("band is required.".into()));
        }
        Ok(_) => {}
        Err(_) => {
            return Err(LogbookError::Validation("band is invalid.".into()));
        }
    }

    match Mode::try_from(qso.mode) {
        Ok(Mode::Unspecified) => {
            return Err(LogbookError::Validation("mode is required.".into()));
        }
        Ok(_) => {}
        Err(_) => {
            return Err(LogbookError::Validation("mode is invalid.".into()));
        }
    }

    Ok(())
}

fn invalid_import_reason(qso: &QsoRecord) -> Option<String> {
    if qso.station_callsign.trim().is_empty() {
        Some("station_callsign is required.".to_string())
    } else if qso.worked_callsign.trim().is_empty() {
        Some("worked_callsign is required.".to_string())
    } else if qso.utc_timestamp.is_none() {
        if let Some(raw_date) = qso.extra_fields.get("QSO_DATE") {
            Some(format!(
                "invalid ADIF date/time '{raw_date}{}'.",
                qso.extra_fields
                    .get("TIME_ON")
                    .map_or(String::new(), |time| format!("/{time}"))
            ))
        } else {
            Some("utc_timestamp is required.".to_string())
        }
    } else {
        match Band::try_from(qso.band) {
            Ok(Band::Unspecified) => Some(match qso.extra_fields.get("BAND") {
                Some(raw_band) => format!("unrecognized ADIF band '{raw_band}'."),
                None => "band is required.".to_string(),
            }),
            Ok(_) => match Mode::try_from(qso.mode) {
                Ok(Mode::Unspecified) => Some(match qso.extra_fields.get("MODE") {
                    Some(raw_mode) => format!("unrecognized ADIF mode '{raw_mode}'."),
                    None => "mode is required.".to_string(),
                }),
                Ok(_) => None,
                Err(_) => Some("mode is invalid.".to_string()),
            },
            Err(_) => Some("band is invalid.".to_string()),
        }
    }
}

fn qso_has_station_context(qso: &QsoRecord) -> bool {
    !qso.station_callsign.trim().is_empty()
        || qso
            .station_snapshot
            .as_ref()
            .is_some_and(station_snapshot_has_values)
}

fn station_profile_label(profile: &StationProfile) -> String {
    non_empty_trimmed_option(profile.profile_name.as_deref())
        .or_else(|| non_empty_trimmed_option(Some(profile.station_callsign.as_str())))
        .unwrap_or_else(|| "active station profile".to_string())
}

fn qsos_match_for_duplicate(existing: &QsoRecord, candidate: &QsoRecord) -> bool {
    timestamps_match(
        existing.utc_timestamp.as_ref(),
        candidate.utc_timestamp.as_ref(),
    ) && existing.band == candidate.band
        && existing.mode == candidate.mode
        && strings_equal_ignore_ascii_case(
            existing.station_callsign.as_str(),
            candidate.station_callsign.as_str(),
        )
        && strings_equal_ignore_ascii_case(
            existing.worked_callsign.as_str(),
            candidate.worked_callsign.as_str(),
        )
        && optional_strings_compatible(existing.submode.as_deref(), candidate.submode.as_deref())
        && frequencies_compatible(existing, candidate)
}

fn stamp_adif_import_fingerprint(qso: &mut QsoRecord) {
    let fingerprint = adif_import_fingerprint(qso);
    qso.extra_fields
        .insert(ADIF_IMPORT_FINGERPRINT_FIELD.to_string(), fingerprint);
}

fn adif_import_fingerprint(qso: &QsoRecord) -> String {
    let timestamp = qso
        .utc_timestamp
        .as_ref()
        .map_or_else(String::new, |stamp| {
            format!("{}.{:09}", stamp.seconds, stamp.nanos)
        });
    [
        "v1".to_string(),
        qso.station_callsign.trim().to_ascii_uppercase(),
        qso.worked_callsign.trim().to_ascii_uppercase(),
        timestamp,
        qso.band.to_string(),
        qso.mode.to_string(),
        qso.submode
            .as_deref()
            .map(str::trim)
            .unwrap_or_default()
            .to_ascii_uppercase(),
        qso.frequency_hz
            .map_or_else(String::new, |frequency| frequency.to_string()),
        {
            #[allow(deprecated)]
            let frequency_khz = qso.frequency_khz;
            frequency_khz.map_or_else(String::new, |frequency| frequency.to_string())
        },
    ]
    .join("\u{1f}")
}

/// Merge import data onto an existing record for refresh mode.
///
/// Non-empty import fields overwrite existing values. Identity fields (`local_id`,
/// `created_at`, `sync_status`, `qrz_logid`, `qrz_bookid`) and QSL confirmation
/// fields are always preserved from the existing record. Extra fields are merged
/// key-by-key (import keys overwrite, existing keys not in import are preserved).
fn merge_qso_for_refresh(mut existing: QsoRecord, import: &QsoRecord) -> QsoRecord {
    // Core contact fields — always take from import (they matched for duplicate).
    existing
        .station_callsign
        .clone_from(&import.station_callsign);
    existing.worked_callsign.clone_from(&import.worked_callsign);
    existing.utc_timestamp = import.utc_timestamp;
    existing.band = import.band;
    existing.mode = import.mode;

    #[allow(deprecated)]
    merge_optional_u64(&mut existing.frequency_khz, import.frequency_khz);
    merge_optional_u64(&mut existing.frequency_hz, import.frequency_hz);
    merge_optional_str(&mut existing.submode, import.submode.as_deref());
    merge_optional_timestamp(&mut existing.utc_end_timestamp, import.utc_end_timestamp);

    // Station snapshot — import snapshot overwrites if present.
    if import.station_snapshot.is_some() {
        existing
            .station_snapshot
            .clone_from(&import.station_snapshot);
    }

    // Signal reports — always take from import when present.
    if import.rst_sent.is_some() {
        existing.rst_sent.clone_from(&import.rst_sent);
    }
    if import.rst_received.is_some() {
        existing.rst_received.clone_from(&import.rst_received);
    }
    merge_optional_str(&mut existing.tx_power, import.tx_power.as_deref());

    // Worked-station enrichment.
    merge_optional_str(
        &mut existing.worked_operator_callsign,
        import.worked_operator_callsign.as_deref(),
    );
    merge_optional_str(
        &mut existing.worked_operator_name,
        import.worked_operator_name.as_deref(),
    );
    merge_optional_str(&mut existing.worked_grid, import.worked_grid.as_deref());
    merge_optional_str(
        &mut existing.worked_country,
        import.worked_country.as_deref(),
    );
    merge_optional_u32(&mut existing.worked_dxcc, import.worked_dxcc);
    merge_optional_str(&mut existing.worked_state, import.worked_state.as_deref());
    merge_optional_u32(&mut existing.worked_cq_zone, import.worked_cq_zone);
    merge_optional_u32(&mut existing.worked_itu_zone, import.worked_itu_zone);
    merge_optional_str(&mut existing.worked_county, import.worked_county.as_deref());
    merge_optional_str(&mut existing.worked_iota, import.worked_iota.as_deref());
    merge_optional_str(
        &mut existing.worked_continent,
        import.worked_continent.as_deref(),
    );
    merge_optional_str(
        &mut existing.worked_arrl_section,
        import.worked_arrl_section.as_deref(),
    );

    // Contest fields.
    merge_optional_str(&mut existing.contest_id, import.contest_id.as_deref());
    merge_optional_str(&mut existing.serial_sent, import.serial_sent.as_deref());
    merge_optional_str(
        &mut existing.serial_received,
        import.serial_received.as_deref(),
    );
    merge_optional_str(&mut existing.exchange_sent, import.exchange_sent.as_deref());
    merge_optional_str(
        &mut existing.exchange_received,
        import.exchange_received.as_deref(),
    );

    // Propagation.
    merge_optional_str(&mut existing.prop_mode, import.prop_mode.as_deref());
    merge_optional_str(&mut existing.sat_name, import.sat_name.as_deref());
    merge_optional_str(&mut existing.sat_mode, import.sat_mode.as_deref());

    // Notes — import overwrites when non-empty.
    merge_optional_str(&mut existing.notes, import.notes.as_deref());
    merge_optional_str(&mut existing.comment, import.comment.as_deref());

    // Extra fields — merge key-by-key.
    for (key, value) in &import.extra_fields {
        existing.extra_fields.insert(key.clone(), value.clone());
    }

    existing.updated_at = Some(now_timestamp());

    existing
}

fn merge_optional_str(target: &mut Option<String>, source: Option<&str>) {
    if let Some(value) = source.and_then(|s| {
        let trimmed = s.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    }) {
        *target = Some(value);
    }
}

fn merge_optional_u32(target: &mut Option<u32>, source: Option<u32>) {
    if let Some(value) = source.filter(|v| *v > 0) {
        *target = Some(value);
    }
}

fn merge_optional_u64(target: &mut Option<u64>, source: Option<u64>) {
    if let Some(value) = source.filter(|v| *v > 0) {
        *target = Some(value);
    }
}

fn merge_optional_timestamp(target: &mut Option<Timestamp>, source: Option<Timestamp>) {
    if source.is_some() {
        *target = source;
    }
}

fn timestamps_match(left: Option<&Timestamp>, right: Option<&Timestamp>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) if left == right => true,
        (Some(left), Some(right)) => {
            timestamp_minute_bucket(left) == timestamp_minute_bucket(right)
                && (timestamp_is_minute_precision(left) || timestamp_is_minute_precision(right))
        }
        _ => false,
    }
}

fn timestamp_minute_bucket(timestamp: &Timestamp) -> i64 {
    timestamp.seconds.div_euclid(60)
}

fn timestamp_is_minute_precision(timestamp: &Timestamp) -> bool {
    timestamp.seconds.rem_euclid(60) == 0 && timestamp.nanos == 0
}

fn offset_timestamp_seconds(timestamp: Timestamp, offset_seconds: i64) -> Timestamp {
    Timestamp {
        seconds: timestamp.seconds.saturating_add(offset_seconds),
        nanos: timestamp.nanos,
    }
}

fn strings_equal_ignore_ascii_case(left: &str, right: &str) -> bool {
    left.trim().eq_ignore_ascii_case(right.trim())
}

fn optional_strings_compatible(left: Option<&str>, right: Option<&str>) -> bool {
    match (
        non_empty_trimmed_option(left),
        non_empty_trimmed_option(right),
    ) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(&right),
        _ => true,
    }
}

fn optional_u64_compatible(left: Option<u64>, right: Option<u64>) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left == right,
        _ => true,
    }
}

/// Compare frequencies for duplicate matching.
/// When both sides have Hz fields, allow small source-format drift.
/// When one or both lack Hz, fall back to kHz compatibility.
#[allow(deprecated)]
fn frequencies_compatible(existing: &QsoRecord, candidate: &QsoRecord) -> bool {
    match (existing.frequency_hz, candidate.frequency_hz) {
        (Some(a), Some(b)) => a.abs_diff(b) <= ADIF_DUPLICATE_FREQUENCY_TOLERANCE_HZ,
        _ => optional_u64_compatible(existing.frequency_khz, candidate.frequency_khz),
    }
}

fn non_empty_trimmed(value: &str) -> Option<String> {
    non_empty_trimmed_option(Some(value))
}

fn non_empty_trimmed_option(value: Option<&str>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn now_timestamp() -> Timestamp {
    crate::utc::to_timestamp(OffsetDateTime::now_utc())
}

/// Return whether a QSO should count as pending upload work.
#[must_use]
pub fn is_pending_sync_status(sync_status: i32) -> bool {
    sync_status != SyncStatus::Synced as i32
}
