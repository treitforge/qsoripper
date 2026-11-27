//! Bidirectional QRZ logbook sync workflow.
//!
//! **Phase 1** — Download QSOs from QRZ and merge into local storage.
//! **Phase 2** — Upload local-only and modified QSOs to QRZ.
//! **Phase 3** — Update sync metadata with the current timestamp.

use std::collections::{HashMap, HashSet};

use prost::Message;
use qsoripper_core::domain::qso::new_local_id;
use qsoripper_core::proto::qsoripper::domain::{ConflictPolicy, QsoRecord, SyncStatus};
use qsoripper_core::proto::qsoripper::services::SyncWithQrzResponse;
use qsoripper_core::qrz_logbook::{
    QrzLogbookClient, QrzLogbookError, QrzLogbookStatus, QrzUploadResult,
};
use qsoripper_core::storage::{DeletedRecordsFilter, LogbookStore, QsoListQuery, SyncMetadata};
use tokio::sync::mpsc;
use tonic::Status;

// ---------------------------------------------------------------------------
// Type aliases (avoids clippy::type_complexity)
// ---------------------------------------------------------------------------

/// Index from QRZ logbook-record-id → local QSO.
type LogidIndex = HashMap<String, QsoRecord>;

/// Index from (callsign, band, mode) → local QSOs with that key.
type FuzzyIndex = HashMap<(String, i32, i32), Vec<QsoRecord>>;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Extra-field keys that historical QRZ ADIF responses may carry for the
/// logbook record id. The canonical key per the QRZ Logbook API is
/// `APP_QRZLOG_LOGID`; `APP_QRZ_LOGID` is kept as a legacy alias because
/// older internal builds wrote that variant. The ADIF mapper now extracts
/// either alias into [`QsoRecord::qrz_logid`] directly, so this fallback is
/// only exercised against records that bypass the mapper (e.g. ones that
/// were already persisted before the mapper started recognising the field
/// and have not yet been backfilled by [`crate::repair::backfill_qrz_logids`]).
const QRZ_LOGID_EXTRA_FIELDS: &[&str] = &["APP_QRZLOG_LOGID", "APP_QRZ_LOGID"];

/// Maximum time difference (seconds) for fuzzy timestamp matching.
const TIMESTAMP_TOLERANCE_SECONDS: i64 = 60;

// ---------------------------------------------------------------------------
// Trait — testable abstraction over the real QRZ HTTP client
// ---------------------------------------------------------------------------

/// QRZ logbook HTTP operations required by the sync workflow.
///
/// Extracted into a trait so unit tests can substitute a mock without hitting
/// the network.
#[tonic::async_trait]
pub(crate) trait QrzLogbookApi: Send + Sync {
    /// Fetch QSOs from the remote logbook, optionally since a date.
    async fn fetch_qsos(&self, since: Option<&str>) -> Result<Vec<QsoRecord>, QrzLogbookError>;

    /// Upload a single QSO and return its QRZ-assigned log ID.
    ///
    /// `book_owner` is the callsign the QRZ logbook is registered to (from a
    /// fresh `STATUS` call, falling back to cached `SyncMetadata`). When
    /// supplied, the upload payload's `STATION_CALLSIGN` is rewritten to the
    /// book owner if it differs (for operators with previous callsigns), so
    /// QRZ accepts the record. See
    /// [`crate::qrz_logbook::rewrite_station_callsign_for_book`].
    async fn upload_qso(
        &self,
        qso: &QsoRecord,
        book_owner: Option<&str>,
    ) -> Result<QrzUploadResult, QrzLogbookError>;

    /// Upload a single QSO with `OPTION=REPLACE`, auto-matching any existing
    /// duplicate on QRZ without requiring a known logid.
    async fn upload_qso_with_replace(
        &self,
        qso: &QsoRecord,
        book_owner: Option<&str>,
    ) -> Result<QrzUploadResult, QrzLogbookError>;

    /// Replace an existing QSO on the remote logbook (preserves logid).
    ///
    /// `book_owner` has the same semantics as in [`Self::upload_qso`].
    async fn replace_qso(
        &self,
        logid: &str,
        qso: &QsoRecord,
        book_owner: Option<&str>,
    ) -> Result<QrzUploadResult, QrzLogbookError>;

    /// Query the remote logbook for the current owner callsign and QSO count.
    async fn fetch_status(&self) -> Result<QrzLogbookStatus, QrzLogbookError>;

    /// Delete a remote QSO by its QRZ logid. Implementations should treat
    /// "not found" as success so the queued-remote-delete loop is idempotent.
    async fn delete_qso(&self, logid: &str) -> Result<(), QrzLogbookError>;
}

#[tonic::async_trait]
impl QrzLogbookApi for QrzLogbookClient {
    async fn fetch_qsos(&self, since: Option<&str>) -> Result<Vec<QsoRecord>, QrzLogbookError> {
        QrzLogbookClient::fetch_qsos(self, since).await
    }

    async fn upload_qso(
        &self,
        qso: &QsoRecord,
        book_owner: Option<&str>,
    ) -> Result<QrzUploadResult, QrzLogbookError> {
        QrzLogbookClient::upload_qso(self, qso, book_owner).await
    }

    async fn upload_qso_with_replace(
        &self,
        qso: &QsoRecord,
        book_owner: Option<&str>,
    ) -> Result<QrzUploadResult, QrzLogbookError> {
        QrzLogbookClient::upload_qso_with_replace(self, qso, book_owner).await
    }

    async fn replace_qso(
        &self,
        logid: &str,
        qso: &QsoRecord,
        book_owner: Option<&str>,
    ) -> Result<QrzUploadResult, QrzLogbookError> {
        QrzLogbookClient::replace_qso(self, logid, qso, book_owner).await
    }

    async fn fetch_status(&self) -> Result<QrzLogbookStatus, QrzLogbookError> {
        QrzLogbookClient::test_connection(self).await
    }

    async fn delete_qso(&self, logid: &str) -> Result<(), QrzLogbookError> {
        QrzLogbookClient::delete_qso(self, logid).await
    }
}

/// Outcome of a destructive local purge that may first delete QRZ rows.
pub(crate) struct PurgeDeletedOutcome {
    pub(crate) purged_count: u32,
    pub(crate) remote_deletes_pushed: u32,
    pub(crate) remote_deletes_failed: u32,
    pub(crate) errors: Vec<String>,
}

/// Purge eligible soft-deleted rows, retaining any row whose requested QRZ
/// delete fails so the operator can retry without losing the local copy.
pub(crate) async fn purge_deleted_qsos(
    client: Option<&dyn QrzLogbookApi>,
    store: &dyn LogbookStore,
    local_ids: &[String],
    older_than_ms: Option<i64>,
    include_pending_remote_deletes: bool,
) -> Result<PurgeDeletedOutcome, qsoripper_core::storage::StorageError> {
    if !include_pending_remote_deletes {
        let purged_count = store.purge_deleted_qsos(local_ids, older_than_ms).await?;
        return Ok(PurgeDeletedOutcome {
            purged_count,
            remote_deletes_pushed: 0,
            remote_deletes_failed: 0,
            errors: Vec::new(),
        });
    }

    let requested_ids = (!local_ids.is_empty())
        .then(|| local_ids.iter().map(String::as_str).collect::<HashSet<_>>());
    let candidates = store
        .list_qsos(&QsoListQuery {
            deleted_filter: DeletedRecordsFilter::DeletedOnly,
            ..QsoListQuery::default()
        })
        .await?;
    let candidates = candidates.into_iter().filter(|qso| {
        let id_matches = requested_ids
            .as_ref()
            .is_none_or(|ids| ids.contains(qso.local_id.as_str()));
        let time_matches = older_than_ms.is_none_or(|cutoff| {
            qso.deleted_at
                .as_ref()
                .is_some_and(|timestamp| timestamp_to_millis(timestamp) <= cutoff)
        });
        id_matches && time_matches
    });

    let mut purge_ids = Vec::new();
    let mut remote_deletes_pushed = 0;
    let mut remote_deletes_failed = 0;
    let mut errors = Vec::new();

    for qso in candidates {
        let logid = qso.qrz_logid.as_deref().unwrap_or("").trim();
        if !qso.pending_remote_delete || logid.is_empty() {
            purge_ids.push(qso.local_id);
            continue;
        }

        let Some(client) = client else {
            remote_deletes_failed += 1;
            errors.push(format!(
                "QRZ delete was not attempted for QSO '{}' because QRZ Logbook is not configured.",
                qso.local_id
            ));
            continue;
        };

        match client.delete_qso(logid).await {
            Ok(()) => {
                remote_deletes_pushed += 1;
                purge_ids.push(qso.local_id);
            }
            Err(error) => {
                remote_deletes_failed += 1;
                errors.push(format!(
                    "QRZ delete failed for QSO '{}': {error}",
                    qso.local_id
                ));
            }
        }
    }

    let purged_count = if purge_ids.is_empty() {
        0
    } else {
        store.purge_deleted_qsos(&purge_ids, older_than_ms).await?
    };

    Ok(PurgeDeletedOutcome {
        purged_count,
        remote_deletes_pushed,
        remote_deletes_failed,
        errors,
    })
}

fn timestamp_to_millis(timestamp: &prost_types::Timestamp) -> i64 {
    timestamp
        .seconds
        .saturating_mul(1_000)
        .saturating_add(i64::from(timestamp.nanos) / 1_000_000)
}

// ---------------------------------------------------------------------------
// Accumulated sync counters
// ---------------------------------------------------------------------------

struct SyncCounters {
    downloaded: u32,
    uploaded: u32,
    conflicts: u32,
    /// Number of remote rows skipped because they match a soft-deleted local row.
    deletes_skipped_remote: u32,
    /// Number of queued remote deletes that were pushed to QRZ in Phase 2.
    remote_deletes_pushed: u32,
    /// Number of uploads retried with OPTION=REPLACE due to duplicate detection.
    duplicate_replaces: u32,
    errors: Vec<String>,
}

impl SyncCounters {
    fn new() -> Self {
        Self {
            downloaded: 0,
            uploaded: 0,
            conflicts: 0,
            deletes_skipped_remote: 0,
            remote_deletes_pushed: 0,
            duplicate_replaces: 0,
            errors: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Core sync orchestrator
// ---------------------------------------------------------------------------

/// Execute a bidirectional sync between the local logbook and QRZ.
///
/// Sends streaming progress updates through `progress_tx`. The final message
/// will have `complete = true`.
pub(crate) async fn execute_sync(
    client: &dyn QrzLogbookApi,
    store: &dyn LogbookStore,
    full_sync: bool,
    conflict_policy: ConflictPolicy,
    progress_tx: &mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
) {
    let mut counters = SyncCounters::new();

    let metadata = match store.get_sync_metadata().await {
        Ok(m) => m,
        Err(err) => {
            eprintln!("[sync] Failed to read sync metadata: {err}");
            counters
                .errors
                .push(format!("Failed to read sync metadata: {err}"));
            SyncMetadata::default()
        }
    };

    let status_result = client.fetch_status().await;
    let book_owner = match &status_result {
        Ok(status) if !status.owner.trim().is_empty() => Some(status.owner.clone()),
        Ok(_) => metadata.qrz_logbook_owner.clone(),
        Err(err) => {
            eprintln!(
                "[sync] STATUS call failed before upload; falling back to cached owner: {err}"
            );
            metadata.qrz_logbook_owner.clone()
        }
    }
    .map(|s| s.trim().to_owned())
    .filter(|s| !s.is_empty());

    let freshly_uploaded_logids = if conflict_policy == ConflictPolicy::LastWriteWins {
        HashSet::new()
    } else {
        upload_phase(
            client,
            store,
            book_owner.as_deref(),
            UploadSelection::ModifiedWithLogid,
            progress_tx,
            &mut counters,
        )
        .await
    };

    let Some(metadata) = download_phase(
        DownloadPhaseInput {
            client,
            store,
            metadata: &metadata,
            remote_qso_count: status_result.as_ref().ok().map(|status| status.qso_count),
            full_sync,
            conflict_policy,
            freshly_uploaded_logids: &freshly_uploaded_logids,
            progress_tx,
        },
        &mut counters,
    )
    .await
    else {
        return;
    };

    upload_phase(
        client,
        store,
        book_owner.as_deref(),
        UploadSelection::Pending,
        progress_tx,
        &mut counters,
    )
    .await;

    push_pending_remote_deletes(client, store, progress_tx, &mut counters).await;

    update_metadata(store, &metadata, status_result, &mut counters).await;

    let error_summary = if counters.errors.is_empty() {
        None
    } else {
        Some(counters.errors.join("; "))
    };

    eprintln!(
        "[sync] Sync completed: downloaded={} uploaded={} duplicate_replaces={} conflicts={} remote_deletes_pushed={} deletes_skipped_remote={} errors={}",
        counters.downloaded,
        counters.uploaded,
        counters.duplicate_replaces,
        counters.conflicts,
        counters.remote_deletes_pushed,
        counters.deletes_skipped_remote,
        counters.errors.len(),
    );

    send_complete(progress_tx, &counters, error_summary).await;
}

// ---------------------------------------------------------------------------
// Phase 1 — Download from QRZ
// ---------------------------------------------------------------------------

struct DownloadPhaseInput<'a> {
    client: &'a dyn QrzLogbookApi,
    store: &'a dyn LogbookStore,
    metadata: &'a SyncMetadata,
    remote_qso_count: Option<u32>,
    full_sync: bool,
    conflict_policy: ConflictPolicy,
    freshly_uploaded_logids: &'a HashSet<String>,
    progress_tx: &'a mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
}

async fn fetch_remote_qsos_for_download(
    client: &dyn QrzLogbookApi,
    since_date: Option<&str>,
    remote_qso_count: Option<u32>,
    local_qso_count: usize,
    counters: &SyncCounters,
    progress_tx: &mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
) -> Result<Vec<QsoRecord>, QrzLogbookError> {
    let mut remote_qsos = client.fetch_qsos(since_date).await?;

    if remote_qsos.is_empty()
        && since_date.is_some()
        && remote_qso_count.is_some_and(|count| count as usize > local_qso_count)
    {
        send_progress(
            progress_tx,
            "Incremental QRZ fetch was empty; retrying a full fetch…",
            counters.downloaded,
            counters.uploaded,
            counters.conflicts,
        )
        .await;

        remote_qsos = client.fetch_qsos(None).await?;
    }

    Ok(remote_qsos)
}

async fn download_phase(
    input: DownloadPhaseInput<'_>,
    counters: &mut SyncCounters,
) -> Option<SyncMetadata> {
    let DownloadPhaseInput {
        client,
        store,
        metadata,
        remote_qso_count,
        full_sync,
        conflict_policy,
        freshly_uploaded_logids,
        progress_tx,
    } = input;

    send_progress(progress_tx, "Fetching QSOs from QRZ…", 0, 0, 0).await;

    // Load all local QSOs (including soft-deleted) so we can both match
    // against active rows AND honor user intent: any remote row whose
    // qrz_logid matches a soft-deleted local row must be skipped, otherwise
    // the next sync would resurrect the trashed QSO.
    let local_qsos = match store
        .list_qsos(&QsoListQuery {
            deleted_filter: DeletedRecordsFilter::All,
            ..QsoListQuery::default()
        })
        .await
    {
        Ok(qsos) => qsos,
        Err(err) => {
            send_complete(
                progress_tx,
                &SyncCounters::new(),
                Some(format!("Failed to load local QSOs: {err}")),
            )
            .await;
            return None;
        }
    };

    // Partition into active (used for matching) and deleted (used as a skip
    // set keyed by qrz_logid). We don't fuzzy-skip on deleted rows; QRZ
    // logid is the only signal stable enough to safely suppress a download.
    let (active_local, deleted_logids) = partition_local_for_sync(&local_qsos);
    let local_qsos = active_local;

    // If the local logbook is empty, force a full remote fetch even for
    // incremental sync requests. This recovers from stale metadata and gives
    // first-time users the expected "download everything" behavior.
    let since_date = if full_sync || local_qsos.is_empty() {
        None
    } else {
        metadata
            .last_sync
            .as_ref()
            .and_then(qsoripper_core::utc::from_timestamp)
            .map(qsoripper_core::utc::format_date)
    };

    let remote_qsos = match fetch_remote_qsos_for_download(
        client,
        since_date.as_deref(),
        remote_qso_count,
        local_qsos.len(),
        counters,
        progress_tx,
    )
    .await
    {
        Ok(qsos) => qsos,
        Err(err) => {
            send_complete(
                progress_tx,
                &SyncCounters::new(),
                Some(format!("Failed to fetch QSOs from QRZ: {err}")),
            )
            .await;
            return None;
        }
    };

    eprintln!(
        "[sync] Fetched {} QSOs from QRZ (full_sync={full_sync})",
        remote_qsos.len()
    );

    send_progress(
        progress_tx,
        &format!("Processing {} downloaded QSOs…", remote_qsos.len()),
        counters.downloaded,
        counters.uploaded,
        counters.conflicts,
    )
    .await;

    // Build lookup indexes.
    let (by_qrz_logid, by_key) = build_local_indexes(&local_qsos);

    process_remote_qsos(
        &remote_qsos,
        &deleted_logids,
        freshly_uploaded_logids,
        by_qrz_logid,
        by_key,
        store,
        conflict_policy,
        counters,
    )
    .await;

    Some(metadata.clone())
}

/// Iterate downloaded remote QSOs, applying the soft-delete skip set, then
/// either inserting a new row or merging with the matched local one.
#[allow(clippy::too_many_arguments)]
async fn process_remote_qsos(
    remote_qsos: &[QsoRecord],
    deleted_logids: &HashSet<String>,
    freshly_uploaded_logids: &HashSet<String>,
    mut by_qrz_logid: LogidIndex,
    mut by_key: FuzzyIndex,
    store: &dyn LogbookStore,
    conflict_policy: ConflictPolicy,
    counters: &mut SyncCounters,
) {
    for remote in remote_qsos {
        // Defense-in-depth: skip records that lack the minimum fields needed
        // for matching and storage. These are artefacts of ADIF
        // header/trailer fragments that slip through the parser.
        if remote.worked_callsign.trim().is_empty() || remote.utc_timestamp.is_none() {
            continue;
        }

        let remote_logid = extract_qrz_logid(remote);

        // If this sync just pushed the local correction to QRZ, ignore a
        // stale remote copy returned by the same FETCH response. QRZ may lag
        // briefly after REPLACE; local storage already reflects the operator's
        // corrected record.
        if remote_logid
            .as_deref()
            .is_some_and(|logid| freshly_uploaded_logids.contains(logid))
        {
            continue;
        }

        // Skip remote rows that match a soft-deleted local row. The user
        // intentionally trashed it; download must not resurrect it.
        if let Some(logid) = remote_logid.as_deref() {
            if deleted_logids.contains(logid) {
                counters.deletes_skipped_remote += 1;
                continue;
            }
        }

        let local_match = remote_logid
            .as_deref()
            .and_then(|logid| by_qrz_logid.get(logid).cloned())
            .or_else(|| fuzzy_match(remote, &by_key));

        match local_match {
            None => {
                insert_new_remote_qso(
                    remote,
                    remote_logid.as_deref(),
                    store,
                    &mut by_qrz_logid,
                    &mut by_key,
                    counters,
                )
                .await;
            }
            Some(local) => {
                merge_with_local(
                    &local,
                    remote_logid.as_deref(),
                    remote,
                    store,
                    conflict_policy,
                    counters,
                )
                .await;
            }
        }
    }
}

/// Insert a QSO downloaded from QRZ that has no local match.
async fn insert_new_remote_qso(
    remote: &QsoRecord,
    remote_logid: Option<&str>,
    store: &dyn LogbookStore,
    by_qrz_logid: &mut LogidIndex,
    by_key: &mut FuzzyIndex,
    counters: &mut SyncCounters,
) {
    let mut new_qso = remote.clone();
    if new_qso.local_id.is_empty() {
        new_qso.local_id = new_local_id();
    }
    new_qso.sync_status = SyncStatus::Synced as i32;
    if let Some(logid) = remote_logid {
        new_qso.qrz_logid = Some(logid.to_string());
    }
    match store.insert_qso(&new_qso).await {
        Ok(()) => {
            counters.downloaded += 1;
            // Keep indexes up to date for subsequent matches.
            if let Some(logid) = remote_logid {
                by_qrz_logid.insert(logid.to_string(), new_qso.clone());
            }
            let key = (
                new_qso.worked_callsign.to_ascii_uppercase(),
                new_qso.band,
                new_qso.mode,
            );
            by_key.entry(key).or_default().push(new_qso);
        }
        Err(err) => {
            eprintln!(
                "[sync] Failed to insert downloaded QSO for {}: {err}",
                remote.worked_callsign
            );
            counters.errors.push(format!(
                "Insert failed for {}: {err}",
                remote.worked_callsign
            ));
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2 — Upload pending local QSOs to QRZ
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum UploadSelection {
    /// Local edits with QRZ identity that must be pushed before remote download
    /// can classify the stale QRZ row as a conflict.
    ModifiedWithLogid,
    /// Normal pending upload pass after download/linking.
    Pending,
}

async fn upload_phase(
    client: &dyn QrzLogbookApi,
    store: &dyn LogbookStore,
    book_owner: Option<&str>,
    selection: UploadSelection,
    progress_tx: &mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
    counters: &mut SyncCounters,
) -> HashSet<String> {
    send_progress(
        progress_tx,
        "Uploading local QSOs to QRZ…",
        counters.downloaded,
        counters.uploaded,
        counters.conflicts,
    )
    .await;

    let pending_qsos: Vec<QsoRecord> = match store.list_qsos(&QsoListQuery::default()).await {
        Ok(qsos) => qsos
            .into_iter()
            .filter(|q| should_upload_qso(q, selection))
            .collect(),
        Err(err) => {
            eprintln!("[sync] Failed to list pending QSOs for upload: {err}");
            counters
                .errors
                .push(format!("Failed to list pending QSOs: {err}"));
            Vec::new()
        }
    };

    eprintln!(
        "[sync] Uploading {} pending QSOs to QRZ",
        pending_qsos.len()
    );

    let mut uploaded_logids = HashSet::new();
    for qso in &pending_qsos {
        match sync_single_qso(client, store, qso, book_owner).await {
            Ok(outcome) => {
                if let Some(logid) = outcome.qso.qrz_logid.as_deref() {
                    if !logid.is_empty() {
                        uploaded_logids.insert(logid.to_string());
                    }
                }
                counters.uploaded += 1;
                if outcome.was_duplicate_replace {
                    counters.duplicate_replaces += 1;
                }
            }
            Err(err) => {
                eprintln!(
                    "[sync] Failed to push QSO {} ({}): {err}",
                    qso.local_id, qso.worked_callsign
                );
                counters
                    .errors
                    .push(format!("Upload failed for {}: {err}", qso.worked_callsign));
            }
        }
    }

    uploaded_logids
}

fn should_upload_qso(qso: &QsoRecord, selection: UploadSelection) -> bool {
    match selection {
        UploadSelection::ModifiedWithLogid => {
            qso.sync_status == SyncStatus::Modified as i32
                && qso
                    .qrz_logid
                    .as_deref()
                    .is_some_and(|logid| !logid.is_empty())
        }
        UploadSelection::Pending => {
            qso.sync_status == SyncStatus::LocalOnly as i32
                || qso.sync_status == SyncStatus::Modified as i32
        }
    }
}

// ---------------------------------------------------------------------------
// Per-operation sync helper (used by Phase 2 and by per-RPC sync_to_qrz=true)
// ---------------------------------------------------------------------------

/// Resolve the QRZ logbook owner callsign to use when rewriting upload
/// payloads.
///
/// QRZ Logbook rejects `STATION_CALLSIGN` mismatches against the book owner.
/// To avoid wrongly rewriting against a stale cached owner (e.g. after the
/// operator switched to a different logbook API key), prefer a fresh `STATUS`
/// call. Fall back to the cached value from `SyncMetadata` only if STATUS
/// fails (rate limit, transient network blip).
///
/// Returns `None` when neither source yields a non-empty owner; callers
/// should then upload without rewriting (and let QRZ surface the error so the
/// user gets a clear signal that their book owner is unknown).
pub(crate) async fn resolve_book_owner_for_upload(
    client: &dyn QrzLogbookApi,
    cached_metadata: &SyncMetadata,
) -> Option<String> {
    match client.fetch_status().await {
        Ok(status) if !status.owner.trim().is_empty() => Some(status.owner),
        Ok(_) => cached_metadata.qrz_logbook_owner.clone(),
        Err(err) => {
            eprintln!(
                "[sync] STATUS call failed while resolving book owner; falling back to cached: {err}"
            );
            cached_metadata.qrz_logbook_owner.clone()
        }
    }
    .map(|s| s.trim().to_owned())
    .filter(|s| !s.is_empty())
}

/// Outcome of a successful `sync_single_qso` call.
#[derive(Debug)]
pub(crate) struct SyncOutcome {
    /// The locally-persisted QSO with refreshed logid and `sync_status`.
    pub qso: QsoRecord,
    /// `true` when the upload succeeded only after a duplicate-retry with REPLACE.
    pub was_duplicate_replace: bool,
}

/// Push a single QSO to QRZ, then mirror the QRZ-assigned logid + Synced
/// state back into local storage. Used by both bulk sync Phase 2 and the
/// per-operation `sync_to_qrz=true` paths on `LogQso` / `UpdateQso`.
///
/// Selection rule:
/// * If the local row already has a non-empty `qrz_logid`, REPLACE in place
///   so QRZ keeps the same row (no duplicate). This applies regardless of
///   whether `sync_status` is Modified or Synced.
/// * Otherwise INSERT a new remote row and adopt the returned logid.
/// * If INSERT fails with a "duplicate" error (the QSO already exists on
///   QRZ but we don't have its logid), retry with `OPTION=REPLACE` to
///   auto-match the existing record and adopt its logid.
///
/// Returns a [`SyncOutcome`] on success. Returns a human-readable error
/// string on either upload failure or local-store write failure; callers
/// surface it as the gRPC `sync_error` field.
pub(crate) async fn sync_single_qso(
    client: &dyn QrzLogbookApi,
    store: &dyn LogbookStore,
    qso: &QsoRecord,
    book_owner: Option<&str>,
) -> Result<SyncOutcome, String> {
    sync_single_qso_with_metadata_patch(client, store, qso, book_owner).await
}

pub(crate) async fn sync_single_qso_metadata_only(
    client: &dyn QrzLogbookApi,
    store: &dyn LogbookStore,
    qso: &QsoRecord,
    book_owner: Option<&str>,
) -> Result<SyncOutcome, String> {
    sync_single_qso_with_metadata_patch(client, store, qso, book_owner).await
}

async fn sync_single_qso_with_metadata_patch(
    client: &dyn QrzLogbookApi,
    store: &dyn LogbookStore,
    qso: &QsoRecord,
    book_owner: Option<&str>,
) -> Result<SyncOutcome, String> {
    let current = store
        .get_qso(&qso.local_id)
        .await
        .map_err(|err| format!("QRZ sync failed to read current local QSO: {err}"))?
        .ok_or_else(|| {
            format!(
                "QRZ sync skipped because local QSO '{}' no longer exists.",
                qso.local_id
            )
        })?;
    if current.deleted_at.is_some() {
        return Err(format!(
            "QRZ sync skipped because local QSO '{}' is deleted.",
            current.local_id
        ));
    }
    let expected_updated_at = current.updated_at;
    let (result, was_duplicate_replace) = upload_single_qso(client, &current, book_owner).await?;

    let patched = store
        .update_qrz_sync_metadata(
            &current.local_id,
            expected_updated_at.as_ref(),
            result.logid.as_str(),
        )
        .await
        .map_err(|err| format!("QRZ upload succeeded but local metadata update failed: {err}"))?
        .ok_or_else(|| {
            format!(
                "QRZ upload succeeded but local QSO '{}' no longer exists.",
                current.local_id
            )
        })?;

    Ok(SyncOutcome {
        qso: patched,
        was_duplicate_replace,
    })
}

async fn upload_single_qso(
    client: &dyn QrzLogbookApi,
    qso: &QsoRecord,
    book_owner: Option<&str>,
) -> Result<(QrzUploadResult, bool), String> {
    let existing_logid = qso.qrz_logid.clone().filter(|s| !s.is_empty());

    let result = match existing_logid.as_deref() {
        Some(logid) => client.replace_qso(logid, qso, book_owner).await,
        None => client.upload_qso(qso, book_owner).await,
    };

    // When a plain INSERT fails because QRZ already has a matching QSO
    // (e.g. uploaded via the QRZ web UI), retry with OPTION=REPLACE so we
    // can adopt the remote logid and stop re-attempting on every sync.
    let (result, was_duplicate_replace) = match result {
        Err(QrzLogbookError::ApiError(ref reason))
            if existing_logid.is_none() && is_duplicate_error(reason) =>
        {
            eprintln!(
                "[sync] INSERT for {} got duplicate; retrying with OPTION=REPLACE",
                qso.worked_callsign
            );
            let r = client
                .upload_qso_with_replace(qso, book_owner)
                .await
                .map_err(|err| format!("QRZ upload failed (REPLACE retry): {err}"));
            (r, true)
        }
        other => (
            other.map_err(|err| format!("QRZ upload failed: {err}")),
            false,
        ),
    };
    let result = result?;

    Ok((result, was_duplicate_replace))
}

/// Check whether a QRZ API error reason indicates a duplicate QSO.
fn is_duplicate_error(reason: &str) -> bool {
    let lower = reason.to_ascii_lowercase();
    lower.contains("duplicate")
}

// ---------------------------------------------------------------------------
// Phase 2.5 — Push queued remote deletes
// ---------------------------------------------------------------------------

/// Push every locally soft-deleted QSO whose `pending_remote_delete` flag is
/// set to QRZ. On success (including QRZ "not found"), clears `qrz_logid`
/// and `pending_remote_delete` while keeping `deleted_at` set.
async fn push_pending_remote_deletes(
    client: &dyn QrzLogbookApi,
    store: &dyn LogbookStore,
    progress_tx: &mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
    counters: &mut SyncCounters,
) {
    let candidates = match store
        .list_qsos(&QsoListQuery {
            deleted_filter: DeletedRecordsFilter::DeletedOnly,
            ..QsoListQuery::default()
        })
        .await
    {
        Ok(qsos) => qsos,
        Err(err) => {
            eprintln!("[sync] Failed to load deleted QSOs for remote-delete pass: {err}");
            counters
                .errors
                .push(format!("List deleted QSOs failed: {err}"));
            return;
        }
    };

    let pending: Vec<&QsoRecord> = candidates
        .iter()
        .filter(|q| {
            q.pending_remote_delete && !q.qrz_logid.as_deref().unwrap_or("").trim().is_empty()
        })
        .collect();

    if pending.is_empty() {
        return;
    }

    send_progress(
        progress_tx,
        &format!("Pushing {} queued remote delete(s)…", pending.len()),
        counters.downloaded,
        counters.uploaded,
        counters.conflicts,
    )
    .await;

    for qso in pending {
        let logid = qso.qrz_logid.as_deref().unwrap_or("").to_string();
        match client.delete_qso(&logid).await {
            Ok(()) => {
                match complete_remote_delete(store, qso).await {
                    Ok(true) => counters.remote_deletes_pushed += 1,
                    Ok(false) => counters.errors.push(format!(
                        "Remote delete cleared for {} could not be applied because the local QSO kept changing.",
                        qso.worked_callsign
                    )),
                    Err(err) => counters.errors.push(format!(
                        "Remote delete cleared for {} failed locally: {err}",
                        qso.worked_callsign
                    )),
                }
            }
            Err(err) => {
                eprintln!(
                    "[sync] Remote delete failed for logid {logid} ({}): {err}",
                    qso.worked_callsign
                );
                counters.errors.push(format!(
                    "Remote delete failed for {}: {err}",
                    qso.worked_callsign
                ));
            }
        }
    }
}

async fn complete_remote_delete(
    store: &dyn LogbookStore,
    deleted_snapshot: &QsoRecord,
) -> Result<bool, qsoripper_core::storage::StorageError> {
    for _ in 0..3 {
        let Some(current) = store.get_qso(&deleted_snapshot.local_id).await? else {
            return Ok(false);
        };
        if current.qrz_logid != deleted_snapshot.qrz_logid {
            return Ok(true);
        }

        let mut cleared = current.clone();
        cleared.qrz_logid = None;
        cleared.pending_remote_delete = false;
        if cleared.deleted_at.is_none() {
            cleared.sync_status = SyncStatus::LocalOnly as i32;
        }
        if store.update_qso_if_unchanged(&current, &cleared).await? {
            return Ok(true);
        }
    }

    Ok(false)
}

// ---------------------------------------------------------------------------
// Phase 3 — Persist updated metadata
// ---------------------------------------------------------------------------

async fn update_metadata(
    store: &dyn LogbookStore,
    prev_metadata: &SyncMetadata,
    status_result: Result<QrzLogbookStatus, QrzLogbookError>,
    counters: &mut SyncCounters,
) {
    let now = time::OffsetDateTime::now_utc();

    // Prefer the authoritative remote STATUS result (already fetched once
    // before the upload phase to avoid double-billing the QRZ API). If that
    // STATUS call failed (auth blip, transient network error) we fall back
    // to estimating from local counts so metadata at least stays
    // approximately correct.
    let (qrz_qso_count, qrz_logbook_owner) = match status_result {
        Ok(status) => {
            let owner = if status.owner.is_empty() {
                prev_metadata.qrz_logbook_owner.clone()
            } else {
                Some(status.owner)
            };
            (status.qso_count, owner)
        }
        Err(err) => {
            eprintln!("[sync] STATUS call failed during metadata refresh: {err}");
            counters
                .errors
                .push(format!("STATUS refresh failed: {err}"));
            let fallback_count = match store.qso_counts().await {
                Ok(counts) => counts
                    .local_qso_count
                    .saturating_sub(counts.pending_upload_count),
                Err(err) => {
                    eprintln!("[sync] Failed to refresh local QSO counts: {err}");
                    prev_metadata.qrz_qso_count
                }
            };
            (fallback_count, prev_metadata.qrz_logbook_owner.clone())
        }
    };

    let updated = SyncMetadata {
        qrz_qso_count,
        last_sync: Some(prost_types::Timestamp {
            seconds: now.unix_timestamp(),
            nanos: 0,
        }),
        qrz_logbook_owner,
        lotw_last_sync: prev_metadata.lotw_last_sync,
        lotw_last_qsl: prev_metadata.lotw_last_qsl.clone(),
    };

    if let Err(err) = store.upsert_sync_metadata(&updated).await {
        eprintln!("[sync] Failed to update sync metadata: {err}");
        counters
            .errors
            .push(format!("Failed to update sync metadata: {err}"));
    }
}

// ---------------------------------------------------------------------------
// Download merge helpers
// ---------------------------------------------------------------------------

/// Merge a remote QSO with a matched local QSO based on sync status.
async fn merge_with_local(
    local: &QsoRecord,
    remote_logid: Option<&str>,
    remote: &QsoRecord,
    store: &dyn LogbookStore,
    conflict_policy: ConflictPolicy,
    counters: &mut SyncCounters,
) {
    match SyncStatus::try_from(local.sync_status) {
        Ok(SyncStatus::Synced) => {
            // Remote wins — update local with remote data.
            let mut updated = merge_remote_authoritative(local, remote);
            updated.local_id.clone_from(&local.local_id);
            updated.sync_status = SyncStatus::Synced as i32;
            updated.qrz_logid = remote_logid
                .map(String::from)
                .or_else(|| local.qrz_logid.clone());

            match store.update_qso_if_unchanged(local, &updated).await {
                Ok(true) => counters.downloaded += 1,
                Ok(false) => {}
                Err(err) => {
                    eprintln!(
                        "[sync] Failed to update QSO {} from remote: {err}",
                        local.local_id
                    );
                    counters.errors.push(format!(
                        "Update failed for {}: {err}",
                        local.worked_callsign
                    ));
                }
            }
        }
        Ok(SyncStatus::LocalOnly) => {
            // The QSO was already on QRZ (e.g. uploaded outside QsoRipper).
            // Link it and mark synced to avoid a duplicate upload in Phase 2,
            // while filling blank local enrichment from QRZ's ADIF copy.
            let mut linked = local.clone();
            fill_missing_remote_enrichment(&mut linked, remote);
            linked.sync_status = SyncStatus::Synced as i32;
            if let Some(logid) = remote_logid {
                linked.qrz_logid = Some(logid.to_string());
            }
            match store.update_qso_if_unchanged(local, &linked).await {
                Ok(true) => counters.downloaded += 1,
                Ok(false) => {}
                Err(err) => {
                    eprintln!(
                        "[sync] Failed to link local-only QSO {} to remote: {err}",
                        local.local_id
                    );
                    counters
                        .errors
                        .push(format!("Link failed for {}: {err}", local.worked_callsign));
                }
            }
        }
        Ok(SyncStatus::Modified) => {
            resolve_modified_conflict(local, remote, store, conflict_policy, counters).await;
        }
        _ => {
            // Conflict or unknown — leave untouched.
        }
    }
}

fn fill_missing_remote_enrichment(target: &mut QsoRecord, remote: &QsoRecord) {
    copy_string_if_missing(
        &mut target.worked_operator_callsign,
        remote.worked_operator_callsign.as_deref(),
    );
    copy_string_if_missing(
        &mut target.worked_operator_name,
        remote.worked_operator_name.as_deref(),
    );
    copy_string_if_missing(&mut target.worked_grid, remote.worked_grid.as_deref());
    copy_string_if_missing(&mut target.worked_country, remote.worked_country.as_deref());
    copy_u32_if_missing(&mut target.worked_dxcc, remote.worked_dxcc);
    copy_string_if_missing(&mut target.worked_state, remote.worked_state.as_deref());
    copy_u32_if_missing(&mut target.worked_cq_zone, remote.worked_cq_zone);
    copy_u32_if_missing(&mut target.worked_itu_zone, remote.worked_itu_zone);
    copy_string_if_missing(&mut target.worked_county, remote.worked_county.as_deref());
    copy_string_if_missing(&mut target.worked_iota, remote.worked_iota.as_deref());
    copy_string_if_missing(
        &mut target.worked_continent,
        remote.worked_continent.as_deref(),
    );
    copy_string_if_missing(
        &mut target.worked_arrl_section,
        remote.worked_arrl_section.as_deref(),
    );
    copy_string_if_missing(&mut target.skcc, remote.skcc.as_deref());
    copy_f64_if_missing(&mut target.worked_latitude, remote.worked_latitude);
    copy_f64_if_missing(&mut target.worked_longitude, remote.worked_longitude);
    copy_f64_if_missing(
        &mut target.worked_altitude_meters,
        remote.worked_altitude_meters,
    );
    copy_string_if_missing(
        &mut target.worked_gridsquare_ext,
        remote.worked_gridsquare_ext.as_deref(),
    );

    for (key, value) in &remote.extra_fields {
        if !value.trim().is_empty()
            && target
                .extra_fields
                .get(key)
                .is_none_or(|existing| existing.trim().is_empty())
        {
            target.extra_fields.insert(key.clone(), value.clone());
        }
    }
}

fn copy_string_if_missing(target: &mut Option<String>, remote: Option<&str>) {
    if target
        .as_deref()
        .is_none_or(|value| value.trim().is_empty())
    {
        if let Some(value) = remote.map(str::trim).filter(|value| !value.is_empty()) {
            *target = Some(value.to_owned());
        }
    }
}

fn copy_u32_if_missing(target: &mut Option<u32>, remote: Option<u32>) {
    if target.is_none_or(|value| value == 0) {
        if let Some(value) = remote.filter(|value| *value > 0) {
            *target = Some(value);
        }
    }
}

fn copy_f64_if_missing(target: &mut Option<f64>, remote: Option<f64>) {
    if target.is_none_or(|value| !value.is_finite()) {
        if let Some(value) = remote.filter(|value| value.is_finite()) {
            *target = Some(value);
        }
    }
}

/// Resolve a modified-vs-remote conflict according to the configured policy.
///
/// - **`LastWriteWins`**: compare `updated_at` timestamps. If remote is newer
///   (or timestamps tie), overwrite local and set `SYNCED`. If local is newer,
///   leave it as `MODIFIED` so the upload phase pushes it.
/// - **`FlagForReview`**: set local to `CONFLICT` without overwriting.
async fn resolve_modified_conflict(
    local: &QsoRecord,
    remote: &QsoRecord,
    store: &dyn LogbookStore,
    policy: ConflictPolicy,
    counters: &mut SyncCounters,
) {
    match policy {
        ConflictPolicy::LastWriteWins => {
            let local_ts = local.updated_at.as_ref().map_or(0, |ts| ts.seconds);
            let remote_ts = remote.updated_at.as_ref().map_or(0, |ts| ts.seconds);

            if remote_ts >= local_ts {
                // Remote is newer (or equal) — overwrite local.
                let mut updated = merge_remote_authoritative(local, remote);
                updated.local_id.clone_from(&local.local_id);
                updated.sync_status = SyncStatus::Synced as i32;
                updated.qrz_logid = extract_qrz_logid(remote).or_else(|| local.qrz_logid.clone());
                match store.update_qso_if_unchanged(local, &updated).await {
                    Ok(true) => counters.downloaded += 1,
                    Ok(false) => {}
                    Err(err) => {
                        eprintln!(
                            "[sync] Failed to overwrite QSO {} with newer remote: {err}",
                            local.local_id
                        );
                        counters.errors.push(format!(
                            "Overwrite failed for {}: {err}",
                            local.worked_callsign,
                        ));
                    }
                }
            } else {
                // Local is newer — keep MODIFIED; upload phase will push it.
                eprintln!(
                    "[sync] Local QSO {} is newer than remote; keeping MODIFIED for upload",
                    local.local_id
                );
            }
        }
        ConflictPolicy::Unspecified | ConflictPolicy::FlagForReview => {
            let mut conflicted = local.clone();
            conflicted.sync_status = SyncStatus::Conflict as i32;
            match store.update_qso_if_unchanged(local, &conflicted).await {
                Ok(true) => counters.conflicts += 1,
                Ok(false) => {}
                Err(err) => {
                    eprintln!(
                        "[sync] Failed to mark QSO {} as conflict: {err}",
                        local.local_id
                    );
                    counters.errors.push(format!(
                        "Conflict mark failed for {}: {err}",
                        local.worked_callsign
                    ));
                }
            }
        }
    }
}

fn merge_remote_authoritative(local: &QsoRecord, remote: &QsoRecord) -> QsoRecord {
    let mut merged = local.clone();
    if merged.merge(remote.encode_to_vec().as_slice()).is_err() {
        return local.clone();
    }
    merged.created_at.clone_from(&local.created_at);
    merged.updated_at.clone_from(&local.updated_at);
    merged.deleted_at.clone_from(&local.deleted_at);
    merged.pending_remote_delete = local.pending_remote_delete;
    merged
}

// ---------------------------------------------------------------------------
// Matching helpers
// ---------------------------------------------------------------------------

fn build_local_indexes(local_qsos: &[QsoRecord]) -> (LogidIndex, FuzzyIndex) {
    let mut by_qrz_logid: LogidIndex = HashMap::new();
    let mut by_key: FuzzyIndex = HashMap::new();

    for qso in local_qsos {
        if let Some(logid) = qso.qrz_logid.as_deref() {
            if !logid.is_empty() {
                by_qrz_logid.insert(logid.to_string(), qso.clone());
            }
        }
        let key = (qso.worked_callsign.to_ascii_uppercase(), qso.band, qso.mode);
        by_key.entry(key).or_default().push(qso.clone());
    }

    (by_qrz_logid, by_key)
}

/// Split rows fetched with `DeletedRecordsFilter::All` into:
/// 1. Active rows usable for matching (returned as a Vec).
/// 2. A `HashSet<String>` of QRZ logids belonging to soft-deleted rows.
///    Phase 1 download uses this set to skip resurrecting trashed QSOs.
fn partition_local_for_sync(local_qsos: &[QsoRecord]) -> (Vec<QsoRecord>, HashSet<String>) {
    let mut active: Vec<QsoRecord> = Vec::with_capacity(local_qsos.len());
    let mut deleted_logids: HashSet<String> = HashSet::new();
    for qso in local_qsos {
        if qso.deleted_at.is_some() {
            if let Some(logid) = qso.qrz_logid.as_deref() {
                if !logid.is_empty() {
                    deleted_logids.insert(logid.to_string());
                }
            }
        } else {
            active.push(qso.clone());
        }
    }
    (active, deleted_logids)
}

/// Extract the QRZ logbook record ID from a QSO.
///
/// Checks the dedicated `qrz_logid` field first (populated by the ADIF
/// mapper), then falls back to the historical `extra_fields` aliases for
/// records that were persisted before the mapper recognised the field.
fn extract_qrz_logid(qso: &QsoRecord) -> Option<String> {
    if let Some(logid) = qso.qrz_logid.as_deref() {
        if !logid.is_empty() {
            return Some(logid.to_string());
        }
    }
    for key in QRZ_LOGID_EXTRA_FIELDS {
        if let Some(value) = qso.extra_fields.get(*key) {
            if !value.is_empty() {
                return Some(value.clone());
            }
        }
    }
    None
}

/// Find a local QSO matching by worked callsign, band, mode, and timestamp
/// (within [`TIMESTAMP_TOLERANCE_SECONDS`]).
fn fuzzy_match(remote: &QsoRecord, by_key: &FuzzyIndex) -> Option<QsoRecord> {
    let key = (
        remote.worked_callsign.to_ascii_uppercase(),
        remote.band,
        remote.mode,
    );
    let candidates = by_key.get(&key)?;
    let remote_ts = remote.utc_timestamp.as_ref()?.seconds;

    candidates
        .iter()
        .find(|local| {
            local
                .utc_timestamp
                .as_ref()
                .is_some_and(|ts| (ts.seconds - remote_ts).abs() <= TIMESTAMP_TOLERANCE_SECONDS)
        })
        .cloned()
}

// ---------------------------------------------------------------------------
// Progress reporting helpers
// ---------------------------------------------------------------------------

async fn send_progress(
    tx: &mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
    action: &str,
    downloaded: u32,
    uploaded: u32,
    conflicts: u32,
) {
    drop(
        tx.send(Ok(SyncWithQrzResponse {
            total_records: downloaded + uploaded,
            processed_records: downloaded + uploaded,
            uploaded_records: uploaded,
            downloaded_records: downloaded,
            conflict_records: conflicts,
            current_action: Some(action.to_string()),
            complete: false,
            error: None,
            remote_deletes_pushed: 0,
            deletes_skipped_remote: 0,
            duplicate_replaces: 0,
        }))
        .await,
    );
}

async fn send_complete(
    tx: &mpsc::Sender<Result<SyncWithQrzResponse, Status>>,
    counters: &SyncCounters,
    error: Option<String>,
) {
    drop(
        tx.send(Ok(SyncWithQrzResponse {
            total_records: counters.downloaded + counters.uploaded,
            processed_records: counters.downloaded + counters.uploaded,
            uploaded_records: counters.uploaded,
            downloaded_records: counters.downloaded,
            conflict_records: counters.conflicts,
            current_action: Some("Sync complete".to_string()),
            complete: true,
            error,
            remote_deletes_pushed: counters.remote_deletes_pushed,
            deletes_skipped_remote: counters.deletes_skipped_remote,
            duplicate_replaces: counters.duplicate_replaces,
        }))
        .await,
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::indexing_slicing
)]
mod tests {
    use std::sync::{Arc, Mutex};

    use prost_types::Timestamp;
    use qsoripper_core::domain::qso::QsoRecordBuilder;
    use qsoripper_core::proto::qsoripper::domain::{
        Band, ConflictPolicy, Mode, QslStatus, QsoCompletion, QsoRecord, RstReport,
        StationSnapshot, SyncStatus,
    };
    use qsoripper_core::qrz_logbook::{QrzLogbookError, QrzLogbookStatus, QrzUploadResult};
    use qsoripper_core::storage::{
        DeletedRecordsFilter, LogbookCounts, LogbookStore, QsoHistoryPage, QsoListQuery,
        SyncMetadata,
    };
    use qsoripper_storage_memory::MemoryStorage;
    use tokio::sync::mpsc;

    use super::{
        execute_sync, purge_deleted_qsos, sync_single_qso, sync_single_qso_metadata_only,
        QrzLogbookApi,
    };

    struct EditAfterReadStorage {
        inner: MemoryStorage,
        injected: Mutex<bool>,
    }

    impl EditAfterReadStorage {
        fn new() -> Self {
            Self {
                inner: MemoryStorage::new(),
                injected: Mutex::new(false),
            }
        }
    }

    #[tonic::async_trait]
    impl LogbookStore for EditAfterReadStorage {
        async fn insert_qso(
            &self,
            qso: &QsoRecord,
        ) -> Result<(), qsoripper_core::storage::StorageError> {
            self.inner.insert_qso(qso).await
        }

        async fn update_qso(
            &self,
            qso: &QsoRecord,
        ) -> Result<bool, qsoripper_core::storage::StorageError> {
            self.inner.update_qso(qso).await
        }

        async fn update_qrz_sync_metadata(
            &self,
            local_id: &str,
            expected_updated_at: Option<&Timestamp>,
            qrz_logid: &str,
        ) -> Result<Option<QsoRecord>, qsoripper_core::storage::StorageError> {
            self.inner
                .update_qrz_sync_metadata(local_id, expected_updated_at, qrz_logid)
                .await
        }

        async fn delete_qso(
            &self,
            local_id: &str,
        ) -> Result<bool, qsoripper_core::storage::StorageError> {
            self.inner.delete_qso(local_id).await
        }

        async fn soft_delete_qso(
            &self,
            local_id: &str,
            deleted_at_ms: i64,
            pending_remote_delete: bool,
        ) -> Result<bool, qsoripper_core::storage::StorageError> {
            self.inner
                .soft_delete_qso(local_id, deleted_at_ms, pending_remote_delete)
                .await
        }

        async fn restore_qso(
            &self,
            local_id: &str,
        ) -> Result<bool, qsoripper_core::storage::StorageError> {
            self.inner.restore_qso(local_id).await
        }

        async fn get_qso(
            &self,
            local_id: &str,
        ) -> Result<Option<QsoRecord>, qsoripper_core::storage::StorageError> {
            let record = self.inner.get_qso(local_id).await?;
            let should_inject = {
                let mut injected = self.injected.lock().unwrap();
                if *injected {
                    false
                } else {
                    *injected = true;
                    true
                }
            };
            if should_inject {
                if let Some(mut edited) = record.clone() {
                    edited.notes = Some("operator edit after QRZ readback".into());
                    edited.updated_at = Some(Timestamp {
                        seconds: edited
                            .updated_at
                            .as_ref()
                            .map_or(1, |stamp| stamp.seconds + 1),
                        nanos: 0,
                    });
                    edited.sync_status = SyncStatus::Modified as i32;
                    self.inner.update_qso(&edited).await?;
                }
            }
            Ok(record)
        }

        async fn list_qsos(
            &self,
            query: &QsoListQuery,
        ) -> Result<Vec<QsoRecord>, qsoripper_core::storage::StorageError> {
            self.inner.list_qsos(query).await
        }

        async fn list_qso_history(
            &self,
            worked_callsign: &str,
            limit: u32,
        ) -> Result<QsoHistoryPage, qsoripper_core::storage::StorageError> {
            self.inner.list_qso_history(worked_callsign, limit).await
        }

        async fn qso_counts(&self) -> Result<LogbookCounts, qsoripper_core::storage::StorageError> {
            self.inner.qso_counts().await
        }

        async fn get_sync_metadata(
            &self,
        ) -> Result<SyncMetadata, qsoripper_core::storage::StorageError> {
            self.inner.get_sync_metadata().await
        }

        async fn upsert_sync_metadata(
            &self,
            metadata: &SyncMetadata,
        ) -> Result<(), qsoripper_core::storage::StorageError> {
            self.inner.upsert_sync_metadata(metadata).await
        }

        async fn purge_deleted_qsos(
            &self,
            local_ids: &[String],
            older_than_ms: Option<i64>,
        ) -> Result<u32, qsoripper_core::storage::StorageError> {
            self.inner
                .purge_deleted_qsos(local_ids, older_than_ms)
                .await
        }
    }

    // -- Mock API -----------------------------------------------------------

    struct MockQrzApi {
        fetch_results: Mutex<Vec<Result<Vec<QsoRecord>, QrzLogbookError>>>,
        fetch_calls: Mutex<Vec<Option<String>>>,
        upload_results: Mutex<Vec<Result<QrzUploadResult, QrzLogbookError>>>,
        upload_calls: Mutex<Vec<(QsoRecord, Option<String>)>>,
        upload_replace_results: Mutex<Vec<Result<QrzUploadResult, QrzLogbookError>>>,
        upload_replace_calls: Mutex<Vec<(QsoRecord, Option<String>)>>,
        replace_calls: Mutex<Vec<(String, String)>>, // (logid, local_id)
        replace_results: Mutex<Vec<Result<QrzUploadResult, QrzLogbookError>>>,
        status_result: Mutex<Option<Result<QrzLogbookStatus, QrzLogbookError>>>,
        delete_calls: Mutex<Vec<String>>,
        delete_results: Mutex<Vec<Result<(), QrzLogbookError>>>,
        fetch_edit: Mutex<Option<(Arc<MemoryStorage>, String)>>,
        delete_restore: Mutex<Option<(Arc<MemoryStorage>, String)>>,
    }

    impl MockQrzApi {
        fn new(
            fetch: Result<Vec<QsoRecord>, QrzLogbookError>,
            uploads: Vec<Result<QrzUploadResult, QrzLogbookError>>,
        ) -> Self {
            Self {
                fetch_results: Mutex::new(vec![fetch]),
                fetch_calls: Mutex::new(Vec::new()),
                upload_results: Mutex::new(uploads),
                upload_calls: Mutex::new(Vec::new()),
                upload_replace_results: Mutex::new(Vec::new()),
                upload_replace_calls: Mutex::new(Vec::new()),
                replace_calls: Mutex::new(Vec::new()),
                replace_results: Mutex::new(Vec::new()),
                status_result: Mutex::new(Some(Ok(QrzLogbookStatus {
                    owner: String::new(),
                    qso_count: 0,
                }))),
                delete_calls: Mutex::new(Vec::new()),
                delete_results: Mutex::new(Vec::new()),
                fetch_edit: Mutex::new(None),
                delete_restore: Mutex::new(None),
            }
        }

        fn with_status(self, status: Result<QrzLogbookStatus, QrzLogbookError>) -> Self {
            *self.status_result.lock().unwrap() = Some(status);
            self
        }

        fn with_fetch_results(self, fetches: Vec<Result<Vec<QsoRecord>, QrzLogbookError>>) -> Self {
            *self.fetch_results.lock().unwrap() = fetches;
            self
        }

        fn with_fetch_edit(self, store: Arc<MemoryStorage>, local_id: String) -> Self {
            *self.fetch_edit.lock().unwrap() = Some((store, local_id));
            self
        }

        fn with_delete_restore(self, store: Arc<MemoryStorage>, local_id: String) -> Self {
            *self.delete_restore.lock().unwrap() = Some((store, local_id));
            self
        }

        fn fetch_calls(&self) -> Vec<Option<String>> {
            self.fetch_calls.lock().unwrap().clone()
        }
    }

    #[tonic::async_trait]
    impl QrzLogbookApi for MockQrzApi {
        async fn fetch_qsos(&self, since: Option<&str>) -> Result<Vec<QsoRecord>, QrzLogbookError> {
            self.fetch_calls
                .lock()
                .unwrap()
                .push(since.map(String::from));

            let fetch_edit = self.fetch_edit.lock().unwrap().take();
            if let Some((store, local_id)) = fetch_edit {
                if let Some(mut edited) = store.get_qso(&local_id).await.unwrap() {
                    edited.notes = Some("operator edit during fetch".into());
                    edited.updated_at = Some(Timestamp {
                        seconds: edited
                            .updated_at
                            .as_ref()
                            .map_or(1, |stamp| stamp.seconds + 1),
                        nanos: 0,
                    });
                    edited.sync_status = SyncStatus::Modified as i32;
                    store.update_qso(&edited).await.unwrap();
                }
            }

            let mut fetch_results = self.fetch_results.lock().unwrap();
            if fetch_results.is_empty() {
                Ok(Vec::new())
            } else {
                fetch_results.remove(0)
            }
        }

        async fn upload_qso(
            &self,
            qso: &QsoRecord,
            book_owner: Option<&str>,
        ) -> Result<QrzUploadResult, QrzLogbookError> {
            self.upload_calls
                .lock()
                .unwrap()
                .push((qso.clone(), book_owner.map(str::to_owned)));
            let mut results = self.upload_results.lock().unwrap();
            if results.is_empty() {
                Err(QrzLogbookError::ApiError(
                    "no more mock upload results".into(),
                ))
            } else {
                results.remove(0)
            }
        }

        async fn upload_qso_with_replace(
            &self,
            qso: &QsoRecord,
            book_owner: Option<&str>,
        ) -> Result<QrzUploadResult, QrzLogbookError> {
            self.upload_replace_calls
                .lock()
                .unwrap()
                .push((qso.clone(), book_owner.map(str::to_owned)));
            let mut results = self.upload_replace_results.lock().unwrap();
            if results.is_empty() {
                // Default: succeed with a synthetic logid.
                Ok(QrzUploadResult {
                    logid: "REPLACE_LOGID".into(),
                })
            } else {
                results.remove(0)
            }
        }

        async fn replace_qso(
            &self,
            logid: &str,
            qso: &QsoRecord,
            _book_owner: Option<&str>,
        ) -> Result<QrzUploadResult, QrzLogbookError> {
            self.replace_calls
                .lock()
                .unwrap()
                .push((logid.to_string(), qso.local_id.clone()));
            let mut results = self.replace_results.lock().unwrap();
            if results.is_empty() {
                // Default: succeed and echo the logid back.
                Ok(QrzUploadResult {
                    logid: logid.to_string(),
                })
            } else {
                results.remove(0)
            }
        }

        async fn fetch_status(&self) -> Result<QrzLogbookStatus, QrzLogbookError> {
            self.status_result
                .lock()
                .unwrap()
                .take()
                .unwrap_or_else(|| {
                    Ok(QrzLogbookStatus {
                        owner: String::new(),
                        qso_count: 0,
                    })
                })
        }

        async fn delete_qso(&self, logid: &str) -> Result<(), QrzLogbookError> {
            self.delete_calls.lock().unwrap().push(logid.to_string());
            let delete_restore = self.delete_restore.lock().unwrap().take();
            if let Some((store, local_id)) = delete_restore {
                store.restore_qso(&local_id).await.unwrap();
            }
            let mut results = self.delete_results.lock().unwrap();
            if results.is_empty() {
                Ok(())
            } else {
                results.remove(0)
            }
        }
    }

    /// Capturing mock that records what `since` value was passed to `fetch_qsos`.
    struct CapturingApi {
        since: Arc<Mutex<Option<String>>>,
    }

    #[tonic::async_trait]
    impl QrzLogbookApi for CapturingApi {
        async fn fetch_qsos(&self, since: Option<&str>) -> Result<Vec<QsoRecord>, QrzLogbookError> {
            *self.since.lock().unwrap() = since.map(String::from);
            Ok(vec![])
        }

        async fn upload_qso(
            &self,
            _qso: &QsoRecord,
            _book_owner: Option<&str>,
        ) -> Result<QrzUploadResult, QrzLogbookError> {
            Ok(QrzUploadResult {
                logid: "ignored".into(),
            })
        }

        async fn upload_qso_with_replace(
            &self,
            _qso: &QsoRecord,
            _book_owner: Option<&str>,
        ) -> Result<QrzUploadResult, QrzLogbookError> {
            Ok(QrzUploadResult {
                logid: "ignored".into(),
            })
        }

        async fn replace_qso(
            &self,
            logid: &str,
            _qso: &QsoRecord,
            _book_owner: Option<&str>,
        ) -> Result<QrzUploadResult, QrzLogbookError> {
            Ok(QrzUploadResult {
                logid: logid.to_string(),
            })
        }

        async fn fetch_status(&self) -> Result<QrzLogbookStatus, QrzLogbookError> {
            Ok(QrzLogbookStatus {
                owner: String::new(),
                qso_count: 0,
            })
        }

        async fn delete_qso(&self, _logid: &str) -> Result<(), QrzLogbookError> {
            Ok(())
        }
    }

    // -- Helpers ------------------------------------------------------------

    fn make_qso(station: &str, worked: &str, band: Band, mode: Mode, ts_seconds: i64) -> QsoRecord {
        QsoRecordBuilder::new(station, worked)
            .band(band)
            .mode(mode)
            .timestamp(Timestamp {
                seconds: ts_seconds,
                nanos: 0,
            })
            .build()
    }

    /// Collect the final streamed response.
    async fn collect_final(
        mut rx: mpsc::Receiver<Result<super::SyncWithQrzResponse, tonic::Status>>,
    ) -> super::SyncWithQrzResponse {
        let mut last = None;
        while let Some(Ok(msg)) = rx.recv().await {
            last = Some(msg);
        }
        last.expect("expected at least one streamed response")
    }

    // -- Test cases ---------------------------------------------------------

    #[tokio::test]
    async fn download_inserts_new_qsos_from_remote() {
        let store = MemoryStorage::new();

        let remote1 = {
            let mut q = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ001".into());
            q
        };
        let remote2 = {
            let mut q = make_qso("W1AW", "JA1ZZZ", Band::Band40m, Mode::Cw, 1_700_000_100);
            q.qrz_logid = Some("QRZ002".into());
            q
        };
        let api = MockQrzApi::new(Ok(vec![remote1, remote2]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 2);
        assert_eq!(final_msg.uploaded_records, 0);
        assert!(final_msg.error.is_none());

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 2);
        for qso in &all {
            assert_eq!(qso.sync_status, SyncStatus::Synced as i32);
            assert!(qso.qrz_logid.is_some());
        }
    }

    #[tokio::test]
    async fn upload_sends_local_only_qsos() {
        let store = MemoryStorage::new();

        let local1 = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
        let local2 = make_qso("W1AW", "DL1ABC", Band::Band40m, Mode::Ssb, 1_700_000_100);
        store.insert_qso(&local1).await.unwrap();
        store.insert_qso(&local2).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![
                Ok(QrzUploadResult {
                    logid: "QRZ100".into(),
                }),
                Ok(QrzUploadResult {
                    logid: "QRZ101".into(),
                }),
            ],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.uploaded_records, 2);
        assert_eq!(final_msg.downloaded_records, 0);
        assert!(final_msg.error.is_none());

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        for qso in &all {
            assert_eq!(qso.sync_status, SyncStatus::Synced as i32);
            assert!(qso.qrz_logid.is_some());
        }
    }

    #[tokio::test]
    async fn modified_qso_with_logid_uses_replace_not_insert() {
        // Regression for the bug where Modified QSOs were uploaded via INSERT,
        // producing a brand-new row on QRZ every sync instead of updating the
        // existing one. The fix routes Modified + existing logid through the
        // REPLACE (ACTION=INSERT&OPTION=REPLACE,LOGID:...) path.
        let store = MemoryStorage::new();
        let mut q = make_qso("W1AW", "K7EDIT", Band::Band20m, Mode::Ft8, 1_700_000_000);
        q.qrz_logid = Some("QRZ-EXISTING".into());
        q.sync_status = SyncStatus::Modified as i32;
        store.insert_qso(&q).await.unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(
            final_msg.uploaded_records, 1,
            "should have uploaded the modified QSO"
        );
        assert!(final_msg.error.is_none(), "error: {:?}", final_msg.error);

        let replace_calls = api.replace_calls.lock().unwrap().clone();
        assert_eq!(
            replace_calls.len(),
            1,
            "modified QSO must use REPLACE, got replace_calls={replace_calls:?}"
        );
        assert_eq!(
            replace_calls[0].0, "QRZ-EXISTING",
            "REPLACE must be called with the existing QRZ logid"
        );

        let upload_results_remaining = api.upload_results.lock().unwrap().len();
        assert_eq!(
            upload_results_remaining, 0,
            "INSERT path should never have been exercised for a modified QSO with a logid"
        );

        // Local row should remain Synced with the same logid intact.
        let saved = store.get_qso(&q.local_id).await.unwrap().unwrap();
        assert_eq!(saved.sync_status, SyncStatus::Synced as i32);
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-EXISTING"));
    }

    // ---- sync_single_qso (per-operation sync_to_qrz=true path) -------------

    #[tokio::test]
    async fn sync_single_qso_inserts_when_no_logid_and_marks_synced() {
        let store = MemoryStorage::new();
        let mut q = make_qso("W1AW", "K7NEW", Band::Band20m, Mode::Ft8, 1_700_000_000);
        q.qrz_logid = None;
        q.sync_status = SyncStatus::LocalOnly as i32;
        store.insert_qso(&q).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-NEW".into(),
            })],
        );

        let outcome = sync_single_qso(&api, &store, &q, None).await.expect("ok");
        assert!(!outcome.was_duplicate_replace);
        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-NEW"));
        assert_eq!(outcome.qso.sync_status, SyncStatus::Synced as i32);

        let saved = store.get_qso(&q.local_id).await.unwrap().unwrap();
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-NEW"));
        assert_eq!(saved.sync_status, SyncStatus::Synced as i32);

        // INSERT was used; REPLACE was not.
        assert!(api.replace_calls.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn sync_single_qso_replaces_when_logid_present_preserving_id() {
        let store = MemoryStorage::new();
        let mut q = make_qso("W1AW", "K7EDIT", Band::Band20m, Mode::Ft8, 1_700_000_000);
        q.qrz_logid = Some("QRZ-EXISTING".into());
        q.sync_status = SyncStatus::Modified as i32;
        store.insert_qso(&q).await.unwrap();

        // No upload_results queued — only REPLACE should be called and it
        // defaults to echoing the logid back.
        let api = MockQrzApi::new(Ok(vec![]), vec![]);

        let outcome = sync_single_qso(&api, &store, &q, None).await.expect("ok");
        assert!(!outcome.was_duplicate_replace);
        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-EXISTING"));
        assert_eq!(outcome.qso.sync_status, SyncStatus::Synced as i32);

        let replace_calls = api.replace_calls.lock().unwrap().clone();
        assert_eq!(replace_calls.len(), 1, "must REPLACE not INSERT");
        assert_eq!(replace_calls[0].0, "QRZ-EXISTING");

        let saved = store.get_qso(&q.local_id).await.unwrap().unwrap();
        assert_eq!(saved.sync_status, SyncStatus::Synced as i32);
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-EXISTING"));
    }

    #[tokio::test]
    async fn sync_single_qso_returns_error_when_upload_fails_and_leaves_local_alone() {
        let store = MemoryStorage::new();
        let mut q = make_qso("W1AW", "K7FAIL", Band::Band20m, Mode::Ft8, 1_700_000_000);
        q.qrz_logid = None;
        q.sync_status = SyncStatus::LocalOnly as i32;
        store.insert_qso(&q).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Err(QrzLogbookError::ApiError("boom".into()))],
        );

        let err = sync_single_qso(&api, &store, &q, None)
            .await
            .expect_err("should return error");
        assert!(err.contains("QRZ upload failed"), "actual error: {err}");

        // Local row must remain LocalOnly with no logid — caller (handler)
        // surfaces the failure as sync_error and the next bulk sync will retry.
        let saved = store.get_qso(&q.local_id).await.unwrap().unwrap();
        assert_eq!(saved.sync_status, SyncStatus::LocalOnly as i32);
        assert!(saved.qrz_logid.is_none());
    }

    #[tokio::test]
    async fn sync_single_qso_uses_current_logid_before_upload() {
        let store = MemoryStorage::new();
        let mut stale_snapshot =
            make_qso("W1AW", "K7SYNC", Band::Band20m, Mode::Ft8, 1_700_000_000);
        stale_snapshot.qrz_logid = None;
        stale_snapshot.sync_status = SyncStatus::LocalOnly as i32;
        stale_snapshot.updated_at = Some(Timestamp {
            seconds: 1_700_000_010,
            nanos: 0,
        });
        store.insert_qso(&stale_snapshot).await.unwrap();

        let mut current = stale_snapshot.clone();
        current.qrz_logid = Some("QRZ-SYNC".into());
        current.sync_status = SyncStatus::Modified as i32;
        current.updated_at = Some(Timestamp {
            seconds: 1_700_000_011,
            nanos: 0,
        });
        store.update_qso(&current).await.unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);
        let outcome = sync_single_qso(&api, &store, &stale_snapshot, None)
            .await
            .expect("sync should use current row");

        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-SYNC"));
        assert_eq!(api.upload_calls.lock().unwrap().len(), 0);
        let replace_calls = api.replace_calls.lock().unwrap().clone();
        assert_eq!(replace_calls.len(), 1);
        assert_eq!(replace_calls[0].0, "QRZ-SYNC");
    }

    #[tokio::test]
    async fn sync_single_qso_does_not_overwrite_edit_between_read_and_write() {
        let store = EditAfterReadStorage::new();
        let mut imported = make_qso("W1AW", "K7SYNC2", Band::Band20m, Mode::Ft8, 1_700_000_000);
        imported.qrz_logid = None;
        imported.sync_status = SyncStatus::LocalOnly as i32;
        imported.updated_at = Some(Timestamp {
            seconds: 1_700_000_010,
            nanos: 0,
        });
        store.insert_qso(&imported).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-SYNC2".into(),
            })],
        );

        let outcome = sync_single_qso(&api, &store, &imported, None)
            .await
            .expect("sync should preserve concurrent edit");

        assert_eq!(
            outcome.qso.notes.as_deref(),
            Some("operator edit after QRZ readback")
        );
        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-SYNC2"));
        assert_eq!(outcome.qso.sync_status, SyncStatus::Modified as i32);
    }

    #[tokio::test]
    async fn sync_single_qso_metadata_only_preserves_newer_local_edits() {
        let store = MemoryStorage::new();
        let mut imported = make_qso("W1AW", "K7RACE", Band::Band20m, Mode::Ft8, 1_700_000_000);
        imported.qrz_logid = None;
        imported.sync_status = SyncStatus::LocalOnly as i32;
        imported.updated_at = Some(Timestamp {
            seconds: 1_700_000_010,
            nanos: 0,
        });
        store.insert_qso(&imported).await.unwrap();

        let mut edited = imported.clone();
        edited.notes = Some("operator corrected note".into());
        edited.updated_at = Some(Timestamp {
            seconds: imported.updated_at.as_ref().unwrap().seconds + 1,
            nanos: 0,
        });
        edited.sync_status = SyncStatus::Modified as i32;
        store.update_qso(&edited).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-RACE".into(),
            })],
        );

        let outcome = sync_single_qso_metadata_only(&api, &store, &imported, None)
            .await
            .expect("metadata-only sync should succeed");
        assert_eq!(
            outcome.qso.notes.as_deref(),
            Some("operator corrected note")
        );
        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-RACE"));
        assert_eq!(outcome.qso.sync_status, SyncStatus::Synced as i32);

        let saved = store.get_qso(&imported.local_id).await.unwrap().unwrap();
        assert_eq!(saved.notes.as_deref(), Some("operator corrected note"));
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-RACE"));
        assert_eq!(saved.sync_status, SyncStatus::Synced as i32);
    }

    #[tokio::test]
    async fn sync_single_qso_metadata_only_uses_current_logid_before_upload() {
        let store = MemoryStorage::new();
        let mut stale_snapshot =
            make_qso("W1AW", "K7CURRENT", Band::Band20m, Mode::Ft8, 1_700_000_000);
        stale_snapshot.qrz_logid = None;
        stale_snapshot.sync_status = SyncStatus::LocalOnly as i32;
        stale_snapshot.updated_at = Some(Timestamp {
            seconds: 1_700_000_010,
            nanos: 0,
        });
        store.insert_qso(&stale_snapshot).await.unwrap();

        let mut current = stale_snapshot.clone();
        current.qrz_logid = Some("QRZ-CURRENT".into());
        current.sync_status = SyncStatus::Modified as i32;
        current.updated_at = Some(Timestamp {
            seconds: 1_700_000_011,
            nanos: 0,
        });
        store.update_qso(&current).await.unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);
        let outcome = sync_single_qso_metadata_only(&api, &store, &stale_snapshot, None)
            .await
            .expect("metadata-only sync should use current row");

        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-CURRENT"));
        assert_eq!(api.upload_calls.lock().unwrap().len(), 0);
        let replace_calls = api.replace_calls.lock().unwrap().clone();
        assert_eq!(replace_calls.len(), 1);
        assert_eq!(replace_calls[0].0, "QRZ-CURRENT");
    }

    #[tokio::test]
    async fn sync_single_qso_metadata_only_does_not_overwrite_edit_between_read_and_write() {
        let store = EditAfterReadStorage::new();
        let mut imported = make_qso("W1AW", "K7CAS", Band::Band20m, Mode::Ft8, 1_700_000_000);
        imported.qrz_logid = None;
        imported.sync_status = SyncStatus::LocalOnly as i32;
        imported.updated_at = Some(Timestamp {
            seconds: 1_700_000_010,
            nanos: 0,
        });
        store.insert_qso(&imported).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-CAS".into(),
            })],
        );

        let outcome = sync_single_qso_metadata_only(&api, &store, &imported, None)
            .await
            .expect("metadata-only sync should preserve concurrent edit");

        assert_eq!(
            outcome.qso.notes.as_deref(),
            Some("operator edit after QRZ readback")
        );
        assert_eq!(outcome.qso.qrz_logid.as_deref(), Some("QRZ-CAS"));
        assert_eq!(outcome.qso.sync_status, SyncStatus::Modified as i32);

        let saved = store.get_qso(&imported.local_id).await.unwrap().unwrap();
        assert_eq!(
            saved.notes.as_deref(),
            Some("operator edit after QRZ readback")
        );
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-CAS"));
        assert_eq!(saved.sync_status, SyncStatus::Modified as i32);
    }

    #[tokio::test]
    async fn sync_single_qso_skips_deleted_current_row_without_uploading() {
        let store = MemoryStorage::new();
        let mut imported = make_qso("W1AW", "K7DEL1", Band::Band20m, Mode::Ft8, 1_700_000_000);
        imported.sync_status = SyncStatus::Modified as i32;
        store.insert_qso(&imported).await.unwrap();
        store
            .soft_delete_qso(&imported.local_id, 1_700_000_500_000, false)
            .await
            .unwrap();
        let api = MockQrzApi::new(
            Ok(Vec::new()),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-DEL1".into(),
            })],
        );

        let err = sync_single_qso(&api, &store, &imported, None)
            .await
            .expect_err("deleted current row should skip upload");

        assert!(err.contains("deleted"), "{err}");
        assert!(api.upload_calls.lock().unwrap().is_empty());
        let stored = store.get_qso(&imported.local_id).await.unwrap().unwrap();
        assert!(stored.deleted_at.is_some());
        assert_eq!(stored.qrz_logid, None);
        assert!(!stored.pending_remote_delete);
    }

    #[tokio::test]
    async fn execute_sync_passes_book_owner_from_status_to_upload_payload() {
        // Regression for issue #337: QSOs whose station_callsign is the
        // operator's previous call (e.g. KB7QOP) must be uploaded with the
        // current logbook owner callsign so QRZ accepts them.
        let store = MemoryStorage::new();
        let mut q = make_qso("KB7QOP", "K7ABC", Band::Band20m, Mode::Cw, 1_700_000_000);
        q.qrz_logid = None;
        q.sync_status = SyncStatus::LocalOnly as i32;
        store.insert_qso(&q).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-NEW".into(),
            })],
        )
        .with_status(Ok(QrzLogbookStatus {
            owner: "AE7XI".into(),
            qso_count: 0,
        }));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.uploaded_records, 1, "upload must succeed");
        assert!(final_msg.error.is_none(), "error: {:?}", final_msg.error);

        let upload_calls = api.upload_calls.lock().unwrap().clone();
        assert_eq!(upload_calls.len(), 1, "exactly one INSERT expected");
        assert_eq!(
            upload_calls[0].1.as_deref(),
            Some("AE7XI"),
            "fresh STATUS owner must reach upload_qso as book_owner"
        );
        // Local storage retains the historical station_callsign — only the
        // upload payload is rewritten.
        let saved = store.get_qso(&q.local_id).await.unwrap().unwrap();
        assert_eq!(saved.station_callsign, "KB7QOP");
        assert_eq!(saved.sync_status, SyncStatus::Synced as i32);
    }

    #[tokio::test]
    async fn full_sync_pushes_modified_qrz_linked_local_correction_before_stale_remote_can_conflict(
    ) {
        let store = MemoryStorage::new();
        let mut local = make_qso("K7TEST", "W1AW/0", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("stale-qrz-logid".to_string());
        store.insert_qso(&local).await.expect("insert local qso");

        let mut stale_remote =
            make_qso("K7TEST", "W1AW/1", Band::Band20m, Mode::Ft8, 1_700_000_000);
        stale_remote.qrz_logid = Some("stale-qrz-logid".to_string());
        let api = MockQrzApi::new(Ok(vec![stale_remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::FlagForReview, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.uploaded_records, 1);
        assert_eq!(final_msg.conflict_records, 0);
        assert_eq!(api.replace_calls.lock().unwrap().len(), 1);

        let saved = store
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("list qsos");
        assert_eq!(saved.len(), 1);
        assert_eq!(saved[0].worked_callsign, "W1AW/0");
        assert_eq!(saved[0].sync_status, SyncStatus::Synced as i32);
        assert_eq!(saved[0].qrz_logid.as_deref(), Some("stale-qrz-logid"));
    }

    #[tokio::test]
    async fn full_sync_pushes_all_modified_qrz_linked_rows_so_stale_qrz_entries_are_resolved() {
        let store = MemoryStorage::new();
        let mut first = make_qso("K7TEST", "W1AW/0", Band::Band20m, Mode::Ft8, 1_700_000_000);
        first.sync_status = SyncStatus::Modified as i32;
        first.qrz_logid = Some("qrz-1".to_string());
        let mut second = make_qso("K7TEST", "K7ABC", Band::Band40m, Mode::Cw, 1_700_000_060);
        second.sync_status = SyncStatus::Modified as i32;
        second.qrz_logid = Some("qrz-2".to_string());
        store.insert_qso(&first).await.expect("insert first qso");
        store.insert_qso(&second).await.expect("insert second qso");

        let mut stale_first = make_qso("K7TEST", "W1AW/1", Band::Band20m, Mode::Ft8, 1_700_000_000);
        stale_first.qrz_logid = Some("qrz-1".to_string());
        let mut stale_second = make_qso("K7TEST", "K7OLD", Band::Band40m, Mode::Cw, 1_700_000_060);
        stale_second.qrz_logid = Some("qrz-2".to_string());
        let api = MockQrzApi::new(Ok(vec![stale_first, stale_second]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::FlagForReview, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.uploaded_records, 2);
        assert_eq!(final_msg.conflict_records, 0);
        assert_eq!(api.replace_calls.lock().unwrap().len(), 2);

        let saved = store
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("list qsos");
        assert_eq!(saved.len(), 2);
        assert!(saved.iter().any(|qso| qso.worked_callsign == "W1AW/0"));
        assert!(saved.iter().any(|qso| qso.worked_callsign == "K7ABC"));
        assert!(saved
            .iter()
            .all(|qso| qso.sync_status == SyncStatus::Synced as i32));
        assert!(!saved.iter().any(|qso| qso.worked_callsign == "W1AW/1"));
        assert!(!saved.iter().any(|qso| qso.worked_callsign == "K7OLD"));
    }

    #[tokio::test]
    async fn execute_sync_falls_back_to_cached_owner_when_status_fails() {
        let store = MemoryStorage::new();
        store
            .upsert_sync_metadata(&SyncMetadata {
                qrz_qso_count: 0,
                last_sync: None,
                qrz_logbook_owner: Some("AE7XI".into()),
                ..SyncMetadata::default()
            })
            .await
            .unwrap();
        let mut q = make_qso("KB7QOP", "K7ABC", Band::Band20m, Mode::Cw, 1_700_000_000);
        q.qrz_logid = None;
        q.sync_status = SyncStatus::LocalOnly as i32;
        store.insert_qso(&q).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-NEW".into(),
            })],
        )
        .with_status(Err(QrzLogbookError::ApiError("transient".into())));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let _final_msg = collect_final(rx).await;

        let upload_calls = api.upload_calls.lock().unwrap().clone();
        assert_eq!(upload_calls.len(), 1);
        assert_eq!(
            upload_calls[0].1.as_deref(),
            Some("AE7XI"),
            "cached owner must be used when STATUS fails"
        );
    }

    #[tokio::test]
    async fn modified_qso_without_logid_falls_back_to_insert() {
        // Edge case: a QSO is flagged Modified but somehow has no logid
        // (legacy data, migrations). We must still upload it, just via INSERT.
        let store = MemoryStorage::new();
        let mut q = make_qso("W1AW", "K7LEGACY", Band::Band40m, Mode::Cw, 1_700_010_000);
        q.qrz_logid = None;
        q.sync_status = SyncStatus::Modified as i32;
        store.insert_qso(&q).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ-NEW".into(),
            })],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.uploaded_records, 1);
        let replace_calls = api.replace_calls.lock().unwrap().clone();
        assert!(
            replace_calls.is_empty(),
            "should NOT have called REPLACE without a logid: {replace_calls:?}"
        );
        let saved = store.get_qso(&q.local_id).await.unwrap().unwrap();
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-NEW"));
    }

    #[tokio::test]
    async fn update_metadata_uses_status_call_result() {
        // Regression: Phase 3 used to derive qrz_qso_count from local counts
        // and just copy the previous owner. Now it must call STATUS and use
        // the remote-authoritative values.
        let store = MemoryStorage::new();
        let api = MockQrzApi::new(Ok(vec![]), vec![]).with_status(Ok(QrzLogbookStatus {
            owner: "NEW_OWNER".to_string(),
            qso_count: 4242,
        }));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        let _ = collect_final(rx).await;

        let meta = store.get_sync_metadata().await.unwrap();
        assert_eq!(meta.qrz_qso_count, 4242);
        assert_eq!(meta.qrz_logbook_owner.as_deref(), Some("NEW_OWNER"));
    }

    #[tokio::test]
    async fn update_metadata_falls_back_when_status_call_fails() {
        // If STATUS fails transiently we still want to finish sync with
        // a best-effort count from the local store, rather than overwriting
        // metadata with zero/garbage.
        let store = MemoryStorage::new();

        // Pre-seed some metadata so we can prove owner is preserved.
        store
            .upsert_sync_metadata(&SyncMetadata {
                qrz_qso_count: 0,
                last_sync: None,
                qrz_logbook_owner: Some("ORIG_OWNER".into()),
                ..SyncMetadata::default()
            })
            .await
            .unwrap();

        // And pre-seed two already-synced local QSOs so count has something
        // to derive from.
        for cs in ["KA", "KB"] {
            let mut q = make_qso("W1AW", cs, Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.sync_status = SyncStatus::Synced as i32;
            store.insert_qso(&q).await.unwrap();
        }

        let api = MockQrzApi::new(Ok(vec![]), vec![])
            .with_status(Err(QrzLogbookError::ApiError("boom".into())));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        let _ = collect_final(rx).await;

        let meta = store.get_sync_metadata().await.unwrap();
        assert_eq!(meta.qrz_logbook_owner.as_deref(), Some("ORIG_OWNER"));
        assert_eq!(meta.qrz_qso_count, 2);
    }

    #[tokio::test]
    async fn mixed_sync_downloads_and_uploads() {
        let store = MemoryStorage::new();

        // One local QSO pending upload.
        let local = make_qso("W1AW", "K7LOCAL", Band::Band10m, Mode::Ft8, 1_700_000_500);
        store.insert_qso(&local).await.unwrap();

        // One remote QSO not yet in local store.
        let remote = {
            let mut q = make_qso("W1AW", "VK3REMOTE", Band::Band20m, Mode::Cw, 1_700_000_600);
            q.qrz_logid = Some("QRZ200".into());
            q
        };

        let api = MockQrzApi::new(
            Ok(vec![remote]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ201".into(),
            })],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1);
        assert_eq!(final_msg.uploaded_records, 1);
        assert!(final_msg.error.is_none());

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn upload_errors_do_not_abort_sync() {
        let store = MemoryStorage::new();

        let local1 = make_qso("W1AW", "K7FAIL", Band::Band20m, Mode::Ft8, 1_700_000_000);
        let local2 = make_qso("W1AW", "K7PASS", Band::Band40m, Mode::Ssb, 1_700_000_100);
        store.insert_qso(&local1).await.unwrap();
        store.insert_qso(&local2).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![
                Err(QrzLogbookError::ApiError("rejected".into())),
                Ok(QrzUploadResult {
                    logid: "QRZ300".into(),
                }),
            ],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        // One upload succeeded, one failed.
        assert_eq!(final_msg.uploaded_records, 1);
        assert!(final_msg.error.is_some());
    }

    #[tokio::test]
    async fn metadata_persisted_after_sync() {
        let store = MemoryStorage::new();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        drop(collect_final(rx).await);

        let metadata = store.get_sync_metadata().await.unwrap();
        assert!(
            metadata.last_sync.is_some(),
            "last_sync should be set after sync"
        );
    }

    #[tokio::test]
    async fn metadata_qrz_count_refreshed_after_download() {
        let store = MemoryStorage::new();

        // Download two remote QSOs — they arrive as Synced.
        let remote1 = {
            let mut q = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ001".into());
            q
        };
        let remote2 = {
            let mut q = make_qso("W1AW", "JA1ZZZ", Band::Band40m, Mode::Cw, 1_700_000_100);
            q.qrz_logid = Some("QRZ002".into());
            q
        };
        let api =
            MockQrzApi::new(Ok(vec![remote1, remote2]), vec![]).with_status(Ok(QrzLogbookStatus {
                owner: String::new(),
                qso_count: 2,
            }));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        drop(collect_final(rx).await);

        let metadata = store.get_sync_metadata().await.unwrap();
        assert!(metadata.last_sync.is_some(), "last_sync should be set");
        assert_eq!(
            metadata.qrz_qso_count, 2,
            "qrz_qso_count should come from STATUS"
        );
    }

    #[tokio::test]
    async fn metadata_last_sync_not_advanced_when_download_fails() {
        let store = MemoryStorage::new();

        let previous = SyncMetadata {
            qrz_qso_count: 42,
            qrz_logbook_owner: Some("W1AW".into()),
            last_sync: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..SyncMetadata::default()
        };
        store.upsert_sync_metadata(&previous).await.unwrap();

        let api = MockQrzApi::new(
            Err(QrzLogbookError::ApiError("download failed".into())),
            vec![],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        drop(collect_final(rx).await);

        let after = store.get_sync_metadata().await.unwrap();
        assert_eq!(
            after.last_sync.as_ref().map(|ts| ts.seconds),
            previous.last_sync.as_ref().map(|ts| ts.seconds),
            "last_sync must not move forward when download fails"
        );
        assert_eq!(
            after.qrz_qso_count, previous.qrz_qso_count,
            "qrz_qso_count must remain unchanged when download fails"
        );
    }

    #[tokio::test]
    async fn fuzzy_match_links_by_callsign_time_band_mode() {
        let store = MemoryStorage::new();

        // Insert a local QSO.
        let local = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
        store.insert_qso(&local).await.unwrap();

        // Remote QSO: same callsign+band+mode, timestamp differs by 30 seconds.
        // No qrz_logid on local, so the match must be fuzzy.
        let remote = {
            let mut q = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_030);
            q.qrz_logid = Some("QRZ400".into());
            // Give it a fresh local_id to make it clearly different from the local one.
            q.local_id = "remote-temp-id".into();
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        // The remote matched the local, so it counts as a download update,
        // not a new insert. No new QSOs created.
        assert_eq!(final_msg.downloaded_records, 1);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1, "fuzzy match should not insert a duplicate");
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
    }

    #[tokio::test]
    async fn synced_local_preserves_all_local_fields_when_remote_omits_them() {
        let store = MemoryStorage::new();

        let mut local = make_qso("W1AW", "K7RST", Band::Band20m, Mode::Cw, 1_700_000_000);
        local.sync_status = SyncStatus::Synced as i32;
        local.qrz_logid = Some("QRZ-RST".into());
        local.rst_sent = Some(RstReport {
            readability: Some(5),
            strength: Some(9),
            tone: Some(9),
            raw: "599".into(),
        });
        local.rst_received = Some(RstReport {
            readability: Some(5),
            strength: Some(7),
            tone: Some(9),
            raw: "579".into(),
        });
        local.frequency_hz = Some(14_025_125);
        local.submode = Some("CW".into());
        local.station_snapshot = Some(StationSnapshot {
            station_callsign: "K7TEST".into(),
            grid: Some("CN87".into()),
            ..Default::default()
        });
        local.tx_power = Some("100".into());
        local.qsl_sent_status = QslStatus::Yes as i32;
        local.lotw_received = Some(true);
        local.contest_id = Some("ARRL-FIELD-DAY".into());
        local.exchange_received = Some("1D WWA".into());
        local.comment = Some("operator correction".into());
        local.worked_latitude = Some(47.61);
        local.frequency_rx_hz = Some(14_030_000);
        local.band_rx = Band::Band20m as i32;
        local.qso_complete = QsoCompletion::Yes as i32;
        local.cw_decode_rx_wpm = Some(24);
        local.cw_decode_transcript = Some("CQ TEST".into());
        local
            .extra_fields
            .insert("APP_LOCAL_ONLY".into(), "preserve-me".into());
        local.created_at = Some(Timestamp {
            seconds: 1_699_999_940,
            nanos: 0,
        });
        local.updated_at = Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        });
        store.insert_qso(&local).await.unwrap();

        let mut remote = make_qso("W1AW", "K7RST", Band::Band20m, Mode::Cw, 1_700_000_000);
        remote.qrz_logid = Some("QRZ-RST".into());
        remote.notes = Some("Fresh from QRZ".into());

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);
        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::FlagForReview, &tx).await;
        drop(tx);
        drop(collect_final(rx).await);

        let saved = store
            .list_qsos(&QsoListQuery::default())
            .await
            .unwrap()
            .pop()
            .expect("saved QSO");
        assert_eq!(saved.notes.as_deref(), Some("Fresh from QRZ"));
        assert_eq!(
            saved.rst_sent.as_ref().map(|rst| rst.raw.as_str()),
            Some("599")
        );
        assert_eq!(
            saved.rst_received.as_ref().map(|rst| rst.raw.as_str()),
            Some("579")
        );
        assert_eq!(saved.frequency_hz, Some(14_025_125));
        assert_eq!(saved.submode.as_deref(), Some("CW"));
        assert_eq!(
            saved
                .station_snapshot
                .as_ref()
                .and_then(|snapshot| snapshot.grid.as_deref()),
            Some("CN87")
        );
        assert_eq!(saved.tx_power.as_deref(), Some("100"));
        assert_eq!(saved.qsl_sent_status, QslStatus::Yes as i32);
        assert_eq!(saved.lotw_received, Some(true));
        assert_eq!(saved.contest_id.as_deref(), Some("ARRL-FIELD-DAY"));
        assert_eq!(saved.exchange_received.as_deref(), Some("1D WWA"));
        assert_eq!(saved.comment.as_deref(), Some("operator correction"));
        assert_eq!(saved.worked_latitude, Some(47.61));
        assert_eq!(saved.frequency_rx_hz, Some(14_030_000));
        assert_eq!(saved.band_rx, Band::Band20m as i32);
        assert_eq!(saved.qso_complete, QsoCompletion::Yes as i32);
        assert_eq!(saved.cw_decode_rx_wpm, Some(24));
        assert_eq!(saved.cw_decode_transcript.as_deref(), Some("CQ TEST"));
        assert_eq!(
            saved.extra_fields.get("APP_LOCAL_ONLY").map(String::as_str),
            Some("preserve-me")
        );
        assert_eq!(saved.created_at, local.created_at);
        assert_eq!(saved.updated_at, local.updated_at);
    }

    #[tokio::test]
    async fn download_preserves_operator_edit_made_while_fetch_is_in_flight() {
        let store = Arc::new(MemoryStorage::new());
        let mut local = make_qso("W1AW", "K7RACE", Band::Band20m, Mode::Cw, 1_700_000_000);
        local.sync_status = SyncStatus::Synced as i32;
        local.qrz_logid = Some("FETCH-RACE".into());
        local.updated_at = Some(Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        });
        store.insert_qso(&local).await.unwrap();

        let mut remote = make_qso("W1AW", "K7RACE", Band::Band20m, Mode::Cw, 1_700_000_000);
        remote.qrz_logid = Some("FETCH-RACE".into());
        remote.notes = Some("remote value".into());
        let api = MockQrzApi::new(Ok(vec![remote]), vec![])
            .with_fetch_edit(Arc::clone(&store), local.local_id.clone());

        let (tx, rx) = mpsc::channel(16);
        execute_sync(
            &api,
            store.as_ref(),
            true,
            ConflictPolicy::FlagForReview,
            &tx,
        )
        .await;
        drop(tx);
        drop(collect_final(rx).await);

        let saved = store
            .get_qso(&local.local_id)
            .await
            .unwrap()
            .expect("saved QSO");
        assert_eq!(saved.notes.as_deref(), Some("operator edit during fetch"));
    }

    #[tokio::test]
    async fn modified_local_with_qrz_logid_uploaded_before_remote_can_conflict() {
        let store = MemoryStorage::new();

        // Local QSO with MODIFIED status.
        let mut local = make_qso("W1AW", "K7MOD", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("QRZ500".into());
        store.insert_qso(&local).await.unwrap();

        // Remote QSO with same logid.
        let remote = {
            let mut q = make_qso("W1AW", "K7MOD", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ500".into());
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::FlagForReview, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.conflict_records, 0);
        assert_eq!(final_msg.uploaded_records, 1);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
    }

    #[tokio::test]
    async fn local_only_linked_to_remote_not_reuploaded() {
        let store = MemoryStorage::new();

        // Local QSO with LOCAL_ONLY status that happens to match a remote QSO.
        let local = make_qso("W1AW", "K7LINK", Band::Band20m, Mode::Ft8, 1_700_000_000);
        assert_eq!(local.sync_status, SyncStatus::LocalOnly as i32);
        store.insert_qso(&local).await.unwrap();

        // Remote QSO matches by callsign+band+mode+timestamp.
        let remote = {
            let mut q = make_qso("W1AW", "K7LINK", Band::Band20m, Mode::Ft8, 1_700_000_010);
            q.qrz_logid = Some("QRZ600".into());
            q.local_id = "remote-temp".into();
            q
        };

        // No upload results needed — the QSO should be linked, not uploaded.
        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1);
        assert_eq!(
            final_msg.uploaded_records, 0,
            "linked QSO should not be re-uploaded"
        );
        assert!(final_msg.error.is_none());

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
        assert_eq!(all[0].qrz_logid.as_deref(), Some("QRZ600"));
    }

    #[tokio::test]
    async fn local_only_linked_to_remote_fills_missing_qrz_enrichment_without_overwriting_local_values(
    ) {
        let store = MemoryStorage::new();

        let mut local = make_qso("W1AW", "K7LINK", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.worked_country = Some("Existing Country".into());
        local.contest_id = Some("ARRL-FIELD-DAY".into());
        local.exchange_received = Some("1D WWA".into());
        store.insert_qso(&local).await.unwrap();

        let mut remote = make_qso("W1AW", "K7LINK", Band::Band20m, Mode::Ft8, 1_700_000_010);
        remote.qrz_logid = Some("QRZ-ENRICHED".into());
        remote.worked_grid = Some("FN31".into());
        remote.worked_country = Some("United States".into());
        remote.worked_dxcc = Some(291);
        remote.worked_state = Some("CT".into());
        remote.worked_county = Some("Hartford".into());
        remote.worked_cq_zone = Some(5);
        remote.worked_itu_zone = Some(8);
        remote.worked_continent = Some("NA".into());
        remote.worked_latitude = Some(41.0);
        remote.worked_longitude = Some(-72.7);
        remote
            .extra_fields
            .insert("QRZ_ENRICHED_ONLY".into(), "yes".into());
        remote.contest_id = Some("QRZ-CONTEST".into());
        remote.exchange_received = Some("REMOTE".into());

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1);
        assert_eq!(final_msg.uploaded_records, 0);
        assert!(final_msg.error.is_none());

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        let saved = &all[0];
        assert_eq!(saved.sync_status, SyncStatus::Synced as i32);
        assert_eq!(saved.qrz_logid.as_deref(), Some("QRZ-ENRICHED"));
        assert_eq!(saved.worked_grid.as_deref(), Some("FN31"));
        assert_eq!(saved.worked_country.as_deref(), Some("Existing Country"));
        assert_eq!(saved.worked_dxcc, Some(291));
        assert_eq!(saved.worked_state.as_deref(), Some("CT"));
        assert_eq!(saved.worked_county.as_deref(), Some("Hartford"));
        assert_eq!(saved.worked_cq_zone, Some(5));
        assert_eq!(saved.worked_itu_zone, Some(8));
        assert_eq!(saved.worked_continent.as_deref(), Some("NA"));
        assert_eq!(saved.worked_latitude, Some(41.0));
        assert_eq!(saved.worked_longitude, Some(-72.7));
        assert_eq!(
            saved
                .extra_fields
                .get("QRZ_ENRICHED_ONLY")
                .map(String::as_str),
            Some("yes")
        );
        assert_eq!(saved.contest_id.as_deref(), Some("ARRL-FIELD-DAY"));
        assert_eq!(saved.exchange_received.as_deref(), Some("1D WWA"));
    }

    #[tokio::test]
    async fn incremental_sync_uses_last_sync_date() {
        let store = MemoryStorage::new();

        // Add at least one local record so incremental sync uses last_sync.
        let local = make_qso("W1AW", "K7LOCAL", Band::Band20m, Mode::Cw, 1_700_000_050);
        store.insert_qso(&local).await.unwrap();

        // Seed metadata with a previous sync timestamp.
        let metadata = SyncMetadata {
            last_sync: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..SyncMetadata::default()
        };
        store.upsert_sync_metadata(&metadata).await.unwrap();

        let since_capture: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let api = CapturingApi {
            since: since_capture.clone(),
        };

        let (tx, rx) = mpsc::channel(16);
        // full_sync = false → should use last_sync date.
        execute_sync(&api, &store, false, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        drop(collect_final(rx).await);

        let captured = since_capture.lock().unwrap().clone();
        assert_eq!(captured.as_deref(), Some("2023-11-14"));
    }

    #[tokio::test]
    async fn incremental_sync_with_empty_local_log_uses_full_fetch() {
        let store = MemoryStorage::new();

        // Metadata exists, but there are no local QSOs yet.
        let metadata = SyncMetadata {
            last_sync: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..SyncMetadata::default()
        };
        store.upsert_sync_metadata(&metadata).await.unwrap();

        let since_capture: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
        let api = CapturingApi {
            since: since_capture.clone(),
        };

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, false, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        drop(collect_final(rx).await);

        let captured = since_capture.lock().unwrap().clone();
        assert!(
            captured.is_none(),
            "empty local log should force a full remote fetch"
        );
    }

    #[tokio::test]
    async fn incremental_sync_retries_full_fetch_when_remote_count_exceeds_local_count() {
        let store = MemoryStorage::new();

        let local = make_qso("W1AW", "K7LOCAL", Band::Band20m, Mode::Cw, 1_700_000_050);
        store.insert_qso(&local).await.unwrap();

        let metadata = SyncMetadata {
            last_sync: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..SyncMetadata::default()
        };
        store.upsert_sync_metadata(&metadata).await.unwrap();

        let mut remote = make_qso("W1AW", "K7REMOTE", Band::Band40m, Mode::Cw, 1_700_000_100);
        remote.qrz_logid = Some("QRZREMOTE".into());
        let api = MockQrzApi::new(Ok(Vec::new()), Vec::new())
            .with_fetch_results(vec![Ok(Vec::new()), Ok(vec![remote])])
            .with_status(Ok(QrzLogbookStatus {
                owner: "W1AW".into(),
                qso_count: 2,
            }));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, false, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1);
        assert_eq!(
            api.fetch_calls(),
            vec![Some("2023-11-14".into()), None],
            "empty incremental fetch should be followed by a full fetch"
        );

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn full_sync_ignores_last_sync_date() {
        let store = MemoryStorage::new();

        let metadata = SyncMetadata {
            last_sync: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            ..SyncMetadata::default()
        };
        store.upsert_sync_metadata(&metadata).await.unwrap();

        let since_capture: Arc<Mutex<Option<String>>> =
            Arc::new(Mutex::new(Some("should-be-cleared".into())));
        let api = CapturingApi {
            since: since_capture.clone(),
        };

        let (tx, rx) = mpsc::channel(16);
        // full_sync = true → should NOT pass a since date.
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        drop(collect_final(rx).await);

        let captured = since_capture.lock().unwrap().clone();
        assert!(
            captured.is_none(),
            "full_sync should not pass a since parameter"
        );
    }

    #[tokio::test]
    async fn extract_logid_from_extra_fields_canonical_key() {
        let qso = QsoRecord {
            extra_fields: [("APP_QRZLOG_LOGID".into(), "EX123".into())]
                .into_iter()
                .collect(),
            ..QsoRecord::default()
        };

        let logid = super::extract_qrz_logid(&qso);
        assert_eq!(logid.as_deref(), Some("EX123"));
    }

    #[tokio::test]
    async fn extract_logid_from_extra_fields_legacy_alias() {
        let qso = QsoRecord {
            extra_fields: [("APP_QRZ_LOGID".into(), "LEGACY".into())]
                .into_iter()
                .collect(),
            ..QsoRecord::default()
        };

        let logid = super::extract_qrz_logid(&qso);
        assert_eq!(logid.as_deref(), Some("LEGACY"));
    }

    #[tokio::test]
    async fn extract_logid_prefers_dedicated_field() {
        let qso = QsoRecord {
            qrz_logid: Some("DIRECT".into()),
            extra_fields: [("APP_QRZLOG_LOGID".into(), "EXTRA".into())]
                .into_iter()
                .collect(),
            ..QsoRecord::default()
        };

        let logid = super::extract_qrz_logid(&qso);
        assert_eq!(logid.as_deref(), Some("DIRECT"));
    }

    // -- Conflict-policy tests -----------------------------------------------

    #[tokio::test]
    async fn last_write_wins_remote_newer_overwrites_local() {
        let store = MemoryStorage::new();

        // Local QSO: MODIFIED, updated_at = 1000.
        let mut local = make_qso("W1AW", "K7LWW", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("QRZ700".into());
        local.updated_at = Some(Timestamp {
            seconds: 1000,
            nanos: 0,
        });
        local.notes = Some("local edit".into());
        store.insert_qso(&local).await.unwrap();

        // Remote QSO: same logid, updated_at = 2000 (newer).
        let remote = {
            let mut q = make_qso("W1AW", "K7LWW", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ700".into());
            q.updated_at = Some(Timestamp {
                seconds: 2000,
                nanos: 0,
            });
            q.notes = Some("remote edit".into());
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1, "remote should overwrite");
        assert_eq!(final_msg.conflict_records, 0);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
        assert_eq!(all[0].notes.as_deref(), Some("remote edit"));
    }

    #[tokio::test]
    async fn last_write_wins_remote_newer_sets_missing_local_qrz_logid() {
        let store = MemoryStorage::new();

        // Local QSO: MODIFIED, missing qrz_logid, updated_at = 1000.
        let mut local = make_qso(
            "W1AW",
            "K7LWW-MISSING-LOGID",
            Band::Band20m,
            Mode::Ft8,
            1_700_000_000,
        );
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = None;
        local.updated_at = Some(Timestamp {
            seconds: 1000,
            nanos: 0,
        });
        local.notes = Some("local edit".into());
        store.insert_qso(&local).await.unwrap();

        // Remote QSO: same key, has qrz_logid, updated_at = 2000 (newer).
        let remote = {
            let mut q = make_qso(
                "W1AW",
                "K7LWW-MISSING-LOGID",
                Band::Band20m,
                Mode::Ft8,
                1_700_000_000,
            );
            q.qrz_logid = Some("QRZ700-MISSING".into());
            q.updated_at = Some(Timestamp {
                seconds: 2000,
                nanos: 0,
            });
            q.notes = Some("remote edit".into());
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1, "remote should overwrite");
        assert_eq!(final_msg.conflict_records, 0);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
        assert_eq!(
            all[0].qrz_logid.as_deref(),
            Some("QRZ700-MISSING"),
            "remote newer overwrite should preserve/set remote qrz_logid when local is missing",
        );
    }

    #[tokio::test]
    async fn last_write_wins_remote_newer_overwrites_local_qrz_logid_when_differs() {
        // Regression for #161: when remote wins under LastWriteWins, the
        // overwritten local row must carry the REMOTE qrz_logid, not the
        // stale local one. Otherwise the local row points at a phantom QRZ
        // record on the next sync.
        let store = MemoryStorage::new();

        let mut local = make_qso(
            "W1AW",
            "K7LWW-DIFFER",
            Band::Band20m,
            Mode::Ft8,
            1_700_000_000,
        );
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("LOG-LOCAL-OLD".into());
        local.updated_at = Some(Timestamp {
            seconds: 1000,
            nanos: 0,
        });
        local.notes = Some("local edit".into());
        store.insert_qso(&local).await.unwrap();

        // Remote is newer AND has a different logid (e.g., remote re-inserted
        // outside QsoRipper, or a logid migration on QRZ).
        let remote = {
            let mut q = make_qso(
                "W1AW",
                "K7LWW-DIFFER",
                Band::Band20m,
                Mode::Ft8,
                1_700_000_000,
            );
            q.qrz_logid = Some("LOG-REMOTE-NEW".into());
            q.updated_at = Some(Timestamp {
                seconds: 2000,
                nanos: 0,
            });
            q.notes = Some("remote authoritative".into());
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1);
        assert_eq!(final_msg.conflict_records, 0);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(
            all[0].qrz_logid.as_deref(),
            Some("LOG-REMOTE-NEW"),
            "remote-wins overwrite must adopt the remote qrz_logid (not keep the stale local one)",
        );
    }

    #[tokio::test]
    async fn last_write_wins_local_newer_keeps_modified() {
        let store = MemoryStorage::new();

        // Local QSO: MODIFIED, updated_at = 3000 (newer).
        let mut local = make_qso("W1AW", "K7LWW2", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("QRZ701".into());
        local.updated_at = Some(Timestamp {
            seconds: 3000,
            nanos: 0,
        });
        local.notes = Some("local newer".into());
        store.insert_qso(&local).await.unwrap();

        // Remote QSO: updated_at = 1000 (older).
        let remote = {
            let mut q = make_qso("W1AW", "K7LWW2", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ701".into());
            q.updated_at = Some(Timestamp {
                seconds: 1000,
                nanos: 0,
            });
            q.notes = Some("remote older".into());
            q
        };

        // Provide an upload result because the upload phase will push the local QSO.
        let api = MockQrzApi::new(
            Ok(vec![remote]),
            vec![Ok(QrzUploadResult {
                logid: "QRZ701".into(),
            })],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(
            final_msg.downloaded_records, 0,
            "local is newer, no download"
        );
        assert_eq!(final_msg.conflict_records, 0);
        // Upload phase should push the locally modified QSO.
        assert_eq!(final_msg.uploaded_records, 1);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        // After upload, status is SYNCED.
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
    }

    #[tokio::test]
    async fn flag_for_review_pushes_qrz_linked_local_edit_before_remote_can_conflict() {
        let store = MemoryStorage::new();

        // Local QSO: MODIFIED.
        let mut local = make_qso("W1AW", "K7FFR", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("QRZ800".into());
        local.updated_at = Some(Timestamp {
            seconds: 1000,
            nanos: 0,
        });
        local.notes = Some("local version".into());
        store.insert_qso(&local).await.unwrap();

        // Remote QSO: newer updated_at but with FLAG_FOR_REVIEW we don't compare timestamps.
        let remote = {
            let mut q = make_qso("W1AW", "K7FFR", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ800".into());
            q.updated_at = Some(Timestamp {
                seconds: 5000,
                nanos: 0,
            });
            q.notes = Some("remote version".into());
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::FlagForReview, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.conflict_records, 0);
        assert_eq!(final_msg.downloaded_records, 0);
        assert_eq!(final_msg.uploaded_records, 1);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
        // Local data preserved, not overwritten by remote.
        assert_eq!(all[0].notes.as_deref(), Some("local version"));
    }

    #[tokio::test]
    async fn last_write_wins_equal_timestamps_remote_wins() {
        let store = MemoryStorage::new();

        // Local QSO: MODIFIED, updated_at = 2000.
        let mut local = make_qso("W1AW", "K7EQ", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.sync_status = SyncStatus::Modified as i32;
        local.qrz_logid = Some("QRZ900".into());
        local.updated_at = Some(Timestamp {
            seconds: 2000,
            nanos: 0,
        });
        local.notes = Some("local tie".into());
        store.insert_qso(&local).await.unwrap();

        // Remote QSO: same updated_at → tie, remote wins.
        let remote = {
            let mut q = make_qso("W1AW", "K7EQ", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ900".into());
            q.updated_at = Some(Timestamp {
                seconds: 2000,
                nanos: 0,
            });
            q.notes = Some("remote tie".into());
            q
        };

        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1, "tie goes to remote");
        assert_eq!(final_msg.conflict_records, 0);

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].sync_status, SyncStatus::Synced as i32);
        assert_eq!(all[0].notes.as_deref(), Some("remote tie"));
    }

    #[tokio::test]
    async fn download_skips_ghost_records_with_empty_callsign() {
        let store = MemoryStorage::new();

        // One valid remote QSO and one ghost (empty callsign, no timestamp).
        let valid = {
            let mut q = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
            q.qrz_logid = Some("QRZ001".into());
            q
        };
        let ghost = QsoRecord::default();

        let api = MockQrzApi::new(Ok(vec![valid, ghost]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 1, "ghost should be skipped");

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(all.len(), 1, "only the valid QSO should be stored");
        assert_eq!(all[0].worked_callsign, "K7ABC");
    }

    #[tokio::test]
    async fn download_skips_records_with_callsign_but_no_timestamp() {
        let store = MemoryStorage::new();

        let no_ts = QsoRecord {
            worked_callsign: "W1AW".to_string(),
            utc_timestamp: None,
            ..Default::default()
        };

        let api = MockQrzApi::new(Ok(vec![no_ts]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(
            final_msg.downloaded_records, 0,
            "record without timestamp should be skipped"
        );

        let all = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert!(all.is_empty());
    }

    // -- Soft-delete sync integration ---------------------------------------

    /// Phase 1 must skip remote downloads whose qrz_logid matches a
    /// soft-deleted local row, otherwise trashed QSOs would be resurrected.
    #[tokio::test]
    async fn download_skips_remote_matching_soft_deleted_local() {
        let store = MemoryStorage::new();

        // Locally have one QSO that's already synced (qrz_logid set), then
        // soft-delete it. It should NOT be re-downloaded by Phase 1.
        let mut local = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.qrz_logid = Some("LOG-DELETED".into());
        local.sync_status = SyncStatus::Synced as i32;
        store.insert_qso(&local).await.unwrap();
        let soft_deleted = store
            .soft_delete_qso(&local.local_id, 1_700_000_500_000, false)
            .await
            .unwrap();
        assert!(soft_deleted);

        let mut remote = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
        remote.qrz_logid = Some("LOG-DELETED".into());
        let api = MockQrzApi::new(Ok(vec![remote]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert_eq!(final_msg.downloaded_records, 0);

        // The local row stays soft-deleted (deleted_at preserved).
        let after = store
            .list_qsos(&QsoListQuery {
                deleted_filter: DeletedRecordsFilter::All,
                ..QsoListQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert!(after[0].deleted_at.is_some());
    }

    /// Phase 2.5 must call QRZ delete for every soft-deleted row whose
    /// `pending_remote_delete` flag is set, then clear the flag + qrz_logid
    /// while keeping `deleted_at` set.
    #[tokio::test]
    async fn push_pending_remote_deletes_succeeds_and_clears_flags() {
        let store = MemoryStorage::new();

        let mut local = make_qso("W1AW", "JA1ZZZ", Band::Band40m, Mode::Cw, 1_700_000_100);
        local.qrz_logid = Some("LOG-PENDING".into());
        local.sync_status = SyncStatus::Synced as i32;
        store.insert_qso(&local).await.unwrap();
        store
            .soft_delete_qso(&local.local_id, 1_700_000_600_000, true)
            .await
            .unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete, "sync should complete cleanly");
        assert!(final_msg.error.is_none(), "no errors expected");

        // QRZ delete was invoked exactly once for the queued logid.
        let calls: Vec<String> = api.delete_calls.lock().unwrap().clone();
        assert_eq!(calls.as_slice(), &["LOG-PENDING".to_string()]);

        // Local row keeps tombstone, but qrz_logid + pending_remote_delete are cleared.
        let after = store
            .list_qsos(&QsoListQuery {
                deleted_filter: DeletedRecordsFilter::All,
                ..QsoListQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert!(after[0].deleted_at.is_some());
        assert!(!after[0].pending_remote_delete);
        assert!(after[0].qrz_logid.is_none() || after[0].qrz_logid.as_deref() == Some(""));
    }

    /// Soft-deleted rows with `pending_remote_delete = false` (e.g. local-only
    /// trash) must NOT trigger a QRZ delete call.
    #[tokio::test]
    async fn push_pending_remote_deletes_skips_when_flag_unset() {
        let store = MemoryStorage::new();

        let mut local = make_qso("W1AW", "K7ABC", Band::Band20m, Mode::Ft8, 1_700_000_000);
        local.qrz_logid = Some("LOG-LOCAL-ONLY-TRASH".into());
        local.sync_status = SyncStatus::Synced as i32;
        store.insert_qso(&local).await.unwrap();
        store
            .soft_delete_qso(&local.local_id, 1_700_000_700_000, false)
            .await
            .unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        let _ = collect_final(rx).await;

        let calls = api.delete_calls.lock().unwrap();
        assert!(
            calls.is_empty(),
            "no remote delete should be attempted when pending flag is false"
        );
    }

    /// When QRZ returns an error for a queued delete, the local pending flag
    /// must remain set so the next sync retries.
    #[tokio::test]
    async fn push_pending_remote_deletes_preserves_state_on_failure() {
        let store = MemoryStorage::new();

        let mut local = make_qso("W1AW", "DL1ABC", Band::Band20m, Mode::Ssb, 1_700_000_200);
        local.qrz_logid = Some("LOG-FAIL".into());
        local.sync_status = SyncStatus::Synced as i32;
        store.insert_qso(&local).await.unwrap();
        store
            .soft_delete_qso(&local.local_id, 1_700_000_800_000, true)
            .await
            .unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![]);
        api.delete_results
            .lock()
            .unwrap()
            .push(Err(QrzLogbookError::ApiError("server angry".into())));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, true, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);

        let final_msg = collect_final(rx).await;
        assert!(final_msg.complete);
        assert!(
            final_msg.error.is_some(),
            "failed remote delete should surface in summary"
        );

        let after = store
            .list_qsos(&QsoListQuery {
                deleted_filter: DeletedRecordsFilter::All,
                ..QsoListQuery::default()
            })
            .await
            .unwrap();
        assert_eq!(after.len(), 1);
        assert!(after[0].deleted_at.is_some());
        assert!(
            after[0].pending_remote_delete,
            "pending flag must remain set so the next sync retries"
        );
        assert_eq!(after[0].qrz_logid.as_deref(), Some("LOG-FAIL"));
    }

    #[tokio::test]
    async fn push_pending_remote_deletes_preserves_concurrent_restore() {
        let store = Arc::new(MemoryStorage::new());
        let mut local = make_qso("W1AW", "JA1ZZZ", Band::Band40m, Mode::Cw, 1_700_000_000);
        local.sync_status = SyncStatus::Synced as i32;
        local.qrz_logid = Some("LOG-RESTORE-RACE".into());
        local.deleted_at = Some(Timestamp {
            seconds: 1_700_000_100,
            nanos: 0,
        });
        local.pending_remote_delete = true;
        store.insert_qso(&local).await.unwrap();

        let api = MockQrzApi::new(Ok(vec![]), vec![])
            .with_delete_restore(Arc::clone(&store), local.local_id.clone());
        let (tx, rx) = mpsc::channel(16);
        execute_sync(
            &api,
            store.as_ref(),
            true,
            ConflictPolicy::FlagForReview,
            &tx,
        )
        .await;
        drop(tx);
        drop(collect_final(rx).await);

        let saved = store
            .get_qso(&local.local_id)
            .await
            .unwrap()
            .expect("saved QSO");
        assert!(saved.deleted_at.is_none());
        assert!(!saved.pending_remote_delete);
        assert!(saved.qrz_logid.as_deref().is_none_or(str::is_empty));
        assert_eq!(saved.sync_status, SyncStatus::LocalOnly as i32);
    }

    #[tokio::test]
    async fn purge_pushes_remote_delete_before_hard_deleting_local_row() {
        let store = MemoryStorage::new();
        let mut qso = make_qso("W1AW", "DL1ABC", Band::Band20m, Mode::Cw, 1_700_000_000);
        qso.qrz_logid = Some("LOG-PURGE".into());
        store.insert_qso(&qso).await.unwrap();
        store
            .soft_delete_qso(&qso.local_id, 1_700_000_500_000, true)
            .await
            .unwrap();
        let api = MockQrzApi::new(Ok(Vec::new()), Vec::new());

        let outcome = purge_deleted_qsos(Some(&api), &store, &[], None, true)
            .await
            .unwrap();

        assert_eq!(outcome.remote_deletes_pushed, 1);
        assert_eq!(outcome.remote_deletes_failed, 0);
        assert_eq!(outcome.purged_count, 1);
        assert_eq!(
            api.delete_calls.lock().unwrap().as_slice(),
            &["LOG-PURGE".to_string()]
        );
        assert!(store.get_qso(&qso.local_id).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn purge_preserves_local_row_when_remote_delete_fails() {
        let store = MemoryStorage::new();
        let mut qso = make_qso("W1AW", "DL1ABC", Band::Band20m, Mode::Cw, 1_700_000_000);
        qso.qrz_logid = Some("LOG-PURGE-FAIL".into());
        store.insert_qso(&qso).await.unwrap();
        store
            .soft_delete_qso(&qso.local_id, 1_700_000_500_000, true)
            .await
            .unwrap();
        let api = MockQrzApi::new(Ok(Vec::new()), Vec::new());
        api.delete_results
            .lock()
            .unwrap()
            .push(Err(QrzLogbookError::ApiError(
                "remote delete failed".into(),
            )));

        let outcome = purge_deleted_qsos(Some(&api), &store, &[], None, true)
            .await
            .unwrap();

        assert_eq!(outcome.remote_deletes_pushed, 0);
        assert_eq!(outcome.remote_deletes_failed, 1);
        assert_eq!(outcome.purged_count, 0);
        assert!(store.get_qso(&qso.local_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn duplicate_insert_retries_with_replace_and_syncs() {
        // Scenario: a local QSO has sync_status = LocalOnly and no qrz_logid,
        // but the same QSO already exists on QRZ (uploaded via web UI). The
        // plain INSERT fails with "duplicate". The sync should automatically
        // retry with OPTION=REPLACE, adopt the returned LOGID, and mark the
        // QSO as Synced.
        let store = MemoryStorage::new();

        let local = make_qso("W1AW", "AK7S", Band::Band20m, Mode::Ft8, 1_700_000_000);
        assert!(local.qrz_logid.is_none());
        assert_eq!(local.sync_status, SyncStatus::LocalOnly as i32);
        store.insert_qso(&local).await.unwrap();

        // INSERT returns duplicate error, then REPLACE retry should succeed.
        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Err(QrzLogbookError::ApiError(
                "Unable to add QSO to database: duplicate".into(),
            ))],
        );
        // Pre-load the upload_replace_results with a success.
        api.upload_replace_results
            .lock()
            .unwrap()
            .push(Ok(QrzUploadResult {
                logid: "QRZ_ADOPTED_123".into(),
            }));

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, false, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        let final_msg = collect_final(rx).await;

        // Should have no errors — the duplicate was handled gracefully.
        assert!(
            final_msg.error.is_none(),
            "expected no sync error but got: {:?}",
            final_msg.error
        );
        assert_eq!(final_msg.uploaded_records, 1);
        assert_eq!(final_msg.duplicate_replaces, 1);

        // Verify upload_qso_with_replace was called.
        {
            let replace_calls = api.upload_replace_calls.lock().unwrap();
            assert_eq!(replace_calls.len(), 1, "expected one REPLACE retry call");
            assert_eq!(replace_calls[0].0.worked_callsign, "AK7S");
        }

        // Verify the local QSO is now Synced with the adopted LOGID.
        let qsos = store.list_qsos(&QsoListQuery::default()).await.unwrap();
        assert_eq!(qsos.len(), 1);
        assert_eq!(qsos[0].sync_status, SyncStatus::Synced as i32);
        assert_eq!(qsos[0].qrz_logid.as_deref(), Some("QRZ_ADOPTED_123"));
    }

    #[tokio::test]
    async fn non_duplicate_upload_error_still_reported() {
        // A non-duplicate API error should still be reported as a sync error
        // and must NOT trigger the REPLACE retry path.
        let store = MemoryStorage::new();

        let local = make_qso("W1AW", "K3SEW", Band::Band40m, Mode::Ssb, 1_700_000_000);
        store.insert_qso(&local).await.unwrap();

        let api = MockQrzApi::new(
            Ok(vec![]),
            vec![Err(QrzLogbookError::ApiError("invalid ADIF record".into()))],
        );

        let (tx, rx) = mpsc::channel(16);
        execute_sync(&api, &store, false, ConflictPolicy::LastWriteWins, &tx).await;
        drop(tx);
        let final_msg = collect_final(rx).await;

        assert!(
            final_msg.error.is_some(),
            "expected a sync error for non-duplicate API failure"
        );
        assert!(
            final_msg
                .error
                .as_deref()
                .unwrap_or_default()
                .contains("invalid ADIF record"),
            "error should mention the original reason"
        );
        assert_eq!(final_msg.uploaded_records, 0);
        assert_eq!(final_msg.duplicate_replaces, 0);

        // REPLACE retry must NOT have been called.
        let replace_calls = api.upload_replace_calls.lock().unwrap();
        assert!(
            replace_calls.is_empty(),
            "REPLACE retry should not fire for non-duplicate errors"
        );
    }
}
