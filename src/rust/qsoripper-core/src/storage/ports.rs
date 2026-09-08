//! Domain-facing storage contracts implemented by persistence adapters.

use crate::proto::qsoripper::domain::QsoRecord;
use crate::storage::{
    LogbookCounts, LookupSnapshot, QsoHistoryPage, QsoListQuery, StorageError, SyncMetadata,
};
use prost_types::Timestamp;

/// Root engine-owned storage abstraction used by application services.
pub trait EngineStorage: Send + Sync {
    /// Return the logbook-oriented storage surface.
    fn logbook(&self) -> &dyn LogbookStore;

    /// Return the persisted lookup snapshot surface.
    fn lookup_snapshots(&self) -> &dyn LookupSnapshotStore;

    /// Return a stable backend name for diagnostics and bootstrap logs.
    fn backend_name(&self) -> &'static str;
}

/// Persistence operations for the QSO logbook and sync metadata.
#[tonic::async_trait]
pub trait LogbookStore: Send + Sync {
    /// Insert a new QSO.
    async fn insert_qso(&self, qso: &QsoRecord) -> Result<(), StorageError>;

    /// Update an existing QSO. Returns `true` when a row was updated.
    async fn update_qso(&self, qso: &QsoRecord) -> Result<bool, StorageError>;

    /// Replace a QSO only when its persisted record is semantically equal to
    /// `expected`. Implementations must not compare protobuf wire bytes because
    /// map serialization order is not stable. Production stores must make the
    /// semantic comparison and update atomic so a stale snapshot cannot
    /// overwrite a concurrent operator edit.
    async fn update_qso_if_unchanged(
        &self,
        expected: &QsoRecord,
        replacement: &QsoRecord,
    ) -> Result<bool, StorageError> {
        let Some(current) = self.get_qso(&expected.local_id).await? else {
            return Ok(false);
        };
        if current != *expected {
            return Ok(false);
        }
        self.update_qso(replacement).await
    }

    /// Patch QRZ sync metadata onto the current row without overwriting other QSO fields.
    ///
    /// Implementations must update atomically against their storage lock/transaction so
    /// concurrent operator edits cannot be replaced by stale upload snapshots. If the
    /// current row still has `expected_updated_at`, the row becomes `SYNCED`; otherwise
    /// only QRZ metadata is patched and the row remains pending as `MODIFIED`, except
    /// existing conflict rows remain `CONFLICT`. If the row was soft-deleted before
    /// the patch, implementations must preserve the tombstone, record the QRZ logid,
    /// and set `pending_remote_delete` so a later sync removes the remote row.
    async fn update_qrz_sync_metadata(
        &self,
        local_id: &str,
        expected_updated_at: Option<&Timestamp>,
        qrz_logid: &str,
    ) -> Result<Option<QsoRecord>, StorageError>;

    /// Delete a QSO by local ID. Returns `true` when a row was removed.
    async fn delete_qso(&self, local_id: &str) -> Result<bool, StorageError>;

    /// Soft-delete a QSO by local ID, setting `deleted_at` and optionally
    /// queuing it for remote QRZ delete on the next sync. Returns `true` when
    /// a row was found and updated.
    ///
    /// `deleted_at_ms` is the wall-clock millisecond stamp recorded as the
    /// tombstone time. `pending_remote_delete` should be `true` when the row
    /// has a `qrz_logid` and the caller asked for remote deletion.
    async fn soft_delete_qso(
        &self,
        local_id: &str,
        deleted_at_ms: i64,
        pending_remote_delete: bool,
    ) -> Result<bool, StorageError>;

    /// Restore a previously soft-deleted QSO. Clears `deleted_at` and
    /// `pending_remote_delete`. Returns `true` when a row was found and
    /// restored.
    async fn restore_qso(&self, local_id: &str) -> Result<bool, StorageError>;

    /// Load a single QSO by local ID.
    async fn get_qso(&self, local_id: &str) -> Result<Option<QsoRecord>, StorageError>;

    /// List QSOs using the provided query object.
    async fn list_qsos(&self, query: &QsoListQuery) -> Result<Vec<QsoRecord>, StorageError>;

    /// Return prior QSOs with a worked callsign (exact match, case-insensitive),
    /// excluding soft-deleted rows. Results are ordered most-recent-first and
    /// capped at `limit` entries; the returned `total` reflects the unbounded
    /// active-row count so callers can render "showing N of TOTAL" without a
    /// second query. A `limit` of zero returns no entries but still populates
    /// `total`.
    async fn list_qso_history(
        &self,
        worked_callsign: &str,
        limit: u32,
    ) -> Result<QsoHistoryPage, StorageError>;

    /// Return aggregate counts derived from locally persisted QSOs.
    async fn qso_counts(&self) -> Result<LogbookCounts, StorageError>;

    /// Return the persisted remote sync metadata snapshot.
    async fn get_sync_metadata(&self) -> Result<SyncMetadata, StorageError>;

    /// Replace the persisted remote sync metadata snapshot.
    async fn upsert_sync_metadata(&self, metadata: &SyncMetadata) -> Result<(), StorageError>;

    /// Permanently remove soft-deleted QSOs matching the provided filters.
    ///
    /// Only rows with `deleted_at IS NOT NULL` are eligible. When `local_ids`
    /// is non-empty, only those IDs are considered. When `older_than_ms` is
    /// `Some(cutoff)`, only rows with `deleted_at <= cutoff` are eligible.
    /// Returns the number of rows removed.
    async fn purge_deleted_qsos(
        &self,
        local_ids: &[String],
        older_than_ms: Option<i64>,
    ) -> Result<u32, StorageError>;
}

/// Persistence operations for callsign lookup snapshots stored below the hot cache.
#[tonic::async_trait]
pub trait LookupSnapshotStore: Send + Sync {
    /// Load a persisted lookup snapshot by callsign.
    async fn get_lookup_snapshot(
        &self,
        callsign: &str,
    ) -> Result<Option<LookupSnapshot>, StorageError>;

    /// Insert or replace a persisted lookup snapshot.
    async fn upsert_lookup_snapshot(&self, snapshot: &LookupSnapshot) -> Result<(), StorageError>;

    /// Delete a persisted lookup snapshot by callsign. Returns `true` when removed.
    async fn delete_lookup_snapshot(&self, callsign: &str) -> Result<bool, StorageError>;
}
