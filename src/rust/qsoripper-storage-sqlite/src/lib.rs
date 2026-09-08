//! `SQLite` storage adapter for `QsoRipper` engine services.

mod builder;
mod migrations;

use prost::Message;
use qsoripper_core::domain::lookup::normalize_callsign;
use qsoripper_core::proto::qsoripper::domain::{LookupResult, QsoRecord, SyncStatus};
use qsoripper_core::storage::{
    DeletedRecordsFilter, EngineStorage, LogbookCounts, LogbookStore, LookupSnapshot,
    LookupSnapshotStore, QsoHistoryPage, QsoListQuery, QsoSortOrder, StorageError, SyncMetadata,
};
use sqlite::{ConnectionThreadSafe, ReadableWithIndex, State, Statement, Value};
use std::sync::{Mutex, MutexGuard};

pub use builder::SqliteStorageBuilder;

/// SQLite-backed storage implementation for engine-owned persistence.
pub struct SqliteStorage {
    pub(crate) connection: Mutex<ConnectionThreadSafe>,
}

impl SqliteStorage {
    fn connection(&self) -> Result<MutexGuard<'_, ConnectionThreadSafe>, StorageError> {
        self.connection
            .lock()
            .map_err(|_| StorageError::backend("SQLite connection mutex was poisoned"))
    }
}

impl EngineStorage for SqliteStorage {
    fn logbook(&self) -> &dyn LogbookStore {
        self
    }

    fn lookup_snapshots(&self) -> &dyn LookupSnapshotStore {
        self
    }

    fn backend_name(&self) -> &'static str {
        "sqlite"
    }
}

#[tonic::async_trait]
impl LogbookStore for SqliteStorage {
    async fn insert_qso(&self, qso: &QsoRecord) -> Result<(), StorageError> {
        let connection = self.connection()?;
        let encoded = encode_message(qso);
        execute_statement(
            &connection,
            "INSERT INTO qsos (
                local_id,
                qrz_logid,
                qrz_bookid,
                station_callsign,
                worked_callsign,
                utc_timestamp_ms,
                band,
                mode,
                contest_id,
                created_at_ms,
                updated_at_ms,
                sync_status,
                record,
                deleted_at_ms,
                pending_remote_delete
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            &[
                Value::from(qso.local_id.as_str()),
                Value::from(qso.qrz_logid.as_deref()),
                Value::from(qso.qrz_bookid.as_deref()),
                Value::from(qso.station_callsign.as_str()),
                Value::from(qso.worked_callsign.as_str()),
                Value::from(timestamp_to_millis(qso.utc_timestamp.as_ref())),
                Value::Integer(i64::from(qso.band)),
                Value::Integer(i64::from(qso.mode)),
                Value::from(qso.contest_id.as_deref()),
                Value::from(timestamp_to_millis(qso.created_at.as_ref())),
                Value::from(timestamp_to_millis(qso.updated_at.as_ref())),
                Value::Integer(i64::from(qso.sync_status)),
                Value::Binary(encoded),
                Value::from(timestamp_to_millis(qso.deleted_at.as_ref())),
                Value::Integer(i64::from(qso.pending_remote_delete)),
            ],
        )
        .map_err(|err| map_insert_error(err, &qso.local_id))?;

        Ok(())
    }

    async fn update_qso(&self, qso: &QsoRecord) -> Result<bool, StorageError> {
        let connection = self.connection()?;
        update_qso_record(&connection, qso)
    }

    async fn update_qso_if_unchanged(
        &self,
        expected: &QsoRecord,
        replacement: &QsoRecord,
    ) -> Result<bool, StorageError> {
        if expected.local_id != replacement.local_id {
            return Ok(false);
        }
        let connection = self.connection()?;
        update_qso_record_if_unchanged(&connection, expected, replacement)
    }

    async fn update_qrz_sync_metadata(
        &self,
        local_id: &str,
        expected_updated_at: Option<&prost_types::Timestamp>,
        qrz_logid: &str,
    ) -> Result<Option<QsoRecord>, StorageError> {
        let connection = self.connection()?;
        let payload = query_optional::<Vec<u8>>(
            &connection,
            "SELECT record FROM qsos WHERE local_id = ?",
            &[Value::from(local_id)],
            0,
        )
        .map_err(map_sqlite_error)?;

        let Some(bytes) = payload else {
            return Ok(None);
        };
        let mut record = decode_qso(&bytes)?;
        let unchanged_since_upload_started = record.updated_at.as_ref() == expected_updated_at;
        record.qrz_logid = Some(qrz_logid.to_string());
        if record.deleted_at.is_some() {
            record.pending_remote_delete = true;
            if record.sync_status != SyncStatus::Conflict as i32 {
                record.sync_status = SyncStatus::Modified as i32;
            }
            update_qso_record(&connection, &record)?;
            return Ok(Some(record));
        }
        record.sync_status = if unchanged_since_upload_started {
            SyncStatus::Synced as i32
        } else if record.sync_status == SyncStatus::Conflict as i32 {
            record.sync_status
        } else {
            SyncStatus::Modified as i32
        };

        update_qso_record(&connection, &record)?;
        Ok(Some(record))
    }

    async fn delete_qso(&self, local_id: &str) -> Result<bool, StorageError> {
        let connection = self.connection()?;
        let rows = execute_statement(
            &connection,
            "DELETE FROM qsos WHERE local_id = ?",
            &[Value::from(local_id)],
        )
        .map_err(map_sqlite_error)?;

        Ok(rows > 0)
    }

    async fn soft_delete_qso(
        &self,
        local_id: &str,
        deleted_at_ms: i64,
        pending_remote_delete: bool,
    ) -> Result<bool, StorageError> {
        let connection = self.connection()?;
        let payload = query_optional::<Vec<u8>>(
            &connection,
            "SELECT record FROM qsos WHERE local_id = ?",
            &[Value::from(local_id)],
            0,
        )
        .map_err(map_sqlite_error)?;
        let Some(bytes) = payload else {
            return Ok(false);
        };
        let mut record = decode_qso(&bytes)?;
        record.deleted_at = millis_to_timestamp(Some(deleted_at_ms));
        record.pending_remote_delete = pending_remote_delete;
        update_qso_record(&connection, &record)
    }

    async fn restore_qso(&self, local_id: &str) -> Result<bool, StorageError> {
        let connection = self.connection()?;
        let payload = query_optional::<Vec<u8>>(
            &connection,
            "SELECT record FROM qsos WHERE local_id = ?",
            &[Value::from(local_id)],
            0,
        )
        .map_err(map_sqlite_error)?;
        let Some(bytes) = payload else {
            return Ok(false);
        };
        let mut record = decode_qso(&bytes)?;
        record.deleted_at = None;
        record.pending_remote_delete = false;
        update_qso_record(&connection, &record)
    }

    async fn get_qso(&self, local_id: &str) -> Result<Option<QsoRecord>, StorageError> {
        let connection = self.connection()?;
        let payload = query_optional::<Vec<u8>>(
            &connection,
            "SELECT record FROM qsos WHERE local_id = ?",
            &[Value::from(local_id)],
            0,
        )
        .map_err(map_sqlite_error)?;

        payload.map(|bytes| decode_qso(&bytes)).transpose()
    }

    async fn list_qsos(&self, query: &QsoListQuery) -> Result<Vec<QsoRecord>, StorageError> {
        let connection = self.connection()?;
        let mut sql = String::from("SELECT record FROM qsos WHERE 1 = 1");
        let mut values = Vec::<Value>::new();

        match query.deleted_filter {
            DeletedRecordsFilter::ActiveOnly => {
                sql.push_str(" AND deleted_at_ms IS NULL");
            }
            DeletedRecordsFilter::DeletedOnly => {
                sql.push_str(" AND deleted_at_ms IS NOT NULL");
            }
            DeletedRecordsFilter::All => {}
        }

        if let Some(after) = query.after.as_ref() {
            sql.push_str(" AND utc_timestamp_ms >= ?");
            values.push(Value::Integer(
                timestamp_to_millis(Some(after)).unwrap_or(0),
            ));
        }

        if let Some(before) = query.before.as_ref() {
            sql.push_str(" AND utc_timestamp_ms <= ?");
            values.push(Value::Integer(
                timestamp_to_millis(Some(before)).unwrap_or(0),
            ));
        }

        if let Some(filter) = query.callsign_filter.as_deref() {
            let pattern = format!("%{}%", filter.trim().to_ascii_uppercase());
            sql.push_str(" AND (UPPER(station_callsign) LIKE ? OR UPPER(worked_callsign) LIKE ?)");
            values.push(Value::String(pattern.clone()));
            values.push(Value::String(pattern));
        }

        if let Some(band) = query.band_filter {
            sql.push_str(" AND band = ?");
            values.push(Value::Integer(i64::from(band as i32)));
        }

        if let Some(mode) = query.mode_filter {
            sql.push_str(" AND mode = ?");
            values.push(Value::Integer(i64::from(mode as i32)));
        }

        if let Some(contest_id) = query.contest_id.as_deref() {
            sql.push_str(" AND contest_id = ?");
            values.push(Value::String(contest_id.to_string()));
        }

        match query.sort {
            QsoSortOrder::NewestFirst => {
                sql.push_str(" ORDER BY utc_timestamp_ms DESC, local_id DESC");
            }
            QsoSortOrder::OldestFirst => {
                sql.push_str(" ORDER BY utc_timestamp_ms ASC, local_id ASC");
            }
        }

        if let Some(limit) = query.limit {
            sql.push_str(" LIMIT ? OFFSET ?");
            values.push(Value::Integer(i64::from(limit)));
            values.push(Value::Integer(i64::from(query.offset)));
        } else if query.offset > 0 {
            sql.push_str(" LIMIT -1 OFFSET ?");
            values.push(Value::Integer(i64::from(query.offset)));
        }

        let mut statement =
            prepare_statement(&connection, &sql, &values).map_err(map_sqlite_error)?;
        let mut payloads = Vec::new();
        while let State::Row = statement.next().map_err(map_sqlite_error)? {
            payloads.push(statement.read::<Vec<u8>, _>(0).map_err(map_sqlite_error)?);
        }

        payloads
            .into_iter()
            .map(|payload| decode_qso(&payload))
            .collect()
    }

    async fn list_qso_history(
        &self,
        worked_callsign: &str,
        limit: u32,
    ) -> Result<QsoHistoryPage, StorageError> {
        let normalized = normalize_callsign(worked_callsign);
        if normalized.is_empty() {
            return Ok(QsoHistoryPage::default());
        }

        let connection = self.connection()?;
        let total_i64 = query_optional::<i64>(
            &connection,
            "SELECT COUNT(*) FROM qsos \
             WHERE deleted_at_ms IS NULL \
             AND UPPER(worked_callsign) = ?",
            &[Value::String(normalized.clone())],
            0,
        )
        .map_err(map_sqlite_error)?
        .unwrap_or(0);
        let total = u32::try_from(total_i64)
            .map_err(|_| StorageError::backend("history total exceeds u32"))?;

        if limit == 0 || total == 0 {
            return Ok(QsoHistoryPage {
                entries: Vec::new(),
                total,
            });
        }

        let mut statement = prepare_statement(
            &connection,
            "SELECT record FROM qsos \
             WHERE deleted_at_ms IS NULL \
             AND UPPER(worked_callsign) = ? \
             ORDER BY utc_timestamp_ms DESC, local_id DESC \
             LIMIT ?",
            &[Value::String(normalized), Value::Integer(i64::from(limit))],
        )
        .map_err(map_sqlite_error)?;
        let mut payloads = Vec::new();
        while let State::Row = statement.next().map_err(map_sqlite_error)? {
            payloads.push(statement.read::<Vec<u8>, _>(0).map_err(map_sqlite_error)?);
        }

        let entries = payloads
            .into_iter()
            .map(|payload| decode_qso(&payload))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(QsoHistoryPage { entries, total })
    }

    async fn qso_counts(&self) -> Result<LogbookCounts, StorageError> {
        let connection = self.connection()?;
        let local_qso_count = query_optional::<i64>(
            &connection,
            "SELECT COUNT(*) FROM qsos WHERE deleted_at_ms IS NULL",
            &[],
            0,
        )
        .map_err(map_sqlite_error)?
        .unwrap_or(0);
        let pending_upload_count = query_optional::<i64>(
            &connection,
            "SELECT COUNT(*) FROM qsos WHERE sync_status != ? AND deleted_at_ms IS NULL",
            &[Value::Integer(i64::from(SyncStatus::Synced as i32))],
            0,
        )
        .map_err(map_sqlite_error)?
        .unwrap_or(0);

        Ok(LogbookCounts {
            local_qso_count: u32::try_from(local_qso_count)
                .map_err(|_| StorageError::backend("local_qso_count exceeds u32"))?,
            pending_upload_count: u32::try_from(pending_upload_count)
                .map_err(|_| StorageError::backend("pending_upload_count exceeds u32"))?,
        })
    }

    async fn get_sync_metadata(&self) -> Result<SyncMetadata, StorageError> {
        let connection = self.connection()?;
        let mut statement = prepare_statement(
            &connection,
            "SELECT qrz_qso_count, last_sync_ms, qrz_logbook_owner,
                    lotw_last_sync_ms, lotw_last_qsl
             FROM sync_metadata
             WHERE id = 1",
            &[],
        )
        .map_err(map_sqlite_error)?;

        match statement.next().map_err(map_sqlite_error)? {
            State::Row => Ok(SyncMetadata {
                qrz_qso_count: u32::try_from(
                    statement.read::<i64, _>(0).map_err(map_sqlite_error)?,
                )
                .map_err(|_| StorageError::backend("qrz_qso_count exceeds u32"))?,
                last_sync: millis_to_timestamp(
                    statement
                        .read::<Option<i64>, _>(1)
                        .map_err(map_sqlite_error)?,
                ),
                qrz_logbook_owner: statement
                    .read::<Option<String>, _>(2)
                    .map_err(map_sqlite_error)?,
                lotw_last_sync: millis_to_timestamp(
                    statement
                        .read::<Option<i64>, _>(3)
                        .map_err(map_sqlite_error)?,
                ),
                lotw_last_qsl: statement
                    .read::<Option<String>, _>(4)
                    .map_err(map_sqlite_error)?,
            }),
            State::Done => Ok(SyncMetadata::default()),
        }
    }

    async fn upsert_sync_metadata(&self, metadata: &SyncMetadata) -> Result<(), StorageError> {
        let connection = self.connection()?;
        execute_statement(
            &connection,
            "INSERT INTO sync_metadata (
                id, qrz_qso_count, last_sync_ms, qrz_logbook_owner,
                lotw_last_sync_ms, lotw_last_qsl
             )
             VALUES (1, ?, ?, ?, ?, ?)
             ON CONFLICT(id) DO UPDATE SET
                qrz_qso_count = excluded.qrz_qso_count,
                last_sync_ms = excluded.last_sync_ms,
                qrz_logbook_owner = excluded.qrz_logbook_owner,
                lotw_last_sync_ms = excluded.lotw_last_sync_ms,
                lotw_last_qsl = excluded.lotw_last_qsl",
            &[
                Value::Integer(i64::from(metadata.qrz_qso_count)),
                Value::from(timestamp_to_millis(metadata.last_sync.as_ref())),
                Value::from(metadata.qrz_logbook_owner.as_deref()),
                Value::from(timestamp_to_millis(metadata.lotw_last_sync.as_ref())),
                Value::from(metadata.lotw_last_qsl.as_deref()),
            ],
        )
        .map_err(map_sqlite_error)?;

        Ok(())
    }

    async fn purge_deleted_qsos(
        &self,
        local_ids: &[String],
        older_than_ms: Option<i64>,
    ) -> Result<u32, StorageError> {
        let connection = self.connection()?;
        let mut total_purged: u32 = 0;

        if local_ids.is_empty() {
            // Purge all soft-deleted rows, optionally filtered by age.
            let mut sql = String::from("DELETE FROM qsos WHERE deleted_at_ms IS NOT NULL");
            let mut values: Vec<Value> = Vec::new();
            if let Some(cutoff) = older_than_ms {
                sql.push_str(" AND deleted_at_ms <= ?");
                values.push(Value::Integer(cutoff));
            }
            let rows = execute_statement(&connection, &sql, &values).map_err(map_sqlite_error)?;
            total_purged = u32::try_from(rows).unwrap_or(u32::MAX);
        } else {
            // Chunk the IN list to avoid hitting SQLite parameter limits.
            const CHUNK_SIZE: usize = 500;
            for chunk in local_ids.chunks(CHUNK_SIZE) {
                let placeholders: String = chunk.iter().map(|_| "?").collect::<Vec<_>>().join(",");
                let mut sql = format!(
                    "DELETE FROM qsos WHERE deleted_at_ms IS NOT NULL AND local_id IN ({placeholders})"
                );
                let mut values: Vec<Value> =
                    chunk.iter().map(|id| Value::from(id.as_str())).collect();
                if let Some(cutoff) = older_than_ms {
                    sql.push_str(" AND deleted_at_ms <= ?");
                    values.push(Value::Integer(cutoff));
                }
                let rows =
                    execute_statement(&connection, &sql, &values).map_err(map_sqlite_error)?;
                total_purged = total_purged.saturating_add(u32::try_from(rows).unwrap_or(u32::MAX));
            }
        }

        Ok(total_purged)
    }
}

#[tonic::async_trait]
impl LookupSnapshotStore for SqliteStorage {
    async fn get_lookup_snapshot(
        &self,
        callsign: &str,
    ) -> Result<Option<LookupSnapshot>, StorageError> {
        let connection = self.connection()?;
        let mut statement = prepare_statement(
            &connection,
            "SELECT callsign, result, stored_at_ms, expires_at_ms
             FROM lookup_snapshots
             WHERE callsign = ?",
            &[Value::from(normalize_callsign(callsign))],
        )
        .map_err(map_sqlite_error)?;

        match statement.next().map_err(map_sqlite_error)? {
            State::Row => {
                let payload = statement.read::<Vec<u8>, _>(1).map_err(map_sqlite_error)?;
                let result = LookupResult::decode(payload.as_slice())
                    .map_err(|err| StorageError::CorruptData(err.to_string()))?;

                Ok(Some(LookupSnapshot {
                    callsign: statement.read::<String, _>(0).map_err(map_sqlite_error)?,
                    result,
                    stored_at: millis_to_timestamp(
                        statement
                            .read::<Option<i64>, _>(2)
                            .map_err(map_sqlite_error)?,
                    )
                    .unwrap_or_default(),
                    expires_at: millis_to_timestamp(
                        statement
                            .read::<Option<i64>, _>(3)
                            .map_err(map_sqlite_error)?,
                    ),
                }))
            }
            State::Done => Ok(None),
        }
    }

    async fn upsert_lookup_snapshot(&self, snapshot: &LookupSnapshot) -> Result<(), StorageError> {
        let connection = self.connection()?;
        let normalized_callsign = normalize_callsign(&snapshot.callsign);
        let encoded = encode_message(&snapshot.result);

        execute_statement(
            &connection,
            "INSERT INTO lookup_snapshots (callsign, result, stored_at_ms, expires_at_ms)
             VALUES (?, ?, ?, ?)
             ON CONFLICT(callsign) DO UPDATE SET
                result = excluded.result,
                stored_at_ms = excluded.stored_at_ms,
                expires_at_ms = excluded.expires_at_ms",
            &[
                Value::from(normalized_callsign.as_str()),
                Value::Binary(encoded),
                Value::from(timestamp_to_millis(Some(&snapshot.stored_at))),
                Value::from(timestamp_to_millis(snapshot.expires_at.as_ref())),
            ],
        )
        .map_err(map_sqlite_error)?;

        Ok(())
    }

    async fn delete_lookup_snapshot(&self, callsign: &str) -> Result<bool, StorageError> {
        let connection = self.connection()?;
        let rows = execute_statement(
            &connection,
            "DELETE FROM lookup_snapshots WHERE callsign = ?",
            &[Value::from(normalize_callsign(callsign))],
        )
        .map_err(map_sqlite_error)?;

        Ok(rows > 0)
    }
}

fn update_qso_record(
    connection: &ConnectionThreadSafe,
    qso: &QsoRecord,
) -> Result<bool, StorageError> {
    let encoded = encode_message(qso);
    let rows = execute_statement(
        connection,
        "UPDATE qsos
         SET qrz_logid = ?,
             qrz_bookid = ?,
             station_callsign = ?,
             worked_callsign = ?,
             utc_timestamp_ms = ?,
             band = ?,
             mode = ?,
             contest_id = ?,
             created_at_ms = ?,
             updated_at_ms = ?,
             sync_status = ?,
             record = ?,
             deleted_at_ms = ?,
             pending_remote_delete = ?
         WHERE local_id = ?",
        &[
            Value::from(qso.qrz_logid.as_deref()),
            Value::from(qso.qrz_bookid.as_deref()),
            Value::from(qso.station_callsign.as_str()),
            Value::from(qso.worked_callsign.as_str()),
            Value::from(timestamp_to_millis(qso.utc_timestamp.as_ref())),
            Value::Integer(i64::from(qso.band)),
            Value::Integer(i64::from(qso.mode)),
            Value::from(qso.contest_id.as_deref()),
            Value::from(timestamp_to_millis(qso.created_at.as_ref())),
            Value::from(timestamp_to_millis(qso.updated_at.as_ref())),
            Value::Integer(i64::from(qso.sync_status)),
            Value::Binary(encoded),
            Value::from(timestamp_to_millis(qso.deleted_at.as_ref())),
            Value::Integer(i64::from(qso.pending_remote_delete)),
            Value::from(qso.local_id.as_str()),
        ],
    )
    .map_err(map_sqlite_error)?;

    Ok(rows > 0)
}

fn update_qso_record_if_unchanged(
    connection: &ConnectionThreadSafe,
    expected: &QsoRecord,
    replacement: &QsoRecord,
) -> Result<bool, StorageError> {
    execute_statement(connection, "BEGIN IMMEDIATE", &[]).map_err(map_sqlite_error)?;
    let result = (|| {
        let payload = query_optional::<Vec<u8>>(
            connection,
            "SELECT record FROM qsos WHERE local_id = ?",
            &[Value::from(expected.local_id.as_str())],
            0,
        )
        .map_err(map_sqlite_error)?;
        let Some(payload) = payload else {
            return Ok(false);
        };
        if decode_qso(&payload)? != *expected {
            return Ok(false);
        }

        update_qso_record(connection, replacement)
    })();

    match result {
        Ok(updated) => match execute_statement(connection, "COMMIT", &[]) {
            Ok(_) => Ok(updated),
            Err(error) => {
                let _ = execute_statement(connection, "ROLLBACK", &[]);
                Err(map_sqlite_error(error))
            }
        },
        Err(error) => {
            let _ = execute_statement(connection, "ROLLBACK", &[]);
            Err(error)
        }
    }
}

fn encode_message<T: Message>(message: &T) -> Vec<u8> {
    message.encode_to_vec()
}

fn decode_qso(payload: &[u8]) -> Result<QsoRecord, StorageError> {
    QsoRecord::decode(payload).map_err(|err| StorageError::CorruptData(err.to_string()))
}

fn prepare_statement<'a>(
    connection: &'a ConnectionThreadSafe,
    sql: &str,
    values: &[Value],
) -> Result<Statement<'a>, sqlite::Error> {
    let mut statement = connection.prepare(sql)?;
    if !values.is_empty() {
        statement.bind(values)?;
    }

    Ok(statement)
}

fn execute_statement(
    connection: &ConnectionThreadSafe,
    sql: &str,
    values: &[Value],
) -> Result<usize, sqlite::Error> {
    let mut statement = prepare_statement(connection, sql, values)?;
    while let State::Row = statement.next()? {}
    Ok(connection.change_count())
}

fn query_optional<T>(
    connection: &ConnectionThreadSafe,
    sql: &str,
    values: &[Value],
    column_index: usize,
) -> Result<Option<T>, sqlite::Error>
where
    T: ReadableWithIndex,
{
    let mut statement = prepare_statement(connection, sql, values)?;
    match statement.next()? {
        State::Row => statement.read::<T, _>(column_index).map(Some),
        State::Done => Ok(None),
    }
}

fn map_insert_error(error: sqlite::Error, local_id: &str) -> StorageError {
    match error.code {
        Some(code)
            if code == isize::try_from(sqlite::ffi::SQLITE_CONSTRAINT).unwrap_or_default() =>
        {
            StorageError::duplicate("qso", local_id)
        }
        _ => map_sqlite_error(error),
    }
}

fn map_sqlite_error(error: sqlite::Error) -> StorageError {
    let message = match (error.code, error.message) {
        (Some(code), Some(message)) => format!("{message} (code {code})"),
        (Some(code), None) => format!("an SQLite error (code {code})"),
        (None, Some(message)) => message,
        (None, None) => "an SQLite error".to_string(),
    };

    StorageError::backend(message)
}

fn timestamp_to_millis(timestamp: Option<&prost_types::Timestamp>) -> Option<i64> {
    timestamp.map(|value| {
        value
            .seconds
            .saturating_mul(1_000)
            .saturating_add(i64::from(value.nanos) / 1_000_000)
    })
}

fn millis_to_timestamp(millis: Option<i64>) -> Option<prost_types::Timestamp> {
    millis.map(|value| prost_types::Timestamp {
        seconds: value.div_euclid(1_000),
        nanos: i32::try_from(value.rem_euclid(1_000) * 1_000_000).unwrap_or(0),
    })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic, clippy::indexing_slicing)]
mod tests {
    use super::{encode_message, SqliteStorageBuilder};
    use prost_types::Timestamp;
    use qsoripper_core::application::logbook::LogbookEngine;
    use qsoripper_core::domain::qso::QsoRecordBuilder;
    use qsoripper_core::proto::qsoripper::domain::{
        Band, LookupResult, LookupState, Mode, SyncStatus,
    };
    use qsoripper_core::storage::{
        EngineStorage, LookupSnapshot, LookupSnapshotStore, QsoListQuery,
    };
    use sqlite::{Connection, State, Value};
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[tokio::test]
    async fn sqlite_storage_round_trips_qsos_through_logbook_engine() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage);
        let qso = QsoRecordBuilder::new("W1AW", "K7ABC")
            .band(Band::Band20m)
            .mode(Mode::Ft8)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .build();

        let stored = engine.log_qso(qso).await.unwrap();
        let loaded = engine.get_qso(&stored.local_id).await.unwrap();

        assert_eq!(loaded.local_id, stored.local_id);
        assert_eq!(loaded.worked_callsign, "K7ABC");
        assert!(loaded.created_at.is_some());
        assert!(loaded.updated_at.is_some());
    }

    #[tokio::test]
    async fn sqlite_storage_patches_qrz_metadata_without_replacing_current_row() {
        let storage = SqliteStorageBuilder::new().in_memory().build().unwrap();
        let mut qso = QsoRecordBuilder::new("W1AW", "K7PATCH")
            .band(Band::Band20m)
            .mode(Mode::Ft8)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .build();
        qso.notes = Some("operator edit".into());
        qso.sync_status = SyncStatus::Modified as i32;
        qso.updated_at = Some(Timestamp {
            seconds: 1_700_000_010,
            nanos: 0,
        });

        let logbook = storage.logbook();
        logbook.insert_qso(&qso).await.unwrap();
        let patched = logbook
            .update_qrz_sync_metadata(
                &qso.local_id,
                Some(&Timestamp {
                    seconds: 1_700_000_000,
                    nanos: 0,
                }),
                "QRZ-PATCH",
            )
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("patched qso"));

        assert_eq!(patched.notes.as_deref(), Some("operator edit"));
        assert_eq!(patched.qrz_logid.as_deref(), Some("QRZ-PATCH"));
        assert_eq!(patched.sync_status, SyncStatus::Modified as i32);

        let reloaded = logbook.get_qso(&qso.local_id).await.unwrap().unwrap();
        assert_eq!(reloaded.notes.as_deref(), Some("operator edit"));
        assert_eq!(reloaded.qrz_logid.as_deref(), Some("QRZ-PATCH"));
        assert_eq!(reloaded.sync_status, SyncStatus::Modified as i32);
    }

    #[tokio::test]
    async fn sqlite_storage_conditional_update_rejects_stale_snapshot() {
        let storage = SqliteStorageBuilder::new().in_memory().build().unwrap();
        let mut original = QsoRecordBuilder::new("W1AW", "K7CAS")
            .band(Band::Band20m)
            .mode(Mode::Ft8)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .build();
        original.extra_fields.insert("APP_A".into(), "one".into());
        original.extra_fields.insert("APP_B".into(), "two".into());
        original.extra_fields.insert("APP_C".into(), "three".into());
        let logbook = storage.logbook();
        logbook.insert_qso(&original).await.unwrap();
        let mut current = original.clone();
        current.notes = Some("operator edit".into());
        logbook.update_qso(&current).await.unwrap();

        let mut stale_replacement = original.clone();
        stale_replacement.notes = Some("stale sync value".into());
        assert!(!logbook
            .update_qso_if_unchanged(&original, &stale_replacement)
            .await
            .unwrap());
        let saved = logbook.get_qso(&original.local_id).await.unwrap().unwrap();
        assert_eq!(saved.notes.as_deref(), Some("operator edit"));
    }

    #[tokio::test]
    async fn sqlite_storage_conditional_update_compares_map_fields_semantically() {
        let storage = SqliteStorageBuilder::new().in_memory().build().unwrap();
        let mut persisted = QsoRecordBuilder::new("W1AW", "K7MAP")
            .band(Band::Band20m)
            .mode(Mode::Ft8)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .build();
        for index in 0..16 {
            persisted
                .extra_fields
                .insert(format!("APP_FIELD_{index}"), format!("value-{index}"));
        }
        let logbook = storage.logbook();
        logbook.insert_qso(&persisted).await.unwrap();

        let mut expected = persisted.clone();
        for _ in 0..100 {
            expected.extra_fields = persisted
                .extra_fields
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect();
            if encode_message(&expected) != encode_message(&persisted) {
                break;
            }
        }
        assert_eq!(expected, persisted);
        assert_ne!(
            encode_message(&expected),
            encode_message(&persisted),
            "test requires different protobuf map encodings"
        );

        let mut replacement = expected.clone();
        replacement.worked_grid = Some("FN31".into());
        assert!(logbook
            .update_qso_if_unchanged(&expected, &replacement)
            .await
            .unwrap());

        let saved = logbook.get_qso(&persisted.local_id).await.unwrap().unwrap();
        assert_eq!(saved.worked_grid.as_deref(), Some("FN31"));
        assert_eq!(saved.extra_fields, persisted.extra_fields);
    }

    #[tokio::test]
    async fn sqlite_storage_lists_qsos_with_filters() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage);

        let first = QsoRecordBuilder::new("W1AW", "K7ONE")
            .band(Band::Band20m)
            .mode(Mode::Ft8)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .contest("CQ-WW")
            .build();
        let second = QsoRecordBuilder::new("W1AW", "K7TWO")
            .band(Band::Band40m)
            .mode(Mode::Cw)
            .timestamp(Timestamp {
                seconds: 1_700_000_100,
                nanos: 0,
            })
            .build();

        let _ = engine.log_qso(first).await.unwrap();
        let _ = engine.log_qso(second).await.unwrap();

        let listed = engine
            .list_qsos(&QsoListQuery {
                contest_id: Some("CQ-WW".into()),
                ..QsoListQuery::default()
            })
            .await
            .unwrap();

        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed.first().map(|record| record.worked_callsign.as_str()),
            Some("K7ONE")
        );
    }

    #[tokio::test]
    async fn sqlite_storage_persists_lookup_snapshots() {
        let storage = SqliteStorageBuilder::new().in_memory().build().unwrap();
        let snapshot = LookupSnapshot {
            callsign: "w1aw".into(),
            result: LookupResult {
                state: LookupState::Found as i32,
                queried_callsign: "W1AW".into(),
                ..LookupResult::default()
            },
            stored_at: Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            },
            expires_at: None,
        };

        storage.upsert_lookup_snapshot(&snapshot).await.unwrap();
        let loaded = storage.get_lookup_snapshot("W1AW").await.unwrap();

        let Some(loaded) = loaded else {
            panic!("Expected persisted lookup snapshot to exist");
        };

        assert_eq!(loaded.callsign, "W1AW");
        assert_eq!(loaded.result.state, LookupState::Found as i32);
    }

    #[test]
    fn sqlite_storage_builder_migrates_legacy_qsos_schema() {
        let path = unique_temp_db_path("legacy-schema");
        let legacy_connection = Connection::open_thread_safe(&path).unwrap();
        legacy_connection
            .execute(crate::migrations::INITIAL_SCHEMA)
            .unwrap();
        drop(legacy_connection);

        let storage = SqliteStorageBuilder::new().path(&path).build().unwrap();
        drop(storage);

        let migrated_connection = Connection::open_thread_safe(&path).unwrap();
        let columns = load_qso_columns(&migrated_connection);

        assert!(columns.iter().any(|column| column == "deleted_at_ms"));
        assert!(columns
            .iter()
            .any(|column| column == "pending_remote_delete"));

        drop(migrated_connection);
        cleanup_sqlite_files(&path);
    }

    #[test]
    fn timestamp_to_millis_saturates_positive_overflow() {
        let value = super::timestamp_to_millis(Some(&Timestamp {
            seconds: i64::MAX,
            nanos: 999_999_999,
        }));

        assert_eq!(value, Some(i64::MAX));
    }

    #[test]
    fn timestamp_to_millis_saturates_negative_overflow() {
        let value = super::timestamp_to_millis(Some(&Timestamp {
            seconds: i64::MIN,
            nanos: -999_999_999,
        }));

        assert_eq!(value, Some(i64::MIN));
    }

    fn load_qso_columns(connection: &sqlite::ConnectionThreadSafe) -> Vec<String> {
        let mut statement = connection.prepare("PRAGMA table_info(qsos);").unwrap();
        let mut columns = Vec::new();
        while let sqlite::State::Row = statement.next().unwrap() {
            columns.push(statement.read::<String, _>(1).unwrap());
        }

        columns
    }

    fn unique_temp_db_path(prefix: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "qsoripper-storage-sqlite-{prefix}-{}-{suffix}.db",
            std::process::id()
        ))
    }

    fn cleanup_sqlite_files(path: &PathBuf) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(path.with_extension("db-shm"));
        let _ = fs::remove_file(path.with_extension("db-wal"));
    }

    #[tokio::test]
    async fn sqlite_purge_removes_only_soft_deleted_rows() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage.clone());

        let active = engine
            .log_qso(
                QsoRecordBuilder::new("W1AW", "K7ACT")
                    .band(Band::Band20m)
                    .mode(Mode::Ft8)
                    .timestamp(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 0,
                    })
                    .build(),
            )
            .await
            .unwrap();
        let deleted = engine
            .log_qso(
                QsoRecordBuilder::new("W1AW", "K7DEL")
                    .band(Band::Band40m)
                    .mode(Mode::Cw)
                    .timestamp(Timestamp {
                        seconds: 1_700_001_000,
                        nanos: 0,
                    })
                    .build(),
            )
            .await
            .unwrap();
        engine.delete_qso(&deleted.local_id, false).await.unwrap();

        let purged = storage
            .logbook()
            .purge_deleted_qsos(&[], None)
            .await
            .unwrap();
        assert_eq!(purged, 1);

        assert!(storage
            .logbook()
            .get_qso(&active.local_id)
            .await
            .unwrap()
            .is_some());
        assert!(storage
            .logbook()
            .get_qso(&deleted.local_id)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn sqlite_purge_filters_by_local_ids() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage.clone());

        let d1 = engine
            .log_qso(
                QsoRecordBuilder::new("W1AW", "K7D1")
                    .band(Band::Band20m)
                    .mode(Mode::Ft8)
                    .timestamp(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 0,
                    })
                    .build(),
            )
            .await
            .unwrap();
        let d2 = engine
            .log_qso(
                QsoRecordBuilder::new("W1AW", "K7D2")
                    .band(Band::Band40m)
                    .mode(Mode::Cw)
                    .timestamp(Timestamp {
                        seconds: 1_700_001_000,
                        nanos: 0,
                    })
                    .build(),
            )
            .await
            .unwrap();
        engine.delete_qso(&d1.local_id, false).await.unwrap();
        engine.delete_qso(&d2.local_id, false).await.unwrap();

        let purged = storage
            .logbook()
            .purge_deleted_qsos(&[d1.local_id.clone()], None)
            .await
            .unwrap();
        assert_eq!(purged, 1);

        assert!(storage
            .logbook()
            .get_qso(&d2.local_id)
            .await
            .unwrap()
            .is_some());
    }

    #[tokio::test]
    async fn sqlite_purge_filters_by_older_than() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage.clone());
        let logbook = storage.logbook();

        let q1 = engine
            .log_qso(
                QsoRecordBuilder::new("W1AW", "K7OLD")
                    .band(Band::Band20m)
                    .mode(Mode::Ft8)
                    .timestamp(Timestamp {
                        seconds: 1_700_000_000,
                        nanos: 0,
                    })
                    .build(),
            )
            .await
            .unwrap();
        let q2 = engine
            .log_qso(
                QsoRecordBuilder::new("W1AW", "K7NEW")
                    .band(Band::Band40m)
                    .mode(Mode::Cw)
                    .timestamp(Timestamp {
                        seconds: 1_700_001_000,
                        nanos: 0,
                    })
                    .build(),
            )
            .await
            .unwrap();

        logbook
            .soft_delete_qso(&q1.local_id, 1000, false)
            .await
            .unwrap();
        logbook
            .soft_delete_qso(&q2.local_id, 5000, false)
            .await
            .unwrap();

        let purged = logbook.purge_deleted_qsos(&[], Some(3000)).await.unwrap();
        assert_eq!(purged, 1);

        assert!(logbook.get_qso(&q1.local_id).await.unwrap().is_none());
        assert!(logbook.get_qso(&q2.local_id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn sqlite_purge_no_match_returns_zero() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let purged = storage
            .logbook()
            .purge_deleted_qsos(&[], None)
            .await
            .unwrap();
        assert_eq!(purged, 0);
    }

    #[tokio::test]
    async fn sqlite_storage_round_trips_cw_decode_rx_wpm() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage);
        let mut qso = QsoRecordBuilder::new("W1AW", "K7ABC")
            .band(Band::Band40m)
            .mode(Mode::Cw)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .build();
        qso.cw_decode_rx_wpm = Some(28);

        let stored = engine.log_qso(qso).await.unwrap();
        let loaded = engine.get_qso(&stored.local_id).await.unwrap();

        assert_eq!(loaded.cw_decode_rx_wpm, Some(28));
    }

    #[tokio::test]
    async fn sqlite_storage_round_trips_no_cw_decode_rx_wpm() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let engine = LogbookEngine::new(storage);
        let qso = QsoRecordBuilder::new("W1AW", "K7ABC")
            .band(Band::Band20m)
            .mode(Mode::Ssb)
            .timestamp(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            })
            .build();

        let stored = engine.log_qso(qso).await.unwrap();
        let loaded = engine.get_qso(&stored.local_id).await.unwrap();

        assert_eq!(loaded.cw_decode_rx_wpm, None);
    }

    #[tokio::test]
    async fn sqlite_storage_history_exact_match_excludes_soft_deleted() {
        let storage: Arc<dyn EngineStorage> =
            Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        let logbook = storage.logbook();

        for (id, worked, ts_secs) in [
            ("a", "K7ABC", 1_700_000_000_i64),
            ("b", "K7ABC", 1_700_001_000),
            ("c", "K7ABCD", 1_700_002_000),
            ("d", "k7abc", 1_700_003_000),
        ] {
            let mut qso = QsoRecordBuilder::new("W1AW", worked)
                .band(Band::Band20m)
                .mode(Mode::Ssb)
                .timestamp(Timestamp {
                    seconds: ts_secs,
                    nanos: 0,
                })
                .build();
            qso.local_id = id.to_string();
            logbook.insert_qso(&qso).await.unwrap();
        }
        logbook
            .soft_delete_qso("b", 1_700_900_000_000, false)
            .await
            .unwrap();

        let page = logbook.list_qso_history("k7abc", 10).await.unwrap();
        assert_eq!(page.total, 2);
        assert_eq!(page.entries.len(), 2);
        assert_eq!(page.entries[0].local_id, "d");
        assert_eq!(page.entries[1].local_id, "a");

        let limited = logbook.list_qso_history("K7ABC", 1).await.unwrap();
        assert_eq!(limited.total, 2);
        assert_eq!(limited.entries.len(), 1);
        assert_eq!(limited.entries[0].local_id, "d");

        let zero = logbook.list_qso_history("K7ABC", 0).await.unwrap();
        assert_eq!(zero.total, 2);
        assert!(zero.entries.is_empty());

        let none = logbook.list_qso_history("NEVER", 5).await.unwrap();
        assert_eq!(none.total, 0);
        assert!(none.entries.is_empty());
    }

    #[tokio::test]
    async fn sqlite_storage_history_query_uses_expression_index() {
        let storage = Arc::new(SqliteStorageBuilder::new().in_memory().build().unwrap());
        // Seed enough rows that the planner prefers the expression index over
        // a full scan. Without enough rows, SQLite may pick SCAN even when an
        // index is available, since the cost difference is negligible for tiny
        // tables.
        for i in 0..32 {
            let mut qso = QsoRecordBuilder::new("W1AW", format!("K7AB{i}"))
                .band(Band::Band20m)
                .mode(Mode::Ssb)
                .timestamp(Timestamp {
                    seconds: 1_700_000_000 + i64::from(i),
                    nanos: 0,
                })
                .build();
            qso.local_id = format!("row-{i}");
            storage.logbook().insert_qso(&qso).await.unwrap();
        }

        let connection = storage.connection().unwrap();
        let mut statement = connection
            .prepare(
                "EXPLAIN QUERY PLAN \
                 SELECT record FROM qsos \
                 WHERE deleted_at_ms IS NULL \
                 AND UPPER(worked_callsign) = ? \
                 ORDER BY utc_timestamp_ms DESC, local_id DESC \
                 LIMIT ?",
            )
            .unwrap();
        statement
            .bind((1, Value::String("K7AB0".to_string())))
            .unwrap();
        statement.bind((2, Value::Integer(10))).unwrap();

        let mut plan = String::new();
        while let State::Row = statement.next().unwrap() {
            let detail = statement.read::<String, _>("detail").unwrap();
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            plan.contains("idx_qsos_worked_callsign_upper"),
            "expected query plan to use idx_qsos_worked_callsign_upper, got:\n{plan}"
        );
    }
}
