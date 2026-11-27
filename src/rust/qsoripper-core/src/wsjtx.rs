//! WSJT-X ingestion helpers that feed the existing ADIF import pipeline.

use crate::adif::parse_adi_qsos_without_header_detection;
use crate::application::logbook::{
    AdifImportDiagnostic, AdifImportSummary, LogbookEngine, LogbookError,
};
use crate::proto::qsoripper::domain::{QsoRecord, StationProfile};
#[cfg(test)]
use serde_json::Value;
use std::fs;
use std::path::Path;
use time::OffsetDateTime;

const WSJTX_MAGIC: u32 = 0xadbc_cbda;
const WSJTX_LOGGED_ADIF_MESSAGE_TYPE: u32 = 12;
const WSJTX_NULL_STRING_LENGTH: u32 = u32::MAX;
const WSJTX_IMPORT_DIAGNOSTIC_FIELD: &str = "APP_QSORIPPER_WSJTX_IMPORT";
const WSJTX_IMPORT_DIAGNOSTIC_PREFIX: &str = "QsoRipper WSJT-X import:";
const WSJTX_UDP_SOURCE: &str = "udp_logged_adif";
const WSJTX_ADIF_TAIL_SOURCE: &str = "adif_tail";

/// Test/simulation helper that accepts permissive WSJT-X-like UDP payloads.
///
/// # Errors
///
/// Returns an error when the datagram has no usable ADIF payload or when the
/// import pipeline rejects the parsed QSO records.
#[cfg(test)]
pub async fn ingest_wsjtx_udp_datagram(
    logbook_engine: &LogbookEngine,
    datagram: &[u8],
    active_station_profile: Option<&StationProfile>,
    refresh: bool,
) -> Result<AdifImportSummary, String> {
    let Some(adif_payload) = extract_adif_payload(datagram)? else {
        return Ok(AdifImportSummary {
            records_skipped: 1,
            ..AdifImportSummary::default()
        });
    };
    Box::pin(ingest_adif_payload(
        logbook_engine,
        &adif_payload,
        active_station_profile,
        refresh,
        WSJTX_UDP_SOURCE,
    ))
    .await
}

/// Import a real WSJT-X Logged ADIF UDP datagram.
///
/// This production entry point accepts only the WSJT-X magic-framed Logged ADIF
/// message and ignores other WSJT-X message types.
///
/// # Errors
///
/// Returns an error when a Logged ADIF datagram is malformed or when the import
/// pipeline rejects the parsed QSO records.
pub async fn ingest_wsjtx_logged_adif_datagram(
    logbook_engine: &LogbookEngine,
    datagram: &[u8],
    active_station_profile: Option<&StationProfile>,
    refresh: bool,
) -> Result<AdifImportSummary, String> {
    let Some(adif_payload) = extract_wsjtx_logged_adif(datagram)? else {
        return Ok(AdifImportSummary {
            records_skipped: 1,
            ..AdifImportSummary::default()
        });
    };
    Box::pin(ingest_adif_payload(
        logbook_engine,
        &adif_payload,
        active_station_profile,
        refresh,
        WSJTX_UDP_SOURCE,
    ))
    .await
}

/// Import any newly appended ADIF data from a WSJT-X log file.
///
/// The current implementation keeps a simple byte offset cursor so the caller can
/// replay appended records without re-importing the full file every time.
///
/// # Errors
///
/// Returns an error when the file cannot be read or when the ADIF import path
/// rejects the newly appended records.
pub async fn ingest_wsjtx_adif_tail(
    logbook_engine: &LogbookEngine,
    path: &Path,
    active_station_profile: Option<&StationProfile>,
    refresh: bool,
    cursor: &mut usize,
) -> Result<AdifImportSummary, String> {
    let bytes =
        fs::read(path).map_err(|error| format!("failed to read {}: {error}", path.display()))?;
    if *cursor > bytes.len() {
        *cursor = 0;
    }
    if *cursor == bytes.len() {
        return Ok(AdifImportSummary::default());
    }

    let start = *cursor;
    let Some(appended) = bytes.get(start..) else {
        return Ok(AdifImportSummary::default());
    };
    let Some(complete_len) = complete_adif_prefix_len(appended) else {
        return Ok(AdifImportSummary::default());
    };
    let Some(complete_payload) = appended.get(..complete_len) else {
        return Ok(AdifImportSummary::default());
    };

    let summary = Box::pin(ingest_adif_payload(
        logbook_engine,
        complete_payload,
        active_station_profile,
        refresh,
        WSJTX_ADIF_TAIL_SOURCE,
    ))
    .await?;
    *cursor = start.saturating_add(complete_len);
    Ok(summary)
}

#[cfg(test)]
fn extract_adif_payload(datagram: &[u8]) -> Result<Option<Vec<u8>>, String> {
    if read_be_u32(datagram, 0)?.is_some_and(|magic| magic == WSJTX_MAGIC) {
        return extract_wsjtx_logged_adif(datagram);
    }

    if let Ok(value) = serde_json::from_slice::<Value>(datagram) {
        if let Some(adif) = value.get("adif").and_then(Value::as_str).map(str::to_owned) {
            return Ok(Some(adif.into_bytes()));
        }

        if let Some(adif) = value.get("qsos").and_then(Value::as_str).map(str::to_owned) {
            return Ok(Some(adif.into_bytes()));
        }

        if let Some(adif) = value
            .get("payload")
            .and_then(Value::as_str)
            .map(str::to_owned)
        {
            return Ok(Some(adif.into_bytes()));
        }
    }

    if datagram
        .iter()
        .any(|byte| *byte == b'<' || *byte == b'>' || *byte == b'\n')
    {
        return Ok(Some(datagram.to_vec()));
    }

    Err("WSJT-X datagram does not contain a usable ADIF payload".to_string())
}

fn extract_wsjtx_logged_adif(datagram: &[u8]) -> Result<Option<Vec<u8>>, String> {
    let Some(magic) = read_be_u32(datagram, 0)? else {
        return Ok(None);
    };
    if magic != WSJTX_MAGIC {
        return Ok(None);
    }

    let Some(_schema) = read_be_u32(datagram, 4)? else {
        return Err("WSJT-X datagram is missing schema field".to_string());
    };
    let Some(message_type) = read_be_u32(datagram, 8)? else {
        return Err("WSJT-X datagram is missing message type field".to_string());
    };
    if message_type != WSJTX_LOGGED_ADIF_MESSAGE_TYPE {
        return Ok(None);
    }

    let mut cursor = 12;
    let _id = read_wsjtx_utf8(datagram, &mut cursor)?;
    let adif = read_wsjtx_utf8(datagram, &mut cursor)?;
    if adif.trim().is_empty() {
        return Err("WSJT-X Logged ADIF datagram has an empty ADIF payload".to_string());
    }

    Ok(Some(adif.into_bytes()))
}

fn read_be_u32(bytes: &[u8], offset: usize) -> Result<Option<u32>, String> {
    let Some(end) = offset.checked_add(4) else {
        return Err("WSJT-X datagram offset overflow".to_string());
    };
    let Some(slice) = bytes.get(offset..end) else {
        return Ok(None);
    };
    let value = u32::from_be_bytes(
        slice
            .try_into()
            .map_err(|_| "WSJT-X datagram u32 field had invalid length".to_string())?,
    );
    Ok(Some(value))
}

fn read_wsjtx_utf8(bytes: &[u8], cursor: &mut usize) -> Result<String, String> {
    let Some(length) = read_be_u32(bytes, *cursor)? else {
        return Err("WSJT-X datagram is missing string length".to_string());
    };
    *cursor = cursor
        .checked_add(4)
        .ok_or_else(|| "WSJT-X datagram cursor overflow".to_string())?;
    if length == WSJTX_NULL_STRING_LENGTH {
        return Ok(String::new());
    }
    let length = usize::try_from(length)
        .map_err(|_| "WSJT-X datagram string length is not addressable".to_string())?;
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| "WSJT-X datagram string length overflow".to_string())?;
    let Some(slice) = bytes.get(*cursor..end) else {
        return Err("WSJT-X datagram string extends past packet end".to_string());
    };
    *cursor = end;
    String::from_utf8(slice.to_vec())
        .map_err(|error| format!("WSJT-X datagram string is not UTF-8: {error}"))
}

async fn ingest_adif_payload(
    logbook_engine: &LogbookEngine,
    adif_payload: &[u8],
    active_station_profile: Option<&StationProfile>,
    refresh: bool,
    source: &str,
) -> Result<AdifImportSummary, String> {
    let mut qsos = parse_adi_qsos_without_header_detection(adif_payload)
        .await
        .map_err(|error| format!("failed to parse ADIF payload: {error}"))?;

    if qsos.is_empty() {
        return Ok(AdifImportSummary::default());
    }

    let imported_at = OffsetDateTime::now_utc();
    for qso in &mut qsos {
        qso.extra_fields.insert(
            WSJTX_IMPORT_DIAGNOSTIC_FIELD.to_string(),
            wsjtx_import_note(source, qso, imported_at),
        );
    }
    let diagnostic = AdifImportDiagnostic {
        extra_field_key: WSJTX_IMPORT_DIAGNOSTIC_FIELD.to_string(),
        note_prefix: WSJTX_IMPORT_DIAGNOSTIC_PREFIX.to_string(),
    };
    Box::pin(logbook_engine.import_adif_qsos_with_diagnostic(
        qsos,
        active_station_profile,
        refresh,
        &diagnostic,
    ))
    .await
    .map_err(format_wsjtx_error)
}

fn wsjtx_import_note(source: &str, qso: &QsoRecord, imported_at: OffsetDateTime) -> String {
    let imported_at_text = crate::utc::format_rfc3339_millis(imported_at);
    let end_delta = qso
        .utc_end_timestamp
        .as_ref()
        .and_then(crate::utc::from_timestamp)
        .map_or_else(
            || "unavailable".to_string(),
            |qso_end| (imported_at - qso_end).whole_milliseconds().to_string(),
        );
    format!(
        "{WSJTX_IMPORT_DIAGNOSTIC_PREFIX} imported_at_utc={imported_at_text}, qso_end_to_import_ms={end_delta}, source={source}, engine=rust"
    )
}

fn format_wsjtx_error(error: LogbookError) -> String {
    match error {
        LogbookError::NoActiveStationProfile => error.to_string(),
        LogbookError::Validation(message) => format!("WSJT-X import validation failed: {message}"),
        LogbookError::NotFound(message) => format!("WSJT-X import failed to find QSO: {message}"),
        LogbookError::AlreadyDeleted(message) => {
            format!("WSJT-X import failed because QSO is deleted: {message}")
        }
        LogbookError::Storage(error) => format!("WSJT-X import storage failed: {error}"),
    }
}

fn complete_adif_prefix_len(bytes: &[u8]) -> Option<usize> {
    let mut last_end = None;
    let mut cursor = 0;
    while cursor < bytes.len() {
        let Some(remaining) = bytes.get(cursor..) else {
            break;
        };
        let Some(tag_start_offset) = remaining.iter().position(|byte| *byte == b'<') else {
            break;
        };
        let tag_start = cursor + tag_start_offset;
        let Some(tag_and_rest) = bytes.get(tag_start..) else {
            break;
        };
        let Some(tag_end_offset) = tag_and_rest.iter().position(|byte| *byte == b'>') else {
            break;
        };
        let tag_end = tag_start + tag_end_offset;
        let Some(tag_body) = bytes.get(tag_start + 1..tag_end) else {
            break;
        };
        let field_name = adif_tag_name(tag_body);
        cursor = tag_end + 1;

        if field_name.eq_ignore_ascii_case(b"eor") {
            last_end = Some(cursor);
            continue;
        }

        if let Some(field_len) = adif_field_len(tag_body) {
            let Some(next_cursor) = cursor_after_utf8_chars(bytes, cursor, field_len) else {
                break;
            };
            cursor = next_cursor;
        }
    }
    last_end
}

fn cursor_after_utf8_chars(bytes: &[u8], start: usize, char_count: usize) -> Option<usize> {
    let remaining = std::str::from_utf8(bytes.get(start..)?).ok()?;
    let mut end_offset = 0;
    for _ in 0..char_count {
        let (offset, ch) = remaining.get(end_offset..)?.char_indices().next()?;
        end_offset = end_offset.checked_add(offset)?.checked_add(ch.len_utf8())?;
    }
    start.checked_add(end_offset)
}

fn adif_tag_name(tag_body: &[u8]) -> &[u8] {
    let end = tag_body
        .iter()
        .position(|byte| *byte == b':' || byte.is_ascii_whitespace())
        .unwrap_or(tag_body.len());
    tag_body.get(..end).unwrap_or(tag_body)
}

fn adif_field_len(tag_body: &[u8]) -> Option<usize> {
    let colon = tag_body.iter().position(|byte| *byte == b':')?;
    let mut length = 0_usize;
    let mut saw_digit = false;
    for byte in tag_body.get(colon + 1..)? {
        if *byte == b':' {
            break;
        }
        if !byte.is_ascii_digit() {
            return None;
        }
        saw_digit = true;
        length = length
            .checked_mul(10)?
            .checked_add(usize::from(*byte - b'0'))?;
    }
    saw_digit.then_some(length)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::{
        complete_adif_prefix_len, extract_adif_payload, ingest_wsjtx_adif_tail,
        ingest_wsjtx_udp_datagram, wsjtx_import_note, WSJTX_ADIF_TAIL_SOURCE,
        WSJTX_IMPORT_DIAGNOSTIC_FIELD, WSJTX_IMPORT_DIAGNOSTIC_PREFIX,
        WSJTX_LOGGED_ADIF_MESSAGE_TYPE, WSJTX_MAGIC, WSJTX_UDP_SOURCE,
    };
    use crate::application::logbook::{AdifImportSummary, LogbookEngine};
    use crate::proto::qsoripper::domain::{QsoRecord, SyncStatus};
    use crate::storage::{
        EngineStorage, LogbookCounts, LogbookStore, LookupSnapshot, LookupSnapshotStore,
        QsoHistoryPage, QsoListQuery, StorageError, SyncMetadata,
    };
    use std::collections::BTreeMap;
    use std::fs;
    use std::sync::Arc;
    use tempfile::NamedTempFile;
    use tokio::sync::RwLock;

    #[derive(Default)]
    struct TestStorage {
        qsos: RwLock<BTreeMap<String, QsoRecord>>,
        sync_metadata: RwLock<SyncMetadata>,
        lookup_snapshots: RwLock<BTreeMap<String, LookupSnapshot>>,
    }

    impl TestStorage {
        fn new() -> Self {
            Self::default()
        }
    }

    impl EngineStorage for TestStorage {
        fn logbook(&self) -> &dyn LogbookStore {
            self
        }

        fn lookup_snapshots(&self) -> &dyn LookupSnapshotStore {
            self
        }

        fn backend_name(&self) -> &'static str {
            "test"
        }
    }

    #[tonic::async_trait]
    impl LogbookStore for TestStorage {
        async fn insert_qso(&self, qso: &QsoRecord) -> Result<(), StorageError> {
            let mut qsos = self.qsos.write().await;
            if qsos.contains_key(&qso.local_id) {
                return Err(StorageError::duplicate("qso", &qso.local_id));
            }
            qsos.insert(qso.local_id.clone(), qso.clone());
            Ok(())
        }

        async fn update_qso(&self, qso: &QsoRecord) -> Result<bool, StorageError> {
            let mut qsos = self.qsos.write().await;
            Ok(qsos.insert(qso.local_id.clone(), qso.clone()).is_some())
        }

        async fn update_qrz_sync_metadata(
            &self,
            local_id: &str,
            expected_updated_at: Option<&prost_types::Timestamp>,
            qrz_logid: &str,
        ) -> Result<Option<QsoRecord>, StorageError> {
            let mut qsos = self.qsos.write().await;
            let Some(record) = qsos.get_mut(local_id) else {
                return Ok(None);
            };

            let unchanged_since_upload_started = record.updated_at.as_ref() == expected_updated_at;
            record.qrz_logid = Some(qrz_logid.to_string());
            record.sync_status = if unchanged_since_upload_started {
                SyncStatus::Synced as i32
            } else if record.sync_status == SyncStatus::Conflict as i32 {
                record.sync_status
            } else {
                SyncStatus::Modified as i32
            };

            Ok(Some(record.clone()))
        }

        async fn delete_qso(&self, local_id: &str) -> Result<bool, StorageError> {
            Ok(self.qsos.write().await.remove(local_id).is_some())
        }

        async fn soft_delete_qso(
            &self,
            local_id: &str,
            deleted_at_ms: i64,
            pending_remote_delete: bool,
        ) -> Result<bool, StorageError> {
            let mut qsos = self.qsos.write().await;
            let Some(record) = qsos.get_mut(local_id) else {
                return Ok(false);
            };
            record.deleted_at = Some(millis_to_timestamp(deleted_at_ms));
            record.pending_remote_delete = pending_remote_delete;
            Ok(true)
        }

        async fn restore_qso(&self, local_id: &str) -> Result<bool, StorageError> {
            let mut qsos = self.qsos.write().await;
            let Some(record) = qsos.get_mut(local_id) else {
                return Ok(false);
            };
            record.deleted_at = None;
            record.pending_remote_delete = false;
            Ok(true)
        }

        async fn get_qso(&self, local_id: &str) -> Result<Option<QsoRecord>, StorageError> {
            Ok(self.qsos.read().await.get(local_id).cloned())
        }

        async fn list_qsos(&self, _query: &QsoListQuery) -> Result<Vec<QsoRecord>, StorageError> {
            Ok(self.qsos.read().await.values().cloned().collect())
        }

        async fn list_qso_history(
            &self,
            _worked_callsign: &str,
            _limit: u32,
        ) -> Result<QsoHistoryPage, StorageError> {
            Ok(QsoHistoryPage::default())
        }

        async fn qso_counts(&self) -> Result<LogbookCounts, StorageError> {
            Ok(LogbookCounts::default())
        }

        async fn get_sync_metadata(&self) -> Result<SyncMetadata, StorageError> {
            Ok(self.sync_metadata.read().await.clone())
        }

        async fn upsert_sync_metadata(&self, metadata: &SyncMetadata) -> Result<(), StorageError> {
            *self.sync_metadata.write().await = metadata.clone();
            Ok(())
        }

        async fn purge_deleted_qsos(
            &self,
            _local_ids: &[String],
            _older_than_ms: Option<i64>,
        ) -> Result<u32, StorageError> {
            Ok(0)
        }
    }

    #[tonic::async_trait]
    impl LookupSnapshotStore for TestStorage {
        async fn get_lookup_snapshot(
            &self,
            callsign: &str,
        ) -> Result<Option<LookupSnapshot>, StorageError> {
            Ok(self.lookup_snapshots.read().await.get(callsign).cloned())
        }

        async fn upsert_lookup_snapshot(
            &self,
            snapshot: &LookupSnapshot,
        ) -> Result<(), StorageError> {
            self.lookup_snapshots
                .write()
                .await
                .insert(snapshot.callsign.clone(), snapshot.clone());
            Ok(())
        }

        async fn delete_lookup_snapshot(&self, callsign: &str) -> Result<bool, StorageError> {
            Ok(self
                .lookup_snapshots
                .write()
                .await
                .remove(callsign)
                .is_some())
        }
    }

    fn millis_to_timestamp(millis: i64) -> prost_types::Timestamp {
        let seconds = millis.div_euclid(1_000);
        let nanos = i32::try_from(millis.rem_euclid(1_000))
            .unwrap_or(0)
            .saturating_mul(1_000_000);
        prost_types::Timestamp { seconds, nanos }
    }

    fn sample_adif() -> &'static [u8] {
        b"<STATION_CALLSIGN:4>W1AW <CALL:5>K7ABC <QSO_DATE:8>20250101 <TIME_ON:4>1200 <QSO_DATE_OFF:8>20250101 <TIME_OFF:6>120100 <BAND:3>20M <MODE:3>FT8 <RST_SENT:3>-10 <RST_RCVD:3>-12 <NOTES:13>Operator note <EOR>"
    }

    fn sample_adif_with_rst_sent(rst_sent: &str) -> Vec<u8> {
        format!(
            "<STATION_CALLSIGN:4>W1AW <CALL:5>K7ABC <QSO_DATE:8>20250101 <TIME_ON:4>1200 <BAND:3>20M <MODE:3>FT8 <RST_SENT:{}>{rst_sent} <RST_RCVD:3>-12 <EOR>",
            rst_sent.len()
        )
        .into_bytes()
    }

    fn wsjtx_frame(message_type: u32, adif: &str) -> Vec<u8> {
        let mut frame = Vec::new();
        append_be_u32(&mut frame, WSJTX_MAGIC);
        append_be_u32(&mut frame, 2);
        append_be_u32(&mut frame, message_type);
        append_wsjtx_utf8(&mut frame, "WSJT-X");
        append_wsjtx_utf8(&mut frame, adif);
        frame
    }

    fn append_be_u32(target: &mut Vec<u8>, value: u32) {
        target.extend_from_slice(&value.to_be_bytes());
    }

    fn append_wsjtx_utf8(target: &mut Vec<u8>, value: &str) {
        let len = u32::try_from(value.len()).expect("test string length");
        append_be_u32(target, len);
        target.extend_from_slice(value.as_bytes());
    }

    fn assert_wsjtx_diagnostic(qso: &QsoRecord, source: &str, engine: &str) {
        let diagnostic = qso
            .extra_fields
            .get(WSJTX_IMPORT_DIAGNOSTIC_FIELD)
            .expect("WSJT-X import diagnostic field");
        assert!(diagnostic.starts_with(WSJTX_IMPORT_DIAGNOSTIC_PREFIX));
        assert!(diagnostic.contains("imported_at_utc="));
        assert!(diagnostic.contains("qso_end_to_import_ms="));
        assert!(!diagnostic.contains("qso_end_to_import_ms=unavailable"));
        assert!(diagnostic.contains(&format!("source={source}")));
        assert!(diagnostic.contains(&format!("engine={engine}")));
        assert!(qso
            .notes
            .as_deref()
            .unwrap_or_default()
            .contains(diagnostic));
        assert!(qso
            .notes
            .as_deref()
            .unwrap_or_default()
            .contains("Operator note"));
    }

    #[test]
    fn wsjtx_import_note_includes_qso_end_delta_milliseconds() {
        let qso = QsoRecord {
            utc_end_timestamp: Some(prost_types::Timestamp {
                seconds: 99,
                nanos: 250_000_000,
            }),
            ..QsoRecord::default()
        };
        let imported_at = time::OffsetDateTime::from_unix_timestamp_nanos(100_500_000_000)
            .expect("import timestamp");

        let note = wsjtx_import_note(WSJTX_UDP_SOURCE, &qso, imported_at);

        assert!(note.contains("qso_end_to_import_ms=1250"));
    }

    #[test]
    fn extract_adif_payload_accepts_json_wrapped_adif() {
        let payload = br#"{"type":"logged_qso","adif":"<CALL:5>K7ABC <EOR>"}"#;

        let extracted = extract_adif_payload(payload)
            .expect("payload")
            .expect("adif");

        assert_eq!(String::from_utf8(extracted).unwrap(), "<CALL:5>K7ABC <EOR>");
    }

    #[test]
    fn wsjtx_logged_adif_frame_extracts_adif_payload() {
        let payload = wsjtx_frame(WSJTX_LOGGED_ADIF_MESSAGE_TYPE, "<CALL:5>K7ABC <EOR>");

        let extracted = extract_adif_payload(&payload)
            .expect("payload")
            .expect("logged adif");

        assert_eq!(String::from_utf8(extracted).unwrap(), "<CALL:5>K7ABC <EOR>");
    }

    #[test]
    fn wsjtx_non_logged_frame_is_ignored() {
        let payload = wsjtx_frame(0, "<CALL:5>K7ABC <EOR>");

        let extracted = extract_adif_payload(&payload).expect("payload");

        assert_eq!(extracted, None);
    }

    #[tokio::test]
    async fn ingest_wsjtx_udp_datagram_imports_qso_from_json_payload() {
        let engine = LogbookEngine::new(Arc::new(TestStorage::new()));
        let payload = br#"{"type":"logged_qso","adif":"<STATION_CALLSIGN:4>W1AW <CALL:5>K7ABC <QSO_DATE:8>20250101 <TIME_ON:4>1200 <BAND:3>20M <MODE:3>FT8 <RST_SENT:3>-10 <RST_RCVD:3>-12 <EOR>"}"#;

        let summary = ingest_wsjtx_udp_datagram(&engine, payload, None, false)
            .await
            .expect("import succeeded");

        assert_eq!(summary.records_imported, 1);
        assert_eq!(
            engine
                .list_qsos(&QsoListQuery::default())
                .await
                .expect("qsos")
                .len(),
            1
        );
    }

    #[tokio::test]
    async fn ingest_wsjtx_udp_datagram_imports_qso_from_logged_adif_frame() {
        let engine = LogbookEngine::new(Arc::new(TestStorage::new()));
        let payload = wsjtx_frame(
            WSJTX_LOGGED_ADIF_MESSAGE_TYPE,
            std::str::from_utf8(sample_adif()).expect("sample adif"),
        );

        let summary = ingest_wsjtx_udp_datagram(&engine, &payload, None, false)
            .await
            .expect("import succeeded");

        assert_eq!(summary.records_imported, 1);
        assert_eq!(summary.records_skipped, 0);
        let qso = engine
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("qsos")
            .into_iter()
            .next()
            .expect("imported qso");
        assert_wsjtx_diagnostic(&qso, WSJTX_UDP_SOURCE, "rust");
    }

    #[tokio::test]
    async fn ingest_wsjtx_udp_datagram_skips_duplicate_logged_adif_frame() {
        let engine = LogbookEngine::new(Arc::new(TestStorage::new()));
        let payload = wsjtx_frame(
            WSJTX_LOGGED_ADIF_MESSAGE_TYPE,
            std::str::from_utf8(sample_adif()).expect("sample adif"),
        );

        let first = ingest_wsjtx_udp_datagram(&engine, &payload, None, false)
            .await
            .expect("first import");
        let second = ingest_wsjtx_udp_datagram(&engine, &payload, None, false)
            .await
            .expect("second import");

        assert_eq!(first.records_imported, 1);
        assert_eq!(second.records_imported, 0);
        assert_eq!(second.records_skipped, 1);
    }

    #[tokio::test]
    async fn ingest_wsjtx_udp_datagram_refreshes_changed_duplicate_but_skips_exact_replay() {
        let storage = Arc::new(TestStorage::new());
        let engine = LogbookEngine::new(storage.clone());
        let first_payload = wsjtx_frame(
            WSJTX_LOGGED_ADIF_MESSAGE_TYPE,
            std::str::from_utf8(&sample_adif_with_rst_sent("-10")).expect("sample adif"),
        );
        let changed_payload = wsjtx_frame(
            WSJTX_LOGGED_ADIF_MESSAGE_TYPE,
            std::str::from_utf8(&sample_adif_with_rst_sent("-05")).expect("changed adif"),
        );

        let first = ingest_wsjtx_udp_datagram(&engine, &first_payload, None, true)
            .await
            .expect("first import");
        let replay = ingest_wsjtx_udp_datagram(&engine, &first_payload, None, true)
            .await
            .expect("exact replay");
        let mut synced_qso = engine
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("qsos")
            .into_iter()
            .next()
            .expect("imported qso");
        synced_qso.sync_status = SyncStatus::Synced as i32;
        synced_qso.qrz_logid = Some("remote-1".to_string());
        storage.update_qso(&synced_qso).await.expect("mark synced");

        let changed = ingest_wsjtx_udp_datagram(&engine, &changed_payload, None, true)
            .await
            .expect("changed import");
        let qsos = engine
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("qsos");

        assert_eq!(first.records_imported, 1);
        assert_eq!(replay.records_skipped, 1);
        assert_eq!(changed.records_updated, 1);
        assert_eq!(qsos.len(), 1);
        let qso = qsos.first().expect("single qso");
        assert_eq!(
            qso.rst_sent.as_ref().map(|rst| rst.raw.as_str()),
            Some("-05")
        );
        assert_eq!(qso.sync_status, SyncStatus::Modified as i32);
        assert_eq!(qso.qrz_logid.as_deref(), Some("remote-1"));
    }

    #[tokio::test]
    async fn ingest_wsjtx_adif_tail_imports_appended_records() {
        let engine = LogbookEngine::new(Arc::new(TestStorage::new()));
        let file = NamedTempFile::new().expect("temp file");
        let path = file.path().to_path_buf();

        fs::write(&path, sample_adif()).expect("write initial record");

        let mut cursor = 0;
        let summary = ingest_wsjtx_adif_tail(&engine, &path, None, false, &mut cursor)
            .await
            .expect("tail import");

        assert_eq!(summary.records_imported, 1);
        let metadata_len =
            usize::try_from(fs::metadata(&path).unwrap().len()).expect("metadata len");
        assert_eq!(cursor, metadata_len);
        let qso = engine
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("qsos")
            .into_iter()
            .next()
            .expect("imported qso");
        assert_wsjtx_diagnostic(&qso, WSJTX_ADIF_TAIL_SOURCE, "rust");
    }

    #[tokio::test]
    async fn first_wsjtx_ingest_source_remains_authoritative() {
        let engine = LogbookEngine::new(Arc::new(TestStorage::new()));
        let file = NamedTempFile::new().expect("temp file");
        let path = file.path().to_path_buf();
        fs::write(&path, sample_adif()).expect("write initial record");
        let mut cursor = 0;

        ingest_wsjtx_adif_tail(&engine, &path, None, false, &mut cursor)
            .await
            .expect("tail import");
        let payload = wsjtx_frame(
            WSJTX_LOGGED_ADIF_MESSAGE_TYPE,
            std::str::from_utf8(sample_adif()).expect("sample adif"),
        );
        ingest_wsjtx_udp_datagram(&engine, &payload, None, true)
            .await
            .expect("udp refresh");

        let qso = engine
            .list_qsos(&QsoListQuery::default())
            .await
            .expect("qsos")
            .into_iter()
            .next()
            .expect("imported qso");
        assert_wsjtx_diagnostic(&qso, WSJTX_ADIF_TAIL_SOURCE, "rust");
        assert!(!qso
            .notes
            .as_deref()
            .unwrap_or_default()
            .contains(WSJTX_UDP_SOURCE));
    }

    #[tokio::test]
    async fn ingest_wsjtx_adif_tail_waits_for_complete_record_before_advancing_cursor() {
        let engine = LogbookEngine::new(Arc::new(TestStorage::new()));
        let file = NamedTempFile::new().expect("temp file");
        let path = file.path().to_path_buf();
        let mut partial = sample_adif().to_vec();
        partial.truncate(partial.len() - "<EOR>".len());
        fs::write(&path, &partial).expect("write partial record");

        let mut cursor = 0;
        let partial_summary = ingest_wsjtx_adif_tail(&engine, &path, None, true, &mut cursor)
            .await
            .expect("partial tail import");

        assert_eq!(partial_summary, AdifImportSummary::default());
        assert_eq!(cursor, 0);

        partial.extend_from_slice(b"<EOR>");
        fs::write(&path, &partial).expect("complete record");
        let complete_summary = ingest_wsjtx_adif_tail(&engine, &path, None, true, &mut cursor)
            .await
            .expect("complete tail import");

        assert_eq!(complete_summary.records_imported, 1);
        assert_eq!(cursor, partial.len());
    }

    #[tokio::test]
    async fn ingest_wsjtx_adif_tail_replay_matches_original_import_after_local_edit_and_delete() {
        let storage = Arc::new(TestStorage::new());
        let engine = LogbookEngine::new(storage.clone());
        let file = NamedTempFile::new().expect("temp file");
        let path = file.path().to_path_buf();
        fs::write(&path, sample_adif()).expect("write initial record");

        let mut cursor = 0;
        let first = ingest_wsjtx_adif_tail(&engine, &path, None, false, &mut cursor)
            .await
            .expect("first import");
        let mut edited = first.affected_qsos.first().expect("affected qso").clone();
        edited.worked_callsign = "K7EDIT".to_string();
        edited.deleted_at = Some(millis_to_timestamp(1_700_000_500_000));
        storage.update_qso(&edited).await.expect("operator edit");

        cursor = 0;
        let replay = ingest_wsjtx_adif_tail(&engine, &path, None, false, &mut cursor)
            .await
            .expect("replay import");
        let stored = storage.qsos.read().await;

        assert_eq!(first.records_imported, 1);
        assert_eq!(replay.records_imported, 0);
        assert_eq!(replay.records_skipped, 1);
        assert_eq!(stored.len(), 1);
        assert!(stored
            .values()
            .next()
            .expect("stored qso")
            .deleted_at
            .is_some());
    }

    #[test]
    fn complete_adif_prefix_len_returns_last_complete_eor_end() {
        let payload = b"<CALL:3>AAA <EOR><CALL:3>BBB";

        assert_eq!(
            complete_adif_prefix_len(payload),
            Some(b"<CALL:3>AAA <EOR>".len())
        );
    }

    #[test]
    fn complete_adif_prefix_len_ignores_eor_inside_field_value() {
        let payload = b"<CALL:4>W1AW<COMMENT:11>abc<EOR>def";

        assert_eq!(complete_adif_prefix_len(payload), None);
    }

    #[test]
    fn complete_adif_prefix_len_counts_adif_field_lengths_as_utf8_characters() {
        let payload = "<COMMENT:11>éééééé<EOR>".as_bytes();

        assert_eq!(complete_adif_prefix_len(payload), None);
    }
}
