//! QSO callsign enrichment backfill workflow.

use std::{collections::BTreeMap, sync::Arc};

use tokio::sync::mpsc;

use crate::{
    domain::lookup::normalize_callsign,
    lookup::LookupCoordinator,
    proto::qsoripper::domain::{CallsignRecord, LookupState, QsoRecord},
    storage::{LogbookStore, QsoListQuery},
};

/// Cumulative progress for one backfill operation.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct EnrichmentBackfillProgress {
    /// Active QSO rows read from storage.
    pub scanned: u32,
    /// Rows that have at least one missing enrichment field.
    pub candidates: u32,
    /// Distinct normalized callsigns selected for lookup.
    pub unique_callsigns: u32,
    /// Callsigns that returned a record.
    pub found: u32,
    /// Callsigns that the provider did not find.
    pub not_found: u32,
    /// Callsigns that failed lookup.
    pub errors: u32,
    /// Rows that would change or changed successfully.
    pub changed: u32,
    /// Candidate rows that received no values.
    pub unchanged: u32,
    /// Rows skipped because their persisted snapshot changed.
    pub concurrent_edits: u32,
    /// Storage operations that failed.
    pub storage_errors: u32,
    /// Whether this is the terminal progress update.
    pub complete: bool,
    /// Callsign processed by the latest progress update.
    pub current_callsign: Option<String>,
}

/// Backfill missing enrichment fields on active QSO records.
///
/// Lookups run one at a time so this maintenance operation does not occupy the
/// interactive lookup concurrency budget.
pub async fn run_enrichment_backfill(
    store: &dyn LogbookStore,
    coordinator: &Arc<LookupCoordinator>,
    query: &QsoListQuery,
    apply: bool,
    progress_sender: &mpsc::Sender<EnrichmentBackfillProgress>,
) {
    let mut progress = EnrichmentBackfillProgress::default();
    let Ok(records) = store.list_qsos(query).await else {
        progress.storage_errors = 1;
        progress.complete = true;
        let _ = progress_sender.send(progress).await;
        return;
    };

    progress.scanned = u32::try_from(records.len()).unwrap_or(u32::MAX);
    let mut grouped = BTreeMap::<String, Vec<QsoRecord>>::new();
    for record in records {
        if !needs_enrichment(&record) {
            continue;
        }

        let callsign = record.worked_callsign.trim();
        if callsign.is_empty() {
            continue;
        }

        progress.candidates += 1;
        grouped
            .entry(normalize_callsign(callsign))
            .or_default()
            .push(record);
    }
    progress.unique_callsigns = u32::try_from(grouped.len()).unwrap_or(u32::MAX);

    if progress_sender.send(progress.clone()).await.is_err() {
        return;
    }

    for (callsign, records) in grouped {
        let lookup_task = coordinator.lookup(&callsign, false);
        tokio::pin!(lookup_task);
        let lookup = tokio::select! {
            () = progress_sender.closed() => {
                let _ = lookup_task.await;
                return;
            },
            result = &mut lookup_task => result,
        };
        match LookupState::try_from(lookup.state).unwrap_or(LookupState::Error) {
            LookupState::Found => {
                if let Some(callsign_record) = lookup.record.as_ref() {
                    progress.found += 1;
                    for record in records {
                        if progress_sender.is_closed() {
                            return;
                        }
                        let mut replacement = record.clone();
                        if merge_missing_enrichment(&mut replacement, callsign_record) {
                            if apply {
                                match store.update_qso_if_unchanged(&record, &replacement).await {
                                    Ok(true) => progress.changed += 1,
                                    Ok(false) => progress.concurrent_edits += 1,
                                    Err(_) => progress.storage_errors += 1,
                                }
                            } else {
                                progress.changed += 1;
                            }
                        } else {
                            progress.unchanged += 1;
                        }
                    }
                } else {
                    progress.errors += 1;
                    progress.unchanged = progress
                        .unchanged
                        .saturating_add(u32::try_from(records.len()).unwrap_or(u32::MAX));
                }
            }
            LookupState::NotFound => {
                progress.not_found += 1;
                progress.unchanged = progress
                    .unchanged
                    .saturating_add(u32::try_from(records.len()).unwrap_or(u32::MAX));
            }
            _ => {
                progress.errors += 1;
                progress.unchanged = progress
                    .unchanged
                    .saturating_add(u32::try_from(records.len()).unwrap_or(u32::MAX));
            }
        }

        progress.current_callsign = Some(callsign.clone());
        if progress_sender.send(progress.clone()).await.is_err() {
            return;
        }
    }

    progress.complete = true;
    progress.current_callsign = None;
    let _ = progress_sender.send(progress).await;
}

fn needs_enrichment(qso: &QsoRecord) -> bool {
    string_is_missing(qso.worked_operator_name.as_deref())
        || string_is_missing(qso.worked_grid.as_deref())
        || string_is_missing(qso.worked_country.as_deref())
        || qso.worked_dxcc.is_none()
        || string_is_missing(qso.worked_state.as_deref())
        || string_is_missing(qso.worked_county.as_deref())
        || qso.worked_cq_zone.is_none()
        || qso.worked_itu_zone.is_none()
        || string_is_missing(qso.worked_continent.as_deref())
        || qso.worked_latitude.is_none()
        || qso.worked_longitude.is_none()
}

fn merge_missing_enrichment(qso: &mut QsoRecord, record: &CallsignRecord) -> bool {
    let mut changed = false;
    let name = record
        .formatted_name
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .map(str::to_owned)
        .or_else(|| {
            let value = format!("{} {}", record.first_name, record.last_name);
            (!value.trim().is_empty()).then(|| value.trim().to_string())
        });

    changed |= fill_string(&mut qso.worked_operator_name, name.as_deref());
    changed |= fill_string(&mut qso.worked_grid, record.grid_square.as_deref());
    changed |= fill_string(
        &mut qso.worked_country,
        record
            .dxcc_country_name
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .or(record.country.as_deref()),
    );
    changed |= fill_u32(
        &mut qso.worked_dxcc,
        (record.dxcc_entity_id != 0).then_some(record.dxcc_entity_id),
    );
    changed |= fill_string(&mut qso.worked_state, record.state.as_deref());
    changed |= fill_string(&mut qso.worked_county, record.county.as_deref());
    changed |= fill_u32(&mut qso.worked_cq_zone, record.cq_zone);
    changed |= fill_u32(&mut qso.worked_itu_zone, record.itu_zone);
    changed |= fill_string(&mut qso.worked_continent, record.dxcc_continent.as_deref());
    changed |= fill_f64(&mut qso.worked_latitude, record.latitude);
    changed |= fill_f64(&mut qso.worked_longitude, record.longitude);
    changed
}

fn string_is_missing(value: Option<&str>) -> bool {
    value.is_none_or(|value| value.trim().is_empty())
}

fn fill_string(target: &mut Option<String>, source: Option<&str>) -> bool {
    if !string_is_missing(target.as_deref()) {
        return false;
    }
    let Some(source) = source.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    *target = Some(source.to_string());
    true
}

fn fill_u32(target: &mut Option<u32>, source: Option<u32>) -> bool {
    if target.is_some() || source.is_none() {
        return false;
    }
    *target = source;
    true
}

fn fill_f64(target: &mut Option<f64>, source: Option<f64>) -> bool {
    if target.is_some() || source.is_none() {
        return false;
    }
    *target = source;
    true
}

#[cfg(test)]
mod tests {
    use super::{merge_missing_enrichment, needs_enrichment};
    use crate::proto::qsoripper::domain::{CallsignRecord, QsoRecord, SyncStatus};

    #[test]
    fn merge_fills_blank_fields_without_overwriting_values_or_sync_metadata() {
        let mut qso = QsoRecord {
            worked_operator_name: Some(" \t".into()),
            worked_grid: Some("CN87".into()),
            worked_country: Some(String::new()),
            worked_dxcc: Some(291),
            worked_state: Some("WA".into()),
            sync_status: SyncStatus::Synced as i32,
            qrz_logid: Some("123".into()),
            ..Default::default()
        };
        let source = CallsignRecord {
            first_name: "Ada".into(),
            last_name: "Lovelace".into(),
            grid_square: Some("FN31".into()),
            dxcc_country_name: Some("  ".into()),
            country: Some("United States".into()),
            dxcc_entity_id: 110,
            state: Some("CT".into()),
            county: Some("Hartford".into()),
            cq_zone: Some(5),
            itu_zone: Some(8),
            dxcc_continent: Some("NA".into()),
            latitude: Some(41.7),
            longitude: Some(-72.7),
            ..Default::default()
        };

        assert!(merge_missing_enrichment(&mut qso, &source));
        assert_eq!(qso.worked_operator_name.as_deref(), Some("Ada Lovelace"));
        assert_eq!(qso.worked_grid.as_deref(), Some("CN87"));
        assert_eq!(qso.worked_country.as_deref(), Some("United States"));
        assert_eq!(qso.worked_country.as_deref(), Some("United States"));
        assert_eq!(qso.worked_dxcc, Some(291));
        assert_eq!(qso.worked_state.as_deref(), Some("WA"));
        assert_eq!(qso.sync_status, SyncStatus::Synced as i32);
        assert_eq!(qso.qrz_logid.as_deref(), Some("123"));
        assert!(!needs_enrichment(&qso));
    }
}
