//! QSO enrichment backfill tests with a fake callsign provider.

#![allow(clippy::expect_used)]

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use prost_types::Timestamp;
use qsoripper_core::application::enrichment_backfill::run_enrichment_backfill;
use qsoripper_core::lookup::{
    CallsignProvider, LookupCoordinator, LookupCoordinatorConfig, ProviderLookup,
    ProviderLookupError,
};
use qsoripper_core::proto::qsoripper::domain::{Band, CallsignRecord, Mode, QsoRecord, SyncStatus};
use qsoripper_core::storage::{DeletedRecordsFilter, LogbookStore, QsoListQuery, QsoSortOrder};
use qsoripper_storage_memory::MemoryStorage;

#[derive(Clone)]
struct FakeProvider {
    calls: Arc<AtomicUsize>,
    record: CallsignRecord,
    edit_store: Option<Arc<MemoryStorage>>,
    delay: Duration,
}

#[tonic::async_trait]
impl CallsignProvider for FakeProvider {
    async fn lookup_callsign(
        &self,
        _callsign: &str,
    ) -> Result<ProviderLookup, ProviderLookupError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        if let Some(store) = &self.edit_store {
            let mut edited = store
                .get_qso("active")
                .await
                .expect("read concurrent row")
                .expect("concurrent row exists");
            edited.notes = Some("operator edit".into());
            store
                .update_qso(&edited)
                .await
                .expect("write concurrent row");
        }
        Ok(ProviderLookup::found(self.record.clone(), Vec::new()))
    }
}

#[tokio::test]
async fn preview_deduplicates_and_apply_fills_only_missing_active_fields() {
    let store = Arc::new(MemoryStorage::new());
    let mut first = qso("one", " w1aw ");
    first.worked_grid = Some(" \t".into());
    first.worked_country = Some("Keep Country".into());
    first.sync_status = SyncStatus::Synced as i32;
    first.qrz_logid = Some("qrz-123".into());
    store.insert_qso(&first).await.expect("insert first");
    store
        .insert_qso(&qso("two", "W1AW"))
        .await
        .expect("insert second");
    store
        .insert_qso(&qso("portable", "W1AW/P"))
        .await
        .expect("insert portable");
    let mut deleted = qso("deleted", "K1ABC");
    deleted.deleted_at = Some(Timestamp {
        seconds: 1_786_157_400,
        nanos: 0,
    });
    store.insert_qso(&deleted).await.expect("insert deleted");

    let calls = Arc::new(AtomicUsize::new(0));
    let coordinator = Arc::new(LookupCoordinator::new(
        Arc::new(FakeProvider {
            calls: Arc::clone(&calls),
            record: callsign_record(),
            edit_store: None,
            delay: Duration::ZERO,
        }),
        LookupCoordinatorConfig::new(Duration::from_secs(300), Duration::from_secs(60)),
    ));
    let query = active_query();

    let preview = execute(&store, &coordinator, &query, false).await;
    assert!(preview.complete);
    assert_eq!(preview.scanned, 3);
    assert_eq!(preview.candidates, 3);
    assert_eq!(preview.unique_callsigns, 2);
    assert_eq!(preview.changed, 3);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        store
            .get_qso("one")
            .await
            .expect("read first")
            .expect("first exists")
            .worked_grid
            .as_deref(),
        Some(" \t")
    );

    let applied = execute(&store, &coordinator, &query, true).await;
    assert_eq!(applied.changed, 3);
    let saved = store
        .get_qso("one")
        .await
        .expect("read first")
        .expect("first exists");
    assert_eq!(saved.worked_grid.as_deref(), Some("FN31"));
    assert_eq!(saved.worked_country.as_deref(), Some("Keep Country"));
    assert_eq!(saved.sync_status, SyncStatus::Synced as i32);
    assert_eq!(saved.qrz_logid.as_deref(), Some("qrz-123"));
    assert!(store
        .get_qso("deleted")
        .await
        .expect("read deleted")
        .expect("deleted exists")
        .worked_grid
        .is_none());

    let repeated = execute(&store, &coordinator, &query, true).await;
    assert_eq!(repeated.candidates, 0);
    assert_eq!(repeated.changed, 0);
}

#[tokio::test]
async fn apply_counts_concurrent_edit_and_preserves_operator_change() {
    let store = Arc::new(MemoryStorage::new());
    store
        .insert_qso(&qso("active", "W1AW"))
        .await
        .expect("insert active");
    let calls = Arc::new(AtomicUsize::new(0));
    let coordinator = Arc::new(LookupCoordinator::new(
        Arc::new(FakeProvider {
            calls: Arc::clone(&calls),
            record: callsign_record(),
            edit_store: Some(Arc::clone(&store)),
            delay: Duration::ZERO,
        }),
        LookupCoordinatorConfig::default(),
    ));

    let summary = execute(&store, &coordinator, &active_query(), true).await;

    assert_eq!(summary.concurrent_edits, 1);
    let saved = store
        .get_qso("active")
        .await
        .expect("read active")
        .expect("active exists");
    assert_eq!(saved.notes.as_deref(), Some("operator edit"));
    assert!(saved.worked_grid.is_none());
}

#[tokio::test]
async fn client_disconnect_stops_scheduling_but_waits_for_the_active_lookup() {
    let store = Arc::new(MemoryStorage::new());
    store
        .insert_qso(&qso("active", "W1AW"))
        .await
        .expect("insert active");
    let calls = Arc::new(AtomicUsize::new(0));
    let coordinator = Arc::new(LookupCoordinator::new(
        Arc::new(FakeProvider {
            calls: Arc::clone(&calls),
            record: callsign_record(),
            edit_store: None,
            delay: Duration::from_millis(200),
        }),
        LookupCoordinatorConfig::default(),
    ));
    let (sender, mut receiver) = tokio::sync::mpsc::channel(1);
    let mut run = tokio::spawn({
        let store = Arc::clone(&store);
        let coordinator = Arc::clone(&coordinator);
        async move {
            run_enrichment_backfill(store.as_ref(), &coordinator, &active_query(), true, &sender)
                .await;
        }
    });

    receiver.recv().await.expect("initial progress");
    while calls.load(Ordering::SeqCst) == 0 {
        tokio::task::yield_now().await;
    }
    drop(receiver);

    assert!(
        tokio::time::timeout(Duration::from_millis(25), &mut run)
            .await
            .is_err(),
        "backfill must retain its active provider operation"
    );
    tokio::time::timeout(Duration::from_secs(1), run)
        .await
        .expect("backfill stops after active lookup")
        .expect("backfill task completes");
    assert!(store
        .get_qso("active")
        .await
        .expect("read active")
        .expect("active exists")
        .worked_grid
        .is_none());
}

async fn execute(
    store: &MemoryStorage,
    coordinator: &Arc<LookupCoordinator>,
    query: &QsoListQuery,
    apply: bool,
) -> qsoripper_core::application::enrichment_backfill::EnrichmentBackfillProgress {
    let (sender, mut receiver) = tokio::sync::mpsc::channel(16);
    run_enrichment_backfill(store, coordinator, query, apply, &sender).await;
    drop(sender);
    let mut final_progress = None;
    while let Some(progress) = receiver.recv().await {
        final_progress = Some(progress);
    }
    final_progress.expect("terminal progress")
}

fn active_query() -> QsoListQuery {
    QsoListQuery {
        sort: QsoSortOrder::OldestFirst,
        deleted_filter: DeletedRecordsFilter::ActiveOnly,
        ..QsoListQuery::default()
    }
}

fn qso(local_id: &str, callsign: &str) -> QsoRecord {
    QsoRecord {
        local_id: local_id.into(),
        station_callsign: "K7RND".into(),
        worked_callsign: callsign.into(),
        utc_timestamp: Some(Timestamp {
            seconds: 1_786_157_400,
            nanos: 0,
        }),
        band: Band::Band20m as i32,
        mode: Mode::Cw as i32,
        ..Default::default()
    }
}

fn callsign_record() -> CallsignRecord {
    CallsignRecord {
        callsign: "W1AW".into(),
        first_name: "Ada".into(),
        last_name: "Lovelace".into(),
        grid_square: Some("FN31".into()),
        country: Some("United States".into()),
        dxcc_entity_id: 291,
        state: Some("CT".into()),
        county: Some("Hartford".into()),
        cq_zone: Some(5),
        itu_zone: Some(8),
        dxcc_continent: Some("NA".into()),
        latitude: Some(41.7),
        longitude: Some(-72.7),
        ..Default::default()
    }
}
