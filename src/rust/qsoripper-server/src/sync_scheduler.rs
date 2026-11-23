//! Periodic background sync scheduler.
//!
//! Spawns a cancellable Tokio task that reads runtime configuration each cycle,
//! builds a [`QrzLogbookClient`], and runs [`execute_sync`] at the configured
//! interval.  The scheduler exposes read-only accessors so the gRPC handler can
//! report live sync state in [`GetSyncStatusResponse`].

use std::sync::Arc;

use prost_types::Timestamp;
use tokio::sync::{watch, Mutex};
use tokio::time::Duration;

use qsoripper_core::proto::qsoripper::domain::ConflictPolicy;
use qsoripper_core::qrz_logbook::{QrzLogbookClient, QrzLogbookConfig};

use crate::runtime_config::{
    RuntimeConfigManager, DEFAULT_QRZ_LOGBOOK_BASE_URL, QRZ_LOGBOOK_API_KEY_ENV_VAR,
    QRZ_LOGBOOK_BASE_URL_ENV_VAR, SYNC_AUTO_ENABLED_ENV_VAR, SYNC_CONFLICT_POLICY_ENV_VAR,
    SYNC_INTERVAL_SECONDS_ENV_VAR,
};
use crate::sync;

// ---------------------------------------------------------------------------
// Public state
// ---------------------------------------------------------------------------

/// Live state produced by the background sync loop and consumed by the gRPC
/// handler to populate `GetSyncStatusResponse`.
pub(crate) struct SyncScheduler {
    is_syncing: Arc<Mutex<bool>>,
    last_error: Arc<Mutex<Option<String>>>,
    next_sync: Arc<Mutex<Option<Timestamp>>>,
    qrz_upload_lock: Arc<Mutex<()>>,
    cancel_tx: watch::Sender<bool>,
}

impl SyncScheduler {
    /// Create a new scheduler.  Call [`start`](Self::start) to spawn the
    /// background loop.
    pub(crate) fn new(qrz_upload_lock: Arc<Mutex<()>>) -> Self {
        let (cancel_tx, _) = watch::channel(false);
        Self {
            is_syncing: Arc::new(Mutex::new(false)),
            last_error: Arc::new(Mutex::new(None)),
            next_sync: Arc::new(Mutex::new(None)),
            qrz_upload_lock,
            cancel_tx,
        }
    }

    /// Spawn the periodic sync loop.
    ///
    /// The task re-reads runtime configuration every cycle so that changes to
    /// the sync interval, auto-sync toggle, or API key take effect without a
    /// server restart.
    pub(crate) fn start(&self, runtime_config: Arc<RuntimeConfigManager>) {
        let is_syncing = self.is_syncing.clone();
        let last_error = self.last_error.clone();
        let next_sync = self.next_sync.clone();
        let qrz_upload_lock = self.qrz_upload_lock.clone();
        let mut cancel_rx = self.cancel_tx.subscribe();

        tokio::spawn(async move {
            loop {
                // ── Read effective config ────────────────────────────
                let values = runtime_config.effective_values().await;

                let auto_enabled = values
                    .get(SYNC_AUTO_ENABLED_ENV_VAR)
                    .is_some_and(|v| v == "true");

                let interval_secs: u64 = values
                    .get(SYNC_INTERVAL_SECONDS_ENV_VAR)
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(300);

                // Clear next_sync when auto-sync is disabled.
                if !auto_enabled {
                    *next_sync.lock().await = None;
                    if wait_or_cancel(&mut cancel_rx, Duration::from_secs(30)).await {
                        break;
                    }
                    continue;
                }

                let api_key = values
                    .get(QRZ_LOGBOOK_API_KEY_ENV_VAR)
                    .cloned()
                    .unwrap_or_default();

                if api_key.trim().is_empty() {
                    *next_sync.lock().await = None;
                    if wait_or_cancel(&mut cancel_rx, Duration::from_secs(30)).await {
                        break;
                    }
                    continue;
                }

                // ── Compute and publish the next sync time ───────────
                let Some(wake_at) =
                    next_sync_timestamp_at(time::OffsetDateTime::now_utc(), interval_secs)
                else {
                    *next_sync.lock().await = None;
                    *last_error.lock().await =
                        Some("Sync interval exceeds the supported timestamp range.".to_string());
                    if wait_or_cancel(&mut cancel_rx, Duration::from_secs(30)).await {
                        break;
                    }
                    continue;
                };
                *next_sync.lock().await = Some(wake_at);

                // ── Wait for the interval (or cancellation) ──────────
                if wait_or_cancel(&mut cancel_rx, Duration::from_secs(interval_secs)).await {
                    break;
                }

                // ── Build client and execute sync ────────────────────
                // Re-read values since we slept for the full interval.
                let values = runtime_config.effective_values().await;

                let base_url = values
                    .get(QRZ_LOGBOOK_BASE_URL_ENV_VAR)
                    .cloned()
                    .unwrap_or_else(|| DEFAULT_QRZ_LOGBOOK_BASE_URL.to_string());

                // Re-read the API key in case it changed during the wait.
                let api_key = values
                    .get(QRZ_LOGBOOK_API_KEY_ENV_VAR)
                    .cloned()
                    .unwrap_or_default();
                if api_key.trim().is_empty() {
                    continue;
                }

                let conflict_policy =
                    match values.get(SYNC_CONFLICT_POLICY_ENV_VAR).map(String::as_str) {
                        Some("flag_for_review") => ConflictPolicy::FlagForReview,
                        _ => ConflictPolicy::LastWriteWins,
                    };

                let config = QrzLogbookConfig::new(api_key, base_url, "QsoRipper/1.0".to_string());

                let client = match QrzLogbookClient::new(config) {
                    Ok(c) => c,
                    Err(err) => {
                        eprintln!("[sync-scheduler] Failed to create QRZ client: {err}");
                        *last_error.lock().await =
                            Some(format!("Failed to create QRZ client: {err}"));
                        continue;
                    }
                };

                let store = runtime_config.logbook_engine().await;
                let logbook_store = store.logbook_store();

                *is_syncing.lock().await = true;
                // Use a channel that we drain immediately — the scheduler does
                // not stream progress anywhere, it just needs the final result.
                let (tx, mut rx) = tokio::sync::mpsc::channel(64);
                let _upload_guard = qrz_upload_lock.lock().await;
                sync::execute_sync(&client, logbook_store, false, conflict_policy, &tx).await;
                drop(tx);

                // Drain to find the final (complete) message.
                let mut sync_error: Option<String> = None;
                while let Some(Ok(msg)) = rx.recv().await {
                    if msg.complete {
                        sync_error.clone_from(&msg.error);
                    }
                }

                *is_syncing.lock().await = false;
                (*last_error.lock().await).clone_from(&sync_error);

                if let Some(ref err) = sync_error {
                    eprintln!("[sync-scheduler] Sync completed with error: {err}");
                } else {
                    eprintln!("[sync-scheduler] Sync completed successfully.");
                }
            }

            // Cleanup on exit.
            *next_sync.lock().await = None;
            eprintln!("[sync-scheduler] Stopped.");
        });
    }

    // ── Read-only accessors for gRPC handler ─────────────────────────────

    pub(crate) async fn is_syncing(&self) -> bool {
        *self.is_syncing.lock().await
    }

    /// Return a guard holding the `is_syncing` lock. While the guard is held,
    /// the sync loop cannot flip the flag, providing mutual exclusion for
    /// operations that must not overlap with sync (e.g., purge).
    pub(crate) async fn sync_guard(&self) -> tokio::sync::MutexGuard<'_, bool> {
        self.is_syncing.lock().await
    }

    pub(crate) async fn last_error(&self) -> Option<String> {
        self.last_error.lock().await.clone()
    }

    pub(crate) async fn next_sync(&self) -> Option<Timestamp> {
        *self.next_sync.lock().await
    }

    /// Signal the background loop to stop.
    pub(crate) fn stop(&self) {
        let _ = self.cancel_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn next_sync_timestamp_at(now: time::OffsetDateTime, interval_secs: u64) -> Option<Timestamp> {
    let duration = time::Duration::try_from(Duration::from_secs(interval_secs)).ok()?;
    let wake_at = now.checked_add(duration)?;
    Some(Timestamp {
        seconds: wake_at.unix_timestamp(),
        nanos: 0,
    })
}

/// Sleep for `duration`, but return early if `cancel_rx` fires.
/// Returns `true` when cancellation was received.
async fn wait_or_cancel(cancel_rx: &mut watch::Receiver<bool>, duration: Duration) -> bool {
    tokio::select! {
        () = tokio::time::sleep(duration) => false,
        _ = cancel_rx.changed() => true,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use tokio::time::Duration;

    #[test]
    fn next_sync_timestamp_preserves_the_configured_interval() {
        let now = time::macros::datetime!(2026-01-01 00:00 UTC);
        assert_eq!(
            next_sync_timestamp_at(now, 300),
            Some(Timestamp {
                seconds: now.unix_timestamp() + 300,
                nanos: 0
            })
        );
        assert_eq!(
            next_sync_timestamp_at(now, 0),
            Some(Timestamp {
                seconds: now.unix_timestamp(),
                nanos: 0
            })
        );
    }

    #[test]
    fn next_sync_timestamp_rejects_overflow() {
        assert!(
            next_sync_timestamp_at(time::macros::datetime!(2026-01-01 00:00 UTC), u64::MAX)
                .is_none()
        );
        assert!(
            next_sync_timestamp_at(time::macros::datetime!(9999-12-31 23:59:59 UTC), 1).is_none()
        );
    }

    /// Helper: build a `RuntimeConfigManager` with custom overrides.
    fn make_config(overrides: &[(&str, &str)]) -> Arc<RuntimeConfigManager> {
        let config_file_values: BTreeMap<String, String> = overrides
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect();
        let cli_overrides = BTreeMap::new();
        Arc::new(
            RuntimeConfigManager::new_with_config_file_values_and_cli_storage_overrides(
                config_file_values,
                &cli_overrides,
            )
            .expect("failed to build RuntimeConfigManager"),
        )
    }

    #[tokio::test]
    async fn scheduler_does_not_sync_when_auto_disabled() {
        let scheduler = SyncScheduler::new(Arc::new(Mutex::new(())));
        let config = make_config(&[("QSORIPPER_SYNC_AUTO_ENABLED", "false")]);
        scheduler.start(config);

        // Give the loop one iteration (the 30-second idle sleep is real time,
        // but we only need to verify initial state).
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(!scheduler.is_syncing().await);
        assert!(scheduler.next_sync().await.is_none());

        scheduler.stop();
    }

    #[tokio::test]
    async fn scheduler_stops_on_cancel() {
        let scheduler = SyncScheduler::new(Arc::new(Mutex::new(())));
        let config = make_config(&[("QSORIPPER_SYNC_AUTO_ENABLED", "false")]);
        scheduler.start(config);

        tokio::time::sleep(Duration::from_millis(50)).await;
        scheduler.stop();
        // The task should exit quickly after cancel; if it deadlocks this test
        // will time out.
        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(!scheduler.is_syncing().await);
        assert!(scheduler.next_sync().await.is_none());
    }

    #[tokio::test]
    async fn scheduler_clears_next_sync_when_no_api_key() {
        let scheduler = SyncScheduler::new(Arc::new(Mutex::new(())));
        // Auto-sync enabled but no API key.
        let config = make_config(&[("QSORIPPER_SYNC_AUTO_ENABLED", "true")]);
        scheduler.start(config);

        tokio::time::sleep(Duration::from_millis(100)).await;

        assert!(scheduler.next_sync().await.is_none());
        assert!(!scheduler.is_syncing().await);

        scheduler.stop();
    }
}
