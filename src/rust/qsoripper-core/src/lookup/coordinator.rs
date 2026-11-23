//! Lookup orchestration with cache policy and in-flight request deduplication.

use std::{
    collections::HashMap,
    num::NonZeroUsize,
    sync::Arc,
    time::{Duration, Instant},
};

use futures::future::{BoxFuture, FutureExt, Shared};
use tokio::sync::{Mutex, RwLock};

use crate::{
    domain::{
        callsign_parser::{annotate_record, parse_callsign},
        lookup::{normalize_callsign, normalize_callsign_for_history},
    },
    proto::qsoripper::domain::{CallsignRecord, LookupResult, LookupState, QsoHistoryEntry},
    storage::{EngineStorage, LookupSnapshot},
};

use super::provider::{
    CallsignProvider, ProviderLookup, ProviderLookupError, ProviderLookupOutcome,
};

type ProviderLookupResult = Result<ProviderLookup, ProviderLookupError>;
type SharedProviderLookup = Shared<BoxFuture<'static, ProviderLookupResult>>;

const DEFAULT_POSITIVE_CACHE_TTL: Duration = Duration::from_secs(15 * 60);
const DEFAULT_NEGATIVE_CACHE_TTL: Duration = Duration::from_secs(2 * 60);
const DEFAULT_MAX_CACHE_ENTRIES: NonZeroUsize = NonZeroUsize::new(1_000).unwrap();

/// Maximum number of prior QSOs returned with a lookup result. Picked to
/// cover dense pileups while staying small enough to keep responses tight
/// (full `QsoRecord` history isn't returned — see [`qso_to_history_entry`]).
const DEFAULT_HISTORY_LIMIT: u32 = 25;

/// Lookup coordinator configuration.
#[derive(Debug, Clone, Copy)]
pub struct LookupCoordinatorConfig {
    positive_ttl: Duration,
    negative_ttl: Duration,
    max_entries: NonZeroUsize,
}

impl LookupCoordinatorConfig {
    /// Create an explicit cache configuration with the default entry cap.
    #[must_use]
    pub fn new(positive_ttl: Duration, negative_ttl: Duration) -> Self {
        Self {
            positive_ttl,
            negative_ttl,
            max_entries: DEFAULT_MAX_CACHE_ENTRIES,
        }
    }

    /// Override the maximum number of entries the in-memory cache may hold.
    ///
    /// When the cache is full, the least-recently-used entry is evicted.
    #[must_use]
    pub fn with_max_entries(mut self, max_entries: NonZeroUsize) -> Self {
        self.max_entries = max_entries;
        self
    }

    /// Positive (found-record) cache TTL.
    #[must_use]
    pub fn positive_ttl(self) -> Duration {
        self.positive_ttl
    }

    /// Negative (not-found) cache TTL.
    #[must_use]
    pub fn negative_ttl(self) -> Duration {
        self.negative_ttl
    }

    /// Maximum number of in-memory cache entries before LRU eviction kicks in.
    #[must_use]
    pub fn max_entries(self) -> NonZeroUsize {
        self.max_entries
    }
}

impl Default for LookupCoordinatorConfig {
    fn default() -> Self {
        Self {
            positive_ttl: DEFAULT_POSITIVE_CACHE_TTL,
            negative_ttl: DEFAULT_NEGATIVE_CACHE_TTL,
            max_entries: DEFAULT_MAX_CACHE_ENTRIES,
        }
    }
}

#[derive(Debug, Clone)]
enum CachedLookup {
    Found(Box<CallsignRecord>),
    NotFound,
}

#[derive(Debug, Clone)]
struct CacheEntry {
    lookup: CachedLookup,
    cached_at: Instant,
}

/// A size-bounded in-memory cache.
///
/// When the cache is at capacity, inserting a new key evicts an arbitrary
/// existing entry to keep memory bounded.  Re-inserting an existing key
/// updates its value without changing the entry count.
struct BoundedCache {
    entries: HashMap<String, CacheEntry>,
    max_entries: usize,
}

impl BoundedCache {
    fn new(max_entries: NonZeroUsize) -> Self {
        Self {
            entries: HashMap::new(),
            max_entries: max_entries.get(),
        }
    }

    fn get(&self, key: &str) -> Option<&CacheEntry> {
        self.entries.get(key)
    }

    fn put(&mut self, key: String, value: CacheEntry) {
        if let Some(existing) = self.entries.get_mut(&key) {
            *existing = value;
            return;
        }
        if self.entries.len() >= self.max_entries {
            if let Some(evict_key) = self.entries.keys().next().cloned() {
                self.entries.remove(&evict_key);
            }
        }
        self.entries.insert(key, value);
    }
}

/// Coordinates lookup policy over an underlying callsign provider.
pub struct LookupCoordinator {
    provider: Arc<dyn CallsignProvider>,
    config: LookupCoordinatorConfig,
    cache: RwLock<BoundedCache>,
    in_flight: Mutex<HashMap<String, SharedProviderLookup>>,
    snapshot_storage: Option<Arc<dyn EngineStorage>>,
}

impl LookupCoordinator {
    /// Create a lookup coordinator around a provider and cache policy.
    #[must_use]
    pub fn new(provider: Arc<dyn CallsignProvider>, config: LookupCoordinatorConfig) -> Self {
        Self {
            provider,
            config,
            cache: RwLock::new(BoundedCache::new(config.max_entries)),
            in_flight: Mutex::new(HashMap::new()),
            snapshot_storage: None,
        }
    }

    /// Create a lookup coordinator backed by a persistent snapshot store.
    ///
    /// Lookup results are persisted to the store and loaded when the
    /// in-memory cache does not contain a fresh entry.
    #[must_use]
    pub fn with_snapshot_store(
        provider: Arc<dyn CallsignProvider>,
        config: LookupCoordinatorConfig,
        storage: Arc<dyn EngineStorage>,
    ) -> Self {
        Self {
            provider,
            config,
            cache: RwLock::new(BoundedCache::new(config.max_entries)),
            in_flight: Mutex::new(HashMap::new()),
            snapshot_storage: Some(storage),
        }
    }

    /// Perform a unary lookup with cache and provider orchestration.
    pub async fn lookup(&self, callsign: &str, skip_cache: bool) -> LookupResult {
        let normalized_callsign = normalize_callsign(callsign);
        if !skip_cache {
            if let Some(entry) = self.get_cache_entry(&normalized_callsign).await {
                if self.is_fresh(&entry) {
                    let mut result =
                        Self::cache_entry_to_result(&entry, &normalized_callsign, None, true);
                    self.populate_history(callsign, &mut result).await;
                    return result;
                }
            }
        }

        let parsed = parse_callsign(&normalized_callsign);
        let base = if parsed.modifier_kind.is_some() {
            Some(parsed.base_callsign.as_str())
        } else {
            None
        };

        let started_at = Instant::now();
        let provider_result = self
            .run_provider_lookup_with_fallback(&normalized_callsign, base)
            .await;
        let latency_ms = duration_to_millis_u32(started_at.elapsed());
        let mut result = self
            .provider_result_to_lookup(provider_result, &normalized_callsign, latency_ms, &parsed)
            .await;
        self.populate_history(callsign, &mut result).await;
        result
    }

    /// Perform a streaming lookup state transition.
    ///
    /// The returned vector is intended to be emitted in-order by a transport layer.
    /// Prefer [`Self::stream_lookup_into`] for true server-streaming behavior:
    /// the vector form buffers every update until the provider lookup completes,
    /// which defeats the point of the `Loading` state. This method is retained for
    /// tests and convenience callers that simply want to inspect the full
    /// transition sequence.
    pub async fn stream_lookup(&self, callsign: &str, skip_cache: bool) -> Vec<LookupResult> {
        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        self.stream_lookup_into(callsign, skip_cache, &sender).await;
        drop(sender);

        let mut updates = Vec::new();
        while let Some(update) = receiver.recv().await {
            updates.push(update);
        }
        updates
    }

    /// Stream lookup state transitions into `sender` as they become available.
    ///
    /// This method emits the initial `Loading` update **before** any awaits on
    /// cache or provider work, so transport layers (e.g., the gRPC server)
    /// forward it to clients with minimal latency. Subsequent updates
    /// (`Stale`, `Found`, `NotFound`, `Error`) are emitted in-order as the
    /// underlying cache check and provider call complete.
    ///
    /// If the receiver is dropped between updates the method returns early.
    pub async fn stream_lookup_into(
        &self,
        callsign: &str,
        skip_cache: bool,
        sender: &tokio::sync::mpsc::UnboundedSender<LookupResult>,
    ) {
        let normalized_callsign = normalize_callsign(callsign);

        let loading = LookupResult {
            state: LookupState::Loading as i32,
            queried_callsign: normalized_callsign.clone(),
            ..Default::default()
        };
        if sender.send(loading).is_err() {
            return;
        }

        if !skip_cache {
            if let Some(entry) = self.get_cache_entry(&normalized_callsign).await {
                if self.is_fresh(&entry) {
                    let mut fresh =
                        Self::cache_entry_to_result(&entry, &normalized_callsign, None, true);
                    self.populate_history(callsign, &mut fresh).await;
                    let _ = sender.send(fresh);
                    return;
                }

                if let CachedLookup::Found(_) = entry.lookup {
                    let mut stale = Self::cache_entry_to_result(
                        &entry,
                        &normalized_callsign,
                        Some(LookupState::Stale),
                        true,
                    );
                    self.populate_history(callsign, &mut stale).await;
                    if sender.send(stale).is_err() {
                        return;
                    }
                }
            }
        }

        let parsed = parse_callsign(&normalized_callsign);
        let base = if parsed.modifier_kind.is_some() {
            Some(parsed.base_callsign.as_str())
        } else {
            None
        };

        let started_at = Instant::now();
        let provider_result = self
            .run_provider_lookup_with_fallback(&normalized_callsign, base)
            .await;
        let latency_ms = duration_to_millis_u32(started_at.elapsed());
        let mut final_update = self
            .provider_result_to_lookup(provider_result, &normalized_callsign, latency_ms, &parsed)
            .await;
        self.populate_history(callsign, &mut final_update).await;
        let _ = sender.send(final_update);
    }

    /// Return a cache-only lookup result.
    pub async fn get_cached_callsign(&self, callsign: &str) -> LookupResult {
        let normalized_callsign = normalize_callsign(callsign);
        let mut result = if let Some(entry) = self.get_cache_entry(&normalized_callsign).await {
            if self.is_fresh(&entry) {
                Self::cache_entry_to_result(&entry, &normalized_callsign, None, true)
            } else {
                LookupResult {
                    state: LookupState::NotFound as i32,
                    queried_callsign: normalized_callsign.clone(),
                    ..Default::default()
                }
            }
        } else {
            LookupResult {
                state: LookupState::NotFound as i32,
                queried_callsign: normalized_callsign.clone(),
                ..Default::default()
            }
        };
        self.populate_history(callsign, &mut result).await;
        result
    }

    async fn get_cache_entry(&self, normalized_callsign: &str) -> Option<CacheEntry> {
        if let Some(entry) = self.cache.read().await.get(normalized_callsign).cloned() {
            return Some(entry);
        }
        // Fall back to persistent snapshot store.
        self.load_snapshot_entry(normalized_callsign).await
    }

    fn is_fresh(&self, entry: &CacheEntry) -> bool {
        entry.cached_at.elapsed() <= self.ttl_for(entry)
    }

    fn ttl_for(&self, entry: &CacheEntry) -> Duration {
        match entry.lookup {
            CachedLookup::Found(_) => self.config.positive_ttl,
            CachedLookup::NotFound => self.config.negative_ttl,
        }
    }

    fn cache_entry_to_result(
        entry: &CacheEntry,
        normalized_callsign: &str,
        state_override: Option<LookupState>,
        cache_hit: bool,
    ) -> LookupResult {
        match &entry.lookup {
            CachedLookup::Found(record) => LookupResult {
                state: state_override.unwrap_or(LookupState::Found) as i32,
                record: Some((**record).clone()),
                cache_hit,
                queried_callsign: normalized_callsign.to_string(),
                ..Default::default()
            },
            CachedLookup::NotFound => LookupResult {
                state: LookupState::NotFound as i32,
                cache_hit,
                queried_callsign: normalized_callsign.to_string(),
                ..Default::default()
            },
        }
    }

    async fn provider_result_to_lookup(
        &self,
        provider_result: ProviderLookupResult,
        normalized_callsign: &str,
        lookup_latency_ms: u32,
        parsed: &crate::domain::callsign_parser::ParsedCallsign,
    ) -> LookupResult {
        match provider_result {
            Ok(ProviderLookup {
                outcome: ProviderLookupOutcome::Found(mut record),
                debug_http_exchanges,
            }) => {
                annotate_record(&mut record, parsed);
                self.store_cache_entry(
                    normalized_callsign,
                    CacheEntry {
                        lookup: CachedLookup::Found(record.clone()),
                        cached_at: Instant::now(),
                    },
                )
                .await;

                LookupResult {
                    state: LookupState::Found as i32,
                    record: Some(*record),
                    lookup_latency_ms,
                    queried_callsign: normalized_callsign.to_string(),
                    debug_http_exchanges,
                    ..Default::default()
                }
            }
            Ok(ProviderLookup {
                outcome: ProviderLookupOutcome::NotFound,
                debug_http_exchanges,
            }) => {
                self.store_cache_entry(
                    normalized_callsign,
                    CacheEntry {
                        lookup: CachedLookup::NotFound,
                        cached_at: Instant::now(),
                    },
                )
                .await;

                LookupResult {
                    state: LookupState::NotFound as i32,
                    lookup_latency_ms,
                    queried_callsign: normalized_callsign.to_string(),
                    debug_http_exchanges,
                    ..Default::default()
                }
            }
            Err(error) => LookupResult {
                state: LookupState::Error as i32,
                error_message: Some(error.to_string()),
                lookup_latency_ms,
                queried_callsign: normalized_callsign.to_string(),
                debug_http_exchanges: error.debug_http_exchanges().to_vec(),
                ..Default::default()
            },
        }
    }

    /// Populate `prior_qsos` and `prior_qso_total_count` from the local
    /// logbook. Called at every non-LOADING return path of [`Self::lookup`],
    /// [`Self::stream_lookup_into`], [`Self::get_cached_callsign`], and the
    /// batch fan-out so the history reflects current logbook state regardless
    /// of cache freshness. A failed history query logs and leaves the result
    /// untouched so a transient storage error never masks the underlying
    /// callsign result.
    async fn populate_history(&self, callsign: &str, result: &mut LookupResult) {
        let Some(ref storage) = self.snapshot_storage else {
            return;
        };
        // Use a history-specific normalizer that returns None for blank input
        // rather than falling back to the developer placeholder. This prevents
        // a blank/whitespace lookup from leaking real prior contacts logged
        // against the placeholder callsign.
        let Some(history_key) = normalize_callsign_for_history(callsign) else {
            return;
        };
        match storage
            .logbook()
            .list_qso_history(&history_key, DEFAULT_HISTORY_LIMIT)
            .await
        {
            Ok(page) => {
                result.prior_qsos = page.entries.iter().map(qso_to_history_entry).collect();
                result.prior_qso_total_count = page.total;
            }
            Err(err) => {
                eprintln!("[lookup] Failed to load QSO history for {history_key}: {err}");
            }
        }
    }

    async fn store_cache_entry(&self, normalized_callsign: &str, entry: CacheEntry) {
        {
            let mut cache = self.cache.write().await;
            cache.put(normalized_callsign.to_string(), entry.clone());
        }

        // Persist to the snapshot store if available.
        if let Some(ref storage) = self.snapshot_storage {
            let ttl = self.ttl_for(&entry);
            let now_utc = time::OffsetDateTime::now_utc();
            let stored_at = prost_types::Timestamp {
                seconds: now_utc.unix_timestamp(),
                nanos: 0,
            };
            let ttl_secs = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
            let expires_at = prost_types::Timestamp {
                seconds: now_utc.unix_timestamp().saturating_add(ttl_secs),
                nanos: 0,
            };
            let result = match &entry.lookup {
                CachedLookup::Found(record) => LookupResult {
                    state: LookupState::Found as i32,
                    record: Some((**record).clone()),
                    ..Default::default()
                },
                CachedLookup::NotFound => LookupResult {
                    state: LookupState::NotFound as i32,
                    ..Default::default()
                },
            };
            let snapshot = LookupSnapshot {
                callsign: normalized_callsign.to_string(),
                result,
                stored_at,
                expires_at: Some(expires_at),
            };
            if let Err(err) = storage
                .lookup_snapshots()
                .upsert_lookup_snapshot(&snapshot)
                .await
            {
                eprintln!(
                    "[lookup] Failed to persist lookup snapshot for {normalized_callsign}: {err}"
                );
            }
        }
    }

    /// Try to load a lookup entry from persistent storage, returning a fresh
    /// in-memory cache entry when the stored snapshot has not yet expired.
    async fn load_snapshot_entry(&self, normalized_callsign: &str) -> Option<CacheEntry> {
        let storage = self.snapshot_storage.as_ref()?;
        let snapshot = match storage
            .lookup_snapshots()
            .get_lookup_snapshot(normalized_callsign)
            .await
        {
            Ok(Some(s)) => s,
            Ok(None) => return None,
            Err(err) => {
                eprintln!(
                    "[lookup] Failed to load lookup snapshot for {normalized_callsign}: {err}"
                );
                return None;
            }
        };

        // Check expiry — if expired, don't use.
        if let Some(ref expires_at) = snapshot.expires_at {
            let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
            if now_secs >= expires_at.seconds {
                return None;
            }
        }

        let lookup = if snapshot.result.state == LookupState::Found as i32 {
            if let Some(record) = snapshot.result.record {
                CachedLookup::Found(Box::new(record))
            } else {
                return None;
            }
        } else {
            CachedLookup::NotFound
        };

        // Reconstruct a cache entry with its Instant set so TTL checks pass.
        // Use the remaining lifetime to back-date `cached_at`.
        let remaining = snapshot.expires_at.map_or(Duration::ZERO, |e| {
            let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
            let remaining_secs =
                u64::try_from(e.seconds.saturating_sub(now_secs).max(0)).unwrap_or(0);
            Duration::from_secs(remaining_secs)
        });
        let ttl = match &lookup {
            CachedLookup::Found(_) => self.config.positive_ttl,
            CachedLookup::NotFound => self.config.negative_ttl,
        };
        let elapsed = ttl.saturating_sub(remaining);
        let cached_at = Instant::now()
            .checked_sub(elapsed)
            .unwrap_or_else(Instant::now);

        let entry = CacheEntry { lookup, cached_at };

        // Promote into in-memory cache so subsequent reads are fast.
        {
            let mut cache = self.cache.write().await;
            cache.put(normalized_callsign.to_string(), entry.clone());
        }

        Some(entry)
    }

    /// Run provider lookup with exact-first / base-callsign-fallback behavior.
    ///
    /// If the exact callsign (e.g. `AE7XI/P`) returns `NotFound`, and a
    /// `base_callsign` is supplied (e.g. `AE7XI`), a second attempt is made
    /// with the base callsign so that providers that do not know slash forms
    /// can still enrich the record.  The cache is always keyed on the original
    /// exact callsign regardless of which attempt succeeded.
    async fn run_provider_lookup_with_fallback(
        &self,
        exact_callsign: &str,
        base_callsign: Option<&str>,
    ) -> ProviderLookupResult {
        let first = self.run_provider_lookup_deduped(exact_callsign).await;

        if let Some(base) = base_callsign {
            if base != exact_callsign {
                if let Ok(ProviderLookup {
                    outcome: ProviderLookupOutcome::NotFound,
                    debug_http_exchanges: first_exchanges,
                }) = first
                {
                    let second = self.run_provider_lookup_deduped(base).await;
                    return second.map(|mut lookup| {
                        lookup.debug_http_exchanges.extend(first_exchanges);
                        lookup
                    });
                }
            }
        }

        first
    }

    async fn run_provider_lookup_deduped(&self, normalized_callsign: &str) -> ProviderLookupResult {
        let normalized_callsign = normalized_callsign.to_string();
        let (lookup_future, owner) = {
            let mut in_flight = self.in_flight.lock().await;
            if let Some(existing) = in_flight.get(&normalized_callsign) {
                (existing.clone(), false)
            } else {
                let provider = Arc::clone(&self.provider);
                let request_callsign = normalized_callsign.clone();
                let future = async move { provider.lookup_callsign(&request_callsign).await }
                    .boxed()
                    .shared();
                in_flight.insert(normalized_callsign.clone(), future.clone());
                (future, true)
            }
        };

        let result = lookup_future.await;
        if owner {
            self.in_flight.lock().await.remove(&normalized_callsign);
        }

        result
    }
}

fn duration_to_millis_u32(duration: Duration) -> u32 {
    match u32::try_from(duration.as_millis()) {
        Ok(value) => value,
        Err(_) => u32::MAX,
    }
}

fn qso_to_history_entry(qso: &crate::proto::qsoripper::domain::QsoRecord) -> QsoHistoryEntry {
    QsoHistoryEntry {
        local_id: qso.local_id.clone(),
        utc_timestamp: qso.utc_timestamp,
        band: qso.band,
        mode: qso.mode,
        submode: qso.submode.clone(),
        frequency_hz: qso.frequency_hz,
        frequency_rx_hz: qso.frequency_rx_hz,
        contest_id: qso.contest_id.clone(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use std::{
        collections::VecDeque,
        num::NonZeroUsize,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    use tokio::{sync::Mutex, time::sleep};

    use crate::proto::qsoripper::domain::LookupState;

    use super::*;

    #[derive(Debug, Clone)]
    struct QueueProvider {
        responses: Arc<Mutex<VecDeque<ProviderLookupResult>>>,
        calls: Arc<AtomicUsize>,
        delay: Duration,
    }

    impl QueueProvider {
        fn new(responses: Vec<ProviderLookupResult>, delay: Duration) -> Self {
            Self {
                responses: Arc::new(Mutex::new(VecDeque::from(responses))),
                calls: Arc::new(AtomicUsize::new(0)),
                delay,
            }
        }

        fn call_count(&self) -> usize {
            self.calls.load(Ordering::Relaxed)
        }
    }

    #[tonic::async_trait]
    impl CallsignProvider for QueueProvider {
        async fn lookup_callsign(
            &self,
            _callsign: &str,
        ) -> Result<ProviderLookup, ProviderLookupError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            if !self.delay.is_zero() {
                sleep(self.delay).await;
            }

            match self.responses.lock().await.pop_front() {
                Some(result) => result,
                None => Ok(ProviderLookup::not_found(Vec::new())),
            }
        }
    }

    fn found_record(callsign: &str, first_name: &str) -> CallsignRecord {
        CallsignRecord {
            callsign: callsign.to_string(),
            cross_ref: callsign.to_string(),
            first_name: first_name.to_string(),
            last_name: "Operator".to_string(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn lookup_returns_cache_hit_on_second_call() {
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Initial"),
                Vec::new(),
            ))],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::new(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(60), Duration::from_secs(60)),
        );

        let first = coordinator.lookup("w1aw", false).await;
        let second = coordinator.lookup("w1aw", false).await;

        assert_eq!(first.state, LookupState::Found as i32);
        assert!(!first.cache_hit);
        assert_eq!(second.state, LookupState::Found as i32);
        assert!(second.cache_hit);
        assert_eq!(provider.call_count(), 1);
    }

    #[tokio::test]
    async fn skip_cache_forces_provider_lookup() {
        let provider = QueueProvider::new(
            vec![
                Ok(ProviderLookup::found(
                    found_record("W1AW", "First"),
                    Vec::new(),
                )),
                Ok(ProviderLookup::found(
                    found_record("W1AW", "Second"),
                    Vec::new(),
                )),
            ],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::new(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(60), Duration::from_secs(60)),
        );

        let _ = coordinator.lookup("w1aw", false).await;
        let second = coordinator.lookup("w1aw", true).await;

        assert_eq!(second.state, LookupState::Found as i32);
        assert!(!second.cache_hit);
        assert_eq!(provider.call_count(), 2);
    }

    #[tokio::test]
    async fn stream_lookup_emits_loading_stale_and_refreshed_found() {
        let provider = QueueProvider::new(
            vec![
                Ok(ProviderLookup::found(
                    found_record("W1AW", "Cached"),
                    Vec::new(),
                )),
                Ok(ProviderLookup::found(
                    found_record("W1AW", "Fresh"),
                    Vec::new(),
                )),
            ],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::new(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_millis(1), Duration::from_secs(60)),
        );

        let _ = coordinator.lookup("w1aw", false).await;
        sleep(Duration::from_millis(5)).await;
        let updates = coordinator.stream_lookup("w1aw", false).await;

        assert_eq!(updates.len(), 3);
        let first = updates.first().expect("loading update expected");
        let second = updates.get(1).expect("stale update expected");
        let third = updates.get(2).expect("found update expected");
        assert_eq!(first.state, LookupState::Loading as i32);
        assert_eq!(second.state, LookupState::Stale as i32);
        assert_eq!(third.state, LookupState::Found as i32);

        let stale_record = second.record.as_ref().expect("stale record expected");
        let fresh_record = third.record.as_ref().expect("fresh result record expected");
        assert_eq!(stale_record.first_name, "Cached");
        assert_eq!(fresh_record.first_name, "Fresh");
        assert_eq!(provider.call_count(), 2);
    }

    #[tokio::test]
    async fn stream_lookup_into_emits_loading_before_provider_completes() {
        let provider_delay = Duration::from_millis(200);
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Slow"),
                Vec::new(),
            ))],
            provider_delay,
        );
        let coordinator = Arc::new(LookupCoordinator::new(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(60), Duration::from_secs(60)),
        ));

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let producer_coordinator = coordinator.clone();
        let producer = tokio::spawn(async move {
            producer_coordinator
                .stream_lookup_into("W1AW", false, &tx)
                .await;
        });

        let started = Instant::now();
        let first = rx
            .recv()
            .await
            .expect("loading update should be emitted immediately");
        let loading_latency = started.elapsed();

        assert_eq!(first.state, LookupState::Loading as i32);
        // Loading must arrive well before the provider's 200ms delay would let
        // the final update through. Allow slack for slow CI but stay strict
        // enough that a regression to the buffered behavior would fail.
        assert!(
            loading_latency < Duration::from_millis(150),
            "Loading update arrived in {loading_latency:?}, expected <150ms"
        );

        let second = rx.recv().await.expect("final update should follow loading");
        assert_eq!(second.state, LookupState::Found as i32);
        assert!(rx.recv().await.is_none(), "stream should be exhausted");

        producer.await.expect("producer task should finish cleanly");
        assert_eq!(provider.call_count(), 1);
    }

    #[tokio::test]
    async fn stream_lookup_into_stops_when_receiver_drops_after_loading() {
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Unwanted"),
                Vec::new(),
            ))],
            Duration::from_millis(100),
        );
        let coordinator = LookupCoordinator::new(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(60), Duration::from_secs(60)),
        );

        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let stream_future = coordinator.stream_lookup_into("W1AW", false, &tx);
        let drain_future = async {
            let first = rx.recv().await.expect("loading update");
            assert_eq!(first.state, LookupState::Loading as i32);
            drop(rx);
        };

        tokio::join!(stream_future, drain_future);
        // Provider may still have been invoked; the contract is just that no
        // panic occurs when the receiver is dropped between updates.
    }

    #[tokio::test]
    async fn concurrent_identical_lookups_share_inflight_request() {
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Shared"),
                Vec::new(),
            ))],
            Duration::from_millis(30),
        );
        let coordinator = Arc::new(LookupCoordinator::new(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(60), Duration::from_secs(60)),
        ));

        let first_coordinator = Arc::clone(&coordinator);
        let second_coordinator = Arc::clone(&coordinator);
        let (first, second) = tokio::join!(
            first_coordinator.lookup("w1aw", true),
            second_coordinator.lookup("W1AW", true)
        );

        assert_eq!(first.state, LookupState::Found as i32);
        assert_eq!(second.state, LookupState::Found as i32);
        assert_eq!(provider.call_count(), 1);
    }

    #[tokio::test]
    async fn lru_eviction_drops_oldest_entry_at_capacity() {
        let cap = NonZeroUsize::new(5).unwrap();
        let provider = QueueProvider::new(Vec::new(), Duration::ZERO);
        let coordinator = LookupCoordinator::new(
            Arc::new(provider),
            LookupCoordinatorConfig::new(Duration::from_secs(300), Duration::from_secs(300))
                .with_max_entries(cap),
        );

        for index in 0..5 {
            let callsign = format!("W1AW{index}");
            let _ = coordinator.lookup(&callsign, false).await;
        }

        let cache_len_before = coordinator.cache.read().await.entries.len();
        assert_eq!(cache_len_before, 5);

        let _ = coordinator.lookup("K7RND", false).await;

        let cache_len_after = coordinator.cache.read().await.entries.len();
        assert_eq!(
            cache_len_after, 5,
            "cache should remain at capacity after LRU eviction"
        );
    }

    // -- Snapshot persistence tests -----------------------------------------

    use std::collections::BTreeMap;

    use crate::storage::{
        EngineStorage, LogbookCounts, LogbookStore, LookupSnapshot, LookupSnapshotStore,
        QsoHistoryPage, QsoListQuery, StorageError, SyncMetadata,
    };

    /// Minimal in-memory implementation of [`EngineStorage`] for snapshot
    /// and lookup-coordinator tests. The logbook surface stores a plain
    /// `Vec<QsoRecord>` keyed by `local_id` so history queries can be
    /// exercised without pulling in the storage-memory crate (which would
    /// create a dependency cycle here).
    struct MockSnapshotStorage {
        snapshots: tokio::sync::RwLock<BTreeMap<String, LookupSnapshot>>,
        qsos: tokio::sync::RwLock<Vec<crate::proto::qsoripper::domain::QsoRecord>>,
    }

    impl MockSnapshotStorage {
        fn new() -> Self {
            Self {
                snapshots: tokio::sync::RwLock::new(BTreeMap::new()),
                qsos: tokio::sync::RwLock::new(Vec::new()),
            }
        }

        async fn seed_qso(&self, qso: crate::proto::qsoripper::domain::QsoRecord) {
            self.qsos.write().await.push(qso);
        }
    }

    impl EngineStorage for MockSnapshotStorage {
        fn logbook(&self) -> &dyn LogbookStore {
            self
        }
        fn lookup_snapshots(&self) -> &dyn LookupSnapshotStore {
            self
        }
        fn backend_name(&self) -> &'static str {
            "mock-snapshot"
        }
    }

    #[tonic::async_trait]
    impl LogbookStore for MockSnapshotStorage {
        async fn insert_qso(
            &self,
            _qso: &crate::proto::qsoripper::domain::QsoRecord,
        ) -> Result<(), StorageError> {
            unimplemented!()
        }
        async fn update_qso(
            &self,
            _qso: &crate::proto::qsoripper::domain::QsoRecord,
        ) -> Result<bool, StorageError> {
            unimplemented!()
        }
        async fn update_qrz_sync_metadata(
            &self,
            _local_id: &str,
            _expected_updated_at: Option<&prost_types::Timestamp>,
            _qrz_logid: &str,
        ) -> Result<Option<crate::proto::qsoripper::domain::QsoRecord>, StorageError> {
            unimplemented!()
        }
        async fn delete_qso(&self, _local_id: &str) -> Result<bool, StorageError> {
            unimplemented!()
        }
        async fn soft_delete_qso(
            &self,
            _local_id: &str,
            _deleted_at_ms: i64,
            _pending_remote_delete: bool,
        ) -> Result<bool, StorageError> {
            unimplemented!()
        }
        async fn restore_qso(&self, _local_id: &str) -> Result<bool, StorageError> {
            unimplemented!()
        }
        async fn get_qso(
            &self,
            _local_id: &str,
        ) -> Result<Option<crate::proto::qsoripper::domain::QsoRecord>, StorageError> {
            unimplemented!()
        }
        async fn list_qsos(
            &self,
            _query: &QsoListQuery,
        ) -> Result<Vec<crate::proto::qsoripper::domain::QsoRecord>, StorageError> {
            unimplemented!()
        }
        async fn list_qso_history(
            &self,
            worked_callsign: &str,
            limit: u32,
        ) -> Result<QsoHistoryPage, StorageError> {
            let qsos = self.qsos.read().await;
            let mut matching: Vec<_> = qsos
                .iter()
                .filter(|q| {
                    q.deleted_at.is_none()
                        && q.worked_callsign.eq_ignore_ascii_case(worked_callsign)
                })
                .cloned()
                .collect();
            let total = u32::try_from(matching.len()).unwrap_or(u32::MAX);
            matching.sort_by(|a, b| {
                let a_secs = a.utc_timestamp.as_ref().map_or(0, |t| t.seconds);
                let b_secs = b.utc_timestamp.as_ref().map_or(0, |t| t.seconds);
                b_secs
                    .cmp(&a_secs)
                    .then_with(|| b.local_id.cmp(&a.local_id))
            });
            matching.truncate(limit as usize);
            Ok(QsoHistoryPage {
                entries: matching,
                total,
            })
        }
        async fn qso_counts(&self) -> Result<LogbookCounts, StorageError> {
            unimplemented!()
        }
        async fn get_sync_metadata(&self) -> Result<SyncMetadata, StorageError> {
            unimplemented!()
        }
        async fn upsert_sync_metadata(&self, _metadata: &SyncMetadata) -> Result<(), StorageError> {
            unimplemented!()
        }
        async fn purge_deleted_qsos(
            &self,
            _local_ids: &[String],
            _older_than_ms: Option<i64>,
        ) -> Result<u32, StorageError> {
            unimplemented!()
        }
    }

    #[tonic::async_trait]
    impl LookupSnapshotStore for MockSnapshotStorage {
        async fn get_lookup_snapshot(
            &self,
            callsign: &str,
        ) -> Result<Option<LookupSnapshot>, StorageError> {
            Ok(self.snapshots.read().await.get(callsign).cloned())
        }
        async fn upsert_lookup_snapshot(
            &self,
            snapshot: &LookupSnapshot,
        ) -> Result<(), StorageError> {
            self.snapshots
                .write()
                .await
                .insert(snapshot.callsign.clone(), snapshot.clone());
            Ok(())
        }
        async fn delete_lookup_snapshot(&self, callsign: &str) -> Result<bool, StorageError> {
            Ok(self.snapshots.write().await.remove(callsign).is_some())
        }
    }

    #[tokio::test]
    async fn lookup_persists_snapshot_to_storage() {
        let storage = Arc::new(MockSnapshotStorage::new());
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Persisted"),
                Vec::new(),
            ))],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::with_snapshot_store(
            Arc::new(provider),
            LookupCoordinatorConfig::new(Duration::from_secs(300), Duration::from_secs(60)),
            storage.clone() as Arc<dyn EngineStorage>,
        );

        let result = coordinator.lookup("W1AW", false).await;
        assert_eq!(result.state, LookupState::Found as i32);

        // The snapshot should now be persisted in storage.
        let snapshot = storage
            .lookup_snapshots()
            .get_lookup_snapshot("W1AW")
            .await
            .expect("storage read")
            .expect("snapshot should exist after lookup");
        assert_eq!(snapshot.callsign, "W1AW");
        assert_eq!(snapshot.result.state, LookupState::Found as i32);
        assert!(snapshot.expires_at.is_some());
    }

    #[tokio::test]
    async fn lookup_loads_fresh_snapshot_from_storage_on_cold_start() {
        let storage = Arc::new(MockSnapshotStorage::new());

        // Pre-populate a fresh snapshot in storage (expires far in the future).
        let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
        let snapshot = LookupSnapshot {
            callsign: "W1AW".to_string(),
            result: LookupResult {
                state: LookupState::Found as i32,
                record: Some(CallsignRecord {
                    callsign: "W1AW".to_string(),
                    first_name: "Stored".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            stored_at: prost_types::Timestamp {
                seconds: now_secs,
                nanos: 0,
            },
            expires_at: Some(prost_types::Timestamp {
                seconds: now_secs + 600,
                nanos: 0,
            }),
        };
        storage
            .lookup_snapshots()
            .upsert_lookup_snapshot(&snapshot)
            .await
            .unwrap();

        // Create a coordinator with an empty in-memory cache. The provider
        // should NOT be called — the persisted snapshot should be used.
        let provider = QueueProvider::new(Vec::new(), Duration::ZERO);
        let coordinator = LookupCoordinator::with_snapshot_store(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(600), Duration::from_secs(60)),
            storage as Arc<dyn EngineStorage>,
        );

        let result = coordinator.lookup("W1AW", false).await;
        assert_eq!(result.state, LookupState::Found as i32);
        assert!(
            result.cache_hit,
            "result should be a cache hit from storage"
        );
        let record = result.record.expect("record should be present");
        assert_eq!(record.first_name, "Stored");
        assert_eq!(provider.call_count(), 0, "provider should not be called");
    }

    #[tokio::test]
    async fn lookup_ignores_expired_snapshot_from_storage() {
        let storage = Arc::new(MockSnapshotStorage::new());

        // Pre-populate an expired snapshot in storage.
        let now_secs = time::OffsetDateTime::now_utc().unix_timestamp();
        let snapshot = LookupSnapshot {
            callsign: "W1AW".to_string(),
            result: LookupResult {
                state: LookupState::Found as i32,
                record: Some(CallsignRecord {
                    callsign: "W1AW".to_string(),
                    first_name: "Expired".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            },
            stored_at: prost_types::Timestamp {
                seconds: now_secs - 1000,
                nanos: 0,
            },
            expires_at: Some(prost_types::Timestamp {
                seconds: now_secs - 1, // already expired
                nanos: 0,
            }),
        };
        storage
            .lookup_snapshots()
            .upsert_lookup_snapshot(&snapshot)
            .await
            .unwrap();

        // The coordinator should ignore the expired snapshot and call the provider.
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Fresh"),
                Vec::new(),
            ))],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::with_snapshot_store(
            Arc::new(provider.clone()),
            LookupCoordinatorConfig::new(Duration::from_secs(600), Duration::from_secs(60)),
            storage as Arc<dyn EngineStorage>,
        );

        let result = coordinator.lookup("W1AW", false).await;
        assert_eq!(result.state, LookupState::Found as i32);
        assert!(!result.cache_hit);
        let record = result.record.expect("record should be present");
        assert_eq!(record.first_name, "Fresh");
        assert_eq!(provider.call_count(), 1, "provider should be called");
    }

    #[tokio::test]
    async fn lookup_populates_prior_qsos_from_logbook_storage() {
        use crate::domain::qso::QsoRecordBuilder;
        use crate::proto::qsoripper::domain::{Band, Mode};

        let storage = Arc::new(MockSnapshotStorage::new());
        for (id, ts_secs) in [
            ("h1", 1_700_000_000_i64),
            ("h2", 1_700_001_000),
            ("h3", 1_700_002_000),
        ] {
            let mut qso = QsoRecordBuilder::new("K7DBG", "W1AW")
                .band(Band::Band20m)
                .mode(Mode::Ssb)
                .timestamp(prost_types::Timestamp {
                    seconds: ts_secs,
                    nanos: 0,
                })
                .build();
            qso.local_id = id.to_string();
            storage.seed_qso(qso).await;
        }
        // Soft-deleted row should be excluded from history.
        let mut soft = QsoRecordBuilder::new("K7DBG", "W1AW")
            .band(Band::Band40m)
            .build();
        soft.local_id = "ghost".to_string();
        soft.deleted_at = Some(prost_types::Timestamp {
            seconds: 1_700_999_000,
            nanos: 0,
        });
        storage.seed_qso(soft).await;
        // Loose-match decoy that must NOT appear in history.
        let mut decoy = QsoRecordBuilder::new("K7DBG", "W1AWX").build();
        decoy.local_id = "decoy".to_string();
        storage.seed_qso(decoy).await;

        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::found(
                found_record("W1AW", "Hiram"),
                Vec::new(),
            ))],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::with_snapshot_store(
            Arc::new(provider),
            LookupCoordinatorConfig::default(),
            storage.clone() as Arc<dyn EngineStorage>,
        );

        let result = coordinator.lookup("W1AW", false).await;
        assert_eq!(result.state, LookupState::Found as i32);
        assert_eq!(result.prior_qso_total_count, 3);
        assert_eq!(result.prior_qsos.len(), 3);
        assert_eq!(result.prior_qsos[0].local_id, "h3");
        assert_eq!(result.prior_qsos[2].local_id, "h1");
        assert_eq!(result.prior_qsos[0].band, Band::Band20m as i32);
        assert_eq!(result.prior_qsos[0].mode, Mode::Ssb as i32);

        // Cache hit on the second call still reports history.
        let cached = coordinator.lookup("W1AW", false).await;
        assert!(cached.cache_hit);
        assert_eq!(cached.prior_qso_total_count, 3);
        assert_eq!(cached.prior_qsos.len(), 3);
    }

    #[tokio::test]
    async fn stream_lookup_does_not_attach_history_to_loading_state() {
        let storage = Arc::new(MockSnapshotStorage::new());
        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::not_found(Vec::new()))],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::with_snapshot_store(
            Arc::new(provider),
            LookupCoordinatorConfig::default(),
            storage as Arc<dyn EngineStorage>,
        );

        let updates = coordinator.stream_lookup("W1AW", false).await;
        let loading = updates
            .iter()
            .find(|u| u.state == LookupState::Loading as i32)
            .expect("loading state expected");
        assert!(loading.prior_qsos.is_empty());
        assert_eq!(loading.prior_qso_total_count, 0);

        let final_update = updates
            .iter()
            .find(|u| u.state == LookupState::NotFound as i32)
            .expect("terminal state expected");
        assert_eq!(final_update.prior_qso_total_count, 0);
    }

    #[tokio::test]
    async fn lookup_with_blank_callsign_does_not_leak_placeholder_history() {
        use crate::domain::qso::QsoRecordBuilder;
        use crate::proto::qsoripper::domain::{Band, Mode};

        let storage = Arc::new(MockSnapshotStorage::new());
        // Seed history rows against the developer placeholder callsign that
        // a blank input falls back to. A blank lookup must not surface them.
        let mut qso = QsoRecordBuilder::new("K7DBG", "K7DBG")
            .band(Band::Band20m)
            .mode(Mode::Ssb)
            .build();
        qso.local_id = "placeholder-history".to_string();
        storage.seed_qso(qso).await;

        let provider = QueueProvider::new(
            vec![Ok(ProviderLookup::not_found(Vec::new()))],
            Duration::ZERO,
        );
        let coordinator = LookupCoordinator::with_snapshot_store(
            Arc::new(provider),
            LookupCoordinatorConfig::default(),
            storage.clone() as Arc<dyn EngineStorage>,
        );

        let result = coordinator.lookup("   ", false).await;
        assert!(
            result.prior_qsos.is_empty(),
            "blank lookup must not surface placeholder history"
        );
        assert_eq!(result.prior_qso_total_count, 0);
    }
}
