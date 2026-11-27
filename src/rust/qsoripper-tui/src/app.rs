//! Top-level application state shared across the main event loop and UI renderer.

use std::time::{Duration, Instant};

use qsoripper_core::proto::qsoripper::domain::QsoRecord;

use crate::form::LogForm;

/// Rig connection status mirroring the proto `RigConnectionStatus` enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RigStatus {
    Connected,
    Disconnected,
    Error,
    Disabled,
}

/// Reachability status of the `QsoRipper` engine over gRPC.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum EngineStatus {
    /// No probe result yet — typically only seen briefly at startup.
    Unknown,
    /// The engine responded successfully to the most recent health probe.
    Connected,
    /// The engine could not be reached; the message describes the failure.
    Unreachable {
        /// Human-readable failure description (tonic Status message or transport error).
        message: String,
    },
}

/// Display-ready rig control snapshot for the TUI.
pub(crate) struct RigInfo {
    /// Formatted frequency string (e.g., `"14.225.000 MHz"`).
    pub(crate) frequency_display: String,
    /// Frequency in Hz (for form auto-population).
    pub(crate) frequency_hz: u64,
    /// Receive frequency in Hz when split operation is active.
    pub(crate) frequency_rx_hz: Option<u64>,
    /// Band name string (ADIF, e.g., `"20M"`).
    pub(crate) band: Option<String>,
    /// Receive band name when split operation is active.
    pub(crate) band_rx: Option<String>,
    /// Mode name string (ADIF, e.g., `"SSB"`).
    pub(crate) mode: Option<String>,
    /// Optional submode from the rig.
    pub(crate) submode: Option<String>,
    /// Transmitter power in watts when reported by the rig.
    pub(crate) tx_power_watts: Option<f64>,
    /// Connection status.
    pub(crate) status: RigStatus,
    /// Error message from the rig provider, if any.
    pub(crate) error_message: Option<String>,
}

/// Top-level view the TUI is currently showing.
pub(crate) enum View {
    /// QSO entry form — the primary screen.
    LogEntry,
    /// Extended field entry overlay (F2 to toggle).
    Advanced,
    /// Full-screen help overlay.
    Help,
    /// Confirmation dialog before deleting a QSO.
    ConfirmDeleteQso,
    /// Confirmation dialog before purging all soft-deleted QSOs.
    ConfirmPurge,
}

/// Resolved callsign information from a QRZ lookup (used for field auto-population).
pub(crate) struct CallsignInfo {
    /// Queried callsign.
    pub(crate) callsign: String,
    /// Operator name (formatted).
    pub(crate) name: Option<String>,
    /// QTH / city.
    pub(crate) qth: Option<String>,
    /// Maidenhead grid square.
    pub(crate) grid: Option<String>,
    /// Country name.
    pub(crate) country: Option<String>,
    /// CQ zone number.
    pub(crate) cq_zone: Option<u32>,
    /// DXCC entity ID.
    pub(crate) dxcc: Option<u32>,
}

/// Display-ready row for the recent QSOs list.
pub(crate) struct RecentQso {
    /// QsoRipper-assigned local UUID.
    pub(crate) local_id: String,
    /// UTC date formatted as `YYYY-MM-DD`.
    pub(crate) date: String,
    /// UTC time formatted as `HH:MM`.
    pub(crate) utc: String,
    /// Worked callsign.
    pub(crate) callsign: String,
    /// Band name string.
    pub(crate) band: String,
    /// Mode name string.
    pub(crate) mode: String,
    /// RST sent report string.
    pub(crate) rst_sent: String,
    /// RST received report string.
    pub(crate) rst_rcvd: String,
    /// Worked entity country.
    pub(crate) country: Option<String>,
    /// Worked grid square.
    pub(crate) grid: Option<String>,
    /// Worked operator name.
    pub(crate) name: Option<String>,
    /// Full proto record from the engine, preserved for lossless round-trip during edits.
    pub(crate) source_record: QsoRecord,
}

impl RecentQso {
    /// Returns `true` if any visible column contains `lower` (case-insensitive, already lowercased).
    pub(crate) fn matches_search(&self, lower: &str) -> bool {
        self.callsign.to_lowercase().contains(lower)
            || self.band.to_lowercase().contains(lower)
            || self.mode.to_lowercase().contains(lower)
            || self.rst_sent.to_lowercase().contains(lower)
            || self.rst_rcvd.to_lowercase().contains(lower)
            || self
                .country
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains(lower)
            || self
                .grid
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains(lower)
            || self
                .name
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains(lower)
            || self
                .source_record
                .comment
                .as_deref()
                .unwrap_or("")
                .to_lowercase()
                .contains(lower)
            || self.date.contains(lower)
            || self.utc.contains(lower)
    }
}

/// Current space weather conditions.
pub(crate) struct SpaceWeatherInfo {
    /// Planetary K-index (0–9).
    pub(crate) k_index: Option<f64>,
    /// Solar flux index (sfu).
    pub(crate) solar_flux: Option<f64>,
    /// International sunspot number.
    pub(crate) sunspot_number: Option<u32>,
}

/// A transient status bar message that auto-expires after three seconds.
pub(crate) struct StatusMessage {
    /// Message text.
    pub(crate) text: String,
    /// `true` for error styling; `false` for success styling.
    pub(crate) is_error: bool,
    /// When the message was created, used to compute expiry.
    pub(crate) created_at: Instant,
}

/// Top-level application state passed through the event loop and renderer.
#[expect(
    clippy::struct_excessive_bools,
    reason = "each bool is a distinct independent flag; a state machine would be more complex"
)]
pub(crate) struct App {
    /// Active view / screen.
    pub(crate) view: View,
    /// QSO entry form state.
    pub(crate) form: LogForm,
    /// Current UTC date+time string for the clock display (`YYYY-MM-DD HH:MM:SS`).
    pub(crate) utc_now: String,
    /// Most recent callsign lookup result (used for field auto-population; not displayed).
    pub(crate) lookup_result: Option<CallsignInfo>,
    /// All QSOs loaded from the engine (unfiltered source of truth).
    pub(crate) recent_qsos: Vec<RecentQso>,
    /// Current space weather snapshot.
    pub(crate) space_weather: Option<SpaceWeatherInfo>,
    /// Current rig control snapshot.
    pub(crate) rig_info: Option<RigInfo>,
    /// Last frequency value written into the form from rig control.
    ///
    /// If the form still contains this value, later rig snapshots may keep it in sync even
    /// after the operator starts typing a callsign. A manual frequency edit breaks that match.
    pub(crate) last_auto_rig_frequency_mhz: Option<String>,
    /// Last transmitter power value written into the form from rig control.
    pub(crate) last_auto_rig_tx_power: Option<String>,
    /// Whether rig control polling is enabled (default: `true`).
    pub(crate) rig_control_enabled: bool,
    /// Transient status bar message.
    pub(crate) status_message: Option<StatusMessage>,
    /// Whether keyboard focus is on the recent QSOs list panel.
    pub(crate) qso_list_focused: bool,
    /// Selected row index within the currently filtered QSO list (when list is focused).
    pub(crate) qso_selected: Option<usize>,
    /// Local ID of the QSO currently being edited (`None` means a new QSO is being entered).
    pub(crate) editing_local_id: Option<String>,
    /// Local ID of the QSO pending deletion (set when `ConfirmDeleteQso` view is active).
    pub(crate) delete_candidate_id: Option<String>,
    /// Search / filter text for the QSO list.
    pub(crate) search_text: String,
    /// Whether keyboard focus is on the search box.
    pub(crate) search_focused: bool,
    /// Whether the main event loop should keep running.
    pub(crate) running: bool,
    /// gRPC server endpoint URL.
    pub(crate) endpoint: String,
    /// When the current QSO started — used for the live duration display.
    ///
    /// Reset by F7, by Esc (form clear), and after each QSO is successfully logged or updated.
    pub(crate) qso_started_at: Instant,
    /// Set when the debounce fires and the duration timer is actively counting.
    ///
    /// Starts `false`; becomes `true` once a callsign has been stable for ~1.5 s.
    /// Cleared on form reset, Esc, and after log/update.
    pub(crate) qso_timer_active: bool,
    /// Reachability of the gRPC engine — updated by a periodic background probe.
    pub(crate) engine_status: EngineStatus,
}

impl App {
    /// Create a new `App` targeting the given gRPC endpoint.
    pub(crate) fn new(endpoint: String) -> Self {
        let now = time::OffsetDateTime::now_utc();
        Self {
            view: View::LogEntry,
            form: LogForm::new(),
            utc_now: qsoripper_core::utc::format_datetime(now),
            lookup_result: None,
            recent_qsos: Vec::new(),
            space_weather: None,
            rig_info: None,
            last_auto_rig_frequency_mhz: None,
            last_auto_rig_tx_power: None,
            rig_control_enabled: true,
            status_message: None,
            qso_list_focused: false,
            qso_selected: None,
            editing_local_id: None,
            delete_candidate_id: None,
            search_text: String::new(),
            search_focused: false,
            running: true,
            endpoint,
            qso_started_at: Instant::now(),
            qso_timer_active: false,
            engine_status: EngineStatus::Unknown,
        }
    }

    /// Returns references to the QSOs that match the current `search_text`.
    ///
    /// Returns all QSOs when `search_text` is empty.
    pub(crate) fn filtered_qsos(&self) -> Vec<&RecentQso> {
        if self.search_text.is_empty() {
            return self.recent_qsos.iter().collect();
        }
        let lower = self.search_text.to_lowercase();
        self.recent_qsos
            .iter()
            .filter(|q| q.matches_search(&lower))
            .collect()
    }

    /// Find a QSO in `recent_qsos` by its local ID.
    pub(crate) fn find_qso_by_id(&self, id: &str) -> Option<&RecentQso> {
        self.recent_qsos.iter().find(|q| q.local_id == id)
    }

    /// Acknowledge that a QSO is underway: start (or restart) the duration timer
    /// and snapshot the current UTC date/time into the form.
    pub(crate) fn acknowledge_qso_start(&mut self) {
        let now = time::OffsetDateTime::now_utc();
        self.qso_started_at = Instant::now();
        self.qso_timer_active = true;
        self.form.set_explicit_timestamp(now);
    }

    /// Reset all timer state — called after logging, updating, or clearing the form.
    pub(crate) fn reset_timer(&mut self) {
        self.qso_started_at = Instant::now();
        self.qso_timer_active = false;
    }

    /// Format the elapsed QSO duration as `M:SS` (< 1 h) or `H:MM:SS`.
    ///
    /// Returns `None` when the timer is not yet active.
    pub(crate) fn qso_duration_str(&self) -> Option<String> {
        if !self.qso_timer_active {
            return None;
        }
        Some(format_qso_duration(self.qso_started_at.elapsed()))
    }

    /// Set a success status message, replacing any current message.
    pub(crate) fn set_status(&mut self, text: impl Into<String>) {
        self.status_message = Some(StatusMessage {
            text: text.into(),
            is_error: false,
            created_at: Instant::now(),
        });
    }

    /// Set an error status message, replacing any current message.
    pub(crate) fn set_error(&mut self, text: impl Into<String>) {
        self.status_message = Some(StatusMessage {
            text: text.into(),
            is_error: true,
            created_at: Instant::now(),
        });
    }

    /// Toggle rig control on or off. Clears the cached snapshot when disabling.
    pub(crate) fn toggle_rig_control(&mut self) {
        self.rig_control_enabled = !self.rig_control_enabled;
        if !self.rig_control_enabled {
            self.rig_info = None;
        }
    }

    /// Clear the status message if it has been visible for more than 3 seconds.
    pub(crate) fn expire_status(&mut self) {
        if let Some(msg) = &self.status_message {
            if msg.created_at.elapsed().as_secs() >= 3 {
                self.status_message = None;
            }
        }
    }
}

fn format_qso_duration(duration: Duration) -> String {
    let secs = duration.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    if h > 0 {
        format!("{h}:{m:02}:{s:02}")
    } else {
        format!("{m}:{s:02}")
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::unchecked_duration_subtraction,
    clippy::items_after_statements
)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn make_qso(id: &str, callsign: &str) -> RecentQso {
        RecentQso {
            local_id: id.to_string(),
            date: "2026-07-08".to_string(),
            utc: "12:00".to_string(),
            callsign: callsign.to_string(),
            band: "20M".to_string(),
            mode: "SSB".to_string(),
            rst_sent: "59".to_string(),
            rst_rcvd: "59".to_string(),
            country: None,
            grid: None,
            name: None,
            source_record: QsoRecord {
                local_id: id.to_string(),
                worked_callsign: callsign.to_string(),
                ..Default::default()
            },
        }
    }

    #[test]
    fn new_app_starts_running() {
        let app = App::new("http://localhost:50051".to_string());
        assert!(app.running);
        assert!(app.recent_qsos.is_empty());
        assert!(!app.qso_timer_active);
        assert!(app.status_message.is_none());
        assert!(app.lookup_result.is_none());
        assert!(app.rig_control_enabled);
        assert!(app.rig_info.is_none());
    }

    #[test]
    fn filtered_qsos_returns_all_when_search_empty() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.recent_qsos.push(make_qso("1", "K7ABC"));
        app.recent_qsos.push(make_qso("2", "W1XYZ"));
        assert_eq!(app.filtered_qsos().len(), 2);
    }

    #[test]
    fn filtered_qsos_filters_by_callsign() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.recent_qsos.push(make_qso("1", "K7ABC"));
        app.recent_qsos.push(make_qso("2", "W1XYZ"));
        app.search_text = "k7".to_string();
        let filtered = app.filtered_qsos();
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].callsign, "K7ABC");
    }

    #[test]
    fn filtered_qsos_returns_none_when_no_match() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.recent_qsos.push(make_qso("1", "K7ABC"));
        app.search_text = "zzzz".to_string();
        assert!(app.filtered_qsos().is_empty());
    }

    #[test]
    fn find_qso_by_id_returns_matching() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.recent_qsos.push(make_qso("abc-123", "K7ABC"));
        let found = app.find_qso_by_id("abc-123");
        assert!(found.is_some());
        assert_eq!(found.unwrap().callsign, "K7ABC");
    }

    #[test]
    fn find_qso_by_id_returns_none_when_missing() {
        let app = App::new("http://localhost:50051".to_string());
        assert!(app.find_qso_by_id("nonexistent").is_none());
    }

    #[test]
    fn set_status_creates_success_message() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.set_status("QSO logged");
        let msg = app.status_message.as_ref().unwrap();
        assert_eq!(msg.text, "QSO logged");
        assert!(!msg.is_error);
    }

    #[test]
    fn set_error_creates_error_message() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.set_error("Connection failed");
        let msg = app.status_message.as_ref().unwrap();
        assert_eq!(msg.text, "Connection failed");
        assert!(msg.is_error);
    }

    #[test]
    fn expire_status_keeps_fresh_message() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.set_status("Fresh");
        app.expire_status();
        assert!(app.status_message.is_some());
    }

    #[test]
    fn expire_status_removes_old_message() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.status_message = Some(StatusMessage {
            text: "Old".to_string(),
            is_error: false,
            created_at: Instant::now() - Duration::from_secs(5),
        });
        app.expire_status();
        assert!(app.status_message.is_none());
    }

    #[test]
    fn expire_status_noop_with_no_message() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.expire_status();
        assert!(app.status_message.is_none());
    }

    #[test]
    fn qso_duration_str_returns_none_when_inactive() {
        let app = App::new("http://localhost:50051".to_string());
        assert!(app.qso_duration_str().is_none());
    }

    #[test]
    fn qso_duration_str_returns_some_when_active() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.qso_timer_active = true;
        let dur = app.qso_duration_str();
        assert!(dur.is_some());
        assert!(dur.unwrap().contains(':'));
    }

    #[test]
    fn format_qso_duration_formats_minutes_and_hours() {
        assert_eq!(format_qso_duration(Duration::from_secs(61)), "1:01");
        assert_eq!(format_qso_duration(Duration::from_secs(3661)), "1:01:01");
    }

    #[test]
    fn reset_timer_clears_all_state() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.qso_timer_active = true;
        app.reset_timer();
        assert!(!app.qso_timer_active);
    }

    #[test]
    fn acknowledge_qso_start_activates_timer() {
        let mut app = App::new("http://localhost:50051".to_string());
        assert!(!app.qso_timer_active);
        app.acknowledge_qso_start();
        assert!(app.qso_timer_active);
    }

    #[test]
    fn acknowledge_qso_start_updates_form_time() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.form.time = "00:00".to_string();
        app.acknowledge_qso_start();
        assert_ne!(app.form.time, "00:00");
    }

    #[test]
    fn timer_does_not_start_on_callsign_typing() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.form.callsign = "K7ABC".to_string();
        // Without calling acknowledge_qso_start, timer should remain inactive
        assert!(!app.qso_timer_active);
    }

    #[test]
    fn matches_search_finds_callsign() {
        let qso = make_qso("1", "K7ABC");
        assert!(qso.matches_search("k7"));
        assert!(!qso.matches_search("w1"));
    }

    #[test]
    fn matches_search_finds_band() {
        let qso = RecentQso {
            local_id: "1".to_string(),
            date: "2026-07-08".to_string(),
            utc: "12:00".to_string(),
            callsign: "K7ABC".to_string(),
            band: "40M".to_string(),
            mode: "CW".to_string(),
            rst_sent: "599".to_string(),
            rst_rcvd: "599".to_string(),
            country: Some("United States".to_string()),
            grid: Some("CN87".to_string()),
            name: Some("John".to_string()),
            source_record: QsoRecord {
                comment: Some("Portable activation".to_string()),
                ..Default::default()
            },
        };
        assert!(qso.matches_search("40m"));
        assert!(qso.matches_search("cw"));
        assert!(qso.matches_search("united"));
        assert!(qso.matches_search("cn87"));
        assert!(qso.matches_search("john"));
        assert!(qso.matches_search("portable"));
        assert!(qso.matches_search("2026-07-08"));
        assert!(qso.matches_search("12:00"));
    }

    #[test]
    fn matches_search_no_match() {
        let qso = make_qso("1", "K7ABC");
        assert!(!qso.matches_search("zz9"));
    }

    #[test]
    fn matches_search_optional_fields_none() {
        let qso = RecentQso {
            local_id: "1".to_string(),
            date: "2026-07-08".to_string(),
            utc: "10:00".to_string(),
            callsign: "W1ABC".to_string(),
            band: "20M".to_string(),
            mode: "SSB".to_string(),
            rst_sent: "59".to_string(),
            rst_rcvd: "59".to_string(),
            country: None,
            grid: None,
            name: None,
            source_record: QsoRecord::default(),
        };
        assert!(!qso.matches_search("united"));
    }

    #[test]
    fn toggle_rig_control_disables_and_clears() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.rig_info = Some(RigInfo {
            frequency_display: "14.225.000 MHz".to_string(),
            frequency_hz: 14_225_000,
            band: Some("20M".to_string()),
            mode: Some("SSB".to_string()),
            submode: None,
            frequency_rx_hz: None,
            band_rx: None,
            tx_power_watts: None,
            status: RigStatus::Connected,
            error_message: None,
        });
        assert!(app.rig_control_enabled);
        app.toggle_rig_control();
        assert!(!app.rig_control_enabled);
        assert!(app.rig_info.is_none());
    }

    #[test]
    fn toggle_rig_control_re_enables() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.rig_control_enabled = false;
        app.toggle_rig_control();
        assert!(app.rig_control_enabled);
    }

    #[test]
    fn new_app_has_unknown_engine_status() {
        let app = App::new("http://localhost:50051".to_string());
        assert_eq!(app.engine_status, EngineStatus::Unknown);
    }

    #[test]
    fn engine_status_can_be_updated() {
        let mut app = App::new("http://localhost:50051".to_string());
        app.engine_status = EngineStatus::Connected;
        assert_eq!(app.engine_status, EngineStatus::Connected);
        app.engine_status = EngineStatus::Unreachable {
            message: "transport error".to_string(),
        };
        assert_eq!(
            app.engine_status,
            EngineStatus::Unreachable {
                message: "transport error".to_string(),
            }
        );
    }

    #[test]
    fn engine_status_unreachable_distinguishes_messages() {
        let a = EngineStatus::Unreachable {
            message: "timeout".to_string(),
        };
        let b = EngineStatus::Unreachable {
            message: "connection refused".to_string(),
        };
        assert_ne!(a, b);
    }

    #[test]
    fn engine_status_unreachable_equal_for_same_message() {
        let a = EngineStatus::Unreachable {
            message: "timeout".to_string(),
        };
        let b = EngineStatus::Unreachable {
            message: "timeout".to_string(),
        };
        assert_eq!(a, b);
    }

    #[test]
    fn engine_status_variants_are_distinct() {
        assert_ne!(EngineStatus::Unknown, EngineStatus::Connected);
        assert_ne!(
            EngineStatus::Connected,
            EngineStatus::Unreachable {
                message: "x".to_string()
            }
        );
    }
}
