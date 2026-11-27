//! QSO entry form state and field navigation.

use qsoripper_core::utc;
use time::OffsetDateTime;

/// Band names in display order, used as ADIF band strings.
pub(crate) const BANDS: &[&str] = &[
    "160M", "80M", "60M", "40M", "30M", "20M", "17M", "15M", "12M", "10M", "6M", "2M", "70CM",
];

/// Mode names in display order, used as ADIF mode strings.
pub(crate) const MODES: &[&str] = &["SSB", "CW", "FT8", "FT4", "RTTY", "PSK31", "AM", "FM"];

/// Default center frequency in MHz for each entry in [`BANDS`], in the same order.
pub(crate) const BAND_DEFAULT_FREQS: &[f64] = &[
    1.900, 3.750, 5.330, 7.150, 10.125, 14.225, 18.100, 21.200, 24.940, 28.400, 50.125, 146.520,
    446.000,
];

/// Index of 20M in [`BANDS`] — used as the startup default.
const DEFAULT_BAND_IDX: usize = 5;

/// Tabs available in the Advanced view.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AdvancedTab {
    Core,
    Lookup,
    Qsl,
    Contest,
    Station,
    Transcript,
    Metadata,
}

impl AdvancedTab {
    pub(crate) const ALL: &'static [AdvancedTab] = &[
        AdvancedTab::Core,
        AdvancedTab::Lookup,
        AdvancedTab::Qsl,
        AdvancedTab::Contest,
        AdvancedTab::Station,
        AdvancedTab::Transcript,
        AdvancedTab::Metadata,
    ];

    pub(crate) fn label(self) -> &'static str {
        match self {
            AdvancedTab::Core => "Core",
            AdvancedTab::Lookup => "Lookup",
            AdvancedTab::Qsl => "QSL",
            AdvancedTab::Contest => "Contest",
            AdvancedTab::Station => "Station",
            AdvancedTab::Transcript => "Transcript",
            AdvancedTab::Metadata => "Metadata",
        }
    }

    pub(crate) fn next(self) -> Self {
        let idx = Self::ALL.iter().position(|tab| *tab == self).unwrap_or(0);
        Self::ALL
            .get((idx + 1) % Self::ALL.len())
            .copied()
            .unwrap_or(Self::Core)
    }

    pub(crate) fn prev(self) -> Self {
        let idx = Self::ALL.iter().position(|tab| *tab == self).unwrap_or(0);
        Self::ALL
            .get((idx + Self::ALL.len() - 1) % Self::ALL.len())
            .copied()
            .unwrap_or(Self::Core)
    }

    /// Return the static slice of fields belonging to this tab.
    pub(crate) fn fields(self) -> &'static [Field] {
        match self {
            AdvancedTab::Core => ADV_CORE_FIELDS,
            AdvancedTab::Lookup => ADV_LOOKUP_FIELDS,
            AdvancedTab::Qsl => ADV_QSL_FIELDS,
            AdvancedTab::Contest => ADV_CONTEST_FIELDS,
            AdvancedTab::Station => ADV_STATION_FIELDS,
            AdvancedTab::Transcript => ADV_TRANSCRIPT_FIELDS,
            AdvancedTab::Metadata => ADV_METADATA_FIELDS,
        }
    }

    /// Return the digit character used as an Alt+digit shortcut for this tab.
    pub(crate) fn shortcut_digit(self) -> char {
        match self {
            AdvancedTab::Core => '1',
            AdvancedTab::Lookup => '2',
            AdvancedTab::Qsl => '3',
            AdvancedTab::Contest => '4',
            AdvancedTab::Station => '5',
            AdvancedTab::Transcript => '6',
            AdvancedTab::Metadata => '7',
        }
    }

    /// Return the first field of this tab — the focus target when switching to it.
    pub(crate) fn first_field(self) -> Field {
        self.fields().first().copied().unwrap_or(Field::Callsign)
    }
}

/// Focusable fields in the QSO entry form.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Field {
    /// Worked callsign.
    Callsign,
    /// Band selector (cycles through [`BANDS`]).
    Band,
    /// Mode selector (cycles through [`MODES`]).
    Mode,
    /// RST sent report.
    RstSent,
    /// RST received report.
    RstRcvd,
    /// Short comment.
    Comment,
    /// Operator notes.
    Notes,
    /// Frequency in MHz (free-text).
    FrequencyMhz,
    /// UTC date (`YYYY-MM-DD`).
    Date,
    /// UTC time on / start (`HH:MM`).
    Time,
    /// UTC time off / end (`HH:MM`).
    TimeOff,
    /// Local station callsign.
    StationCallsign,
    /// Worked station QTH (city/location).
    Qth,
    // Advanced fields shown in the advanced view.
    /// Transmitter power.
    TxPower,
    /// Submode override (e.g., USB, LSB, FT4).
    Submode,
    /// Contest identifier.
    ContestId,
    /// Serial number sent.
    SerialSent,
    /// Serial number received.
    SerialRcvd,
    /// Exchange sent.
    ExchangeSent,
    /// Exchange received.
    ExchangeRcvd,
    /// Propagation mode (ADIF `PROP_MODE`).
    PropMode,
    /// Satellite name.
    SatName,
    /// Satellite mode.
    SatMode,
    /// IOTA designator.
    Iota,
    /// ARRL section.
    ArrlSection,
    /// Worked state (US state abbreviation).
    WorkedState,
    /// Worked county.
    WorkedCounty,
    /// Worked operator callsign.
    WorkedOperatorCallsign,
    /// Worked operator name.
    WorkedName,
    /// Worked grid square.
    WorkedGrid,
    /// Worked country.
    WorkedCountry,
    /// Worked DXCC entity number.
    WorkedDxcc,
    /// Worked CQ zone.
    WorkedCqZone,
    /// Worked ITU zone.
    WorkedItuZone,
    /// Worked continent.
    WorkedContinent,
    /// SKCC membership number of the worked station.
    Skcc,
    /// Paper QSL sent status.
    QslSentStatus,
    /// Paper QSL sent date.
    QslSentDate,
    /// Paper QSL received status.
    QslReceivedStatus,
    /// Paper QSL received date.
    QslReceivedDate,
    /// `LoTW` sent state.
    LotwSent,
    /// `LoTW` received state.
    LotwReceived,
    /// eQSL sent state.
    EqslSent,
    /// eQSL received state.
    EqslReceived,
    /// QRZ log record ID.
    QrzLogId,
    /// QRZ book ID.
    QrzBookId,
    /// Station profile name captured at save time.
    SnapshotProfileName,
    /// Local station callsign captured at save time.
    SnapshotStationCallsign,
    /// Local operator callsign captured at save time.
    SnapshotOperatorCallsign,
    /// Local operator name captured at save time.
    SnapshotOperatorName,
    /// Local station grid captured at save time.
    SnapshotGrid,
    /// Local station country captured at save time.
    SnapshotCountry,
    /// Local station state captured at save time.
    SnapshotState,
    /// Local station county captured at save time.
    SnapshotCounty,
    /// Local station ARRL section captured at save time.
    SnapshotArrlSection,
    /// Local station DXCC captured at save time.
    SnapshotDxcc,
    /// Local station CQ zone captured at save time.
    SnapshotCqZone,
    /// Local station ITU zone captured at save time.
    SnapshotItuZone,
    /// Local station latitude captured at save time.
    SnapshotLatitude,
    /// Local station longitude captured at save time.
    SnapshotLongitude,
    /// Captured CW receive speed.
    CwDecodeRxWpm,
    /// Captured CW transcript.
    CwDecodeTranscript,
    /// Engine-assigned local ID.
    LocalId,
    /// Engine sync status.
    SyncStatus,
    /// Engine created timestamp.
    CreatedAt,
    /// Engine updated timestamp.
    UpdatedAt,
    /// Extra ADIF fields.
    ExtraFields,
}

/// Primary navigation order for Tab/Shift-Tab in the log entry view.
const FIELD_ORDER: &[Field] = &[
    Field::Callsign,
    Field::Band,
    Field::Mode,
    Field::RstSent,
    Field::RstRcvd,
    Field::Comment,
    Field::Notes,
    Field::FrequencyMhz,
    Field::Date,
    Field::Time,
    Field::TimeOff,
    Field::Qth,
];

/// Fields for the Advanced "Core" tab — identity and time/frequency basics.
const ADV_CORE_FIELDS: &[Field] = &[
    Field::Callsign,
    Field::Date,
    Field::Time,
    Field::TimeOff,
    Field::StationCallsign,
    Field::Band,
    Field::Mode,
    Field::FrequencyMhz,
    Field::RstSent,
    Field::RstRcvd,
    Field::TxPower,
    Field::Submode,
    Field::Comment,
    Field::Notes,
    Field::CwDecodeRxWpm,
];

/// Fields for the Advanced "Lookup" tab.
const ADV_LOOKUP_FIELDS: &[Field] = &[
    Field::WorkedOperatorCallsign,
    Field::WorkedName,
    Field::WorkedGrid,
    Field::WorkedCountry,
    Field::WorkedDxcc,
    Field::WorkedState,
    Field::WorkedCqZone,
    Field::WorkedItuZone,
    Field::WorkedCounty,
    Field::Iota,
    Field::WorkedContinent,
    Field::ArrlSection,
    Field::Skcc,
];

/// Fields for the Advanced "QSL" tab.
const ADV_QSL_FIELDS: &[Field] = &[
    Field::QslSentStatus,
    Field::QslSentDate,
    Field::QslReceivedStatus,
    Field::QslReceivedDate,
    Field::LotwSent,
    Field::LotwReceived,
    Field::EqslSent,
    Field::EqslReceived,
    Field::QrzLogId,
    Field::QrzBookId,
];

/// Fields for the Advanced "Contest" tab.
const ADV_CONTEST_FIELDS: &[Field] = &[
    Field::ContestId,
    Field::SerialSent,
    Field::SerialRcvd,
    Field::ExchangeSent,
    Field::ExchangeRcvd,
    Field::PropMode,
    Field::SatName,
    Field::SatMode,
];

/// Fields for the Advanced "Station" tab.
const ADV_STATION_FIELDS: &[Field] = &[
    Field::SnapshotStationCallsign,
    Field::SnapshotOperatorCallsign,
    Field::SnapshotOperatorName,
    Field::SnapshotGrid,
    Field::SnapshotCountry,
    Field::SnapshotState,
    Field::SnapshotCounty,
    Field::SnapshotProfileName,
    Field::SnapshotArrlSection,
    Field::SnapshotDxcc,
    Field::SnapshotCqZone,
    Field::SnapshotItuZone,
    Field::SnapshotLatitude,
    Field::SnapshotLongitude,
];

/// Fields for the Advanced "Transcript" tab.
const ADV_TRANSCRIPT_FIELDS: &[Field] = &[Field::CwDecodeRxWpm, Field::CwDecodeTranscript];

/// Fields for the Advanced "Metadata" tab.
const ADV_METADATA_FIELDS: &[Field] = &[
    Field::LocalId,
    Field::SyncStatus,
    Field::CreatedAt,
    Field::UpdatedAt,
    Field::ExtraFields,
];

/// State of the QSO entry form (basic + advanced fields).
#[derive(Clone)]
pub(crate) struct LogForm {
    /// Currently focused field.
    pub(crate) focused: Field,
    /// When `true`, the focused field's text is fully selected; typing replaces it.
    pub(crate) field_selected: bool,
    /// Cursor position within the focused text field, measured in Unicode scalar values.
    pub(crate) field_cursor: usize,
    /// Active tab in the Advanced view.
    pub(crate) advanced_tab: AdvancedTab,
    /// Worked callsign text.
    pub(crate) callsign: String,
    /// Index into [`BANDS`].
    pub(crate) band_idx: usize,
    /// Index into [`MODES`].
    pub(crate) mode_idx: usize,
    /// Frequency in MHz as a display string.
    pub(crate) frequency_mhz: String,
    /// Rig-derived receive frequency in Hz for split logging.
    pub(crate) rig_frequency_rx_hz: Option<u64>,
    /// Rig-derived receive band in ADIF form for split logging.
    pub(crate) rig_band_rx: Option<String>,
    /// Date in `YYYY-MM-DD` format.
    pub(crate) date: String,
    /// Time on (start) in `HH:MM` format.
    pub(crate) time: String,
    /// Whether the date and time can follow the start of a new callsign entry.
    pub(crate) timestamp_automatic: bool,
    /// Time off (end) in `HH:MM` format; empty means same as time on.
    pub(crate) time_off: String,
    /// Worked station QTH (city/location).
    pub(crate) qth: String,
    /// Local station callsign.
    pub(crate) station_callsign: String,
    /// RST sent report string.
    pub(crate) rst_sent: String,
    /// RST received report string.
    pub(crate) rst_rcvd: String,
    /// Short comment.
    pub(crate) comment: String,
    /// Operator notes.
    pub(crate) notes: String,
    // Advanced — Contest tab
    /// Transmitter power (e.g., "100W", "5W").
    pub(crate) tx_power: String,
    /// Submode override supplied by operator (overrides mode-derived submode).
    pub(crate) submode_override: String,
    /// Contest identifier (e.g., "CQWW", "ARRL-DX").
    pub(crate) contest_id: String,
    /// Contest serial number sent.
    pub(crate) serial_sent: String,
    /// Contest serial number received.
    pub(crate) serial_rcvd: String,
    /// Full exchange sent string.
    pub(crate) exchange_sent: String,
    /// Full exchange received string.
    pub(crate) exchange_rcvd: String,
    // Advanced card lookup and station details
    /// Propagation mode (ADIF `PROP_MODE` value, e.g., "ES", "TEP", "SAT").
    pub(crate) prop_mode: String,
    /// Satellite name (e.g., "AO-7").
    pub(crate) sat_name: String,
    /// Satellite mode (e.g., "V/U").
    pub(crate) sat_mode: String,
    // Advanced — Station tab
    /// IOTA designator (e.g., "EU-005").
    pub(crate) iota: String,
    /// ARRL section abbreviation (e.g., "WWA", "ENY").
    pub(crate) arrl_section: String,
    /// Worked US state abbreviation.
    pub(crate) worked_state: String,
    /// Worked county name.
    pub(crate) worked_county: String,
    /// Worked operator callsign.
    pub(crate) worked_operator_callsign: String,
    /// Worked operator name (from lookup or manual entry).
    pub(crate) worked_name: String,
    /// Worked grid square.
    pub(crate) worked_grid: String,
    /// Worked country.
    pub(crate) worked_country: String,
    /// Worked DXCC entity number.
    pub(crate) worked_dxcc: String,
    /// Worked CQ zone.
    pub(crate) worked_cq_zone: String,
    /// Worked ITU zone.
    pub(crate) worked_itu_zone: String,
    /// Worked continent.
    pub(crate) worked_continent: String,
    /// SKCC membership number of the worked station.
    pub(crate) skcc: String,
    /// Paper QSL sent status as ADIF code.
    pub(crate) qsl_sent_status: String,
    /// Paper QSL sent date as `YYYY-MM-DD`.
    pub(crate) qsl_sent_date: String,
    /// Paper QSL received status as ADIF code.
    pub(crate) qsl_received_status: String,
    /// Paper QSL received date as `YYYY-MM-DD`.
    pub(crate) qsl_received_date: String,
    /// `LoTW` sent state (`Y`, `N`, or empty).
    pub(crate) lotw_sent: String,
    /// `LoTW` received state (`Y`, `N`, or empty).
    pub(crate) lotw_received: String,
    /// eQSL sent state (`Y`, `N`, or empty).
    pub(crate) eqsl_sent: String,
    /// eQSL received state (`Y`, `N`, or empty).
    pub(crate) eqsl_received: String,
    /// QRZ log record ID.
    pub(crate) qrz_log_id: String,
    /// QRZ logbook ID.
    pub(crate) qrz_book_id: String,
    /// Captured station profile name.
    pub(crate) snapshot_profile_name: String,
    /// Captured station callsign.
    pub(crate) snapshot_station_callsign: String,
    /// Captured operator callsign.
    pub(crate) snapshot_operator_callsign: String,
    /// Captured operator name.
    pub(crate) snapshot_operator_name: String,
    /// Captured station grid.
    pub(crate) snapshot_grid: String,
    /// Captured station country.
    pub(crate) snapshot_country: String,
    /// Captured station state.
    pub(crate) snapshot_state: String,
    /// Captured station county.
    pub(crate) snapshot_county: String,
    /// Captured station ARRL section.
    pub(crate) snapshot_arrl_section: String,
    /// Captured station DXCC.
    pub(crate) snapshot_dxcc: String,
    /// Captured station CQ zone.
    pub(crate) snapshot_cq_zone: String,
    /// Captured station ITU zone.
    pub(crate) snapshot_itu_zone: String,
    /// Captured station latitude.
    pub(crate) snapshot_latitude: String,
    /// Captured station longitude.
    pub(crate) snapshot_longitude: String,
    /// Captured CW receive speed.
    pub(crate) cw_decode_rx_wpm: String,
    /// Captured CW transcript.
    pub(crate) cw_decode_transcript: String,
    /// Engine-assigned local ID.
    pub(crate) local_id: String,
    /// Engine sync status display.
    pub(crate) sync_status: String,
    /// Engine created timestamp display.
    pub(crate) created_at: String,
    /// Engine updated timestamp display.
    pub(crate) updated_at: String,
    /// Extra ADIF fields as `KEY=value` lines.
    pub(crate) extra_fields: String,
}

impl Default for LogForm {
    fn default() -> Self {
        Self::new()
    }
}

impl LogForm {
    /// Create a new form initialised with current UTC date/time and 20M/SSB defaults.
    pub(crate) fn new() -> Self {
        let now = OffsetDateTime::now_utc();
        let mut form = Self {
            focused: Field::Callsign,
            field_selected: false,
            field_cursor: 0,
            advanced_tab: AdvancedTab::Core,
            callsign: String::new(),
            band_idx: DEFAULT_BAND_IDX,
            mode_idx: 0,
            frequency_mhz: String::new(),
            rig_frequency_rx_hz: None,
            rig_band_rx: None,
            date: utc::format_date(now),
            time: utc::format_minutes(now),
            timestamp_automatic: true,
            time_off: String::new(),
            qth: String::new(),
            station_callsign: String::new(),
            rst_sent: String::new(),
            rst_rcvd: String::new(),
            comment: String::new(),
            notes: String::new(),
            tx_power: String::new(),
            submode_override: String::new(),
            contest_id: String::new(),
            serial_sent: String::new(),
            serial_rcvd: String::new(),
            exchange_sent: String::new(),
            exchange_rcvd: String::new(),
            prop_mode: String::new(),
            sat_name: String::new(),
            sat_mode: String::new(),
            iota: String::new(),
            arrl_section: String::new(),
            worked_state: String::new(),
            worked_county: String::new(),
            worked_operator_callsign: String::new(),
            worked_name: String::new(),
            worked_grid: String::new(),
            worked_country: String::new(),
            worked_dxcc: String::new(),
            worked_cq_zone: String::new(),
            worked_itu_zone: String::new(),
            worked_continent: String::new(),
            skcc: String::new(),
            qsl_sent_status: String::new(),
            qsl_sent_date: String::new(),
            qsl_received_status: String::new(),
            qsl_received_date: String::new(),
            lotw_sent: String::new(),
            lotw_received: String::new(),
            eqsl_sent: String::new(),
            eqsl_received: String::new(),
            qrz_log_id: String::new(),
            qrz_book_id: String::new(),
            snapshot_profile_name: String::new(),
            snapshot_station_callsign: String::new(),
            snapshot_operator_callsign: String::new(),
            snapshot_operator_name: String::new(),
            snapshot_grid: String::new(),
            snapshot_country: String::new(),
            snapshot_state: String::new(),
            snapshot_county: String::new(),
            snapshot_arrl_section: String::new(),
            snapshot_dxcc: String::new(),
            snapshot_cq_zone: String::new(),
            snapshot_itu_zone: String::new(),
            snapshot_latitude: String::new(),
            snapshot_longitude: String::new(),
            cw_decode_rx_wpm: String::new(),
            cw_decode_transcript: String::new(),
            local_id: String::new(),
            sync_status: String::new(),
            created_at: String::new(),
            updated_at: String::new(),
            extra_fields: String::new(),
        };
        form.on_band_change();
        form
    }

    /// Refresh the automatic timestamp when the operator starts a new callsign.
    pub(crate) fn refresh_automatic_timestamp(&mut self) {
        self.refresh_automatic_timestamp_at(OffsetDateTime::now_utc());
    }

    fn refresh_automatic_timestamp_at(&mut self, now: OffsetDateTime) {
        if self.timestamp_automatic {
            self.date = utc::format_date(now);
            self.time = utc::format_minutes(now);
        }
    }

    /// Set an explicit QSO start timestamp that callsign entry must not replace.
    pub(crate) fn set_explicit_timestamp(&mut self, now: OffsetDateTime) {
        self.date = utc::format_date(now);
        self.time = utc::format_minutes(now);
        self.timestamp_automatic = false;
    }

    /// Move focus to the next basic field, wrapping around, and select its text.
    pub(crate) fn next_field(&mut self) {
        let idx = FIELD_ORDER
            .iter()
            .position(|f| f == &self.focused)
            .unwrap_or(0);
        self.focused = FIELD_ORDER
            .get((idx + 1) % FIELD_ORDER.len())
            .copied()
            .unwrap_or(Field::Callsign);
        self.field_selected = true;
        self.field_cursor = self.focused_text_len();
    }

    /// Move focus to the previous basic field, wrapping around, and select its text.
    pub(crate) fn prev_field(&mut self) {
        let idx = FIELD_ORDER
            .iter()
            .position(|f| f == &self.focused)
            .unwrap_or(0);
        let new_idx = if idx == 0 {
            FIELD_ORDER.len().saturating_sub(1)
        } else {
            idx - 1
        };
        self.focused = FIELD_ORDER.get(new_idx).copied().unwrap_or(Field::Callsign);
        self.field_selected = true;
        self.field_cursor = self.focused_text_len();
    }

    /// Return the field list for the current advanced tab.
    pub(crate) fn current_advanced_fields(&self) -> &'static [Field] {
        match self.advanced_tab {
            AdvancedTab::Core => ADV_CORE_FIELDS,
            AdvancedTab::Lookup => ADV_LOOKUP_FIELDS,
            AdvancedTab::Qsl => ADV_QSL_FIELDS,
            AdvancedTab::Contest => ADV_CONTEST_FIELDS,
            AdvancedTab::Station => ADV_STATION_FIELDS,
            AdvancedTab::Transcript => ADV_TRANSCRIPT_FIELDS,
            AdvancedTab::Metadata => ADV_METADATA_FIELDS,
        }
    }

    /// Move focus to the next field in the current advanced tab, and select its text.
    pub(crate) fn next_advanced_field(&mut self) {
        let fields = self.current_advanced_fields();
        if fields.is_empty() {
            self.field_selected = true;
            return;
        }
        let idx = fields.iter().position(|f| f == &self.focused).unwrap_or(0);
        self.focused = fields
            .get((idx + 1) % fields.len())
            .copied()
            .unwrap_or(Field::Callsign);
        self.field_selected = true;
        self.field_cursor = self.focused_text_len();
    }

    /// Move focus to the previous field in the current advanced tab, and select its text.
    pub(crate) fn prev_advanced_field(&mut self) {
        let fields = self.current_advanced_fields();
        if fields.is_empty() {
            self.field_selected = true;
            return;
        }
        let idx = fields.iter().position(|f| f == &self.focused).unwrap_or(0);
        let new_idx = if idx == 0 {
            fields.len().saturating_sub(1)
        } else {
            idx - 1
        };
        self.focused = fields.get(new_idx).copied().unwrap_or(Field::Callsign);
        self.field_selected = true;
        self.field_cursor = self.focused_text_len();
    }

    /// Switch to the next advanced tab and focus its first field.
    pub(crate) fn next_advanced_tab(&mut self) {
        self.advanced_tab = self.advanced_tab.next();
        self.focused = self
            .current_advanced_fields()
            .first()
            .copied()
            .unwrap_or(Field::Callsign);
        self.field_selected = true;
        self.field_cursor = self.focused_text_len();
    }

    /// Switch to the previous advanced tab and focus its first field.
    pub(crate) fn prev_advanced_tab(&mut self) {
        self.advanced_tab = self.advanced_tab.prev();
        self.focused = self
            .current_advanced_fields()
            .first()
            .copied()
            .unwrap_or(Field::Callsign);
        self.field_selected = true;
        self.field_cursor = self.focused_text_len();
    }

    /// Update frequency and RST defaults after the band changes.
    pub(crate) fn on_band_change(&mut self) {
        let freq = BAND_DEFAULT_FREQS
            .get(self.band_idx)
            .copied()
            .unwrap_or(14.225);
        self.frequency_mhz = format_default_frequency_mhz(freq);
        self.on_mode_change();
    }

    /// Update RST defaults to match the currently selected mode.
    pub(crate) fn on_mode_change(&mut self) {
        let rst = default_rst_for_mode(self.mode_idx);
        self.rst_sent = rst.to_string();
        self.rst_rcvd = rst.to_string();
    }

    /// Return a mutable reference to the focused field's text buffer.
    ///
    /// Returns `None` for cycle-only fields (`Band`, `Mode`).
    pub(crate) fn current_field_text_mut(&mut self) -> Option<&mut String> {
        match self.focused {
            Field::Callsign => Some(&mut self.callsign),
            Field::FrequencyMhz => Some(&mut self.frequency_mhz),
            Field::Date => Some(&mut self.date),
            Field::Time => Some(&mut self.time),
            Field::TimeOff => Some(&mut self.time_off),
            Field::Qth => Some(&mut self.qth),
            Field::StationCallsign => Some(&mut self.station_callsign),
            Field::RstSent => Some(&mut self.rst_sent),
            Field::RstRcvd => Some(&mut self.rst_rcvd),
            Field::Comment => Some(&mut self.comment),
            Field::Notes => Some(&mut self.notes),
            Field::TxPower => Some(&mut self.tx_power),
            Field::Submode => Some(&mut self.submode_override),
            Field::ContestId => Some(&mut self.contest_id),
            Field::SerialSent => Some(&mut self.serial_sent),
            Field::SerialRcvd => Some(&mut self.serial_rcvd),
            Field::ExchangeSent => Some(&mut self.exchange_sent),
            Field::ExchangeRcvd => Some(&mut self.exchange_rcvd),
            Field::PropMode => Some(&mut self.prop_mode),
            Field::SatName => Some(&mut self.sat_name),
            Field::SatMode => Some(&mut self.sat_mode),
            Field::Iota => Some(&mut self.iota),
            Field::ArrlSection => Some(&mut self.arrl_section),
            Field::WorkedState => Some(&mut self.worked_state),
            Field::WorkedCounty => Some(&mut self.worked_county),
            Field::WorkedOperatorCallsign => Some(&mut self.worked_operator_callsign),
            Field::WorkedName => Some(&mut self.worked_name),
            Field::WorkedGrid => Some(&mut self.worked_grid),
            Field::WorkedCountry => Some(&mut self.worked_country),
            Field::WorkedDxcc => Some(&mut self.worked_dxcc),
            Field::WorkedCqZone => Some(&mut self.worked_cq_zone),
            Field::WorkedItuZone => Some(&mut self.worked_itu_zone),
            Field::WorkedContinent => Some(&mut self.worked_continent),
            Field::Skcc => Some(&mut self.skcc),
            Field::QslSentStatus => Some(&mut self.qsl_sent_status),
            Field::QslSentDate => Some(&mut self.qsl_sent_date),
            Field::QslReceivedStatus => Some(&mut self.qsl_received_status),
            Field::QslReceivedDate => Some(&mut self.qsl_received_date),
            Field::LotwSent => Some(&mut self.lotw_sent),
            Field::LotwReceived => Some(&mut self.lotw_received),
            Field::EqslSent => Some(&mut self.eqsl_sent),
            Field::EqslReceived => Some(&mut self.eqsl_received),
            Field::QrzLogId => Some(&mut self.qrz_log_id),
            Field::QrzBookId => Some(&mut self.qrz_book_id),
            Field::SnapshotProfileName => Some(&mut self.snapshot_profile_name),
            Field::SnapshotStationCallsign => Some(&mut self.snapshot_station_callsign),
            Field::SnapshotOperatorCallsign => Some(&mut self.snapshot_operator_callsign),
            Field::SnapshotOperatorName => Some(&mut self.snapshot_operator_name),
            Field::SnapshotGrid => Some(&mut self.snapshot_grid),
            Field::SnapshotCountry => Some(&mut self.snapshot_country),
            Field::SnapshotState => Some(&mut self.snapshot_state),
            Field::SnapshotCounty => Some(&mut self.snapshot_county),
            Field::SnapshotArrlSection => Some(&mut self.snapshot_arrl_section),
            Field::SnapshotDxcc => Some(&mut self.snapshot_dxcc),
            Field::SnapshotCqZone => Some(&mut self.snapshot_cq_zone),
            Field::SnapshotItuZone => Some(&mut self.snapshot_itu_zone),
            Field::SnapshotLatitude => Some(&mut self.snapshot_latitude),
            Field::SnapshotLongitude => Some(&mut self.snapshot_longitude),
            Field::CwDecodeRxWpm => Some(&mut self.cw_decode_rx_wpm),
            Field::CwDecodeTranscript => Some(&mut self.cw_decode_transcript),
            Field::ExtraFields => Some(&mut self.extra_fields),
            Field::Band
            | Field::Mode
            | Field::LocalId
            | Field::SyncStatus
            | Field::CreatedAt
            | Field::UpdatedAt => None,
        }
    }

    /// Return the focused field's text buffer.
    ///
    /// Returns `None` for cycle-only or read-only fields.
    pub(crate) fn current_field_text(&self) -> Option<&str> {
        match self.focused {
            Field::Callsign => Some(&self.callsign),
            Field::FrequencyMhz => Some(&self.frequency_mhz),
            Field::Date => Some(&self.date),
            Field::Time => Some(&self.time),
            Field::TimeOff => Some(&self.time_off),
            Field::Qth => Some(&self.qth),
            Field::StationCallsign => Some(&self.station_callsign),
            Field::RstSent => Some(&self.rst_sent),
            Field::RstRcvd => Some(&self.rst_rcvd),
            Field::Comment => Some(&self.comment),
            Field::Notes => Some(&self.notes),
            Field::TxPower => Some(&self.tx_power),
            Field::Submode => Some(&self.submode_override),
            Field::ContestId => Some(&self.contest_id),
            Field::SerialSent => Some(&self.serial_sent),
            Field::SerialRcvd => Some(&self.serial_rcvd),
            Field::ExchangeSent => Some(&self.exchange_sent),
            Field::ExchangeRcvd => Some(&self.exchange_rcvd),
            Field::PropMode => Some(&self.prop_mode),
            Field::SatName => Some(&self.sat_name),
            Field::SatMode => Some(&self.sat_mode),
            Field::Iota => Some(&self.iota),
            Field::ArrlSection => Some(&self.arrl_section),
            Field::WorkedState => Some(&self.worked_state),
            Field::WorkedCounty => Some(&self.worked_county),
            Field::WorkedOperatorCallsign => Some(&self.worked_operator_callsign),
            Field::WorkedName => Some(&self.worked_name),
            Field::WorkedGrid => Some(&self.worked_grid),
            Field::WorkedCountry => Some(&self.worked_country),
            Field::WorkedDxcc => Some(&self.worked_dxcc),
            Field::WorkedCqZone => Some(&self.worked_cq_zone),
            Field::WorkedItuZone => Some(&self.worked_itu_zone),
            Field::WorkedContinent => Some(&self.worked_continent),
            Field::Skcc => Some(&self.skcc),
            Field::QslSentStatus => Some(&self.qsl_sent_status),
            Field::QslSentDate => Some(&self.qsl_sent_date),
            Field::QslReceivedStatus => Some(&self.qsl_received_status),
            Field::QslReceivedDate => Some(&self.qsl_received_date),
            Field::LotwSent => Some(&self.lotw_sent),
            Field::LotwReceived => Some(&self.lotw_received),
            Field::EqslSent => Some(&self.eqsl_sent),
            Field::EqslReceived => Some(&self.eqsl_received),
            Field::QrzLogId => Some(&self.qrz_log_id),
            Field::QrzBookId => Some(&self.qrz_book_id),
            Field::SnapshotProfileName => Some(&self.snapshot_profile_name),
            Field::SnapshotStationCallsign => Some(&self.snapshot_station_callsign),
            Field::SnapshotOperatorCallsign => Some(&self.snapshot_operator_callsign),
            Field::SnapshotOperatorName => Some(&self.snapshot_operator_name),
            Field::SnapshotGrid => Some(&self.snapshot_grid),
            Field::SnapshotCountry => Some(&self.snapshot_country),
            Field::SnapshotState => Some(&self.snapshot_state),
            Field::SnapshotCounty => Some(&self.snapshot_county),
            Field::SnapshotArrlSection => Some(&self.snapshot_arrl_section),
            Field::SnapshotDxcc => Some(&self.snapshot_dxcc),
            Field::SnapshotCqZone => Some(&self.snapshot_cq_zone),
            Field::SnapshotItuZone => Some(&self.snapshot_itu_zone),
            Field::SnapshotLatitude => Some(&self.snapshot_latitude),
            Field::SnapshotLongitude => Some(&self.snapshot_longitude),
            Field::CwDecodeRxWpm => Some(&self.cw_decode_rx_wpm),
            Field::CwDecodeTranscript => Some(&self.cw_decode_transcript),
            Field::ExtraFields => Some(&self.extra_fields),
            Field::Band
            | Field::Mode
            | Field::LocalId
            | Field::SyncStatus
            | Field::CreatedAt
            | Field::UpdatedAt => None,
        }
    }

    /// Length of the focused text field in Unicode scalar values.
    pub(crate) fn focused_text_len(&self) -> usize {
        self.current_field_text()
            .map_or(0, |text| text.chars().count())
    }

    /// Move the focused text field cursor one character left, clamping at the start.
    pub(crate) fn move_cursor_left(&mut self) {
        if self.current_field_text().is_none() {
            return;
        }
        if self.field_selected {
            self.field_cursor = 0;
        } else {
            self.field_cursor = self.field_cursor.saturating_sub(1);
        }
        self.field_selected = false;
    }

    /// Move the focused text field cursor one character right, clamping at the end.
    pub(crate) fn move_cursor_right(&mut self) {
        let len = self.focused_text_len();
        if self.current_field_text().is_none() {
            return;
        }
        if self.field_selected {
            self.field_cursor = len;
        } else {
            self.field_cursor = (self.field_cursor + 1).min(len);
        }
        self.field_selected = false;
    }

    /// Move the focused text field cursor to the beginning.
    pub(crate) fn move_cursor_home(&mut self) {
        if self.current_field_text().is_some() {
            self.field_cursor = 0;
            self.field_selected = false;
        }
    }

    /// Move the focused text field cursor to the end.
    pub(crate) fn move_cursor_end(&mut self) {
        if self.current_field_text().is_some() {
            self.field_cursor = self.focused_text_len();
            self.field_selected = false;
        }
    }

    /// Insert `ch` into the focused text field at the cursor position.
    pub(crate) fn insert_char_at_cursor(&mut self, ch: char) {
        if matches!(self.focused, Field::Date | Field::Time) {
            self.timestamp_automatic = false;
        }
        if self.field_selected {
            if let Some(text) = self.current_field_text_mut() {
                text.clear();
            }
            self.field_cursor = 0;
            self.field_selected = false;
        }
        let cursor = self.field_cursor;
        if let Some(text) = self.current_field_text_mut() {
            let idx = byte_index_for_char(text, cursor);
            text.insert(idx, ch);
            self.field_cursor = cursor + 1;
        }
    }

    /// Remove the character before the cursor in the focused text field.
    pub(crate) fn backspace_at_cursor(&mut self) {
        if self.field_selected {
            if matches!(self.focused, Field::Date | Field::Time) {
                self.timestamp_automatic = false;
            }
            if let Some(text) = self.current_field_text_mut() {
                text.clear();
            }
            self.field_cursor = 0;
            self.field_selected = false;
            return;
        }
        if self.field_cursor == 0 {
            return;
        }
        if matches!(self.focused, Field::Date | Field::Time) {
            self.timestamp_automatic = false;
        }
        let cursor = self.field_cursor;
        if let Some(text) = self.current_field_text_mut() {
            let start = byte_index_for_char(text, cursor - 1);
            let end = byte_index_for_char(text, cursor);
            text.replace_range(start..end, "");
            self.field_cursor = cursor - 1;
        }
    }

    /// Remove the character at the cursor in the focused text field.
    pub(crate) fn delete_at_cursor(&mut self) {
        if self.field_selected {
            if matches!(self.focused, Field::Date | Field::Time) {
                self.timestamp_automatic = false;
            }
            if let Some(text) = self.current_field_text_mut() {
                text.clear();
            }
            self.field_cursor = 0;
            self.field_selected = false;
            return;
        }
        let cursor = self.field_cursor;
        if matches!(self.focused, Field::Date | Field::Time) && cursor < self.focused_text_len() {
            self.timestamp_automatic = false;
        }
        if let Some(text) = self.current_field_text_mut() {
            if cursor >= text.chars().count() {
                return;
            }
            let start = byte_index_for_char(text, cursor);
            let end = byte_index_for_char(text, cursor + 1);
            text.replace_range(start..end, "");
        }
    }

    /// Clear the focused text field and place the cursor at the beginning.
    pub(crate) fn clear_focused_text_field(&mut self) {
        if matches!(self.focused, Field::Date | Field::Time) {
            self.timestamp_automatic = false;
        }
        if let Some(text) = self.current_field_text_mut() {
            text.clear();
        }
        self.field_cursor = 0;
        self.field_selected = false;
    }

    /// Current band name from the [`BANDS`] slice.
    pub(crate) fn band_str(&self) -> &str {
        BANDS.get(self.band_idx).copied().unwrap_or("20M")
    }

    /// Current mode name from the [`MODES`] slice.
    pub(crate) fn mode_str(&self) -> &str {
        MODES.get(self.mode_idx).copied().unwrap_or("SSB")
    }

    /// Returns `true` if the focused field is a Left/Right cycle selector.
    pub(crate) fn is_cycle_field(&self) -> bool {
        matches!(self.focused, Field::Band | Field::Mode)
    }

    /// Cycle to the next band whose name starts with `ch` (case-insensitive).
    ///
    /// Repeated calls with the same char advance through all matching bands, wrapping around.
    pub(crate) fn type_select_band(&mut self, ch: char) {
        let ch_lo = ch.to_ascii_lowercase();
        let matches: Vec<usize> = BANDS
            .iter()
            .enumerate()
            .filter(|(_, b)| b.chars().next().map(|c| c.to_ascii_lowercase()) == Some(ch_lo))
            .map(|(i, _)| i)
            .collect();
        if matches.is_empty() {
            return;
        }
        let pos = matches.iter().position(|&i| i == self.band_idx);
        let next = pos.map_or(0, |p| (p + 1) % matches.len());
        if let Some(&idx) = matches.get(next) {
            self.band_idx = idx;
            self.on_band_change();
        }
    }

    /// Cycle to the next mode whose name starts with `ch` (case-insensitive).
    ///
    /// Repeated calls with the same char advance through all matching modes, wrapping around.
    pub(crate) fn type_select_mode(&mut self, ch: char) {
        let ch_lo = ch.to_ascii_lowercase();
        let matches: Vec<usize> = MODES
            .iter()
            .enumerate()
            .filter(|(_, m)| m.chars().next().map(|c| c.to_ascii_lowercase()) == Some(ch_lo))
            .map(|(i, _)| i)
            .collect();
        if matches.is_empty() {
            return;
        }
        let pos = matches.iter().position(|&i| i == self.mode_idx);
        let next = pos.map_or(0, |p| (p + 1) % matches.len());
        if let Some(&idx) = matches.get(next) {
            self.mode_idx = idx;
            self.on_mode_change();
        }
    }
}

fn byte_index_for_char(text: &str, char_idx: usize) -> usize {
    text.char_indices()
        .nth(char_idx)
        .map_or_else(|| text.len(), |(idx, _)| idx)
}

/// Format a band default frequency using the same Hz-preserving display path as rig updates.
fn format_default_frequency_mhz(freq: f64) -> String {
    crate::grpc::format_frequency_mhz(crate::grpc::mhz_to_hz(freq))
}

/// Return the default RST string for the given mode index.
fn default_rst_for_mode(mode_idx: usize) -> &'static str {
    match MODES.get(mode_idx).copied().unwrap_or("SSB") {
        "SSB" | "AM" | "FM" => "59",
        _ => "599",
    }
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::type_complexity,
    clippy::items_after_statements
)]
mod tests {
    use super::*;

    #[test]
    fn new_form_starts_on_20m_ssb() {
        let form = LogForm::new();
        assert_eq!(form.band_str(), "20M");
        assert_eq!(form.mode_str(), "SSB");
        assert_eq!(form.focused, Field::Callsign);
        assert!(!form.field_selected);
    }

    #[test]
    fn new_form_sets_default_frequency() {
        let form = LogForm::new();
        assert_eq!(form.frequency_mhz, "14.225.000");
    }

    #[test]
    fn new_form_sets_rst_defaults_for_ssb() {
        let form = LogForm::new();
        assert_eq!(form.rst_sent, "59");
        assert_eq!(form.rst_rcvd, "59");
    }

    #[test]
    fn next_field_advances_focus() {
        let mut form = LogForm::new();
        assert_eq!(form.focused, Field::Callsign);
        form.next_field();
        assert_eq!(form.focused, Field::Band);
        assert!(form.field_selected);
    }

    #[test]
    fn next_field_wraps_to_start() {
        let mut form = LogForm::new();
        for _ in 0..FIELD_ORDER.len() {
            form.next_field();
        }
        assert_eq!(form.focused, Field::Callsign);
    }

    #[test]
    fn prev_field_moves_to_last_from_first() {
        let mut form = LogForm::new();
        form.prev_field();
        assert_eq!(form.focused, *FIELD_ORDER.last().unwrap());
        assert!(form.field_selected);
    }

    #[test]
    fn prev_field_from_second_returns_to_first() {
        let mut form = LogForm::new();
        form.next_field();
        form.prev_field();
        assert_eq!(form.focused, Field::Callsign);
    }

    #[test]
    fn on_mode_change_sets_cw_rst() {
        let mut form = LogForm::new();
        form.mode_idx = MODES.iter().position(|&m| m == "CW").unwrap();
        form.on_mode_change();
        assert_eq!(form.rst_sent, "599");
        assert_eq!(form.rst_rcvd, "599");
    }

    #[test]
    fn on_mode_change_sets_ft8_rst() {
        let mut form = LogForm::new();
        form.mode_idx = MODES.iter().position(|&m| m == "FT8").unwrap();
        form.on_mode_change();
        assert_eq!(form.rst_sent, "599");
        assert_eq!(form.rst_rcvd, "599");
    }

    #[test]
    fn on_mode_change_sets_ft4_rst() {
        let mut form = LogForm::new();
        form.mode_idx = MODES.iter().position(|&m| m == "FT4").unwrap();
        form.on_mode_change();
        assert_eq!(form.rst_sent, "599");
    }

    #[test]
    fn on_mode_change_sets_am_rst() {
        let mut form = LogForm::new();
        form.mode_idx = MODES.iter().position(|&m| m == "AM").unwrap();
        form.on_mode_change();
        assert_eq!(form.rst_sent, "59");
    }

    #[test]
    fn on_mode_change_sets_fm_rst() {
        let mut form = LogForm::new();
        form.mode_idx = MODES.iter().position(|&m| m == "FM").unwrap();
        form.on_mode_change();
        assert_eq!(form.rst_sent, "59");
    }

    #[test]
    fn on_mode_change_out_of_bounds_defaults_to_ssb_rst() {
        let mut form = LogForm::new();
        form.mode_idx = 999;
        form.on_mode_change();
        assert_eq!(form.rst_sent, "59");
    }

    #[test]
    fn on_band_change_updates_frequency_for_40m() {
        let mut form = LogForm::new();
        form.band_idx = BANDS.iter().position(|&b| b == "40M").unwrap();
        form.on_band_change();
        assert_eq!(form.frequency_mhz, "7.150.000");
    }

    #[test]
    fn on_band_change_updates_frequency_for_160m() {
        let mut form = LogForm::new();
        form.band_idx = 0;
        form.on_band_change();
        assert_eq!(form.frequency_mhz, "1.900.000");
    }

    #[test]
    fn on_band_change_out_of_bounds_uses_fallback() {
        let mut form = LogForm::new();
        form.band_idx = 999;
        form.on_band_change();
        assert_eq!(form.frequency_mhz, "14.225.000");
    }

    #[test]
    fn format_default_frequency_preserves_sub_kilohertz_fraction() {
        assert_eq!(format_default_frequency_mhz(14.225_678), "14.225.678");
    }

    #[test]
    fn band_str_out_of_bounds_returns_20m() {
        let mut form = LogForm::new();
        form.band_idx = 999;
        assert_eq!(form.band_str(), "20M");
    }

    #[test]
    fn mode_str_out_of_bounds_returns_ssb() {
        let mut form = LogForm::new();
        form.mode_idx = 999;
        assert_eq!(form.mode_str(), "SSB");
    }

    #[test]
    fn is_cycle_field_true_for_band_and_mode() {
        let mut form = LogForm::new();
        form.focused = Field::Band;
        assert!(form.is_cycle_field());
        form.focused = Field::Mode;
        assert!(form.is_cycle_field());
    }

    #[test]
    fn is_cycle_field_false_for_callsign() {
        let form = LogForm::new();
        assert!(!form.is_cycle_field());
    }

    #[test]
    fn current_field_text_mut_callsign() {
        let mut form = LogForm::new();
        form.focused = Field::Callsign;
        {
            let text = form.current_field_text_mut().unwrap();
            text.push_str("K7ABC");
        }
        assert_eq!(form.callsign, "K7ABC");
    }

    #[test]
    fn current_field_text_mut_all_text_fields() {
        let fields_and_setters: Vec<(Field, Box<dyn Fn(&LogForm) -> &str>)> = vec![
            (
                Field::FrequencyMhz,
                Box::new(|f: &LogForm| f.frequency_mhz.as_str()),
            ),
            (Field::Date, Box::new(|f: &LogForm| f.date.as_str())),
            (Field::Time, Box::new(|f: &LogForm| f.time.as_str())),
            (Field::TimeOff, Box::new(|f: &LogForm| f.time_off.as_str())),
            (Field::Qth, Box::new(|f: &LogForm| f.qth.as_str())),
            (Field::RstSent, Box::new(|f: &LogForm| f.rst_sent.as_str())),
            (Field::RstRcvd, Box::new(|f: &LogForm| f.rst_rcvd.as_str())),
            (Field::Comment, Box::new(|f: &LogForm| f.comment.as_str())),
            (Field::Notes, Box::new(|f: &LogForm| f.notes.as_str())),
            (Field::TxPower, Box::new(|f: &LogForm| f.tx_power.as_str())),
            (
                Field::Submode,
                Box::new(|f: &LogForm| f.submode_override.as_str()),
            ),
            (
                Field::ContestId,
                Box::new(|f: &LogForm| f.contest_id.as_str()),
            ),
            (
                Field::SerialSent,
                Box::new(|f: &LogForm| f.serial_sent.as_str()),
            ),
            (
                Field::SerialRcvd,
                Box::new(|f: &LogForm| f.serial_rcvd.as_str()),
            ),
            (
                Field::ExchangeSent,
                Box::new(|f: &LogForm| f.exchange_sent.as_str()),
            ),
            (
                Field::ExchangeRcvd,
                Box::new(|f: &LogForm| f.exchange_rcvd.as_str()),
            ),
            (
                Field::PropMode,
                Box::new(|f: &LogForm| f.prop_mode.as_str()),
            ),
            (Field::SatName, Box::new(|f: &LogForm| f.sat_name.as_str())),
            (Field::SatMode, Box::new(|f: &LogForm| f.sat_mode.as_str())),
            (Field::Iota, Box::new(|f: &LogForm| f.iota.as_str())),
            (
                Field::ArrlSection,
                Box::new(|f: &LogForm| f.arrl_section.as_str()),
            ),
            (
                Field::WorkedState,
                Box::new(|f: &LogForm| f.worked_state.as_str()),
            ),
            (
                Field::WorkedCounty,
                Box::new(|f: &LogForm| f.worked_county.as_str()),
            ),
            (
                Field::WorkedName,
                Box::new(|f: &LogForm| f.worked_name.as_str()),
            ),
            (
                Field::WorkedGrid,
                Box::new(|f: &LogForm| f.worked_grid.as_str()),
            ),
            (
                Field::WorkedCountry,
                Box::new(|f: &LogForm| f.worked_country.as_str()),
            ),
            (
                Field::WorkedDxcc,
                Box::new(|f: &LogForm| f.worked_dxcc.as_str()),
            ),
            (
                Field::WorkedCqZone,
                Box::new(|f: &LogForm| f.worked_cq_zone.as_str()),
            ),
            (
                Field::WorkedItuZone,
                Box::new(|f: &LogForm| f.worked_itu_zone.as_str()),
            ),
            (
                Field::WorkedContinent,
                Box::new(|f: &LogForm| f.worked_continent.as_str()),
            ),
            (Field::Skcc, Box::new(|f: &LogForm| f.skcc.as_str())),
        ];
        for (field, _getter) in &fields_and_setters {
            let mut form = LogForm::new();
            form.focused = *field;
            assert!(
                form.current_field_text_mut().is_some(),
                "Field {field:?} should return Some"
            );
        }
    }

    #[test]
    fn current_field_text_mut_none_for_cycle_fields() {
        let mut form = LogForm::new();
        form.focused = Field::Band;
        assert!(form.current_field_text_mut().is_none());
        form.focused = Field::Mode;
        assert!(form.current_field_text_mut().is_none());
    }

    #[test]
    fn type_select_band_cycles_through_matching() {
        let mut form = LogForm::new();
        form.band_idx = BANDS.iter().position(|&b| b == "160M").unwrap();
        form.type_select_band('1');
        assert_eq!(form.band_str(), "17M");
        form.type_select_band('1');
        assert_eq!(form.band_str(), "15M");
        form.type_select_band('1');
        assert_eq!(form.band_str(), "12M");
        form.type_select_band('1');
        assert_eq!(form.band_str(), "10M");
        form.type_select_band('1');
        assert_eq!(form.band_str(), "160M");
    }

    #[test]
    fn type_select_band_ignores_unknown_char() {
        let mut form = LogForm::new();
        let original = form.band_idx;
        form.type_select_band('z');
        assert_eq!(form.band_idx, original);
    }

    #[test]
    fn type_select_mode_selects_by_first_char() {
        let mut form = LogForm::new();
        form.type_select_mode('c');
        assert_eq!(form.mode_str(), "CW");
    }

    #[test]
    fn type_select_mode_cycles_through_matching() {
        let mut form = LogForm::new();
        form.type_select_mode('f');
        let first = form.mode_str().to_string();
        form.type_select_mode('f');
        let second = form.mode_str().to_string();
        assert_ne!(first, second);
    }

    #[test]
    fn type_select_mode_ignores_unknown_char() {
        let mut form = LogForm::new();
        let original = form.mode_idx;
        form.type_select_mode('z');
        assert_eq!(form.mode_idx, original);
    }

    #[test]
    fn advanced_tab_shortcut_digits() {
        assert_eq!(AdvancedTab::Core.shortcut_digit(), '1');
        assert_eq!(AdvancedTab::Lookup.shortcut_digit(), '2');
        assert_eq!(AdvancedTab::Qsl.shortcut_digit(), '3');
        assert_eq!(AdvancedTab::Contest.shortcut_digit(), '4');
        assert_eq!(AdvancedTab::Station.shortcut_digit(), '5');
        assert_eq!(AdvancedTab::Transcript.shortcut_digit(), '6');
        assert_eq!(AdvancedTab::Metadata.shortcut_digit(), '7');
    }

    #[test]
    fn advanced_tab_first_fields() {
        assert_eq!(AdvancedTab::Core.first_field(), Field::Callsign);
        assert_eq!(
            AdvancedTab::Lookup.first_field(),
            Field::WorkedOperatorCallsign
        );
        assert_eq!(AdvancedTab::Qsl.first_field(), Field::QslSentStatus);
        assert_eq!(AdvancedTab::Contest.first_field(), Field::ContestId);
        assert_eq!(
            AdvancedTab::Station.first_field(),
            Field::SnapshotStationCallsign
        );
        assert_eq!(AdvancedTab::Transcript.first_field(), Field::CwDecodeRxWpm);
        assert_eq!(AdvancedTab::Metadata.first_field(), Field::LocalId);
    }

    #[test]
    fn advanced_tab_fields_consistent_with_adv_slices() {
        assert_eq!(AdvancedTab::Core.fields(), ADV_CORE_FIELDS);
        assert_eq!(AdvancedTab::Lookup.fields(), ADV_LOOKUP_FIELDS);
        assert_eq!(AdvancedTab::Qsl.fields(), ADV_QSL_FIELDS);
        assert_eq!(AdvancedTab::Contest.fields(), ADV_CONTEST_FIELDS);
        assert_eq!(AdvancedTab::Station.fields(), ADV_STATION_FIELDS);
        assert_eq!(AdvancedTab::Transcript.fields(), ADV_TRANSCRIPT_FIELDS);
        assert_eq!(AdvancedTab::Metadata.fields(), ADV_METADATA_FIELDS);
    }

    #[test]
    fn advanced_tab_labels() {
        assert_eq!(AdvancedTab::Core.label(), "Core");
        assert_eq!(AdvancedTab::Lookup.label(), "Lookup");
        assert_eq!(AdvancedTab::Qsl.label(), "QSL");
        assert_eq!(AdvancedTab::Contest.label(), "Contest");
        assert_eq!(AdvancedTab::Station.label(), "Station");
        assert_eq!(AdvancedTab::Transcript.label(), "Transcript");
        assert_eq!(AdvancedTab::Metadata.label(), "Metadata");
    }

    #[test]
    fn advanced_tab_next_cycles_all() {
        assert_eq!(AdvancedTab::Core.next(), AdvancedTab::Lookup);
        assert_eq!(AdvancedTab::Lookup.next(), AdvancedTab::Qsl);
        assert_eq!(AdvancedTab::Qsl.next(), AdvancedTab::Contest);
        assert_eq!(AdvancedTab::Contest.next(), AdvancedTab::Station);
        assert_eq!(AdvancedTab::Station.next(), AdvancedTab::Transcript);
        assert_eq!(AdvancedTab::Transcript.next(), AdvancedTab::Metadata);
        assert_eq!(AdvancedTab::Metadata.next(), AdvancedTab::Core);
    }

    #[test]
    fn advanced_tab_prev_cycles_all() {
        assert_eq!(AdvancedTab::Core.prev(), AdvancedTab::Metadata);
        assert_eq!(AdvancedTab::Metadata.prev(), AdvancedTab::Transcript);
        assert_eq!(AdvancedTab::Transcript.prev(), AdvancedTab::Station);
        assert_eq!(AdvancedTab::Station.prev(), AdvancedTab::Contest);
        assert_eq!(AdvancedTab::Contest.prev(), AdvancedTab::Qsl);
        assert_eq!(AdvancedTab::Qsl.prev(), AdvancedTab::Lookup);
        assert_eq!(AdvancedTab::Lookup.prev(), AdvancedTab::Core);
    }

    #[test]
    fn all_advanced_tabs_count() {
        assert_eq!(AdvancedTab::ALL.len(), 7);
    }

    #[test]
    fn next_advanced_tab_updates_focus() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Core;
        form.next_advanced_tab();
        assert_eq!(form.advanced_tab, AdvancedTab::Lookup);
        assert!(form.field_selected);
    }

    #[test]
    fn prev_advanced_tab_updates_focus() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Core;
        form.prev_advanced_tab();
        assert_eq!(form.advanced_tab, AdvancedTab::Metadata);
        assert!(form.field_selected);
    }

    #[test]
    fn next_advanced_field_wraps_in_contest_tab() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Contest;
        let count = ADV_CONTEST_FIELDS.len();
        form.focused = ADV_CONTEST_FIELDS[0];
        for _ in 0..count {
            form.next_advanced_field();
        }
        assert_eq!(form.focused, ADV_CONTEST_FIELDS[0]);
    }

    #[test]
    fn prev_advanced_field_from_first_wraps_to_last() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Lookup;
        let fields = form.current_advanced_fields();
        form.focused = fields[0];
        form.prev_advanced_field();
        assert_eq!(form.focused, *fields.last().unwrap());
    }

    #[test]
    fn current_advanced_fields_core_tab() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Core;
        let fields = form.current_advanced_fields();
        assert!(fields.contains(&Field::Callsign));
        assert!(fields.contains(&Field::FrequencyMhz));
    }

    #[test]
    fn current_advanced_fields_lookup_tab() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Lookup;
        let fields = form.current_advanced_fields();
        assert!(fields.contains(&Field::WorkedName));
        assert!(fields.contains(&Field::WorkedGrid));
        assert!(fields.contains(&Field::Skcc));
    }

    #[test]
    fn current_advanced_fields_station_tab() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Station;
        let fields = form.current_advanced_fields();
        assert!(fields.contains(&Field::SnapshotStationCallsign));
        assert!(fields.contains(&Field::SnapshotGrid));
    }

    #[test]
    fn current_advanced_fields_contest_tab() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Contest;
        let fields = form.current_advanced_fields();
        assert!(fields.contains(&Field::ContestId));
        assert!(fields.contains(&Field::SerialSent));
    }

    #[test]
    fn current_advanced_fields_avalonia_parity_tabs_are_populated() {
        let mut form = LogForm::new();
        form.advanced_tab = AdvancedTab::Qsl;
        assert!(form
            .current_advanced_fields()
            .contains(&Field::QslSentStatus));
        form.advanced_tab = AdvancedTab::Transcript;
        assert!(form
            .current_advanced_fields()
            .contains(&Field::CwDecodeTranscript));
        form.advanced_tab = AdvancedTab::Metadata;
        assert!(form.current_advanced_fields().contains(&Field::ExtraFields));
    }

    #[test]
    fn skcc_field_text_mut_returns_buffer() {
        let mut form = LogForm::new();
        form.focused = Field::Skcc;
        let buf = form.current_field_text_mut().unwrap();
        buf.push_str("12345T");
        assert_eq!(form.skcc, "12345T");
    }

    #[test]
    fn skcc_initialises_empty() {
        let form = LogForm::new();
        assert!(form.skcc.is_empty());
    }

    #[test]
    fn automatic_timestamp_refreshes_when_callsign_entry_starts() {
        let mut form = LogForm::new();
        form.date = "2020-01-02".to_string();
        form.time = "03:04".to_string();
        let now = time::macros::datetime!(2026-08-19 02:30 UTC);

        form.refresh_automatic_timestamp_at(now);

        assert_eq!(form.date, "2026-08-19");
        assert_eq!(form.time, "02:30");
    }

    #[test]
    fn manual_timestamp_is_not_refreshed() {
        let mut form = LogForm::new();
        form.date = "2020-01-02".to_string();
        form.time = "03:04".to_string();
        form.focused = Field::Time;
        form.field_selected = true;
        form.insert_char_at_cursor('5');
        let edited_time = form.time.clone();
        let now = time::macros::datetime!(2026-08-19 02:30 UTC);

        form.refresh_automatic_timestamp_at(now);

        assert_eq!(form.date, "2020-01-02");
        assert_eq!(form.time, edited_time);
    }
}
