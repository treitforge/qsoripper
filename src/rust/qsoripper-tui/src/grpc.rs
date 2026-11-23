//! gRPC client helpers: channel creation, QSO logging, listing, lookup, and space weather.

use std::collections::HashMap;

use anyhow::{bail, Context};
use qsoripper_core::utc;
use tonic::transport::{Channel, Endpoint};

use qsoripper_core::domain::band::{band_from_adif, band_to_adif};
use qsoripper_core::domain::mode::{mode_from_adif, mode_to_adif};
use qsoripper_core::domain::qso::{qsl_status_from_adif, qsl_status_to_adif};
use qsoripper_core::proto::qsoripper::domain::{
    Band, LookupState, Mode, QslStatus, QsoRecord, RstReport, StationSnapshot,
};
use qsoripper_core::proto::qsoripper::services::{
    logbook_service_client::LogbookServiceClient, lookup_service_client::LookupServiceClient,
    rig_control_service_client::RigControlServiceClient,
    space_weather_service_client::SpaceWeatherServiceClient, DeleteQsoRequest,
    GetCurrentSpaceWeatherRequest, GetRigSnapshotRequest, ListQsosRequest, LogQsoRequest,
    LookupRequest, PurgeDeletedQsosRequest, UpdateQsoRequest,
};

use crate::app::{CallsignInfo, RecentQso, RigInfo, RigStatus, SpaceWeatherInfo};
use crate::form::{LogForm, BANDS, MODES};

/// Enrichment snapshot from a callsign lookup: `(grid, country, cq_zone, dxcc)`.
type LookupEnrichment = Option<(Option<String>, Option<String>, Option<u32>, Option<u32>)>;

/// Create a tonic transport channel connected to the given endpoint URI.
pub(crate) fn create_channel(endpoint: &str) -> anyhow::Result<Channel> {
    let endpoint = Endpoint::from_shared(endpoint.to_string()).context("invalid endpoint URI")?;
    Ok(endpoint.connect_lazy())
}

/// Log a QSO from the form and return the engine-assigned `local_id`.
///
/// `lookup` carries enrichment from the callsign lookup: `(grid, country, cq_zone, dxcc)`.
pub(crate) async fn log_qso(
    channel: Channel,
    form: &LogForm,
    lookup: LookupEnrichment,
) -> anyhow::Result<String> {
    let mut client = LogbookServiceClient::new(channel);

    let band: Band = BANDS
        .get(form.band_idx)
        .and_then(|s| band_from_adif(s))
        .unwrap_or(Band::Unspecified);

    let mode_str = MODES.get(form.mode_idx).copied().unwrap_or("SSB");
    let (mode, submode) = resolve_mode(mode_str);

    let utc_timestamp = parse_timestamp(&form.date, &form.time).ok();
    let utc_end_timestamp = if form.time_off.is_empty() {
        None
    } else {
        parse_timestamp(&form.date, &form.time_off).ok()
    };

    let frequency_hz = parse_frequency_mhz(&form.frequency_mhz);

    let (lookup_grid, lookup_country, lookup_cq_zone, lookup_dxcc) =
        lookup.unwrap_or((None, None, None, None));

    let qso = QsoRecord {
        worked_callsign: form.callsign.to_uppercase(),
        station_callsign: form.station_callsign.to_uppercase(),
        band: i32::from(band),
        mode: i32::from(mode),
        utc_timestamp,
        utc_end_timestamp,
        frequency_hz,
        frequency_rx_hz: form.rig_frequency_rx_hz,
        band_rx: form
            .rig_band_rx
            .as_deref()
            .and_then(band_from_adif)
            .map(i32::from)
            .unwrap_or_default(),
        submode: if form.submode_override.is_empty() {
            submode.map(str::to_string)
        } else {
            Some(form.submode_override.clone())
        },
        rst_sent: parse_rst(&form.rst_sent),
        rst_received: parse_rst(&form.rst_rcvd),
        comment: opt_string(&form.comment),
        notes: opt_string(&form.notes),
        tx_power: opt_string(&form.tx_power),
        qsl_sent_status: i32::from(parse_qsl_status(&form.qsl_sent_status)?),
        qsl_received_status: i32::from(parse_qsl_status(&form.qsl_received_status)?),
        lotw_sent: parse_optional_bool(&form.lotw_sent)?,
        lotw_received: parse_optional_bool(&form.lotw_received)?,
        eqsl_sent: parse_optional_bool(&form.eqsl_sent)?,
        eqsl_received: parse_optional_bool(&form.eqsl_received)?,
        qsl_sent_date: parse_optional_date(&form.qsl_sent_date)?,
        qsl_received_date: parse_optional_date(&form.qsl_received_date)?,
        qrz_logid: opt_string(&form.qrz_log_id),
        qrz_bookid: opt_string(&form.qrz_book_id),
        contest_id: opt_string(&form.contest_id),
        serial_sent: opt_string(&form.serial_sent),
        serial_received: opt_string(&form.serial_rcvd),
        exchange_sent: opt_string(&form.exchange_sent),
        exchange_received: opt_string(&form.exchange_rcvd),
        worked_grid: opt_string(&form.worked_grid).or(lookup_grid),
        worked_country: opt_string(&form.worked_country).or(lookup_country),
        worked_cq_zone: opt_u32(&form.worked_cq_zone).or(lookup_cq_zone),
        worked_dxcc: opt_u32(&form.worked_dxcc).or(lookup_dxcc),
        worked_itu_zone: opt_u32(&form.worked_itu_zone),
        worked_continent: opt_string(&form.worked_continent),
        worked_operator_callsign: opt_string(&form.worked_operator_callsign.to_uppercase()),
        worked_operator_name: opt_string(&form.worked_name),
        worked_iota: opt_string(&form.iota),
        worked_arrl_section: opt_string(&form.arrl_section),
        worked_state: opt_string(&form.worked_state),
        worked_county: opt_string(&form.worked_county),
        skcc: opt_string(&form.skcc),
        prop_mode: opt_string(&form.prop_mode),
        sat_name: opt_string(&form.sat_name),
        sat_mode: opt_string(&form.sat_mode),
        station_snapshot: station_snapshot_from_form(form)?,
        extra_fields: parse_extra_fields(&form.extra_fields)?,
        cw_decode_rx_wpm: opt_u32(&form.cw_decode_rx_wpm),
        cw_decode_transcript: opt_string(&form.cw_decode_transcript),
        ..Default::default()
    };

    let request = LogQsoRequest {
        qso: Some(qso),
        sync_to_qrz: false,
        sync_to_lotw: false,
    };

    let response = client.log_qso(request).await?.into_inner();
    Ok(response.local_id)
}

/// Fetch the most recent `limit` QSOs from the logbook service.
pub(crate) async fn list_recent_qsos(
    channel: Channel,
    limit: u32,
) -> anyhow::Result<Vec<RecentQso>> {
    let mut client = LogbookServiceClient::new(channel);

    let request = ListQsosRequest {
        limit,
        offset: 0,
        sort: 0, // QSO_SORT_ORDER_NEWEST_FIRST
        ..Default::default()
    };

    let mut stream = client.list_qsos(request).await?.into_inner();
    let mut result = Vec::new();

    while let Some(response) = stream.message().await? {
        let Some(qso) = response.qso else { continue };

        let (date, utc) = qso
            .utc_timestamp
            .as_ref()
            .and_then(utc::from_timestamp)
            .map(|dt| (utc::format_date(dt), utc::format_minutes(dt)))
            .unwrap_or_default();

        let band = Band::try_from(qso.band)
            .ok()
            .and_then(band_to_adif)
            .unwrap_or("?")
            .to_string();

        let mode = Mode::try_from(qso.mode)
            .ok()
            .and_then(mode_to_adif)
            .unwrap_or("?")
            .to_string();

        let rst_sent = qso
            .rst_sent
            .as_ref()
            .map(|r| r.raw.clone())
            .unwrap_or_default();
        let rst_rcvd = qso
            .rst_received
            .as_ref()
            .map(|r| r.raw.clone())
            .unwrap_or_default();

        result.push(RecentQso {
            local_id: qso.local_id.clone(),
            date,
            utc,
            callsign: qso.worked_callsign.clone(),
            band,
            mode,
            rst_sent,
            rst_rcvd,
            country: qso.worked_country.clone(),
            grid: qso.worked_grid.clone(),
            name: qso.worked_operator_name.clone(),
            source_record: qso,
        });
    }

    Ok(result)
}

/// Look up a callsign via the lookup service and return display-ready info.
pub(crate) async fn lookup_callsign(
    channel: Channel,
    callsign: &str,
) -> anyhow::Result<Option<CallsignInfo>> {
    let mut client = LookupServiceClient::new(channel);

    let request = LookupRequest {
        callsign: callsign.to_string(),
        skip_cache: false,
    };

    let response = client.lookup(request).await?.into_inner();

    let Some(result) = response.result else {
        return Ok(None);
    };

    if result.state != LookupState::Found as i32 {
        return Ok(None);
    }

    let Some(record) = result.record else {
        return Ok(None);
    };

    let name = record.formatted_name.or_else(|| {
        let full = format!("{} {}", record.first_name, record.last_name);
        let trimmed = full.trim().to_string();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed)
        }
    });

    Ok(Some(CallsignInfo {
        callsign: record.callsign,
        name,
        qth: match (record.addr2.as_deref(), record.state.as_deref()) {
            (Some(city), Some(st)) if !st.is_empty() => Some(format!("{city}, {st}")),
            (Some(city), _) => Some(city.to_string()),
            (None, Some(st)) if !st.is_empty() => Some(st.to_string()),
            _ => None,
        },
        grid: record.grid_square,
        country: record.country,
        cq_zone: record.cq_zone,
        dxcc: if record.dxcc_entity_id == 0 {
            None
        } else {
            Some(record.dxcc_entity_id)
        },
    }))
}

/// Fetch the current space weather snapshot.
pub(crate) async fn get_space_weather(
    channel: Channel,
) -> anyhow::Result<Option<SpaceWeatherInfo>> {
    let mut client = SpaceWeatherServiceClient::new(channel);

    let response = client
        .get_current_space_weather(GetCurrentSpaceWeatherRequest {})
        .await?
        .into_inner();

    let Some(snapshot) = response.snapshot else {
        return Ok(None);
    };

    Ok(Some(SpaceWeatherInfo {
        k_index: snapshot.planetary_k_index,
        solar_flux: snapshot.solar_flux_index,
        sunspot_number: snapshot.sunspot_number,
    }))
}

/// Fetch the current rig snapshot from the rig control service.
pub(crate) async fn get_rig_snapshot(channel: Channel) -> anyhow::Result<Option<RigInfo>> {
    use qsoripper_core::proto::qsoripper::domain::RigConnectionStatus;

    let mut client = RigControlServiceClient::new(channel);

    let response = client
        .get_rig_snapshot(GetRigSnapshotRequest {})
        .await?
        .into_inner();

    let Some(snapshot) = response.snapshot else {
        return Ok(None);
    };

    let status = match RigConnectionStatus::try_from(snapshot.status) {
        Ok(RigConnectionStatus::Connected) => RigStatus::Connected,
        Ok(RigConnectionStatus::Error) => RigStatus::Error,
        Ok(RigConnectionStatus::Disabled) => RigStatus::Disabled,
        _ => RigStatus::Disconnected,
    };

    let band = Band::try_from(snapshot.band)
        .ok()
        .and_then(band_to_adif)
        .map(str::to_string);

    let mode = Mode::try_from(snapshot.mode)
        .ok()
        .and_then(mode_to_adif)
        .map(str::to_string);

    let band_rx = Band::try_from(snapshot.band_rx)
        .ok()
        .and_then(band_to_adif)
        .map(str::to_string);

    let frequency_display = if snapshot.frequency_hz > 0 {
        format!("{} MHz", format_frequency_mhz(snapshot.frequency_hz))
    } else {
        String::new()
    };

    Ok(Some(RigInfo {
        frequency_display,
        frequency_hz: snapshot.frequency_hz,
        frequency_rx_hz: snapshot.frequency_rx_hz,
        band,
        band_rx,
        mode,
        submode: snapshot.submode,
        tx_power_watts: snapshot.tx_power_watts,
        status,
        error_message: snapshot.error_message,
    }))
}

/// Update an existing QSO record identified by `local_id` with data from the form.
///
/// `base` is the original `QsoRecord` loaded during editing. When present, form values
/// are overlaid on the clone so that non-form fields (QSL status, metadata, extra ADIF
/// fields, etc.) are preserved.  When `None`, falls back to a default record.
pub(crate) async fn update_qso(
    channel: Channel,
    local_id: &str,
    form: &LogForm,
    lookup: LookupEnrichment,
    base: Option<QsoRecord>,
) -> anyhow::Result<()> {
    let mut client = LogbookServiceClient::new(channel);

    let band: Band = BANDS
        .get(form.band_idx)
        .and_then(|s| band_from_adif(s))
        .unwrap_or(Band::Unspecified);

    let mode_str = MODES.get(form.mode_idx).copied().unwrap_or("SSB");
    let (mode, submode) = resolve_mode(mode_str);

    let utc_timestamp = parse_timestamp(&form.date, &form.time).ok();
    let utc_end_timestamp = if form.time_off.is_empty() {
        None
    } else {
        parse_timestamp(&form.date, &form.time_off).ok()
    };
    let frequency_hz = parse_frequency_mhz(&form.frequency_mhz);

    let (lookup_grid, lookup_country, lookup_cq_zone, lookup_dxcc) =
        lookup.unwrap_or((None, None, None, None));

    // Start from the original record to preserve non-form fields, then overlay
    // every field that the edit form controls.
    let mut qso = base.unwrap_or_default();
    qso.local_id = local_id.to_string();
    qso.worked_callsign = form.callsign.to_uppercase();
    qso.station_callsign = form.station_callsign.to_uppercase();
    qso.band = i32::from(band);
    qso.mode = i32::from(mode);
    qso.utc_timestamp = utc_timestamp;
    qso.utc_end_timestamp = utc_end_timestamp;
    qso.frequency_hz = frequency_hz;
    qso.frequency_rx_hz = form.rig_frequency_rx_hz;
    qso.band_rx = form
        .rig_band_rx
        .as_deref()
        .and_then(band_from_adif)
        .map(i32::from)
        .unwrap_or_default();
    qso.submode = if form.submode_override.is_empty() {
        submode.map(str::to_string)
    } else {
        Some(form.submode_override.clone())
    };
    qso.rst_sent = parse_rst(&form.rst_sent);
    qso.rst_received = parse_rst(&form.rst_rcvd);
    qso.comment = opt_string(&form.comment);
    qso.notes = opt_string(&form.notes);
    qso.tx_power = opt_string(&form.tx_power);
    qso.qsl_sent_status = i32::from(parse_qsl_status(&form.qsl_sent_status)?);
    qso.qsl_received_status = i32::from(parse_qsl_status(&form.qsl_received_status)?);
    qso.lotw_sent = parse_optional_bool(&form.lotw_sent)?;
    qso.lotw_received = parse_optional_bool(&form.lotw_received)?;
    qso.eqsl_sent = parse_optional_bool(&form.eqsl_sent)?;
    qso.eqsl_received = parse_optional_bool(&form.eqsl_received)?;
    qso.qsl_sent_date = parse_optional_date(&form.qsl_sent_date)?;
    qso.qsl_received_date = parse_optional_date(&form.qsl_received_date)?;
    qso.qrz_logid = opt_string(&form.qrz_log_id);
    qso.qrz_bookid = opt_string(&form.qrz_book_id);
    qso.contest_id = opt_string(&form.contest_id);
    qso.serial_sent = opt_string(&form.serial_sent);
    qso.serial_received = opt_string(&form.serial_rcvd);
    qso.exchange_sent = opt_string(&form.exchange_sent);
    qso.exchange_received = opt_string(&form.exchange_rcvd);
    qso.worked_grid = opt_string(&form.worked_grid).or(lookup_grid);
    qso.worked_country = opt_string(&form.worked_country).or(lookup_country);
    qso.worked_cq_zone = opt_u32(&form.worked_cq_zone).or(lookup_cq_zone);
    qso.worked_dxcc = opt_u32(&form.worked_dxcc).or(lookup_dxcc);
    qso.worked_itu_zone = opt_u32(&form.worked_itu_zone);
    qso.worked_continent = opt_string(&form.worked_continent);
    qso.worked_operator_callsign = opt_string(&form.worked_operator_callsign.to_uppercase());
    qso.worked_operator_name = opt_string(&form.worked_name);
    qso.worked_iota = opt_string(&form.iota);
    qso.worked_arrl_section = opt_string(&form.arrl_section);
    qso.worked_state = opt_string(&form.worked_state);
    qso.worked_county = opt_string(&form.worked_county);
    qso.skcc = opt_string(&form.skcc);
    qso.prop_mode = opt_string(&form.prop_mode);
    qso.sat_name = opt_string(&form.sat_name);
    qso.sat_mode = opt_string(&form.sat_mode);
    qso.station_snapshot = station_snapshot_from_form(form)?;
    qso.extra_fields = parse_extra_fields(&form.extra_fields)?;
    qso.cw_decode_rx_wpm = opt_u32(&form.cw_decode_rx_wpm);
    qso.cw_decode_transcript = opt_string(&form.cw_decode_transcript);

    client
        .update_qso(UpdateQsoRequest {
            qso: Some(qso),
            sync_to_qrz: false,
            sync_to_lotw: false,
        })
        .await?;
    Ok(())
}

/// Delete a QSO by its local ID.
pub(crate) async fn delete_qso(channel: Channel, local_id: &str) -> anyhow::Result<()> {
    let mut client = LogbookServiceClient::new(channel);
    let request = DeleteQsoRequest {
        local_id: local_id.to_string(),
        delete_from_qrz: false,
    };
    client.delete_qso(request).await?;
    Ok(())
}

/// Permanently purge all soft-deleted QSOs and return the number of records removed.
pub(crate) async fn purge_deleted_qsos(channel: Channel) -> anyhow::Result<u32> {
    let mut client = LogbookServiceClient::new(channel);
    let request = PurgeDeletedQsosRequest {
        local_ids: vec![],
        older_than: None,
        include_pending_remote_deletes: false,
        confirm: true,
    };
    let response = client.purge_deleted_qsos(request).await?.into_inner();
    Ok(response.purged_count)
}

/// Convert a frequency in MHz to Hz as a `u64`.
pub(crate) fn mhz_to_hz(mhz: f64) -> u64 {
    let hz = mhz * 1_000_000.0_f64;
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "frequency is always a small positive value well within u64 range"
    )]
    {
        hz.round() as u64
    }
}

pub(crate) fn format_frequency_mhz(hz: u64) -> String {
    let whole = hz / 1_000_000;
    let khz = (hz % 1_000_000) / 1_000;
    let fractional_hz = hz % 1_000;
    format!("{whole}.{khz:03}.{fractional_hz:03}")
}

fn parse_frequency_mhz(value: &str) -> Option<u64> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    if let Some(hz) = parse_radio_style_frequency(trimmed) {
        return Some(hz);
    }

    trimmed.parse::<f64>().ok().map(mhz_to_hz)
}

fn parse_radio_style_frequency(value: &str) -> Option<u64> {
    let mut parts = value.split('.');
    let mhz = parts.next()?.parse::<u64>().ok()?;
    let khz = parts.next()?.parse::<u64>().ok()?;
    let tail = parts.next()?;
    if parts.next().is_some() || khz > 999 || tail.is_empty() || tail.len() > 3 {
        return None;
    }

    let mut fractional_hz = tail.parse::<u64>().ok()?;
    if fractional_hz > 999 {
        return None;
    }

    if tail.len() <= 2 {
        fractional_hz *= 10;
    }

    mhz.checked_mul(1_000_000)?
        .checked_add(khz.checked_mul(1_000)?)?
        .checked_add(fractional_hz)
}

/// Return `Some(s.to_string())` if non-empty, `None` otherwise.
fn opt_string(s: &str) -> Option<String> {
    if s.is_empty() {
        None
    } else {
        Some(s.to_string())
    }
}

fn opt_u32(s: &str) -> Option<u32> {
    s.trim().parse().ok()
}

fn opt_f64(s: &str) -> anyhow::Result<Option<f64>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    trimmed
        .parse()
        .map(Some)
        .with_context(|| format!("invalid number: {trimmed}"))
}

fn parse_qsl_status(s: &str) -> anyhow::Result<QslStatus> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(QslStatus::Unspecified);
    }
    let status = qsl_status_from_adif(trimmed);
    if matches!(status, QslStatus::Unspecified) {
        bail!("invalid QSL status: {trimmed}; use N, Y, R, Q, I, or blank");
    }
    Ok(status)
}

fn parse_optional_bool(s: &str) -> anyhow::Result<Option<bool>> {
    match s.trim().to_ascii_uppercase().as_str() {
        "" => Ok(None),
        "Y" | "YES" | "TRUE" | "1" => Ok(Some(true)),
        "N" | "NO" | "FALSE" | "0" => Ok(Some(false)),
        other => bail!("invalid boolean value: {other}; use Y, N, or blank"),
    }
}

pub(crate) fn format_optional_bool(value: Option<bool>) -> String {
    match value {
        Some(true) => "Y".to_string(),
        Some(false) => "N".to_string(),
        None => String::new(),
    }
}

fn parse_optional_date(s: &str) -> anyhow::Result<Option<prost_types::Timestamp>> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    let date = utc::parse_date(trimmed).context("invalid QSL date")?;
    Ok(Some(utc::to_timestamp(date.midnight().assume_utc())))
}

pub(crate) fn format_qsl_status(status: i32) -> String {
    QslStatus::try_from(status)
        .ok()
        .and_then(qsl_status_to_adif)
        .unwrap_or_default()
        .to_string()
}

pub(crate) fn format_optional_date(ts: Option<&prost_types::Timestamp>) -> String {
    ts.and_then(utc::from_timestamp)
        .map(utc::format_date)
        .unwrap_or_default()
}

pub(crate) fn format_optional_timestamp(ts: Option<&prost_types::Timestamp>) -> String {
    ts.and_then(utc::from_timestamp)
        .map(|dt| format!("{}Z", utc::format_datetime(dt)))
        .unwrap_or_default()
}

pub(crate) fn format_extra_fields(extra_fields: &HashMap<String, String>) -> String {
    let mut entries = extra_fields.iter().collect::<Vec<_>>();
    entries.sort_by(|(left, _), (right, _)| left.cmp(right));
    entries
        .into_iter()
        .map(|(key, value)| format!("{key}={value}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn parse_extra_fields(text: &str) -> anyhow::Result<HashMap<String, String>> {
    let mut fields = HashMap::new();
    for (idx, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((key, value)) = trimmed.split_once('=') else {
            bail!(
                "invalid extra ADIF field on line {}: use KEY=value",
                idx + 1
            );
        };
        let key = key.trim().to_ascii_uppercase();
        if key.is_empty() {
            bail!("invalid extra ADIF field on line {}: key is empty", idx + 1);
        }
        fields.insert(key, value.trim().to_string());
    }
    Ok(fields)
}

fn station_snapshot_from_form(form: &LogForm) -> anyhow::Result<Option<StationSnapshot>> {
    let has_snapshot = [
        form.snapshot_profile_name.as_str(),
        form.snapshot_station_callsign.as_str(),
        form.snapshot_operator_callsign.as_str(),
        form.snapshot_operator_name.as_str(),
        form.snapshot_grid.as_str(),
        form.snapshot_country.as_str(),
        form.snapshot_state.as_str(),
        form.snapshot_county.as_str(),
        form.snapshot_arrl_section.as_str(),
        form.snapshot_dxcc.as_str(),
        form.snapshot_cq_zone.as_str(),
        form.snapshot_itu_zone.as_str(),
        form.snapshot_latitude.as_str(),
        form.snapshot_longitude.as_str(),
    ]
    .iter()
    .any(|value| !value.trim().is_empty());

    if !has_snapshot {
        return Ok(None);
    }

    Ok(Some(StationSnapshot {
        profile_name: opt_string(&form.snapshot_profile_name),
        station_callsign: form.snapshot_station_callsign.to_uppercase(),
        operator_callsign: opt_string(&form.snapshot_operator_callsign.to_uppercase()),
        operator_name: opt_string(&form.snapshot_operator_name),
        grid: opt_string(&form.snapshot_grid),
        county: opt_string(&form.snapshot_county),
        state: opt_string(&form.snapshot_state),
        country: opt_string(&form.snapshot_country),
        dxcc: opt_u32(&form.snapshot_dxcc),
        cq_zone: opt_u32(&form.snapshot_cq_zone),
        itu_zone: opt_u32(&form.snapshot_itu_zone),
        latitude: opt_f64(&form.snapshot_latitude)?,
        longitude: opt_f64(&form.snapshot_longitude)?,
        arrl_section: opt_string(&form.snapshot_arrl_section),
        altitude_meters: None,
        gridsquare_ext: None,
    }))
}

/// Parse a date string (`YYYY-MM-DD`) and time string (`HH:MM`) into a protobuf timestamp.
fn parse_timestamp(date: &str, time: &str) -> anyhow::Result<prost_types::Timestamp> {
    let date = utc::parse_date(date).context("invalid date")?;
    let time = utc::parse_minutes(time).context("invalid time")?;
    Ok(utc::to_timestamp(date.with_time(time).assume_utc()))
}

/// Parse an RST string (e.g., `"59"` or `"599"`) into an [`RstReport`].
fn parse_rst(s: &str) -> Option<RstReport> {
    if s.is_empty() {
        return None;
    }
    let digits: Vec<u32> = s.chars().filter_map(|c| c.to_digit(10)).collect();
    let raw = s.to_string();
    let report = match digits.as_slice() {
        [r, st] => RstReport {
            readability: Some(*r),
            strength: Some(*st),
            tone: None,
            raw,
        },
        [r, st, t] => RstReport {
            readability: Some(*r),
            strength: Some(*st),
            tone: Some(*t),
            raw,
        },
        _ => RstReport {
            readability: None,
            strength: None,
            tone: None,
            raw,
        },
    };
    Some(report)
}

/// Map a MODES display string to a proto [`Mode`] enum value plus an optional submode string.
fn resolve_mode(mode_str: &str) -> (Mode, Option<&'static str>) {
    match mode_str {
        "FT4" => (Mode::Mfsk, Some("FT4")),
        "PSK31" => (Mode::Psk, Some("PSK31")),
        s => (mode_from_adif(s).unwrap_or(Mode::Unspecified), None),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn create_channel_does_not_require_server_to_be_online() {
        let channel_result = create_channel("http://127.0.0.1:9");
        assert!(channel_result.is_ok());
    }

    #[test]
    fn qsl_status_parser_accepts_adif_codes() {
        assert_eq!(parse_qsl_status("").unwrap(), QslStatus::Unspecified);
        assert_eq!(parse_qsl_status("n").unwrap(), QslStatus::No);
        assert_eq!(parse_qsl_status("Y").unwrap(), QslStatus::Yes);
        assert_eq!(parse_qsl_status("R").unwrap(), QslStatus::Requested);
        assert_eq!(parse_qsl_status("Q").unwrap(), QslStatus::Queued);
        assert_eq!(parse_qsl_status("I").unwrap(), QslStatus::Ignore);
        assert!(parse_qsl_status("maybe").is_err());
    }

    #[test]
    fn optional_bool_parser_accepts_common_values() {
        assert_eq!(parse_optional_bool("").unwrap(), None);
        assert_eq!(parse_optional_bool("Y").unwrap(), Some(true));
        assert_eq!(parse_optional_bool("no").unwrap(), Some(false));
        assert!(parse_optional_bool("sometimes").is_err());
    }

    #[test]
    fn frequency_formatter_preserves_hz_fractional_digits() {
        assert_eq!(format_frequency_mhz(14_000_000), "14.000.000");
        assert_eq!(format_frequency_mhz(14_074_000), "14.074.000");
        assert_eq!(format_frequency_mhz(14_074_123), "14.074.123");
        assert_eq!(format_frequency_mhz(14_074_120), "14.074.120");
    }

    #[test]
    fn frequency_parser_accepts_decimal_and_radio_style_mhz() {
        assert_eq!(parse_frequency_mhz("14.074123"), Some(14_074_123));
        assert_eq!(parse_frequency_mhz("14.074.123"), Some(14_074_123));
        assert_eq!(parse_frequency_mhz("14.074.12"), Some(14_074_120));
        assert_eq!(parse_frequency_mhz("nope"), None);
    }

    #[test]
    fn extra_fields_are_sorted_and_parsed_as_uppercase_keys() {
        let parsed = parse_extra_fields("app_test=value\ncall=K7ABC").unwrap();
        assert_eq!(parsed.get("APP_TEST"), Some(&"value".to_string()));
        assert_eq!(parsed.get("CALL"), Some(&"K7ABC".to_string()));
        assert_eq!(format_extra_fields(&parsed), "APP_TEST=value\nCALL=K7ABC");
        assert!(parse_extra_fields("BROKEN").is_err());
    }

    #[test]
    fn station_snapshot_from_form_uses_snapshot_fields() {
        let mut form = LogForm::new();
        form.snapshot_station_callsign = "k7abc".to_string();
        form.snapshot_operator_callsign = "n0op".to_string();
        form.snapshot_latitude = "47.6".to_string();
        form.snapshot_longitude = "-122.3".to_string();

        let snapshot = station_snapshot_from_form(&form).unwrap().unwrap();
        assert_eq!(snapshot.station_callsign, "K7ABC");
        assert_eq!(snapshot.operator_callsign, Some("N0OP".to_string()));
        assert_eq!(snapshot.latitude, Some(47.6));
        assert_eq!(snapshot.longitude, Some(-122.3));
    }
}
