//! Dry-run-first maintenance tool for soft-deleting duplicate QSO imports.

use std::collections::HashMap;
use std::env;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use prost_types::Timestamp;
use qsoripper_core::proto::qsoripper::domain::{QsoRecord, SyncStatus};
use qsoripper_core::storage::{LogbookStore, QsoListQuery};
use qsoripper_storage_sqlite::SqliteStorageBuilder;
use time::OffsetDateTime;

#[derive(Debug)]
struct Options {
    db_path: PathBuf,
    apply: bool,
    time_window_ms: i64,
    created_after_ms: Option<i64>,
    created_before_ms: Option<i64>,
    backup_path: Option<PathBuf>,
}

#[derive(Debug, Clone)]
struct CandidateGroup {
    keeper: QsoRecord,
    losers: Vec<QsoRecord>,
    ambiguous: Vec<QsoRecord>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct RowScore {
    has_logid: bool,
    is_synced: bool,
    enrichment_count: u16,
    record_quality: u16,
    older_created_rank: i64,
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("error: {err}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let options = parse_args(env::args().skip(1))?;
    let storage = SqliteStorageBuilder::new()
        .path(&options.db_path)
        .build()
        .map_err(|err| {
            format!(
                "failed to open database {}: {err}",
                options.db_path.display()
            )
        })?;

    let qsos = storage
        .list_qsos(&QsoListQuery::default())
        .await
        .map_err(|err| format!("failed to list QSOs: {err}"))?;
    let groups = find_duplicate_groups(&qsos, &options);
    let loser_count: usize = groups.iter().map(|group| group.losers.len()).sum();
    let ambiguous_count: usize = groups.iter().map(|group| group.ambiguous.len()).sum();

    println!("Database: {}", options.db_path.display());
    println!("Active QSOs scanned: {}", qsos.len());
    println!("Duplicate groups with safe losers: {}", groups.len());
    println!("Rows that would be soft-deleted: {loser_count}");
    println!("Ambiguous duplicate candidates skipped: {ambiguous_count}");

    for (index, group) in groups.iter().enumerate() {
        print_group(index + 1, group);
    }

    if !options.apply {
        println!("Dry run only. Re-run with --apply to soft-delete the listed loser rows.");
        return Ok(());
    }

    if loser_count == 0 {
        println!("No safe duplicate rows to soft-delete.");
        return Ok(());
    }

    let backup_path = create_backup(&options)?;
    println!("Backup written: {}", backup_path.display());

    let deleted_at = now_timestamp();
    for group in groups {
        for mut loser in group.losers {
            loser.deleted_at = Some(deleted_at);
            loser.pending_remote_delete = false;
            storage
                .update_qso(&loser)
                .await
                .map_err(|err| format!("failed to soft-delete {}: {err}", loser.local_id))?;
        }
    }

    println!("Soft-deleted {loser_count} duplicate rows.");
    Ok(())
}

fn parse_args(mut args: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut db_path = None;
    let mut apply = false;
    let mut time_window_ms = 0_i64;
    let mut created_after_ms = None;
    let mut created_before_ms = None;
    let mut backup_path = None;

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--db" => db_path = Some(PathBuf::from(required_value(&arg, &mut args)?)),
            "--apply" => apply = true,
            "--time-window-seconds" => {
                let seconds = parse_i64(&arg, &required_value(&arg, &mut args)?)?;
                time_window_ms = seconds
                    .checked_mul(1_000)
                    .ok_or_else(|| "--time-window-seconds is too large".to_string())?;
                if time_window_ms < 0 {
                    return Err("--time-window-seconds must be non-negative".to_string());
                }
            }
            "--created-after" => {
                created_after_ms = Some(parse_datetime_ms(&required_value(&arg, &mut args)?)?);
            }
            "--created-before" => {
                created_before_ms = Some(parse_datetime_ms(&required_value(&arg, &mut args)?)?);
            }
            "--backup" => backup_path = Some(PathBuf::from(required_value(&arg, &mut args)?)),
            "--help" | "-h" => return Err(help_text()),
            other => return Err(format!("unknown argument '{other}'\n\n{}", help_text())),
        }
    }

    Ok(Options {
        db_path: db_path.unwrap_or_else(|| PathBuf::from("data/qsoripper.db")),
        apply,
        time_window_ms,
        created_after_ms,
        created_before_ms,
        backup_path,
    })
}

fn required_value(flag: &str, args: &mut impl Iterator<Item = String>) -> Result<String, String> {
    args.next()
        .ok_or_else(|| format!("{flag} requires a value"))
}

fn parse_i64(flag: &str, value: &str) -> Result<i64, String> {
    value
        .parse::<i64>()
        .map_err(|err| format!("{flag} value '{value}' is not an integer: {err}"))
}

fn parse_datetime_ms(value: &str) -> Result<i64, String> {
    OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339)
        .map_err(|err| format!("'{value}' is not an RFC3339 timestamp: {err}"))
        .map(|dt| (dt.unix_timestamp() * 1_000) + i64::from(dt.millisecond()))
}

fn help_text() -> String {
    "usage: purge_duplicate_qsos --db <path> [--time-window-seconds <n>] [--created-after <rfc3339>] [--created-before <rfc3339>] [--backup <path>] [--apply]\n\
     \n\
     Dry-run by default. The tool soft-deletes only active local-only rows that have a clearly better duplicate keeper with the same station, worked callsign, band, mode, compatible submode, compatible frequency, and timestamp within the configured window."
        .to_string()
}

fn find_duplicate_groups(qsos: &[QsoRecord], options: &Options) -> Vec<CandidateGroup> {
    let mut keyed: HashMap<(String, String, i32, i32), Vec<usize>> = HashMap::new();
    for (index, qso) in qsos.iter().enumerate() {
        if qso.deleted_at.is_some() {
            continue;
        }
        keyed
            .entry((
                normalize(&qso.station_callsign),
                normalize(&qso.worked_callsign),
                qso.band,
                qso.mode,
            ))
            .or_default()
            .push(index);
    }

    let mut groups = Vec::new();
    for indexes in keyed.values() {
        if indexes.len() < 2 {
            continue;
        }
        for cluster in clusters_for_key(qsos, indexes, options.time_window_ms) {
            if cluster.len() < 2 {
                continue;
            }
            if let Some(group) = classify_cluster(qsos, &cluster, options) {
                groups.push(group);
            }
        }
    }

    groups
}

fn clusters_for_key(qsos: &[QsoRecord], indexes: &[usize], time_window_ms: i64) -> Vec<Vec<usize>> {
    let mut parent: Vec<usize> = (0..indexes.len()).collect();
    for left in 0..indexes.len() {
        for right in (left + 1)..indexes.len() {
            let Some(left_qso) = indexes.get(left).and_then(|index| qsos.get(*index)) else {
                continue;
            };
            let Some(right_qso) = indexes.get(right).and_then(|index| qsos.get(*index)) else {
                continue;
            };
            if qsos_match(left_qso, right_qso, time_window_ms) {
                union(&mut parent, left, right);
            }
        }
    }

    let mut by_root: HashMap<usize, Vec<usize>> = HashMap::new();
    for local_index in 0..indexes.len() {
        let root = find(&mut parent, local_index);
        if let Some(qso_index) = indexes.get(local_index) {
            by_root.entry(root).or_default().push(*qso_index);
        }
    }
    by_root.into_values().collect()
}

fn qsos_match(left: &QsoRecord, right: &QsoRecord, time_window_ms: i64) -> bool {
    timestamp_delta_ms(left, right).is_some_and(|delta| delta <= time_window_ms)
        && (same_non_empty_logid(left, right)
            || (optional_strings_compatible(left.submode.as_deref(), right.submode.as_deref())
                && frequencies_compatible(left, right)))
}

fn timestamp_delta_ms(left: &QsoRecord, right: &QsoRecord) -> Option<i64> {
    let left_ms = timestamp_to_ms(left.utc_timestamp.as_ref())?;
    let right_ms = timestamp_to_ms(right.utc_timestamp.as_ref())?;
    Some(left_ms.abs_diff(right_ms).try_into().unwrap_or(i64::MAX))
}

fn classify_cluster(
    qsos: &[QsoRecord],
    cluster: &[usize],
    options: &Options,
) -> Option<CandidateGroup> {
    let mut rows: Vec<QsoRecord> = cluster
        .iter()
        .filter_map(|index| qsos.get(*index).cloned())
        .collect();
    rows.sort_by(|left, right| {
        score(right)
            .cmp(&score(left))
            .then_with(|| created_ms(left).cmp(&created_ms(right)))
            .then_with(|| left.local_id.cmp(&right.local_id))
    });
    let keeper = rows.first()?.clone();
    let keeper_score = score(&keeper);
    let mut losers = Vec::new();
    let mut ambiguous = Vec::new();

    for row in rows.into_iter().skip(1) {
        if is_safe_loser(&keeper, keeper_score, &row, options) {
            losers.push(row);
        } else {
            ambiguous.push(row);
        }
    }

    (!losers.is_empty()).then_some(CandidateGroup {
        keeper,
        losers,
        ambiguous,
    })
}

fn is_safe_loser(
    keeper: &QsoRecord,
    keeper_score: RowScore,
    row: &QsoRecord,
    options: &Options,
) -> bool {
    let row_score = score(row);
    if same_non_empty_logid(keeper, row) {
        return keeper_score.record_quality > row_score.record_quality
            && created_in_range(row, options);
    }

    !has_logid(row)
        && row.sync_status == SyncStatus::LocalOnly as i32
        && keeper_score > row_score
        && (has_logid(keeper)
            || keeper.sync_status == SyncStatus::Synced as i32
            || keeper_score.enrichment_count > row_score.enrichment_count)
        && created_in_range(row, options)
}

fn created_in_range(qso: &QsoRecord, options: &Options) -> bool {
    let created = created_ms(qso);
    options
        .created_after_ms
        .is_none_or(|after| created >= Some(after))
        && options
            .created_before_ms
            .is_none_or(|before| created <= Some(before))
}

fn score(qso: &QsoRecord) -> RowScore {
    RowScore {
        has_logid: has_logid(qso),
        is_synced: qso.sync_status == SyncStatus::Synced as i32,
        enrichment_count: enrichment_count(qso),
        record_quality: record_quality(qso),
        older_created_rank: created_ms(qso).map_or(i64::MIN, |ms| -ms),
    }
}

fn has_logid(qso: &QsoRecord) -> bool {
    qso.qrz_logid
        .as_deref()
        .is_some_and(|value| !value.trim().is_empty())
}

fn same_non_empty_logid(left: &QsoRecord, right: &QsoRecord) -> bool {
    match (
        non_empty_trimmed(left.qrz_logid.as_deref()),
        non_empty_trimmed(right.qrz_logid.as_deref()),
    ) {
        (Some(left), Some(right)) => left == right,
        _ => false,
    }
}

fn enrichment_count(qso: &QsoRecord) -> u16 {
    let mut count = 0_u16;
    for value in [
        qso.worked_operator_callsign.as_deref(),
        qso.worked_operator_name.as_deref(),
        qso.worked_grid.as_deref(),
        qso.worked_country.as_deref(),
        qso.worked_state.as_deref(),
        qso.worked_county.as_deref(),
        qso.worked_iota.as_deref(),
        qso.worked_continent.as_deref(),
        qso.worked_arrl_section.as_deref(),
        qso.skcc.as_deref(),
        qso.worked_gridsquare_ext.as_deref(),
    ] {
        if value.is_some_and(|text| !text.trim().is_empty()) {
            count = count.saturating_add(1);
        }
    }
    for present in [
        qso.worked_dxcc.is_some_and(|value| value > 0),
        qso.worked_cq_zone.is_some_and(|value| value > 0),
        qso.worked_itu_zone.is_some_and(|value| value > 0),
        qso.worked_latitude.is_some_and(f64::is_finite),
        qso.worked_longitude.is_some_and(f64::is_finite),
        qso.worked_altitude_meters.is_some_and(f64::is_finite),
    ] {
        if present {
            count = count.saturating_add(1);
        }
    }
    count
}

fn record_quality(qso: &QsoRecord) -> u16 {
    let mut quality = 0_u16;
    if has_logid(qso) {
        quality = quality.saturating_add(100);
    }
    if qso.sync_status == SyncStatus::Synced as i32 {
        quality = quality.saturating_add(50);
    }
    quality.saturating_add(enrichment_count(qso))
}

fn created_ms(qso: &QsoRecord) -> Option<i64> {
    timestamp_to_ms(qso.created_at.as_ref())
}

fn timestamp_to_ms(timestamp: Option<&Timestamp>) -> Option<i64> {
    timestamp.map(|value| {
        value
            .seconds
            .saturating_mul(1_000)
            .saturating_add(i64::from(value.nanos) / 1_000_000)
    })
}

fn optional_strings_compatible(left: Option<&str>, right: Option<&str>) -> bool {
    match (non_empty_trimmed(left), non_empty_trimmed(right)) {
        (Some(left), Some(right)) => left.eq_ignore_ascii_case(right),
        _ => true,
    }
}

fn non_empty_trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

#[allow(deprecated)]
fn frequencies_compatible(left: &QsoRecord, right: &QsoRecord) -> bool {
    match (left.frequency_hz, right.frequency_hz) {
        (Some(left_hz), Some(right_hz)) => left_hz == right_hz,
        _ => optional_u64_compatible(left.frequency_khz, right.frequency_khz),
    }
}

fn optional_u64_compatible(left: Option<u64>, right: Option<u64>) -> bool {
    match (left, right) {
        (Some(left_value), Some(right_value)) => left_value == right_value,
        _ => true,
    }
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_uppercase()
}

fn union(parent: &mut [usize], left: usize, right: usize) {
    let left_root = find(parent, left);
    let right_root = find(parent, right);
    if left_root != right_root {
        if let Some(slot) = parent.get_mut(right_root) {
            *slot = left_root;
        }
    }
}

fn find(parent: &mut [usize], index: usize) -> usize {
    let Some(&next) = parent.get(index) else {
        return index;
    };
    if next == index {
        return index;
    }
    let root = find(parent, next);
    if let Some(slot) = parent.get_mut(index) {
        *slot = root;
    }
    root
}

fn create_backup(options: &Options) -> Result<PathBuf, String> {
    let backup_path = options
        .backup_path
        .clone()
        .unwrap_or_else(|| default_backup_path(&options.db_path));
    let backup_sql = format!("VACUUM INTO '{}'", escape_sql_path(&backup_path));
    let connection = sqlite::Connection::open(&options.db_path)
        .map_err(|err| format!("failed to open database for backup: {err}"))?;
    connection
        .execute(backup_sql)
        .map_err(|err| format!("failed to create backup {}: {err}", backup_path.display()))?;
    Ok(backup_path)
}

fn default_backup_path(db_path: &Path) -> PathBuf {
    let now = OffsetDateTime::now_utc();
    let stamp = format!(
        "{}-{}",
        qsoripper_core::utc::format_adif_date(now),
        qsoripper_core::utc::format_adif_time(now)
    );
    let file_name = db_path
        .file_name()
        .and_then(|name| name.to_str())
        .map_or_else(|| "qsoripper.db".to_string(), ToString::to_string);
    db_path.with_file_name(format!("{file_name}.dedupe-{stamp}.bak"))
}

fn escape_sql_path(path: &Path) -> String {
    path.to_string_lossy().replace('\'', "''")
}

fn now_timestamp() -> Timestamp {
    qsoripper_core::utc::to_timestamp(OffsetDateTime::now_utc())
}

fn print_group(index: usize, group: &CandidateGroup) {
    println!(
        "\nGroup {index}: keep {} ({})",
        group.keeper.local_id,
        describe(&group.keeper)
    );
    for loser in &group.losers {
        println!("  soft-delete {} ({})", loser.local_id, describe(loser));
    }
    for row in &group.ambiguous {
        println!("  skipped ambiguous {} ({})", row.local_id, describe(row));
    }
}

fn describe(qso: &QsoRecord) -> String {
    format!(
        "{} {} band={} mode={} freq_hz={} sync={} logid={} enrich={} created={}",
        qso.station_callsign,
        qso.worked_callsign,
        qso.band,
        qso.mode,
        effective_frequency_hz(qso).map_or_else(|| "-".to_string(), |value| value.to_string()),
        qso.sync_status,
        qso.qrz_logid.as_deref().unwrap_or("-"),
        enrichment_count(qso),
        created_ms(qso).map_or_else(|| "-".to_string(), |value| value.to_string())
    )
}

#[allow(deprecated)]
fn effective_frequency_hz(qso: &QsoRecord) -> Option<u64> {
    qso.frequency_hz
        .or_else(|| qso.frequency_khz.map(|khz| khz.saturating_mul(1_000)))
}

#[cfg(test)]
mod tests {
    use super::{find_duplicate_groups, Options};
    use prost_types::Timestamp;
    use qsoripper_core::proto::qsoripper::domain::{Band, Mode, QsoRecord, SyncStatus};
    use std::path::PathBuf;

    fn options() -> Options {
        Options {
            db_path: PathBuf::from("unused.db"),
            apply: false,
            time_window_ms: 0,
            created_after_ms: None,
            created_before_ms: None,
            backup_path: None,
        }
    }

    fn qso(local_id: &str, sync_status: SyncStatus, created_seconds: i64) -> QsoRecord {
        QsoRecord {
            local_id: local_id.to_string(),
            station_callsign: "W1AW".to_string(),
            worked_callsign: "K7ABC".to_string(),
            utc_timestamp: Some(Timestamp {
                seconds: 1_700_000_000,
                nanos: 0,
            }),
            band: Band::Band20m as i32,
            mode: Mode::Ft8 as i32,
            frequency_hz: Some(14_074_000),
            created_at: Some(Timestamp {
                seconds: created_seconds,
                nanos: 0,
            }),
            sync_status: sync_status as i32,
            ..QsoRecord::default()
        }
    }

    #[test]
    fn finds_unenriched_local_only_loser_when_keeper_is_synced_and_enriched() {
        let mut keeper = qso("keeper", SyncStatus::Synced, 100);
        keeper.qrz_logid = Some("QRZ1".to_string());
        keeper.worked_grid = Some("FN31".to_string());
        keeper.worked_country = Some("United States".to_string());
        let loser = qso("loser", SyncStatus::LocalOnly, 200);

        let groups = find_duplicate_groups(&[keeper, loser], &options());

        assert_eq!(groups.len(), 1);
        let [group] = groups.as_slice() else { return };
        assert_eq!(group.keeper.local_id, "keeper");
        assert_eq!(group.losers.len(), 1);
        let [loser] = group.losers.as_slice() else {
            return;
        };
        assert_eq!(loser.local_id, "loser");
    }

    #[test]
    fn skips_ambiguous_local_only_rows_without_better_keeper() {
        let first = qso("first", SyncStatus::LocalOnly, 100);
        let second = qso("second", SyncStatus::LocalOnly, 200);

        let groups = find_duplicate_groups(&[first, second], &options());

        assert!(groups.is_empty());
    }

    #[test]
    fn finds_less_enriched_synced_loser_when_qrz_logid_matches() {
        let mut keeper = qso("keeper", SyncStatus::Synced, 100);
        keeper.qrz_logid = Some("QRZ1".to_string());
        keeper.worked_grid = Some("FN31".to_string());
        keeper.worked_country = Some("United States".to_string());
        let mut loser = qso("loser", SyncStatus::Synced, 200);
        loser.qrz_logid = Some("QRZ1".to_string());

        let groups = find_duplicate_groups(&[keeper, loser], &options());

        assert_eq!(groups.len(), 1);
        let [group] = groups.as_slice() else { return };
        assert_eq!(group.keeper.local_id, "keeper");
        assert_eq!(group.losers.len(), 1);
        let [loser] = group.losers.as_slice() else {
            return;
        };
        assert_eq!(loser.local_id, "loser");
    }

    #[test]
    fn skips_matching_qrz_logid_rows_when_quality_is_tied() {
        let mut first = qso("first", SyncStatus::Synced, 100);
        first.qrz_logid = Some("QRZ1".to_string());
        let mut second = qso("second", SyncStatus::Synced, 200);
        second.qrz_logid = Some("QRZ1".to_string());

        let groups = find_duplicate_groups(&[first, second], &options());

        assert!(groups.is_empty());
    }

    #[test]
    fn matching_qrz_logid_overrides_frequency_difference() {
        let mut keeper = qso("keeper", SyncStatus::Synced, 100);
        keeper.qrz_logid = Some("QRZ1".to_string());
        keeper.worked_grid = Some("FN31".to_string());
        keeper.worked_country = Some("United States".to_string());
        keeper.frequency_hz = Some(14_043_600);
        let mut loser = qso("loser", SyncStatus::Synced, 120);
        loser.qrz_logid = Some("QRZ1".to_string());
        loser.frequency_hz = Some(14_044_000);

        let groups = find_duplicate_groups(&[keeper, loser], &options());

        assert_eq!(groups.len(), 1);
        let [group] = groups.as_slice() else { return };
        let [loser] = group.losers.as_slice() else {
            return;
        };
        assert_eq!(loser.local_id, "loser");
    }
}
