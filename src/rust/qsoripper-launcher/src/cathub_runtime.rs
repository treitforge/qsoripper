//! Read the endpoint manifest from one launcher-managed `CatHub` process.

use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use serde::Deserialize;
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};

const SUPPORTED_SCHEMA_VERSION: u32 = 1;

/// Effective endpoints published by a ready `CatHub` process.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(crate) struct CatHubRuntimeInfo {
    pub(crate) schema_version: u32,
    pub(crate) pid: u32,
    pub(crate) winkeyer_endpoint: Option<String>,
}

/// Keep runtime state beside the selected configuration file.
pub(crate) fn runtime_info_path(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("cathub-runtime.json")
}

/// Remove a stale manifest before a new managed process starts.
pub(crate) fn clear_runtime_info(path: &Path) -> Result<()> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error).with_context(|| format!("removing {}", path.display())),
    }
}

/// Wait for the manifest from the exact process that the launcher started.
pub(crate) fn wait_for_runtime_info(
    path: &Path,
    expected_pid: u32,
    timeout: Duration,
) -> Result<CatHubRuntimeInfo> {
    let started = Instant::now();
    let mut last_error = None;
    while started.elapsed() < timeout {
        match read_runtime_info(path, Some(expected_pid)) {
            Ok(info) => return Ok(info),
            Err(error) => last_error = Some(error),
        }
        thread::sleep(Duration::from_millis(100));
    }
    let detail = last_error.map_or_else(
        || "runtime manifest was not created".to_string(),
        |error| error.to_string(),
    );
    bail!("CatHub did not publish ready endpoints: {detail}")
}

/// Read a manifest only when its publishing `CatHub` process still exists.
pub(crate) fn read_live_runtime_info(path: &Path) -> Result<CatHubRuntimeInfo> {
    let info = read_runtime_info(path, None)?;
    let pid = Pid::from_u32(info.pid);
    let mut system = System::new();
    system.refresh_processes_specifics(
        ProcessesToUpdate::Some(&[pid]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let Some(process) = system.process(pid) else {
        bail!("runtime manifest publisher PID {} is not running", info.pid);
    };
    if !is_cathub_process_name(process.name()) {
        bail!("runtime manifest publisher PID {} is not CatHub", info.pid);
    }
    Ok(info)
}

fn is_cathub_process_name(name: &std::ffi::OsStr) -> bool {
    name.to_string_lossy().eq_ignore_ascii_case("cathub")
        || name.to_string_lossy().eq_ignore_ascii_case("cathub.exe")
}

fn read_runtime_info(path: &Path, expected_pid: Option<u32>) -> Result<CatHubRuntimeInfo> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let info: CatHubRuntimeInfo =
        serde_json::from_slice(&bytes).with_context(|| format!("parsing {}", path.display()))?;
    if info.schema_version != SUPPORTED_SCHEMA_VERSION {
        bail!(
            "unsupported runtime schema {}; expected {SUPPORTED_SCHEMA_VERSION}",
            info.schema_version
        );
    }
    if let Some(expected_pid) = expected_pid {
        if info.pid != expected_pid {
            bail!(
                "runtime manifest belongs to PID {}, expected {expected_pid}",
                info.pid
            );
        }
    }
    if let Some(endpoint) = info.winkeyer_endpoint.as_deref() {
        validate_loopback_http_endpoint(endpoint)?;
    }
    Ok(info)
}

fn validate_loopback_http_endpoint(endpoint: &str) -> Result<()> {
    let address = endpoint
        .strip_prefix("http://")
        .context("WinKeyer endpoint must use http://")?
        .parse::<SocketAddr>()
        .context("WinKeyer endpoint must contain a socket address")?;
    if !matches!(address.ip(), IpAddr::V4(ip) if ip.is_loopback())
        && !matches!(address.ip(), IpAddr::V6(ip) if ip.is_loopback())
    {
        bail!("WinKeyer endpoint must use a loopback address");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn reads_manifest_for_expected_process() {
        let directory = tempfile::tempdir().expect("temp directory");
        let path = directory.path().join("runtime.json");
        fs::write(
            &path,
            r#"{"schema_version":1,"pid":1234,"winkeyer_endpoint":"http://127.0.0.1:54321"}"#,
        )
        .expect("write runtime info");

        let info = read_runtime_info(&path, Some(1234)).expect("read runtime info");

        assert_eq!(info.pid, 1234);
        assert_eq!(
            info.winkeyer_endpoint.as_deref(),
            Some("http://127.0.0.1:54321")
        );
    }

    #[test]
    fn rejects_stale_or_non_loopback_manifest() {
        let directory = tempfile::tempdir().expect("temp directory");
        let path = directory.path().join("runtime.json");
        fs::write(
            &path,
            r#"{"schema_version":1,"pid":99,"winkeyer_endpoint":"http://192.0.2.1:54321"}"#,
        )
        .expect("write runtime info");

        assert!(read_runtime_info(&path, Some(1234)).is_err());
        assert!(read_runtime_info(&path, Some(99)).is_err());
    }

    #[test]
    fn rejects_manifest_from_non_cathub_process() {
        let directory = tempfile::tempdir().expect("temp directory");
        let path = directory.path().join("runtime.json");
        fs::write(
            &path,
            format!(
                r#"{{"schema_version":1,"pid":{},"winkeyer_endpoint":null}}"#,
                std::process::id()
            ),
        )
        .expect("write runtime info");

        assert!(read_live_runtime_info(&path).is_err());
    }

    #[test]
    fn recognizes_cathub_process_names() {
        assert!(is_cathub_process_name(std::ffi::OsStr::new("cathub")));
        assert!(is_cathub_process_name(std::ffi::OsStr::new("CatHub.exe")));
        assert!(!is_cathub_process_name(std::ffi::OsStr::new(
            "IntelSoftwareAssetManagerService"
        )));
    }

    #[test]
    fn derives_runtime_path_from_selected_config() {
        assert_eq!(
            runtime_info_path(Path::new("/station/config.toml")),
            PathBuf::from("/station/cathub-runtime.json")
        );
    }
}
