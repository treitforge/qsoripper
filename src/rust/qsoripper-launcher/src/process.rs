//! Spawn detached child processes and remember their PIDs so we can stop them.

use std::collections::BTreeMap;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use std::time::UNIX_EPOCH;

use anyhow::{bail, Context, Result};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, Signal, System, UpdateKind};

use crate::catalog::{ComponentId, ComponentSpec};

const MINIMUM_CATHUB_VERSION: (u32, u32, u32) = (0, 2, 0);
const MAXIMUM_CATHUB_VERSION: (u32, u32, u32) = (0, 3, 0);
const SUPPORTED_CATHUB_RANGE: &str = ">=0.2.0 <0.3.0";

/// A child the launcher started, tracked by OS PID so it survives the launcher
/// exiting and so a later "stop" can find it again.
#[derive(Debug, Clone)]
pub(crate) struct LaunchedProcess {
    pub(crate) component: ComponentId,
    pub(crate) pid: u32,
    #[allow(dead_code)]
    pub(crate) executable: PathBuf,
}

/// Records launched-process PIDs by component id.
#[derive(Debug, Default)]
pub(crate) struct ProcessRegistry {
    inner: BTreeMap<ComponentId, LaunchedProcess>,
}

impl ProcessRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(&mut self, p: LaunchedProcess) {
        self.inner.insert(p.component, p);
    }

    #[allow(dead_code)]
    pub(crate) fn get(&self, id: ComponentId) -> Option<&LaunchedProcess> {
        self.inner.get(&id)
    }

    pub(crate) fn remove(&mut self, id: ComponentId) -> Option<LaunchedProcess> {
        self.inner.remove(&id)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &LaunchedProcess> {
        self.inner.values()
    }
}

/// Build the [`Command`] used to launch `spec` from its published artifact.
///
/// `env` entries are applied after the parent environment so callers can
/// inject `QSORIPPER_ENGINE` and `QSORIPPER_ENDPOINT` per-UI.
pub(crate) fn build_command<I, K, V>(
    spec: &ComponentSpec,
    exe: &Path,
    args: &[&OsStr],
    env: I,
) -> Command
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    if spec.wants_console {
        return build_console_command(spec, exe, args, env);
    }

    let mut cmd = Command::new(exe);
    cmd.args(args);
    if let Some(dir) = exe.parent() {
        cmd.current_dir(dir);
    }
    for (k, v) in env {
        cmd.env(k, v);
    }
    // Don't let child stdio inherit the launcher's raw-mode terminal.
    cmd.stdin(Stdio::null());
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::null());

    apply_detach_flags(&mut cmd);

    // Touch `spec` so unused-warning lints don't fire on platforms where we
    // don't otherwise read from it; in practice this also keeps the signature
    // open for future per-component spawn tweaks.
    let _ = spec.id;
    cmd
}

/// Spawn a terminal app into its own visible terminal window so its TUI has
/// somewhere to render. On Windows that means `CREATE_NEW_CONSOLE` and
/// inherited stdio; on Unix we wrap with the user's terminal emulator.
fn build_console_command<I, K, V>(
    spec: &ComponentSpec,
    exe: &Path,
    args: &[&OsStr],
    env: I,
) -> Command
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<OsStr>,
    V: AsRef<OsStr>,
{
    let _ = spec.id;
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        // CREATE_NEW_CONSOLE | CREATE_NEW_PROCESS_GROUP
        const CREATE_NEW_CONSOLE: u32 = 0x0000_0010;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

        let mut cmd = if let Some(wt) = find_on_path("wt.exe") {
            // Open in a new Windows Terminal window so the TUI gets a real,
            // visible terminal with proper input handling. `-w new` forces a
            // fresh window; `-d` sets the new tab's working directory; the
            // double dash separates wt's args from the child invocation.
            let mut c = Command::new(wt);
            c.arg("-w").arg("new");
            if let Some(dir) = exe.parent() {
                c.arg("-d").arg(dir);
            }
            let title = format!("QsoRipper - {}", spec.display_name);
            c.arg("--title").arg(title);
            c.arg("--").arg(exe);
            c.args(args);
            c
        } else {
            // Fallback for systems without Windows Terminal: classic conhost
            // window via CREATE_NEW_CONSOLE.
            let mut c = Command::new(exe);
            c.args(args);
            c.creation_flags(CREATE_NEW_CONSOLE | CREATE_NEW_PROCESS_GROUP);
            c
        };
        if let Some(dir) = exe.parent() {
            cmd.current_dir(dir);
        }
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd
    }

    #[cfg(not(windows))]
    {
        use std::os::unix::process::CommandExt;
        let path = std::env::var_os("PATH").unwrap_or_default();
        let (terminal, terminal_args) = pick_terminal(&path);

        let mut cmd = Command::new(terminal);
        cmd.args(terminal_args);
        cmd.arg(exe);
        cmd.args(args);
        if let Some(dir) = exe.parent() {
            cmd.current_dir(dir);
        }
        for (k, v) in env {
            cmd.env(k, v);
        }
        cmd.process_group(0);
        cmd
    }
}

#[cfg(windows)]
fn find_on_path(name: &str) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&path) {
        let candidate = dir.join(name);
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    None
}

#[cfg(not(windows))]
fn pick_terminal(path: &OsStr) -> (&'static str, &'static [&'static str]) {
    // Probe a short list of well-known terminal emulators. We pick the first
    // one on PATH; user can override by symlinking `x-terminal-emulator`.
    const PTYXIS_ARGS: &[&str] = &["--standalone", "--new-window", "--"];
    const CANDIDATES: &[(&str, &[&str])] = &[
        ("x-terminal-emulator", &["-e"]),
        ("ptyxis", PTYXIS_ARGS),
        ("gnome-terminal", &["--"]),
        ("konsole", &["-e"]),
        ("xfce4-terminal", &["-e"]),
        ("alacritty", &["-e"]),
        ("kitty", &["--"]),
        ("xterm", &["-e"]),
    ];
    for (cmd, args) in CANDIDATES {
        for dir in std::env::split_paths(path) {
            let candidate = dir.join(cmd);
            if candidate.is_file() {
                // The system alternative can point to Ptyxis, which uses -- instead of -e.
                if *cmd == "x-terminal-emulator"
                    && candidate
                        .canonicalize()
                        .is_ok_and(|resolved| resolved.file_name() == Some(OsStr::new("ptyxis")))
                {
                    return (cmd, PTYXIS_ARGS);
                }
                return (cmd, args);
            }
        }
    }
    // Last resort: `xterm -e <cmd>` will surface a clear "not found" error if
    // none of the candidates are installed, instead of silently launching a
    // headless TUI.
    ("xterm", &["-e"])
}

#[cfg(windows)]
fn apply_detach_flags(cmd: &mut Command) {
    use std::os::windows::process::CommandExt;
    // CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS
    const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
    const DETACHED_PROCESS: u32 = 0x0000_0008;
    cmd.creation_flags(CREATE_NEW_PROCESS_GROUP | DETACHED_PROCESS);
}

#[cfg(not(windows))]
fn apply_detach_flags(cmd: &mut Command) {
    use std::os::unix::process::CommandExt;
    // Put the child in its own process group so a SIGINT delivered to the
    // launcher's foreground group does not propagate to UIs we launched.
    // `process_group(0)` is enough for cleanly-exiting launches; we do not
    // need a full new session for the TUI launcher use case.
    cmd.process_group(0);
}

/// Open `url` in the user's default browser. Returns an error if the OS
/// reports the open command failed to start; success of the launch is
/// best-effort beyond that.
pub(crate) fn open_url(url: &str) -> Result<()> {
    #[cfg(windows)]
    {
        // `cmd /C start "" "<url>"` honors the user's default browser and
        // does not require shell escaping the URL beyond the surrounding
        // quotes. The empty "" is start's window title argument.
        Command::new("cmd")
            .args(["/C", "start", "", url])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to launch browser for {url}"))?;
        Ok(())
    }

    #[cfg(target_os = "macos")]
    {
        Command::new("open")
            .arg(url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to launch browser for {url}"))?;
        Ok(())
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Command::new("xdg-open")
            .arg(url)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .with_context(|| format!("failed to launch browser for {url}"))?;
        Ok(())
    }
}

/// Spawn the executable behind `spec` and record its PID in `registry`.
pub(crate) fn spawn(
    spec: &ComponentSpec,
    exe: &Path,
    args: &[&OsStr],
    env: &[(String, String)],
    registry: &mut ProcessRegistry,
) -> Result<LaunchedProcess> {
    if !exe.exists() {
        bail!(
            "missing artifact for {}: {} (was the build run?)",
            spec.display_name,
            exe.display()
        );
    }
    let mut cmd = build_command(
        spec,
        exe,
        args,
        env.iter().map(|(k, v)| (k.as_str(), v.as_str())),
    );
    let child = cmd
        .spawn()
        .with_context(|| format!("spawning {}", exe.display()))?;
    let entry = LaunchedProcess {
        component: spec.id,
        pid: child.id(),
        executable: exe.to_path_buf(),
    };
    // Drop the Child handle so the launcher does not block on waitpid; the
    // OS reaps via the new session/process group. Tracking is by PID from here.
    drop(child);
    registry.insert(entry.clone());
    Ok(entry)
}

/// Read and validate the standalone `CatHub` executable version before launch.
pub(crate) fn verify_cathub_version(exe: &Path) -> Result<String> {
    if !exe.exists() {
        bail!("CatHub executable is unavailable: {}", exe.display());
    }
    let output = Command::new(exe)
        .arg("--version")
        .output()
        .with_context(|| format!("reading CatHub version from {}", exe.display()))?;
    if !output.status.success() {
        bail!("CatHub executable did not report a version");
    }
    let reported = String::from_utf8_lossy(&output.stdout);
    let version = parse_cathub_version(&reported)
        .context("CatHub returned an unrecognized version response")?;
    if !is_supported_cathub_version(version) {
        bail!("incompatible CatHub version {version}; QsoRipper supports {SUPPORTED_CATHUB_RANGE}");
    }
    Ok(version.to_owned())
}

fn is_supported_cathub_version(version: &str) -> bool {
    parse_version_triplet(version)
        .is_some_and(|parsed| parsed >= MINIMUM_CATHUB_VERSION && parsed < MAXIMUM_CATHUB_VERSION)
}

fn parse_version_triplet(version: &str) -> Option<(u32, u32, u32)> {
    let version = version.split_once('-').map_or(version, |(core, _)| core);
    let mut fields = version.split('.');
    let major = fields.next()?.parse().ok()?;
    let minor = fields.next()?.parse().ok()?;
    let patch = fields.next()?.parse().ok()?;
    fields.next().is_none().then_some((major, minor, patch))
}

fn parse_cathub_version(reported: &str) -> Option<&str> {
    let mut fields = reported.split_whitespace();
    fields
        .next()
        .is_some_and(|name| name.eq_ignore_ascii_case("cathub"))
        .then(|| fields.next())
        .flatten()
}

/// Ask the OS to terminate the given PID. Uses SIGTERM-equivalent semantics.
pub(crate) fn stop_pid(pid: u32) -> bool {
    let mut sys = System::new();
    sys.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[Pid::from_u32(pid)]),
        true,
        ProcessRefreshKind::nothing(),
    );
    let Some(proc_handle) = sys.process(Pid::from_u32(pid)) else {
        return false;
    };
    proc_handle
        .kill_with(Signal::Term)
        .unwrap_or_else(|| proc_handle.kill())
}

/// Decide whether a running process is a stale copy of the published binary at
/// `target_exe`. "Stale" means: it is the same executable we manage (same
/// directory and either the same file name or its side-lined
/// `<name>.locked-*.old` variant left behind by a rebuild) AND it was started
/// before the binary on disk was last built (`built_secs`, seconds since the
/// Unix epoch). A process running a different executable, or one started after
/// the current build, is never considered stale.
fn is_stale_published_copy(
    proc_exe: &Path,
    proc_start_secs: u64,
    target_exe: &Path,
    built_secs: u64,
) -> bool {
    if proc_start_secs >= built_secs {
        return false;
    }
    is_managed_published_copy(proc_exe, target_exe)
}

/// Decide whether a running process uses the published executable managed by
/// this launcher. A side-lined locked copy also belongs to the launcher.
#[expect(
    clippy::case_sensitive_file_extension_comparisons,
    reason = "comparing a known side-lined suffix, not a real file extension; casing is normalized on Windows and intentionally exact on Unix"
)]
fn is_managed_published_copy(proc_exe: &Path, target_exe: &Path) -> bool {
    let (Some(proc_parent), Some(target_parent)) = (proc_exe.parent(), target_exe.parent()) else {
        return false;
    };
    if canonical_or(proc_parent) != canonical_or(target_parent) {
        return false;
    }
    let (Some(proc_name), Some(target_name)) = (proc_exe.file_name(), target_exe.file_name())
    else {
        return false;
    };
    if proc_name == target_name {
        return true;
    }
    // A rebuild side-lines an in-use binary as "<name>.locked-<ts>.old" before
    // dropping the fresh file at the original path; the still-running stale
    // process keeps that side-lined image path.
    let proc_name = proc_name.to_string_lossy();
    let target_name = target_name.to_string_lossy();
    if cfg!(windows) {
        // Windows paths are case-insensitive and sysinfo may report different
        // casing than the catalog-derived path.
        if proc_name.eq_ignore_ascii_case(&target_name) {
            return true;
        }
        let prefix = format!("{}.locked-", target_name.to_ascii_lowercase());
        let lowered = proc_name.to_ascii_lowercase();
        lowered.starts_with(&prefix) && lowered.ends_with(".old")
    } else {
        proc_name.starts_with(&format!("{target_name}.locked-")) && proc_name.ends_with(".old")
    }
}

fn canonical_or(path: &Path) -> PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf())
}

/// Stop any process that is a stale copy of the published binary at `exe`
/// (see [`is_stale_published_copy`]). This frees a held engine/daemon port so
/// the next launch spawns fresh code instead of attaching to outdated code left
/// running by an earlier launcher session or rebuild. Genuinely external
/// engines (a different executable, or a build newer than what is on disk) are
/// left untouched. Best-effort: returns the number of stale copies stopped, or
/// `0` if the binary's build time cannot be read.
pub(crate) fn reap_stale_published_copies(exe: &Path) -> usize {
    let built_secs = match std::fs::metadata(exe).and_then(|m| m.modified()) {
        Ok(modified) => match modified.duration_since(UNIX_EPOCH) {
            Ok(delta) => delta.as_secs(),
            Err(_) => return 0,
        },
        Err(_) => return 0,
    };

    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_exe(UpdateKind::Always),
    );

    let mut stopped = 0usize;
    for proc_handle in sys.processes().values() {
        let Some(proc_exe) = proc_handle.exe() else {
            continue;
        };
        if !is_stale_published_copy(proc_exe, proc_handle.start_time(), exe, built_secs) {
            continue;
        }
        let killed = proc_handle
            .kill_with(Signal::Term)
            .unwrap_or_else(|| proc_handle.kill());
        if killed {
            stopped += 1;
        }
    }
    stopped
}

/// Stop processes that use the launcher's published engine executable.
///
/// A new launcher session cannot recover its previous in-memory registry. It
/// uses this executable match to reclaim engines that it started earlier. A
/// process from a different executable remains external and is not stopped.
pub(crate) fn reap_managed_published_copies(exe: &Path) -> usize {
    let mut sys = System::new();
    sys.refresh_processes_specifics(
        ProcessesToUpdate::All,
        true,
        ProcessRefreshKind::nothing().with_exe(UpdateKind::Always),
    );

    let mut stopped = 0usize;
    for proc_handle in sys.processes().values() {
        let Some(proc_exe) = proc_handle.exe() else {
            continue;
        };
        if !is_managed_published_copy(proc_exe, exe) {
            continue;
        }
        let killed = proc_handle
            .kill_with(Signal::Term)
            .unwrap_or_else(|| proc_handle.kill());
        if killed {
            stopped += 1;
        }
    }
    stopped
}

#[cfg(test)]
#[allow(
    clippy::expect_used,
    clippy::unwrap_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::similar_names,
    clippy::too_many_lines,
    clippy::needless_pass_by_value
)]
mod tests {
    use super::*;
    use std::ffi::OsString;

    use crate::catalog::{find, ENGINE_RUST, UI_TUI};

    #[test]
    fn build_command_sets_args_and_env() {
        let spec = find(ENGINE_RUST).expect("rust engine");
        let exe = PathBuf::from("/no/such/path/qsoripper-server");
        let arg1: OsString = "--listen".into();
        let arg2: OsString = "127.0.0.1:50051".into();
        let args = [arg1.as_os_str(), arg2.as_os_str()];
        let cmd = build_command(&spec, &exe, &args, [("FOO", "bar")]);
        let argv: Vec<&OsStr> = cmd.get_args().collect();
        assert_eq!(argv, vec![arg1.as_os_str(), arg2.as_os_str()]);
        let env_pairs: Vec<(&OsStr, Option<&OsStr>)> = cmd.get_envs().collect();
        assert!(env_pairs
            .iter()
            .any(|(k, v)| *k == OsStr::new("FOO")
                && v.map(OsStr::to_os_string) == Some("bar".into())));
    }

    #[cfg(windows)]
    #[test]
    fn build_command_for_tui_targets_a_terminal_on_windows() {
        let spec = find(UI_TUI).expect("tui");
        assert!(spec.wants_console, "TUI must request a console");
        let exe = PathBuf::from("C:/no/such/path/qsoripper-tui.exe");
        let cmd = build_command(&spec, &exe, &[], std::iter::empty::<(&str, &str)>());
        let program = cmd.get_program().to_string_lossy().to_lowercase();
        let args: Vec<&OsStr> = cmd.get_args().collect();
        if program.ends_with("wt.exe") {
            assert!(
                args.iter().any(|a| *a == exe.as_os_str()),
                "wt wrapper must pass the TUI exe as an argument"
            );
        } else {
            assert_eq!(cmd.get_program(), exe.as_os_str());
        }
    }

    #[cfg(not(windows))]
    #[test]
    fn build_command_for_tui_wraps_with_terminal_on_unix() {
        let spec = find(UI_TUI).expect("tui");
        assert!(spec.wants_console, "TUI must request a console");
        let exe = PathBuf::from("/no/such/path/qsoripper-tui");
        let cmd = build_command(&spec, &exe, &[], std::iter::empty::<(&str, &str)>());
        // The terminal program should be the first argv, and the exe should
        // appear later in the argument list (after the emulator's separator).
        assert_ne!(cmd.get_program(), exe.as_os_str());
        let args: Vec<&OsStr> = cmd.get_args().collect();
        assert!(args.iter().any(|a| *a == exe.as_os_str()));
    }

    #[cfg(not(windows))]
    #[test]
    fn ptyxis_system_alternative_uses_its_argument_separator() {
        let directory = tempfile::tempdir().expect("terminal directory");
        let ptyxis = directory.path().join("ptyxis");
        std::fs::write(&ptyxis, "").expect("terminal executable");
        std::os::unix::fs::symlink(ptyxis, directory.path().join("x-terminal-emulator"))
            .expect("system terminal alternative");

        let (program, args) = pick_terminal(directory.path().as_os_str());

        assert_eq!(program, "x-terminal-emulator");
        assert_eq!(args, ["--standalone", "--new-window", "--"]);
    }

    #[test]
    fn registry_round_trip() {
        let mut reg = ProcessRegistry::new();
        reg.insert(LaunchedProcess {
            component: ENGINE_RUST,
            pid: 1234,
            executable: PathBuf::from("x"),
        });
        assert_eq!(reg.get(ENGINE_RUST).map(|p| p.pid), Some(1234));
        assert!(reg.remove(ENGINE_RUST).is_some());
        assert!(reg.get(ENGINE_RUST).is_none());
    }

    #[test]
    fn parses_cathub_version_output() {
        assert_eq!(parse_cathub_version("cathub 0.1.0\n"), Some("0.1.0"));
        assert_eq!(parse_cathub_version("other 0.1.0"), None);
        assert_eq!(parse_cathub_version("cathub"), None);
    }

    #[test]
    fn accepts_only_supported_cathub_version_series() {
        assert!(!is_supported_cathub_version("0.1"));
        assert!(!is_supported_cathub_version("0.1.0"));
        assert!(is_supported_cathub_version("0.2.0"));
        assert!(is_supported_cathub_version("0.2.12"));
        assert!(!is_supported_cathub_version("0.0.9"));
        assert!(!is_supported_cathub_version("0.3.0"));
        assert!(!is_supported_cathub_version("0.10.0"));
    }

    #[test]
    fn stale_copy_matches_same_exe_started_before_build() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        // Started at t=100, binary built at t=200 -> stale.
        assert!(is_stale_published_copy(&target, 100, &target, 200));
    }

    #[test]
    fn managed_copy_matches_same_exe_without_build_time_check() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        assert!(is_managed_published_copy(&target, &target));
    }

    #[test]
    fn managed_copy_rejects_same_name_from_another_directory() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        let external = PathBuf::from("/debug/qsoripper-server.exe");
        assert!(!is_managed_published_copy(&external, &target));
    }

    #[test]
    fn fresh_copy_started_after_build_is_not_stale() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        // Started at t=300, binary built at t=200 -> current, keep it.
        assert!(!is_stale_published_copy(&target, 300, &target, 200));
        // Started exactly at the build second -> treat as current (keep).
        assert!(!is_stale_published_copy(&target, 200, &target, 200));
    }

    #[test]
    fn side_lined_locked_variant_in_same_dir_is_stale() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        let side_lined = PathBuf::from("/publish/Release/qsoripper-server.exe.locked-20260530.old");
        assert!(is_stale_published_copy(&side_lined, 100, &target, 200));
    }

    #[test]
    fn different_executable_is_never_stale() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        // Different file name in the same directory.
        let other_name = PathBuf::from("/publish/Release/unrelated-cathub.exe");
        assert!(!is_stale_published_copy(&other_name, 100, &target, 200));
        // Same file name but a different directory (e.g. a dev `cargo run`).
        let other_dir = PathBuf::from("/debug/qsoripper-server.exe");
        assert!(!is_stale_published_copy(&other_dir, 100, &target, 200));
    }

    #[test]
    fn unrelated_locked_prefix_is_not_matched() {
        let target = PathBuf::from("/publish/Release/qsoripper-server.exe");
        // A file that merely starts with the same stem but is not the
        // "<name>.locked-...old" side-lined form must not match.
        let unrelated = PathBuf::from("/publish/Release/qsoripper-server.exe.bak");
        assert!(!is_stale_published_copy(&unrelated, 100, &target, 200));
        // The ".locked-" prefix without the ".old" suffix is also rejected.
        let no_old = PathBuf::from("/publish/Release/qsoripper-server.exe.locked-20260530");
        assert!(!is_stale_published_copy(&no_old, 100, &target, 200));
    }
}
