//! Component catalog: every engine and UI the launcher knows how to start.

use std::path::PathBuf;

use crate::discovery::ArtifactRoot;

/// Stable identifier used for persistence and the binding map.
pub(crate) type ComponentId = &'static str;

/// Whether a component is an engine (acts as a gRPC server) or a UI client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ComponentKind {
    /// Background daemon/service that must be up before engines connect
    /// (e.g. the CAT hub that owns the radio serial port).
    Daemon,
    /// Engine that exposes a gRPC endpoint on a known TCP port.
    Engine,
    /// UI client that connects to one of the running engines.
    Ui,
}

/// Static metadata for a launchable component.
#[derive(Debug, Clone)]
pub(crate) struct ComponentSpec {
    pub id: ComponentId,
    pub display_name: &'static str,
    pub kind: ComponentKind,
    /// Canonical 127.0.0.1 readiness port: an engine's gRPC port, or a
    /// daemon's primary listening port. `None` for UIs and portless services.
    pub engine_port: Option<u16>,
    /// `true` if the UI honors `QSORIPPER_ENGINE` / `QSORIPPER_ENDPOINT` envs
    /// to pick an engine. Used to decide whether to show a binding picker.
    pub engine_bindable: bool,
    /// `true` for terminal apps that need their own console window to render
    /// (e.g. ratatui-based TUIs). On Windows we spawn with `CREATE_NEW_CONSOLE`
    /// and inherit stdio; on Unix we route through a terminal emulator.
    pub wants_console: bool,
    /// Resolves the executable path under the published artifact tree.
    pub artifact: ArtifactSpec,
}

/// How to locate a component's executable inside `artifacts/publish/`.
#[derive(Debug, Clone)]
pub(crate) struct ArtifactSpec {
    /// Subdirectory name under `artifacts/publish/<sub>/<Configuration>/`.
    pub publish_subdir: &'static str,
    /// File name of the executable on the current platform (without `.exe`;
    /// platform suffix is appended in [`ArtifactSpec::executable_path`]).
    pub executable_stem: &'static str,
    /// Optional environment variable containing an explicitly installed executable.
    /// When present, resolution may fall back to external development and install locations.
    pub external_executable_env: Option<&'static str>,
}

impl ArtifactSpec {
    /// Resolve the full executable path under the published artifact root.
    pub(crate) fn executable_path(&self, root: &ArtifactRoot) -> PathBuf {
        self.executable_candidates(root)
            .into_iter()
            .next()
            .unwrap_or_else(|| self.bundled_executable_path(root))
    }

    /// Return executable candidates in precedence order.
    ///
    /// External components can come from an explicit override, the publish tree,
    /// a sibling development repository, or `PATH`.
    pub(crate) fn executable_candidates(&self, root: &ArtifactRoot) -> Vec<PathBuf> {
        let file_name = self.executable_file_name();
        let bundled = self.bundled_executable_path(root);
        if self.external_executable_env.is_none() {
            return vec![bundled];
        }

        let mut candidates = Vec::new();
        if let Some(variable) = self.external_executable_env {
            if let Some(path) = std::env::var_os(variable).map(PathBuf::from) {
                push_existing_candidate(&mut candidates, path);
            }
        }
        push_existing_candidate(&mut candidates, bundled.clone());
        if let Some(sibling) = sibling_repository_executable(root, &file_name) {
            push_existing_candidate(&mut candidates, sibling);
        }
        if let Some(installed) = find_on_path(file_name.as_ref()) {
            push_existing_candidate(&mut candidates, installed);
        }
        if candidates.is_empty() {
            candidates.push(bundled);
        }
        candidates
    }

    fn executable_file_name(&self) -> String {
        let mut file_name = self.executable_stem.to_owned();
        if cfg!(windows) {
            file_name.push_str(".exe");
        }
        file_name
    }

    fn bundled_executable_path(&self, root: &ArtifactRoot) -> PathBuf {
        let bundled = root
            .path()
            .join(self.publish_subdir)
            .join(root.configuration())
            .join(self.executable_file_name());
        bundled
    }
}

fn push_existing_candidate(candidates: &mut Vec<PathBuf>, candidate: PathBuf) {
    if candidate.is_file() && !candidates.contains(&candidate) {
        candidates.push(candidate);
    }
}

fn sibling_repository_executable(root: &ArtifactRoot, file_name: &str) -> Option<PathBuf> {
    let repo_root = root.path().parent()?.parent()?;
    let repositories_root = repo_root.parent()?;
    let profile = root.configuration().to_ascii_lowercase();
    ["cathub", "CatHub"]
        .into_iter()
        .map(|directory| {
            repositories_root
                .join(directory)
                .join("target")
                .join(&profile)
                .join(file_name)
        })
        .find(|candidate| candidate.is_file())
}

fn find_on_path(file_name: &std::ffi::OsStr) -> Option<PathBuf> {
    let path = std::env::var_os("PATH")?;
    std::env::split_paths(&path)
        .map(|directory| directory.join(file_name))
        .find(|candidate| candidate.is_file())
}

const fn bundled_artifact(
    publish_subdir: &'static str,
    executable_stem: &'static str,
) -> ArtifactSpec {
    ArtifactSpec {
        publish_subdir,
        executable_stem,
        external_executable_env: None,
    }
}

pub(crate) const DAEMON_CATHUB: ComponentId = "cathub";
pub(crate) const ENGINE_RUST: ComponentId = "rust-engine";
pub(crate) const ENGINE_DOTNET: ComponentId = "dotnet-engine";
pub(crate) const UI_GUI: ComponentId = "gui";
pub(crate) const UI_DEBUGHOST: ComponentId = "debughost";
pub(crate) const UI_TUI: ComponentId = "tui";
pub(crate) const UI_CWSCOPE: ComponentId = "cwscope";
#[cfg(any(windows, test))]
pub(crate) const UI_WIN32: ComponentId = "win32";

/// Static list of every component the launcher manages.
pub(crate) fn catalog() -> Vec<ComponentSpec> {
    vec![
        ComponentSpec {
            id: DAEMON_CATHUB,
            display_name: "CatHub standalone service",
            kind: ComponentKind::Daemon,
            engine_port: Some(4532),
            engine_bindable: false,
            wants_console: false,
            artifact: ArtifactSpec {
                publish_subdir: "cathub",
                executable_stem: "cathub",
                external_executable_env: Some("CATHUB_EXECUTABLE"),
            },
        },
        ComponentSpec {
            id: ENGINE_RUST,
            display_name: "Rust engine (qsoripper-server)",
            kind: ComponentKind::Engine,
            engine_port: Some(50051),
            engine_bindable: false,
            wants_console: false,
            artifact: bundled_artifact("qsoripper-server", "qsoripper-server"),
        },
        ComponentSpec {
            id: ENGINE_DOTNET,
            display_name: ".NET engine (QsoRipper.Engine.DotNet)",
            kind: ComponentKind::Engine,
            engine_port: Some(50052),
            engine_bindable: false,
            wants_console: false,
            artifact: bundled_artifact("qsoripper-engine-dotnet", "QsoRipper.Engine.DotNet"),
        },
        ComponentSpec {
            id: UI_GUI,
            display_name: "Avalonia GUI (QsoRipper.Gui)",
            kind: ComponentKind::Ui,
            engine_port: None,
            engine_bindable: true,
            wants_console: false,
            artifact: bundled_artifact("qsoripper-gui", "QsoRipper.Gui"),
        },
        ComponentSpec {
            id: UI_DEBUGHOST,
            display_name: "DebugHost (http://localhost:5082)",
            kind: ComponentKind::Ui,
            engine_port: None,
            engine_bindable: true,
            wants_console: false,
            artifact: bundled_artifact("qsoripper-debughost", "QsoRipper.DebugHost"),
        },
        ComponentSpec {
            id: UI_TUI,
            display_name: "Terminal UI (qsoripper-tui)",
            kind: ComponentKind::Ui,
            engine_port: None,
            engine_bindable: true,
            wants_console: true,
            artifact: bundled_artifact("qsoripper-tui", "qsoripper-tui"),
        },
        ComponentSpec {
            id: UI_CWSCOPE,
            display_name: "CW Scope (CwDecoderGui)",
            kind: ComponentKind::Ui,
            engine_port: None,
            engine_bindable: false,
            wants_console: false,
            artifact: bundled_artifact("cw-decoder-gui", "CwDecoderGui"),
        },
        #[cfg(windows)]
        ComponentSpec {
            id: UI_WIN32,
            display_name: "Win32 GUI (qsoripper-win32)",
            kind: ComponentKind::Ui,
            engine_port: None,
            // The Win32 client does not yet honor QSORIPPER_ENGINE /
            // QSORIPPER_ENDPOINT; engine selection is its own follow-up.
            engine_bindable: false,
            wants_console: false,
            artifact: bundled_artifact("qsoripper-win32", "qsoripper-win32"),
        },
    ]
}

/// Look up a component by id.
pub(crate) fn find(id: &str) -> Option<ComponentSpec> {
    catalog().into_iter().find(|c| c.id == id)
}

/// Look up the canonical engine endpoint for the given engine component id.
pub(crate) fn engine_endpoint(id: &str) -> Option<String> {
    find(id)
        .filter(|c| c.kind == ComponentKind::Engine)
        .and_then(|c| c.engine_port)
        .map(|p| format!("http://127.0.0.1:{p}"))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn win32_ui_requires_windows() {
        let spec = find(UI_WIN32);
        if !cfg!(windows) {
            assert!(spec.is_none());
            return;
        }
        let spec = spec.expect("win32 ui in catalog on Windows");
        assert_eq!(spec.kind, ComponentKind::Ui);
        assert!(!spec.wants_console);
        assert!(!spec.engine_bindable);
        assert_eq!(spec.artifact.publish_subdir, "qsoripper-win32");
        assert_eq!(spec.artifact.executable_stem, "qsoripper-win32");
    }

    #[test]
    fn catalog_has_unique_component_ids() {
        let ids: Vec<&'static str> = catalog().into_iter().map(|c| c.id).collect();
        let mut sorted = ids.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert_eq!(sorted.len(), ids.len(), "duplicate component id in catalog");
    }

    #[test]
    fn cathub_daemon_is_in_catalog() {
        let spec = find(DAEMON_CATHUB).expect("cathub daemon in catalog");
        assert_eq!(spec.kind, ComponentKind::Daemon);
        assert_eq!(spec.engine_port, Some(4532));
        assert!(!spec.engine_bindable);
        assert!(!spec.wants_console);
        assert_eq!(spec.artifact.publish_subdir, "cathub");
        assert_eq!(spec.artifact.executable_stem, "cathub");
        assert_eq!(
            spec.artifact.external_executable_env,
            Some("CATHUB_EXECUTABLE")
        );
    }

    #[test]
    fn cathub_is_not_an_engine_endpoint() {
        // The daemon carries a port for readiness probing, but must never be
        // offered as a gRPC engine endpoint a UI can bind to.
        assert!(engine_endpoint(DAEMON_CATHUB).is_none());
    }

    #[test]
    fn cathub_discovers_sibling_repository_build() {
        let directory = tempfile::tempdir().expect("temporary repositories root");
        let qso_root = directory.path().join("qsoripper");
        let artifact_root =
            ArtifactRoot::from_repo_root(&qso_root, crate::discovery::Configuration::Release);
        let file_name = if cfg!(windows) {
            "cathub.exe"
        } else {
            "cathub"
        };
        let sibling_executable = directory
            .path()
            .join("cathub")
            .join("target")
            .join("release")
            .join(file_name);
        std::fs::create_dir_all(
            sibling_executable
                .parent()
                .expect("sibling executable parent"),
        )
        .expect("create sibling target directory");
        std::fs::write(&sibling_executable, b"test executable").expect("write sibling executable");

        assert_eq!(
            sibling_repository_executable(&artifact_root, file_name).as_deref(),
            Some(sibling_executable.as_path())
        );
    }
}
