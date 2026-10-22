//! qsoripper-cathub: a multi-client CAT hub daemon.
//!
//! The daemon is the single owner of the radio link and fans it out to many client faces
//! (HDSDR/OmniRig, N1MM Logger+, ARCP-590, WSJT-X, Log4OM, and the QsoRipper engine) over
//! their native protocols. It serializes every write, owns the radio's native push stream,
//! serves reads from a universal cache, arbitrates PTT with a single-owner lease, and never
//! retargets a VFO during polling — eliminating the A/B oscillation, frequency drift, and
//! transmit conflicts that come from many apps fighting over one serial port.
//!
//! See `docs/design/cathub-multi-client-cat-hub.md` for the full design.

#![allow(clippy::doc_markdown)]

mod backend;
mod config;
mod dialect;
mod error;
mod events;
mod hamlib_net;
mod logging;
mod model;
mod permissions;
mod ptt;
mod radio;
mod serial_face;
mod state;

#[cfg(test)]
mod emulator;

#[cfg(test)]
mod integration;

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use tokio::net::TcpStream;
use tracing_appender::non_blocking::WorkerGuard;

use crate::backend::kenwood::ts590::Ts590Backend;
use crate::backend::loopback::LoopbackBackend;
use crate::backend::rigctld::RigctldBackend;
use crate::backend::{BackendError, RadioBackend};
use crate::config::{Config, RadioConfig};
use crate::dialect::kenwood::ts2000::Ts2000Dialect;
use crate::dialect::kenwood::ts590::Ts590Dialect;
use crate::dialect::{ClientDialect, FaceContext};
use crate::events::{spawn_poller, POLLER_FACE};
use crate::hamlib_net::run_listener;
use crate::model::StateMutation;
use crate::ptt::PttManager;
use crate::radio::{
    link_channel, run_transport_supervised, spawn_scheduler, OpKind, Priority, RECONNECT_INITIAL,
    RECONNECT_MAX,
};
use crate::serial_face::{open_serial, run_face};
use crate::state::StateHandle;

pub use crate::error::CatHubError;

/// Validate that a unified `config.toml` body contains a `[cat_hub]` section the
/// cathub daemon will accept. This is exposed for the QsoRipper engine's setup
/// wizard tests so a regression in the engine's CAT hub writer is caught against
/// the daemon's real parser/validator rather than a hand-maintained copy.
///
/// # Errors
///
/// Returns a [`CatHubError`] if the document cannot be parsed or the resulting
/// `[cat_hub]` configuration fails the daemon's semantic validation.
#[doc(hidden)]
pub fn validate_cat_hub_toml(text: &str) -> Result<(), CatHubError> {
    Config::parse_document(text)
        .map(|_| ())
        .map_err(CatHubError::Config)
}

/// Command-line arguments.
#[derive(Debug, Parser)]
#[command(
    name = "qsoripper-cathub",
    about = "Multi-client CAT hub daemon for sharing one radio across many applications"
)]
pub struct Cli {
    /// Path to the configuration file (defaults to the platform config path).
    #[arg(short, long)]
    pub config: Option<PathBuf>,
    /// Optional explicit log file path (informational; logging also writes a rolling file).
    #[arg(long)]
    pub log: Option<PathBuf>,
    /// Load and validate the configuration, print it, and exit without touching hardware.
    #[arg(long)]
    pub dry_run: bool,
}

/// Initialize tracing for the process. The returned guard must be kept alive for the
/// process lifetime so the non-blocking file writer flushes on shutdown.
pub fn init_logging() -> WorkerGuard {
    logging::init()
}

/// Build the configured radio backend.
fn build_backend(cfg: &Config) -> Result<Arc<dyn RadioBackend>, CatHubError> {
    match cfg.radio.backend.as_str() {
        "ts590" => Ok(Arc::new(Ts590Backend::new())),
        "rigctld" => Ok(Arc::new(RigctldBackend::new(
            cfg.radio.model.clone(),
            cfg.radio.certified,
        ))),
        "loopback" => Ok(Arc::new(LoopbackBackend::new())),
        other => Err(CatHubError::Backend(format!("unknown backend '{other}'"))),
    }
}

/// Build a client dialect by name.
fn dialect_for(name: &str) -> Result<Arc<dyn ClientDialect>, CatHubError> {
    match name {
        "ts590" => Ok(Arc::new(Ts590Dialect::new())),
        "ts2000" => Ok(Arc::new(Ts2000Dialect::new())),
        other => Err(CatHubError::Backend(format!("unknown dialect '{other}'"))),
    }
}

/// An opened radio transport.
enum OpenedTransport {
    /// A real serial port.
    Serial(serial2_tokio::SerialPort),
    /// A TCP socket (for a `tcp` transport or rigctld bridge endpoint).
    Tcp(TcpStream),
}

/// Open the radio transport described by the `[radio]` section.
async fn open_transport(radio: &RadioConfig) -> Result<OpenedTransport, CatHubError> {
    match radio.transport.as_str() {
        "serial" => Ok(OpenedTransport::Serial(open_radio_serial(radio)?)),
        "tcp" => Ok(OpenedTransport::Tcp(open_radio_tcp(radio).await?)),
        other => Err(CatHubError::Backend(format!(
            "unknown radio.transport '{other}'"
        ))),
    }
}

/// Open and condition the radio serial port.
///
/// Assert the RTS and DTR modem-control lines. Some radios (notably the Kenwood TS-590) gate
/// their CAT transmit on RTS and send no replies at all unless it is high, so without this the
/// daemon opens the port but every poll times out. This matches the default line state that
/// OmniRig/Hamlib clients use.
fn open_radio_serial(radio: &RadioConfig) -> std::io::Result<serial2_tokio::SerialPort> {
    let port = serial2_tokio::SerialPort::open(&radio.port, radio.baud)?;
    port.set_rts(true)?;
    port.set_dtr(true)?;
    Ok(port)
}

/// Connect the radio TCP transport (a `tcp` radio or a rigctld bridge endpoint).
async fn open_radio_tcp(radio: &RadioConfig) -> std::io::Result<TcpStream> {
    TcpStream::connect((radio.host.as_str(), radio.tcp_port)).await
}

/// Run the daemon to completion (until Ctrl+C).
///
/// # Errors
///
/// Returns a [`CatHubError`] if the configuration cannot be loaded or validated, the
/// backend or a dialect cannot be built, the radio transport or a face port cannot be
/// opened, or the process fails to install its Ctrl+C handler.
#[allow(clippy::too_many_lines)] // The wiring is one cohesive bring-up sequence.
pub async fn run(cli: Cli) -> Result<(), CatHubError> {
    let path = cli
        .config
        .clone()
        .unwrap_or_else(Config::default_config_path);
    if let Some(log) = &cli.log {
        tracing::debug!(log = %log.display(), "log path override requested");
    }
    let cfg = Config::load(&path)?;

    if cli.dry_run {
        println!("{}", cfg.describe());
        return Ok(());
    }

    let backend = build_backend(&cfg)?;
    let caps = backend.capabilities();
    tracing::info!(caps = %caps.summary(), "backend ready");

    let state = StateHandle::new();
    let ptt = PttManager::new(cfg.ptt_max_tx());

    // Wire the transport to the serialized radio link. The loopback backend needs no real
    // transport (it never submits raw bytes), so we just drop the receiver in that case.
    //
    // Native push is "in play" whenever the operator enabled it and the backend has a push
    // command. The supervisor (below) actually issues the enable on the first connect and
    // re-issues it on every reconnect, so the poller can use this flag to decide back-off.
    let (link, raw_rx) = link_channel();
    let native_push_active = cfg.radio.backend != "loopback"
        && cfg.events.native_push
        && backend.native_push_enable().is_some();
    if cfg.radio.backend == "loopback" {
        drop(raw_rx);
    } else {
        // The transport is supervised: if the serial/TCP link drops (unplug, radio
        // power-cycle, write error) the daemon reopens it with backoff and keeps serving the
        // same command queue, instead of leaving every client wired to a dead radio link until
        // the whole daemon is restarted.
        let push_link = native_push_active.then(|| link.clone());
        let backend_t = backend.clone();
        let state_t = state.clone();
        match open_transport(&cfg.radio).await? {
            OpenedTransport::Serial(port) => {
                let radio_cfg = cfg.radio.clone();
                tokio::spawn(run_transport_supervised(
                    port,
                    raw_rx,
                    backend_t,
                    state_t,
                    push_link,
                    move || {
                        let radio_cfg = radio_cfg.clone();
                        async move {
                            open_radio_serial(&radio_cfg)
                                .map_err(|e| BackendError::Transport(e.to_string()))
                        }
                    },
                    RECONNECT_INITIAL,
                    RECONNECT_MAX,
                ));
            }
            OpenedTransport::Tcp(stream) => {
                let radio_cfg = cfg.radio.clone();
                tokio::spawn(run_transport_supervised(
                    stream,
                    raw_rx,
                    backend_t,
                    state_t,
                    push_link,
                    move || {
                        let radio_cfg = radio_cfg.clone();
                        async move {
                            open_radio_tcp(&radio_cfg)
                                .await
                                .map_err(|e| BackendError::Transport(e.to_string()))
                        }
                    },
                    RECONNECT_INITIAL,
                    RECONNECT_MAX,
                ));
            }
        }
    }

    let radio = spawn_scheduler(backend.clone(), link, state.clone());

    spawn_poller(
        radio.clone(),
        state.clone(),
        native_push_active,
        cfg.baseline_interval(),
        cfg.heartbeat_interval(),
    );

    // Prime the universal state with one awaited poll before any face begins serving, so the
    // first client read (e.g. HDSDR/OmniRig connecting at startup) sees real radio state
    // instead of defaults. Best-effort and time-bounded: a slow or absent radio must not
    // block startup, since the baseline poller keeps retrying afterwards.
    match tokio::time::timeout(
        Duration::from_millis(1_000),
        radio.submit(POLLER_FACE, Priority::Poll, OpKind::Poll),
    )
    .await
    {
        Ok(Ok(_)) => tracing::info!("primed universal state from initial poll"),
        Ok(Err(error)) => {
            tracing::warn!(%error, "initial priming poll failed; serving defaults until next poll");
        }
        Err(_) => {
            tracing::warn!("initial priming poll timed out; serving defaults until next poll");
        }
    }

    let next_id = Arc::new(AtomicU64::new(1));

    for face in &cfg.face {
        let dialect = dialect_for(&face.dialect)?;
        let id = next_id.fetch_add(1, Ordering::SeqCst);
        let ctx = FaceContext::new(
            id,
            face.permissions(),
            state.clone(),
            radio.clone(),
            ptt.clone(),
            caps.clone(),
        );
        let port = open_serial(&face.name, &face.transport, face.baud)?;
        tokio::spawn(run_face(port, dialect, ctx, b';'));
        tracing::info!(
            face = %face.name,
            id,
            hub_port = %face.transport,
            "serial face listening; hub owns this port -- point the application at the paired \
             com0com port, not this one"
        );
    }

    for ep in &cfg.hamlib_net {
        let id = next_id.fetch_add(1, Ordering::SeqCst);
        let template = FaceContext::new(
            id,
            ep.permissions(),
            state.clone(),
            radio.clone(),
            ptt.clone(),
            caps.clone(),
        );
        let bind = ep.bind.clone();
        let name = ep.name.clone();
        let ids = next_id.clone();
        tokio::spawn(async move {
            if let Err(e) = run_listener(&bind, ids, template).await {
                tracing::error!(endpoint = %name, error = %e, "hamlib_net listener stopped");
            }
        });
        tracing::info!(endpoint = %ep.name, bind = %ep.bind, "hamlib_net endpoint listening");
    }

    // PTT safety watchdog: a transmitter that exceeds the configured ceiling is unkeyed at
    // the radio first, then released, so the ceiling is a real stuck-transmitter backstop
    // and the lease is never freed while the radio is still keyed.
    {
        let ptt = ptt.clone();
        let radio = radio.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(500)).await;
                if let Some(face) = ptt.expired_owner() {
                    let _ = radio
                        .submit(
                            face,
                            Priority::Ptt,
                            OpKind::Apply(StateMutation::SetPtt {
                                keyed: false,
                                source: crate::model::PttSource::Generic,
                            }),
                        )
                        .await;
                    ptt.unkey(face);
                    tracing::warn!(face, "PTT safety ceiling reached; transmitter released");
                }
            }
        });
    }

    tracing::info!("cathub running; press Ctrl+C to stop");
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutdown requested");

    // Best-effort orderly stop: never leave the transmitter keyed (design §8.5). A hard
    // crash cannot run this; the ptt_max_tx_ms ceiling and the radio's own TX timeout are
    // the ultimate backstops.
    if let Some(owner) = ptt.owner() {
        let _ = tokio::time::timeout(
            Duration::from_millis(500),
            radio.submit(
                owner,
                Priority::Ptt,
                OpKind::Apply(StateMutation::SetPtt {
                    keyed: false,
                    source: crate::model::PttSource::Generic,
                }),
            ),
        )
        .await;
        ptt.unkey(owner);
        tracing::info!(face = owner, "released PTT on shutdown");
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    #[test]
    fn build_backend_dispatches_each_kind() {
        let cfg = Config::parse(
            "[radio]\nbackend = \"ts590\"\nport = \"COM3\"\n\
             [[face]]\nname=\"f\"\ntransport=\"COM5\"\ndialect=\"ts590\"\n",
        )
        .expect("parse");
        assert_eq!(
            build_backend(&cfg).expect("ts590").capabilities().model,
            "TS-590"
        );

        let cfg = Config::parse(
            "[radio]\nbackend = \"rigctld\"\nmodel=\"TS-590SG\"\ntransport=\"tcp\"\n\
             [[face]]\nname=\"f\"\ntransport=\"COM5\"\ndialect=\"ts590\"\n",
        )
        .expect("parse");
        assert!(build_backend(&cfg)
            .expect("rigctld")
            .capabilities()
            .model
            .starts_with("rigctld:"));

        let cfg = Config::parse(
            "[radio]\nbackend = \"loopback\"\n[[face]]\nname=\"f\"\ntransport=\"COM5\"\ndialect=\"ts590\"\n",
        )
        .expect("parse");
        assert_eq!(
            build_backend(&cfg).expect("loopback").capabilities().model,
            "loopback"
        );
    }

    #[test]
    fn dialect_for_known_and_unknown() {
        assert!(dialect_for("ts590").is_ok());
        assert!(dialect_for("ts2000").is_ok());
        assert!(dialect_for("yaesu").is_err());
    }

    #[tokio::test]
    async fn dry_run_prints_config_and_exits() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("cathub-dryrun-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "[radio]\nbackend = \"loopback\"\n\
             [[face]]\nname=\"f\"\ntransport=\"COM5\"\ndialect=\"ts590\"\n",
        )
        .expect("write");
        let cli = Cli {
            config: Some(path.clone()),
            log: None,
            dry_run: true,
        };
        assert!(run(cli).await.is_ok());
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn run_fails_on_missing_config() {
        let cli = Cli {
            config: Some(PathBuf::from("does-not-exist-cathub.toml")),
            log: None,
            dry_run: true,
        };
        assert!(run(cli).await.is_err());
    }

    #[tokio::test]
    async fn open_transport_rejects_unknown_transport() {
        let mut cfg = Config::parse(
            "[radio]\nbackend = \"ts590\"\ntransport = \"serial\"\nport = \"COM3\"\n\
             [[face]]\nname=\"f\"\ntransport=\"COM5\"\ndialect=\"ts590\"\n",
        )
        .expect("parse");
        // open_transport defends against a transport string that bypassed validation.
        cfg.radio.transport = "usb".to_string();
        assert!(open_transport(&cfg.radio).await.is_err());
    }

    #[tokio::test]
    async fn run_wires_loopback_then_fails_opening_a_bogus_face_port() {
        // Exercises the full bring-up: backend, state, PTT, scheduler, native-push probe,
        // and poller, failing only when it tries to open a face's (nonexistent) serial port.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("cathub-run-{}.toml", std::process::id()));
        std::fs::write(
            &path,
            "[radio]\nbackend = \"loopback\"\n\
             [[face]]\nname = \"bogus\"\ntransport = \"COM_DOES_NOT_EXIST\"\ndialect = \"ts590\"\n",
        )
        .expect("write");
        let cli = Cli {
            config: Some(path.clone()),
            log: None,
            dry_run: false,
        };
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), run(cli)).await;
        let _ = std::fs::remove_file(&path);
        assert!(matches!(result, Ok(Err(_))), "expected a face open error");
    }
}
