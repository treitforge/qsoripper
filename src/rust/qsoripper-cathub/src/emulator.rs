//! A scriptable in-memory Kenwood TS-590 wire emulator for development and tests.
//!
//! Unlike the per-test request→reply closures scattered through the backend tests, this is a
//! stateful radio: it holds independent VFO A and VFO B truth (frequency and mode), an active
//! receive/transmit VFO, split, and PTT, and it answers the native TS-590 CAT command set with
//! **VFO-dependent** semantics. It can also emit an unsolicited `IF;` status frame to simulate
//! a front-panel change under auto-information (`AI2;`). That fidelity is what lets it reproduce
//! the class of bug where the radio is parked on (or moved to) VFO B but the daemon serves
//! stale frequency/mode data — which a fixed-string fake cannot exhibit.
//!
//! The emulator is transport-agnostic: [`FakeTs590::serve`] drives it over any
//! `AsyncRead + AsyncWrite` duplex, so the same core can sit behind an in-process
//! `tokio::io::duplex` (the tests below) or, later, a standalone TCP listener for live
//! HDSDR/OmniRig/N1MM testing with no radio attached.
//!
//! Only the command surface the daemon actually drives during polling, modeled writes, and the
//! bug-reproduction scenarios is implemented. Real-radio fidelity notes are inline.

use std::sync::{Arc, Mutex, PoisonError};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::mpsc;

use crate::model::Vfo;

/// A point-in-time view of the emulator's internal truth, used by tests to assert which VFO a
/// write actually landed on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FakeState {
    /// VFO A frequency in Hz.
    pub(crate) a_hz: u64,
    /// VFO B frequency in Hz.
    pub(crate) b_hz: u64,
    /// VFO A mode as its Kenwood `MD` digit byte.
    pub(crate) a_mode: u8,
    /// VFO B mode as its Kenwood `MD` digit byte.
    pub(crate) b_mode: u8,
    /// The active receive VFO.
    pub(crate) rx_vfo: Vfo,
    /// The active transmit VFO (equals `rx_vfo` unless split is on).
    pub(crate) tx_vfo: Vfo,
    /// Whether split is enabled.
    pub(crate) split: bool,
    /// Whether the transmitter is keyed.
    pub(crate) ptt: bool,
}

impl FakeState {
    /// The frequency of the active receive VFO.
    pub(crate) fn active_hz(&self) -> u64 {
        match self.rx_vfo {
            Vfo::A => self.a_hz,
            Vfo::B => self.b_hz,
        }
    }
}

/// A front-panel action an operator performs on the emulated radio while it is being served.
pub(crate) enum OperatorEvent {
    /// Tune `vfo` to `hz`, make it the active receive VFO, and push one unsolicited `IF;`
    /// status frame — exactly what a real TS-590 with `AI2;` enabled does when the operator
    /// turns the knob and selects a VFO.
    FrontPanelSwitch { vfo: Vfo, hz: u64 },
}

/// A handle a test uses to drive front-panel changes on a [`FakeTs590`] being served.
pub(crate) struct FrontPanel {
    tx: mpsc::UnboundedSender<OperatorEvent>,
}

impl FrontPanel {
    /// Simulate the operator turning the dial to `hz` on `vfo` and selecting it. The emulator
    /// applies the change and emits a single unsolicited `IF;` push frame.
    pub(crate) fn front_panel_switch(&self, vfo: Vfo, hz: u64) {
        let _ = self.tx.send(OperatorEvent::FrontPanelSwitch { vfo, hz });
    }
}

/// Create a [`FrontPanel`] handle and the receiver [`FakeTs590::serve`] consumes.
pub(crate) fn front_panel() -> (FrontPanel, mpsc::UnboundedReceiver<OperatorEvent>) {
    let (tx, rx) = mpsc::unbounded_channel();
    (FrontPanel { tx }, rx)
}

/// A scriptable, clonable TS-590 wire emulator. Clones share one truth, so a test can hold a
/// handle to inspect state while [`FakeTs590::serve`] runs the wire loop on another clone.
#[derive(Clone)]
pub(crate) struct FakeTs590 {
    inner: Arc<Mutex<FakeState>>,
}

impl FakeTs590 {
    /// A radio parked on VFO A with sane defaults (A on 20 m, B on 40 m, both USB).
    pub(crate) fn new() -> Self {
        FakeTs590 {
            inner: Arc::new(Mutex::new(FakeState {
                a_hz: 14_074_000,
                b_hz: 7_074_000,
                // 2 == USB.
                a_mode: b'2',
                b_mode: b'2',
                rx_vfo: Vfo::A,
                tx_vfo: Vfo::A,
                split: false,
                ptt: false,
            })),
        }
    }

    /// Park the radio on VFO B (receive and, absent split, transmit).
    pub(crate) fn on_vfo_b(self) -> Self {
        {
            let mut s = self.lock();
            s.rx_vfo = Vfo::B;
            s.tx_vfo = Vfo::B;
        }
        self
    }

    /// Override the two VFO frequencies.
    pub(crate) fn with_vfo_freqs(self, a_hz: u64, b_hz: u64) -> Self {
        {
            let mut s = self.lock();
            s.a_hz = a_hz;
            s.b_hz = b_hz;
        }
        self
    }

    /// A snapshot of the emulator's truth.
    pub(crate) fn snapshot(&self) -> FakeState {
        *self.lock()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, FakeState> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Drive the emulator over a duplex byte stream until it closes. Each `;`-terminated client
    /// frame is handled in order (queries reply, sets do not, like a real Kenwood radio), and
    /// front-panel events from `events` apply a change and emit an unsolicited `IF;` push.
    pub(crate) async fn serve<S>(
        self,
        stream: S,
        mut events: mpsc::UnboundedReceiver<OperatorEvent>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Send + 'static,
    {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let mut frame = Vec::with_capacity(48);
        let mut chunk = [0u8; 256];
        let mut events_open = true;
        loop {
            tokio::select! {
                read = reader.read(&mut chunk) => {
                    let n = match read {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    for &byte in chunk.get(..n).unwrap_or(&[]) {
                        frame.push(byte);
                        if byte == b';' {
                            if let Some(reply) = self.handle(&frame) {
                                if writer.write_all(&reply).await.is_err() {
                                    return;
                                }
                                let _ = writer.flush().await;
                            }
                            frame.clear();
                        }
                    }
                }
                event = events.recv(), if events_open => {
                    match event {
                        Some(OperatorEvent::FrontPanelSwitch { vfo, hz }) => {
                            {
                                let mut s = self.lock();
                                match vfo {
                                    Vfo::A => s.a_hz = hz,
                                    Vfo::B => s.b_hz = hz,
                                }
                                s.rx_vfo = vfo;
                                s.tx_vfo = vfo;
                                s.split = false;
                            }
                            let push = self.synth_if();
                            if writer.write_all(&push).await.is_err() {
                                return;
                            }
                            let _ = writer.flush().await;
                        }
                        None => events_open = false,
                    }
                }
            }
        }
    }

    /// Handle one `;`-terminated CAT frame, returning the reply bytes (or `None` for a set).
    fn handle(&self, frame: &[u8]) -> Option<Vec<u8>> {
        let (verb, payload) = split_frame(frame)?;
        let read = payload.is_empty();
        match verb {
            b"FA" => {
                if read {
                    Some(format!("FA{:011};", self.lock().a_hz).into_bytes())
                } else {
                    self.lock().a_hz = parse_u64(payload)?;
                    None
                }
            }
            b"FB" => {
                if read {
                    Some(format!("FB{:011};", self.lock().b_hz).into_bytes())
                } else {
                    self.lock().b_hz = parse_u64(payload)?;
                    None
                }
            }
            // `MD` operates on the *displayed* (active) VFO on a real TS-590, not VFO A.
            b"MD" => {
                if read {
                    let digit = active_mode(&self.lock());
                    Some(vec![b'M', b'D', digit, b';'])
                } else {
                    let digit = *payload.first()?;
                    set_active_mode(&mut self.lock(), digit);
                    None
                }
            }
            // The DATA flag is not modeled per-VFO here; the daemon only reads it during poll.
            b"DA" => read.then(|| b"DA0;".to_vec()),
            b"IF" if read => Some(self.synth_if()),
            // VFO-select: `FR` sets the receive VFO (and collapses split); `FT` sets the
            // transmit VFO and toggles split when it differs from RX. The daemon must only ever
            // send these for an explicit VFO/split mutation, never during polling (design §8.8).
            b"FR" => {
                if read {
                    Some(vec![b'F', b'R', vfo_digit(self.lock().rx_vfo), b';'])
                } else {
                    let vfo = vfo_from_digit(*payload.first()?)?;
                    let mut s = self.lock();
                    s.rx_vfo = vfo;
                    s.tx_vfo = vfo;
                    s.split = false;
                    None
                }
            }
            b"FT" => {
                if read {
                    Some(vec![b'F', b'T', vfo_digit(self.lock().tx_vfo), b';'])
                } else {
                    let vfo = vfo_from_digit(*payload.first()?)?;
                    let mut s = self.lock();
                    s.tx_vfo = vfo;
                    s.split = s.rx_vfo != vfo;
                    None
                }
            }
            // `TX`/`TX0`/`TX1` key; `RX` unkeys. Sets are unacknowledged.
            b"TX" => {
                self.lock().ptt = true;
                None
            }
            b"RX" => {
                self.lock().ptt = false;
                None
            }
            b"ID" if read => Some(b"ID021;".to_vec()),
            b"PS" if read => Some(b"PS1;".to_vec()),
            // Unknown frames: a real radio ignores a set and may not answer an unknown query.
            // Returning no reply keeps the daemon's reply matcher from latching onto a bogus
            // frame; the daemon never polls anything outside the set above.
            _ => None,
        }
    }

    /// Synthesize a Kenwood `IF;` status answer (38 bytes) from the active VFO, in the exact
    /// field layout the native backend's `parse_if_status` decodes.
    fn synth_if(&self) -> Vec<u8> {
        let s = self.lock();
        let freq = s.active_hz();
        let tx = u8::from(s.ptt);
        let mode = active_mode(&s) - b'0';
        let rx_vfo = vfo_digit(s.rx_vfo) - b'0';
        let split = u8::from(s.split);
        format!("IF{freq:011}0000+0000000000{tx}{mode}{rx_vfo}0{split}0000;").into_bytes()
    }
}

/// The active VFO's mode digit.
fn active_mode(s: &FakeState) -> u8 {
    match s.rx_vfo {
        Vfo::A => s.a_mode,
        Vfo::B => s.b_mode,
    }
}

/// Set the active VFO's mode digit.
fn set_active_mode(s: &mut FakeState, digit: u8) {
    match s.rx_vfo {
        Vfo::A => s.a_mode = digit,
        Vfo::B => s.b_mode = digit,
    }
}

fn vfo_digit(vfo: Vfo) -> u8 {
    match vfo {
        Vfo::A => b'0',
        Vfo::B => b'1',
    }
}

fn vfo_from_digit(digit: u8) -> Option<Vfo> {
    match digit {
        b'0' => Some(Vfo::A),
        b'1' => Some(Vfo::B),
        _ => None,
    }
}

/// Split a `;`-terminated frame into its leading alphabetic verb and the remaining payload.
fn split_frame(frame: &[u8]) -> Option<(&[u8], &[u8])> {
    let body = match frame.iter().position(|&b| b == b';') {
        Some(end) => frame.get(..end)?,
        None => frame,
    };
    let verb_len = body
        .iter()
        .position(|b| !b.is_ascii_alphabetic())
        .unwrap_or(body.len());
    if verb_len == 0 {
        return None;
    }
    Some((body.get(..verb_len)?, body.get(verb_len..).unwrap_or(&[])))
}

fn parse_u64(bytes: &[u8]) -> Option<u64> {
    std::str::from_utf8(bytes).ok()?.trim().parse::<u64>().ok()
}

#[allow(clippy::expect_used, clippy::unwrap_used, clippy::indexing_slicing)]
mod tests {
    use super::*;

    use std::time::{Duration, Instant};

    use crate::backend::kenwood::ts590::Ts590Backend;
    use crate::backend::RadioBackend;
    use crate::dialect::kenwood::ts2000::Ts2000Dialect;
    use crate::dialect::{ClientDialect, FaceContext};
    use crate::permissions::FacePermissions;
    use crate::ptt::PttManager;
    use crate::radio::{link_channel, run_transport, spawn_scheduler, OpKind, Priority};
    use crate::state::StateHandle;

    #[test]
    fn fa_fb_round_trip_targets_named_vfo() {
        let fake = FakeTs590::new();
        assert_eq!(fake.handle(b"FA;").unwrap(), b"FA00014074000;");
        assert!(fake.handle(b"FA00007123000;").is_none());
        assert_eq!(fake.snapshot().a_hz, 7_123_000);
        // Setting A must not disturb B.
        assert_eq!(fake.handle(b"FB;").unwrap(), b"FB00007074000;");
    }

    #[test]
    fn if_reports_active_vfo_and_is_38_bytes() {
        let fake = FakeTs590::new()
            .on_vfo_b()
            .with_vfo_freqs(14_010_000, 7_100_000);
        let reply = fake.handle(b"IF;").expect("IF reply");
        assert_eq!(reply.len(), 38);
        assert!(reply.starts_with(b"IF00007100000"), "active VFO is B");
        // The RX-VFO digit (payload offset 28, i.e. byte 30 after the `IF` prefix) reports B.
        assert_eq!(reply[2 + 28], b'1');
    }

    #[test]
    fn md_targets_the_active_vfo() {
        let fake = FakeTs590::new().on_vfo_b();
        // Default both USB (2); a set while on B changes B, not A.
        assert!(fake.handle(b"MD3;").is_none());
        assert_eq!(fake.snapshot().b_mode, b'3');
        assert_eq!(fake.snapshot().a_mode, b'2');
        assert_eq!(fake.handle(b"MD;").unwrap(), b"MD3;");
    }

    #[test]
    fn fr_ft_set_active_vfo_and_split() {
        let fake = FakeTs590::new();
        assert!(fake.handle(b"FR1;").is_none());
        assert_eq!(fake.snapshot().rx_vfo, Vfo::B);
        assert!(!fake.snapshot().split);
        // Split: receive on B, transmit on A.
        assert!(fake.handle(b"FT0;").is_none());
        assert!(fake.snapshot().split);
        assert_eq!(fake.snapshot().tx_vfo, Vfo::A);
    }

    async fn wait_until(mut cond: impl FnMut() -> bool, timeout: Duration) {
        let start = Instant::now();
        while !cond() {
            assert!(
                start.elapsed() < timeout,
                "condition not met within {timeout:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    /// Reproduces the VFO-B "stale frequency" bug end to end, with no radio hardware.
    ///
    /// The native TS-590 backend is brought up against the [`FakeTs590`] emulator. After a
    /// front-panel switch to VFO B (announced by a single unsolicited `IF;` push frame), an
    /// HDSDR/OmniRig-style TS-2000 face must serve VFO B's *new* frequency.
    ///
    /// - On `main`, the native-push path (`route_event` → `parse_event`) extracts only the
    ///   active VFO from an `IF;` frame, so the face learns VFO B is active but keeps serving
    ///   the stale cached B frequency. **This assertion fails (red), reproducing the bug.**
    /// - PR #491 adds `record_event`, which records the full `IF;` status (frequency, mode,
    ///   PTT, split, VFO). **The assertion passes (green), confirming the fix.**
    #[tokio::test]
    #[ignore = "reproduces the pre-#491 VFO-B native-push stale-frequency bug: RED on main, \
                GREEN once PR #491 (record_event full IF status) lands. Run with --ignored."]
    async fn native_push_vfo_b_switch_updates_frequency_for_ts2000_face() {
        // Radio parked on VFO A; VFO B starts on a frequency the daemon will cache.
        let fake = FakeTs590::new().with_vfo_freqs(14_074_000, 7_074_000);
        let (panel, events) = front_panel();

        let backend = Arc::new(Ts590Backend::new());
        let arc: Arc<dyn RadioBackend> = backend.clone();
        let state = StateHandle::new();
        let (link, raw_rx) = link_channel();
        let (radio_side, server) = tokio::io::duplex(4096);
        tokio::spawn(run_transport(server, arc.clone(), state.clone(), raw_rx));
        tokio::spawn(fake.clone().serve(radio_side, events));
        let radio = spawn_scheduler(arc, link, state.clone());

        // Prime: one poll so both VFO frequencies and the active VFO are cached.
        radio
            .submit(0, Priority::Poll, OpKind::Poll)
            .await
            .expect("priming poll");

        // An HDSDR/OmniRig-style TS-2000 (foreign-dialect) face.
        let ptt = PttManager::new(Duration::from_secs(300));
        let ctx = FaceContext::new(
            1,
            FacePermissions::read_only(),
            state.clone(),
            radio.clone(),
            ptt,
            backend.capabilities(),
        );
        let dialect = Ts2000Dialect::new();

        // Sanity: while on VFO A the face serves VFO A's frequency.
        let reply = dialect.handle(b"IF;", &ctx).await;
        assert!(
            reply.starts_with(b"IF00014074000"),
            "expected active VFO A frequency, got {}",
            String::from_utf8_lossy(&reply)
        );

        // Operator turns the knob: VFO B moves to 7.125 MHz and the radio switches to VFO B,
        // announced by a single unsolicited IF push frame (AI2 auto-information).
        panel.front_panel_switch(Vfo::B, 7_125_000);

        // The active-VFO fact reaches the universal state on both main and #491 (parse_event
        // already extracts SetRxVfo); wait for it before checking the frequency the face serves.
        wait_until(|| state.snapshot().rx_vfo == Vfo::B, Duration::from_secs(2)).await;

        // The TS-2000 face must now serve VFO B's *new* frequency, not the stale cached value.
        let reply = dialect.handle(b"IF;", &ctx).await;
        assert!(
            reply.starts_with(b"IF00007125000"),
            "TS-2000 face served a stale VFO-B frequency after a front-panel switch \
             (the pre-#491 bug); got {}",
            String::from_utf8_lossy(&reply)
        );
    }
}
