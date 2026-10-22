# QsoRipper

High-performance ham radio logging platform built around shared gRPC/protobuf contracts, interchangeable engine hosts, and keyboard-first clients.

## Architecture

QsoRipper is a **gRPC/protobuf-first** project. The stable core is the contract in `proto/`, not any single process implementation. An engine host implements those services. A client consumes them. Because both sides meet at the same protobuf/gRPC seam, engines and clients can be mixed and matched across languages without changing the contract.

```
┌─────────────────────────────────────────────┐
│ Clients                                     │
│ Rust TUI | .NET CLI/GUI/DebugHost | Web | ... │
└──────────────────┬──────────────────────────┘
                   │ gRPC + protobuf
┌──────────────────▼──────────────────────────┐
│ Shared contracts in proto/                  │
│ EngineService, SetupService, LookupService, │
│ LogbookService, StationProfileService, ...  │
└──────────────────┬──────────────────────────┘
         ┌─────────┴─────────┐
         ▼                   ▼
┌─────────────────┐  ┌────────────────────┐
│ Rust engine     │  │ .NET engine        │
│ rust-tonic      │  │ dotnet-aspnet      │
└─────────────────┘  └────────────────────┘
```

The repository currently ships two engine hosts behind the same contracts:

- **Rust engine (`rust-tonic`)** for the main engine/runtime implementation
- **.NET engine (`dotnet-aspnet`)** as a second real host proving the contract is not Rust-only

It also ships multiple clients on top of that seam: the Rust TUI plus the .NET CLI, GUI, and DebugHost. Nothing in the contract privileges a specific engine language or client stack.

### Engine and client decoupling

Any engine implementation only needs to satisfy the shared service contracts. Any client implementation only needs a gRPC client. Examples of swappable pieces:

- A **Rust** or **.NET** engine host today, with room for future Go, Java, or other implementations.
- A **terminal UI** built with ratatui, crossterm, or any TUI library in any language.
- A **native desktop GUI** using Avalonia, WPF, Win32, GTK, Qt, or similar.
- A **web UI**, **mobile app**, or **CLI tool**.
- Multiple clients can run simultaneously against the same engine instance.

No engine host or client is privileged. The protobuf/gRPC contract is the only shared interface.

### Protocol Buffers

Proto files under `proto/` are the **single source of truth** for all shared types (`QsoRecord`, `CallsignRecord`, `LookupResult`, bands, modes, etc.). Code can be generated for any consuming language -- zero hand-duplicated types:

- **Rust** (engine): `prost` + `tonic-build` generate structs and gRPC server stubs
- **Any client language**: standard protobuf/gRPC tooling generates client stubs (e.g., `Grpc.Tools` for C#, `protoc-gen-go` for Go, `grpc-web` for browsers)
- **Schema quality**: `buf lint` and `buf breaking` enforce conventions and backward compatibility
- **Contract shape**: protobuf 1-1-1 is the default — one top-level entity per file, service files that contain only the `service`, and unique `XxxRequest` / `XxxResponse` envelopes for every RPC

### gRPC Services

| Service | Purpose |
|---|---|
| **EngineService** | Engine identity, version, and capability discovery |
| **SetupService** | First-run and shared engine settings, persisted config status, bootstrap storage/station defaults |
| **StationProfileService** | Persisted station profile CRUD, active profile selection, bounded session overrides |
| **LookupService** | Callsign lookups -- unary, streaming, cached, plus optional batch/DXCC surfaces |
| **LogbookService** | QSO CRUD, QRZ logbook sync, ADIF import/export |
| **DeveloperControlService** | Developer-only runtime config inspection and mutation |
| **SpaceWeatherService** | Current NOAA SWPC snapshot reads and explicit refresh for engine clients |

The built-in engine hosts intentionally advertise only the lookup capabilities they actually implement in this first slice (`lookup-callsign`, `lookup-stream`, `lookup-cache`). That keeps engine discovery truthful while `BatchLookup` and `GetDxccEntity` remain out of scope in both hosts.

**Building a client or a new engine host?** See the [Engine API Documentation](docs/api/README.md) for the shared contract reference, stub generation guidance, transport notes, and implementation-status details.

### ADIF

ADIF (Amateur Data Interchange Format) is used **only at the edges** -- QRZ API calls and file I/O. Internal communication always uses protobuf. Engine-specific ADIF adapters convert to/from proto types at the boundary, with an `extra_fields` map for lossless round-tripping.

## Getting Started

### Prerequisites

**Rust toolchain** -- install via [rustup](https://rustup.rs/):

```
# Windows
winget install Rustlang.Rustup

# Linux (Debian/Ubuntu)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

**.NET SDK 10** -- required for the .NET workspace under `src/dotnet/`, including the developer debug workbench and engine validation CLI:

```
# Windows
winget install Microsoft.DotNet.SDK.10

# Linux (Debian/Ubuntu)
sudo apt install dotnet-sdk-10.0
```

The repository pins SDK `10.0.201` in `global.json`.

**Node.js + npm** -- required for the repo-local Playwright tooling and for bootstrapping the local Terminalizer runtime used by terminal capture:

```
# Windows
winget install OpenJS.NodeJS.LTS

# Linux (Debian/Ubuntu)
sudo apt install nodejs npm
```

Node 22 LTS is the safest default for local UI automation work. A newer globally installed Node is fine as long as `npm` is available; `capture-tui.ps1` bootstraps its own repo-local Node 22 runtime for Terminalizer.

**PowerShell 7** -- required for the repo automation scripts under `scripts/`, including Avalonia and terminal capture:

```powershell
# Windows
winget install Microsoft.PowerShell
```

**Protocol Buffers compiler** -- needed to generate gRPC code from proto files:

```
# Windows
winget install Google.Protobuf

# Linux (Debian/Ubuntu)
sudo apt install protobuf-compiler

# Linux (Fedora)
sudo dnf install protobuf-compiler
```

**C compiler** -- required for the native FFI libraries under `src/c/`. On Windows, install the "Desktop development with C++" workload in Visual Studio or the [Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/). On Linux, `gcc` or `clang` is typically already available; install with `sudo apt install build-essential` if needed. The `cc` crate finds the compiler automatically on both platforms.

**buf** (optional) -- for linting and breaking change detection on proto files:

```
# Windows
winget install Bufbuild.Buf

# Linux
# See https://buf.build/docs/installation
```

**cppcheck** (optional, recommended for Win32 work) -- enables extra static analysis for `src\c\qsoripper-win32` when you run `.\build.ps1` or configure the CMake project:

```
# Windows
winget install Cppcheck.Cppcheck

# Linux (Debian/Ubuntu)
sudo apt install cppcheck

# Linux (Fedora)
sudo dnf install cppcheck
```

If `cppcheck` is not installed, `.\build.ps1` still builds the Win32 app and skips only that extra analysis step.

### Build and Test

**Repository build script:**

```powershell
.\build.ps1
.\build.ps1 -Configuration Debug
.\build.ps1 check
```

By default, `.\build.ps1` builds the Rust workspace in **Release**, publishes the Native AOT CLI to `artifacts\publish\qsoripper-cli\Release\`, and publishes the desktop GUI to `artifacts\publish\qsoripper-gui\Release\`. Use `-Configuration Debug` to switch the Rust build and both .NET publish outputs to `Debug`.

For engine-neutral local validation, use the split checks plus the shared conformance harness:

```powershell
.\build.ps1 check-dotnet
.\build.ps1 check-rust
.\tests\Run-EngineConformance.ps1
```

The conformance harness runs the common CLI slice against both built-in engine hosts so cross-language engine behavior stays aligned at the gRPC/protobuf seam.

**Rust engine:**

```
cd src/rust
cargo build
cargo test
```

This compiles the C libraries via FFI, generates Rust types from the proto files, and builds the engine. All tests (unit + integration) run with `cargo test`.

### UI inspection and automation setup

The repo now includes three developer-facing UX inspection lanes:

- **Web** screenshots and diffs with Playwright
- **Avalonia desktop** deterministic capture plus Windows UI automation
- **Terminal** workflow capture to GIF/transcript via a repo-local Terminalizer runtime (**Windows-only** today)
- **Terminal/TUI live automation** through a repo-local PTY harness with JSON action scripts and screen snapshots

One-time setup after cloning:

```powershell
npm install
npx playwright install chromium
```

- `npm install` restores the root TypeScript and Playwright tooling used by `scripts\capture-web.ts` and `scripts\capture-web-diff.ts`.
- The same repo-local Node toolchain now also drives `scripts\drive-tui.ts`, browser-rendered terminal snapshots, and the sample terminal fixture used for TUI automation smoke coverage.
- `npx playwright install chromium` installs the browser binary used for web captures.
- `scripts\capture-tui.ps1` is currently **Windows-only**. It does **not** require a global Terminalizer install; on first run it bootstraps a repo-local Node 22 + Terminalizer runtime under `tools\terminalizer-bootstrap\` and `tools\terminalizer-runtime\`.
- `scripts\drive-avalonia.ps1` is **Windows-only** and needs an interactive desktop session because it uses Windows UI Automation APIs. It does not require WinAppDriver.

Common entry points:

```powershell
# Web capture / diff
npm run ux:capture:web -- --scenario debughost-home --launch-debughost
npm run ux:diff:web -- --scenario debughost-home --launch-debughost

# Deterministic Avalonia capture
.\scripts\capture-avalonia.ps1 -Scenario main-window

# Windows UI automation against the live Avalonia window
.\scripts\drive-avalonia.ps1 -ActionScript .\scripts\automation\avalonia-main-window-smoke.json

# Cross-platform terminal/TUI automation against the sample fixture
npm run ux:drive:tui -- --action-script .\scripts\automation\tui-sample-smoke.json

# Terminal workflow capture (Windows-only today)
.\scripts\capture-tui.ps1 -Scenario cli-help
```

Artifacts are written under `artifacts\ux\current\`, `artifacts\ux\baseline\`, and `artifacts\ux\diff\`.

For the full dependency matrix and per-lane setup notes, see `docs\development\ui-inspection.md`.

**Local engine launcher (recommended):**

```powershell
.\start-qsoripper.ps1 -Engine local-rust
.\start-qsoripper.ps1 -Engine local-dotnet
```

Built-in local profiles:

| Profile | Engine ID | Default endpoint | Default storage |
|---|---|---|---|
| `local-rust` | `rust-tonic` | `http://127.0.0.1:50051` | `sqlite` |
| `local-dotnet` | `dotnet-aspnet` | `http://127.0.0.1:50052` | `memory` |

`start-qsoripper.ps1` is the clean local abstraction for those profiles. It imports `.env`, builds the selected engine if needed, starts it in the background from the repository root, and records process state plus stdout/stderr logs under `artifacts\run\`. `-ForceRestart` only restarts the requested profile, so `local-rust` and `local-dotnet` can run side-by-side on their default ports.

**Direct engine host launch:**

Rust engine:

```
cd src/rust
cargo run -p qsoripper-server
```

.NET engine:

```
dotnet run --project src/dotnet/QsoRipper.Engine.DotNet/QsoRipper.Engine.DotNet.csproj
```

Those start the built-in engine hosts directly. The Rust host defaults to `127.0.0.1:50051`; the .NET host defaults to `127.0.0.1:50052`.

The server can now swap storage implementations at startup:

```powershell
cd src\rust
cargo run -p qsoripper-server -- --storage memory
cargo run -p qsoripper-server -- --storage sqlite --sqlite-path .\data\qsoripper.db
cargo run -p qsoripper-server -- --config .\config\qsoripper.toml
```

Equivalent environment variables are also supported:

```powershell
$env:QSORIPPER_STORAGE_BACKEND = "sqlite"
$env:QSORIPPER_SQLITE_PATH = ".\data\qsoripper.db"
$env:QSORIPPER_CONFIG_PATH = ".\config\qsoripper.toml"
cargo run -p qsoripper-server
```

When no explicit config override is provided, the server uses a persisted setup file at:

- **Windows:** `%APPDATA%\qsoripper\config.toml`
- **Linux:** `~/.config/qsoripper/config.toml` (or `XDG_CONFIG_HOME`)

If you want an engine to stay running in the background while you log QSOs from another terminal, use the repo-root helper scripts:

```powershell
.\start-qsoripper.ps1 -Engine local-rust
.\artifacts\publish\qsoripper-cli\Release\QsoRipper.Cli.exe log W1AW 20m FT8
.\stop-qsoripper.ps1
```

To stop a specific profile (or all tracked profiles):

```powershell
.\stop-qsoripper.ps1 -Engine local-rust
.\stop-qsoripper.ps1 -Engine local-dotnet
.\stop-qsoripper.ps1 -All
```

The shared client-side engine selector uses `QSORIPPER_ENGINE` (legacy `QSORIPPER_ENGINE_IMPLEMENTATION`) and `QSORIPPER_ENDPOINT`. The built-in profiles are `local-rust` and `local-dotnet`.

In the Avalonia GUI, you can switch between running local engines at runtime from **Tools → Use Rust Engine** / **Use .NET Engine**. The GUI also shows active/available engine status in the top/status chrome.

**Stress host and dashboard:**

```powershell
cd src\rust
cargo run -p qsoripper-stress
```

In a second terminal:

```powershell
cd src\rust
cargo run -p qsoripper-stress-tui
```

The stress host listens on `127.0.0.1:50061` by default and exposes a developer-only gRPC control surface for starting, stopping, and monitoring long-haul stress runs. The TUI connects to that endpoint, renders per-vector activity, shows rolling calls-per-second plus process CPU and memory, and keeps a bounded recent-event log with representative sample inputs from the active vectors.

Built-in stress profiles use a dedicated engine endpoint at `127.0.0.1:55051`. When the harness auto-starts that engine it points it at a separate stress-owned SQLite file under `artifacts\stress\storage\`. Stress runs do not reuse or mutate your normal logbook.

Each stress run writes a persistent event log under `artifacts\stress\logs\stress-run-<run-id>.log`. The dashboard shows a bounded in-memory event pane, but the file is the durable place to check overnight panic, crash, and notable internal-failure details.

When the dashboard targets a local loopback endpoint and no stress host is running yet, it auto-starts a local `qsoripper-stress` instance before entering the UI. Remote endpoints still need an already-running host.

Use `cargo run -p qsoripper-stress -- --help` and `cargo run -p qsoripper-stress-tui -- --help` for alternate endpoints. The dashboard keymap is:

| Key | Action |
|---|---|
| `s` | Start the selected profile |
| `x` | Stop the active run |
| `r` | Restart the selected profile |
| `p` | Cycle between built-in profiles |
| `tab` | Switch focus between vectors and events |
| `up` / `down` | Move the current selection |
| `esc` | Clear the current error banner |
| `q` | Quit the dashboard |

### Avalonia GUI automation

For repeatable desktop UX inspection, the Avalonia GUI now supports a fixture-backed live inspection mode plus a Windows automation driver. The driver builds the GUI into a per-run output folder under `artifacts\ux\automation-bin\`, launches it with deterministic fixture data, performs scripted UI actions, and saves screenshots plus UI tree dumps under `artifacts\ux\current\`. By default, `drive-avalonia.ps1` deletes the current run's temporary build output when the run finishes, and it also prunes leftover old build-run folders while keeping the latest 3 runs per scenario/configuration. Use `-KeepBuildOutput`, `-KeepBuildRuns`, or `-NoPruneBuildOutputs` to override that behavior.

The inspection harness supports `MainWindow`, `Settings`, and `Wizard` surfaces. Scenarios can select a surface with `inspectSurface` in the action JSON, or you can override it with `-Surface` on `drive-avalonia.ps1`.

```powershell
.\scripts\drive-avalonia.ps1 -ActionScript .\scripts\automation\avalonia-main-window-smoke.json
.\scripts\drive-avalonia.ps1 -ActionScript .\scripts\automation\avalonia-settings-smoke.json
.\scripts\drive-avalonia.ps1 -Fixture .\scripts\fixtures\ux-setup-wizard.fixture.json -ActionScript .\scripts\automation\avalonia-setup-wizard-smoke.json
```

Use `-KeepOpen` when you want the fixture-backed window to stay open after the scripted steps finish.

### Terminal / TUI automation

The repo also includes a first-class PTY-backed terminal automation lane for interactive text UIs. It complements `capture-tui.ps1`:

- `scripts\drive-tui.ts` drives a live terminal session from a JSON action script.
- It writes artifacts under `artifacts\ux\current\<scenario>\`.
- Snapshot actions save:
  - `*.screen.png` — rendered terminal image for the visible viewport
  - `*.screen.txt` — visible viewport text
  - `*.screen.json` — viewport metadata and lines
  - `*.ansi.txt` — serialized ANSI screen content
- Every run also writes `transcript.txt` plus `report.json`.

Today's built-in fixture is `sample-tui`, a deterministic menu/filter/list/details demo used to validate the harness before a production TUI exists.

```powershell
npm run ux:drive:tui -- --action-script .\scripts\automation\tui-sample-smoke.json
```

Use `scripts\capture-tui.ps1` when you specifically want a rendered GIF. Use `scripts\drive-tui.ts` when you need repeatable interactive input, screen-state assertions, and step-by-step screen artifacts with rendered PNG snapshots.

### Local engine configuration

Use `.env.example` as the local template for QRZ settings and optional local-station defaults:

```
Copy-Item .env.example .env
```

Common local overrides include:

```powershell
QSORIPPER_CONFIG_PATH=C:\Users\yourname\OneDrive\qsoripper\config.toml
QSORIPPER_SQLITE_PATH=C:\Users\yourname\OneDrive\qsoripper\qsoripper.db
```

The QRZ credentials are easy to mix up, so keep this split in mind:

| Setting | What it must contain |
|---|---|
| `QSORIPPER_QRZ_XML_USERNAME` | Your QRZ account username |
| `QSORIPPER_QRZ_XML_PASSWORD` | Your **actual QRZ account password** for the XML lookup service |
| `QSORIPPER_QRZ_LOGBOOK_API_KEY` | Your separate **QRZ Logbook API access key** from the QRZ website |

**Important:** `QSORIPPER_QRZ_XML_PASSWORD` and `QSORIPPER_QRZ_LOGBOOK_API_KEY` are **not** the same value and are **not** interchangeable. Using the logbook API key as the XML password will cause QRZ XML login failures and may trigger a temporary lockout.

Current space weather can also be enabled for engine clients through the NOAA SWPC-backed service:

```powershell
QSORIPPER_NOAA_SPACE_WEATHER_ENABLED=true
QSORIPPER_NOAA_HTTP_TIMEOUT_SECONDS=8
QSORIPPER_NOAA_REFRESH_INTERVAL_SECONDS=900
QSORIPPER_NOAA_STALE_AFTER_SECONDS=3600
```

Optional endpoint overrides are available with `QSORIPPER_NOAA_KP_INDEX_URL` and `QSORIPPER_NOAA_SOLAR_INDICES_URL` if you need to point the engine at alternate NOAA-compatible feeds during local testing.

For lockout-safe debugging, you can temporarily set:

```
QSORIPPER_QRZ_XML_CAPTURE_ONLY=true
```

In capture mode, QsoRipper builds the outgoing QRZ XML request and returns redacted request diagnostics without sending any HTTP traffic to QRZ.

You can also set `QSORIPPER_STATION_*` values in `.env` to define the active station profile that the Rust engine snapshots into newly logged QSOs.

For the bootstrap and shared engine-settings surface, `SetupService` persists the engine's log file path, initial station profile, optional QRZ XML credentials, QRZ sync settings, and shared rig-control defaults to `config.toml`, then hot-applies those persisted values to the running engine. Setup wizards can guide the common first-run subset, while settings screens can edit the broader shared engine configuration through the same service. After setup, `StationProfileService` manages additional station profiles, persisted active-profile selection, and bounded in-memory session overrides for portable or event operation. The Debug Host `/engine` page now exposes setup and station-profile editor forms for these contract surfaces, so local bootstrap/profile lifecycle testing no longer requires `grpcurl`.

### Local lookup debug workflow

For local QRZ lookup debugging, use two terminals: one for the Rust engine and one for the .NET debug workbench.

1. Copy the local template and fill in your real QRZ values:

   ```powershell
   Copy-Item .env.example .env
   notepad .env
   ```

2. Optional PowerShell profile helper for loading `.env` into the current terminal session:

   ```powershell
   # Load .env file into current terminal session
   function loadenv {
       Get-Content .env | ForEach-Object {
           if ($_ -notmatch '^\s*#' -and $_ -match '=') {
               $name, $value = $_ -split '=', 2
               Set-Item -Path "Env:$($name.Trim())" -Value $value.Trim()
           }
       }
   }

   Set-Alias -Name env -Value loadenv
   ```

   After adding that to your PowerShell profile, run `env` from the repository root whenever you want to load `.env` into the current shell.

3. Start the Rust engine in the first terminal:

   ```powershell
   Set-Location C:\path\to\qsoripper
   env
   Set-Location src\rust
   cargo run -p qsoripper-server
   ```

   The developer engine listens on `http://localhost:50051` by default.

4. Start the developer debug workbench in a second terminal:

   ```powershell
   Set-Location C:\path\to\qsoripper
   env
   Set-Location src\dotnet
   dotnet run --project QsoRipper.DebugHost
   ```

5. Open the workbench in a browser:

   ```
   http://localhost:5082/lookup-workbench
   ```

6. Exercise the lookup flow:

   - **Live lookup** calls the engine's unary lookup.
   - **Stream lookup** shows the state transition flow.
   - **Cache lookup** checks only the engine cache.

If you want to inspect request shape without touching QRZ, set `QSORIPPER_QRZ_XML_CAPTURE_ONLY=true` in `.env`, run `env` again in the engine shell, and restart `qsoripper-server`.

**.NET workspace:**

```
cd src/dotnet
dotnet build QsoRipper.slnx
dotnet test QsoRipper.slnx
```

This builds the shared .NET workspace, including the developer debug host and the CLI project used for engine validation over gRPC. To publish the Native AOT CLI from the repository root, use `.\build.ps1` or `.\build.ps1 dotnet`.

### Code Coverage

Both the Rust engine and .NET components are instrumented for coverage on every CI run. Coverage reports are uploaded as workflow artifacts.

**Thresholds:**

| Surface  | Tool             | Threshold | Measured baseline |
|----------|------------------|-----------|-------------------|
| Rust     | cargo-llvm-cov   | 80% lines | ~86% lines        |
| .NET     | Coverlet         | 8% lines  | ~10% lines        |

> **Note:** The .NET threshold is intentionally low because coverage is currently skewed by auto-generated protobuf/gRPC stubs which have no direct unit tests. The hand-written service and model code has significantly higher coverage. Ratchet the threshold up incrementally as tests are added and generated code is excluded.

**Run Rust coverage locally** (requires `llvm-tools-preview` component and `cargo-llvm-cov`):

```
rustup component add llvm-tools-preview
cargo install cargo-llvm-cov --locked
cd src/rust
cargo llvm-cov --all --open
```

To check the threshold locally:

```
cd src/rust
cargo llvm-cov --all --fail-under-lines 80
```

**Run .NET coverage locally** (requires `dotnet-reportgenerator-globaltool`):

```
dotnet tool install -g dotnet-reportgenerator-globaltool
cd src/dotnet
dotnet test QsoRipper.slnx --collect:"XPlat Code Coverage" --results-directory coverage
reportgenerator -reports:"coverage/**/coverage.cobertura.xml" -targetdir:"coverage/report" -reporttypes:"Html"
```

Then open `src/dotnet/coverage/report/index.html` in a browser.

### Developer Debug Workbench

The repository now includes a **developer-only Blazor Server debug host** under `src/dotnet/QsoRipper.DebugHost`. This is not the product logger UX. It is an internal workbench for:

- configuring and probing a local Rust engine endpoint
- inspecting generated protobuf payloads and sample data
- exercising the callsign lookup flow as live gRPC services come online
- running curated Rust/.NET validation commands from a safe allowlist

Build, test, and run it with:

```
cd src/dotnet
dotnet build QsoRipper.Debug.sln
dotnet test QsoRipper.Debug.sln
dotnet run --project QsoRipper.DebugHost
```

### Engine Validation CLI

The repository also includes a minimal **.NET 10 CLI tool** under `src/dotnet/QsoRipper.Cli` for validating connectivity to the Rust engine over gRPC.

Run it directly from source with:

```
cd src/dotnet
dotnet run --project QsoRipper.Cli -- status
dotnet run --project QsoRipper.Cli -- --endpoint http://localhost:50051 status
```

Or publish the Native AOT build and run the produced executable:

```powershell
.\build.ps1 dotnet
.\artifacts\publish\qsoripper-cli\Release\QsoRipper.Cli.exe status
```

The CLI generates client stubs from the shared proto contracts at build time and currently includes commands for status, lookup, and local logbook operations over gRPC.

## Project Structure

```
proto/                    Shared IDL (language-neutral)
  domain/                 Reusable domain messages/enums (one top-level entity per file)
  services/               Service declarations plus per-RPC envelopes/support types (one top-level entity per file)
src/
  rust/                   Rust workspace (Cargo.toml at this level)
    qsoripper-core/       Engine: storage, lookups, cache, ADIF, gRPC server
  dotnet/
    QsoRipper.slnx        Root .NET workspace solution
    QsoRipper.Cli/        Minimal CLI for engine validation over gRPC
    QsoRipper.DebugHost/  Developer-only Blazor Server debug workbench
    QsoRipper.DebugHost.Tests/  Tests for debug-host services and payload builders
  c/                      Native C libraries called by the engine via FFI
    qsoripper-dsp/        Signal processing helpers (DSP, filtering, audio)
tests/
  fixtures/               Shared test data (ADIF files, etc.)
docs/
  architecture/           Data model docs, design decisions
  integrations/           ADIF spec reference, provider notes
```

## License

MIT
