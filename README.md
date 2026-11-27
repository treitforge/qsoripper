# QsoRipper

High-performance ham radio logging platform built around shared gRPC/protobuf contracts, interchangeable engine hosts, and keyboard-first clients.

## Architecture

QsoRipper is a **gRPC/protobuf-first** project. The contract in `proto/` is the stable core. An engine host implements the services. A client uses them. Projects can combine engines and clients from different languages without a contract change.

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

Proto files under `proto/` are the **single source of truth** for all shared types. Tools can generate code for any client language:

- **Rust** (engine): `prost` + `tonic-build` generate structs and gRPC server stubs
- **Any client language**: standard protobuf/gRPC tooling generates client stubs (for example, `Grpc.Tools` for C#, `protoc-gen-go` for Go, `grpc-web` for browsers)
- **Schema quality**: `buf lint` and `buf breaking` enforce conventions and backward compatibility
- **Contract shape**: Protobuf 1-1-1 is the default.
  Each file has one top-level entity.
  A service file contains only the `service`.
  Each RPC has unique `XxxRequest` and `XxxResponse` envelopes.

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

The built-in engine hosts advertise detailed lookup capabilities. Thus, discovery matches the implemented surface. Both Rust and .NET implement `BatchLookup` and DXCC code lookup. DXCC prefix lookup returns `UNIMPLEMENTED`.

**Building a client or a new engine host?**
See the [Engine API Documentation](docs/api/README.md).
It gives contract, stub generation, transport, and implementation information.

### ADIF

QsoRipper uses ADIF **only at the edges** for QRZ API calls and file I/O. Internal communication uses protobuf. Engine ADIF adapters convert boundary data. The `extra_fields` map keeps unrecognized data.

## Getting Started

The repository selects Rust through `rust-toolchain.toml`.
`global.json` sets .NET SDK 10.0.100 as the minimum and permits roll-forward.

### Windows

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

The repository requires SDK `10.0.404` or a later .NET 10 SDK through `global.json`.
This minimum includes the security update for the Native AOT compiler.
To update an existing Windows installation, run:

```powershell
winget upgrade --id Microsoft.DotNet.SDK.10 --exact
```

**Node.js + npm** -- required for the repo-local Playwright tooling and for bootstrapping the local Terminalizer runtime used by terminal capture:

```
# Windows
winget install OpenJS.NodeJS.LTS

# Linux (Debian/Ubuntu)
sudo apt install nodejs npm
```

Node 22 LTS is the safest default for local UI automation work. A newer globally installed Node is fine as long as `npm` is available. `capture-tui.ps1` bootstraps its own repo-local Node 22 runtime for Terminalizer.

**PowerShell 7** -- required for the repo automation scripts under `scripts/`, including Avalonia and terminal capture:
Install the tools in PowerShell.

```powershell
winget install Rustlang.Rustup
winget install Microsoft.DotNet.SDK.10
winget install Microsoft.PowerShell
winget install Google.Protobuf
winget install --id Kitware.CMake --exact
```

Install the **Desktop development with C++** workload in Visual Studio or [Visual Studio Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/).
The Win32 tests need CMake and CTest. Use CMake 4.2 or later with Visual Studio 2026.
Open PowerShell 7. Check `protoc --version`, `cmake --version`, and `ctest --version`.
Use Developer PowerShell if you use the tools that come with Visual Studio.

Run these commands from the repository root.

```powershell
.\build.ps1
.\build.ps1 -Configuration Debug
.\build.ps1 check
.\test.ps1
.\build-and-test.ps1
```

The build uses Release by default and writes the published apps to `artifacts/publish/`.
Install Pester with `Install-Module Pester -Scope CurrentUser` to include the PowerShell tests.
Optional tools include [Buf](https://buf.build/docs/installation) for protobuf checks and Cppcheck for Win32 analysis (`winget install Cppcheck.Cppcheck`).
Create `$env:APPDATA\qsoripper` before first-run setup if you use the default logbook path.

### Ubuntu 26.04

These commands were tested on x86-64. Omit `sudo` in a container where you run as root.

```sh
sudo apt update
sudo apt install --no-install-recommends ca-certificates curl build-essential protobuf-compiler libprotobuf-dev dotnet-sdk-10.0
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain none
. "$HOME/.cargo/env"
```

From the repository root, build and test the workspaces directly.

```sh
cargo build --manifest-path src/rust/Cargo.toml
cargo test --manifest-path src/rust/Cargo.toml
dotnet build src/dotnet/QsoRipper.slnx
dotnet test src/dotnet/QsoRipper.slnx --no-build
```

Run the tests before you start the engines. The Rust FFI tests skip live checks when no engine is available.
To run the live checks, first configure a station profile on the engine.
Install [PowerShell 7](https://learn.microsoft.com/en-us/powershell/scripting/install/install-ubuntu) for the repository helpers.
Run `pwsh -File tests/Run-EngineConformance.ps1` with both engine ports free to test both implementations.

For the Avalonia desktop, install the X11 libraries and fonts.

```sh
sudo apt install --no-install-recommends libx11-6 libice6 libsm6 libfontconfig1 fonts-dejavu-core
mkdir -p "${XDG_CONFIG_HOME:-$HOME/.config}/qsoripper"
```

The setup wizard needs this directory for the default logbook path.
Use the [launch commands below](#direct-launch) for both engines and Avalonia.
In a container without a display, install `xvfb` and `xauth`. Prefix the GUI command with `xvfb-run -a`.
To build the optional CW decoder, install `pkg-config` and `libasound2-dev` before you run `pwsh -File build.ps1 cw-decoder`.

### UI inspection and automation

Install Node.js 22 or newer and npm for web captures and TUI screenshots.
On Windows, use `winget install OpenJS.NodeJS.LTS`.
On Ubuntu, use `sudo apt install --no-install-recommends nodejs npm`.
From the repository root, run `npm ci`.

Install Chromium with `npx playwright install chromium` on Windows.
On Ubuntu, use `npx playwright install --with-deps chromium` to also install the system libraries.

```
npm run ux:capture:web -- --scenario debughost-home --launch-debughost
npm run ux:drive:tui -- --action-script scripts/automation/tui-sample-smoke.json
```

To create a web baseline, run `npm run ux:diff:web -- --scenario debughost-home --launch-debughost --update-baseline`.
Remove `--update-baseline` to compare a new capture with the saved baseline.

Use `scripts/capture-avalonia.ps1` for fixture screenshots on Windows.
Windows also supports live Avalonia automation and Terminalizer recordings.
See the [UI inspection guide](docs/development/ui-inspection.md).

### Interactive launcher

On Windows, run the launcher from the repository root.

```powershell
.\launcher.ps1
```

Or invoke cargo directly:

```powershell
cd src\rust
cargo run -p qsoripper-launcher --release
```

`qsoripper-launcher` is a fast-starting ratatui application.
It lists the available engines and user interfaces.
Rust uses port 50051, and .NET uses port 50052.
Press `Space` to select an item.
Use the third column to bind each user interface to an engine.
Press `Enter` to start the selected items.

Press `S` to stop launcher-managed processes.

Selections persist below `[launcher]` in `config.toml`.
On Windows, run `.\build.ps1` first to publish the apps.
The PowerShell wrapper validates a sibling CatHub build on each start.
It builds CatHub when the sibling executable is missing or incompatible.

### Local engine launcher

```powershell
pwsh -File ./start-qsoripper.ps1 -Engine local-rust
pwsh -File ./start-qsoripper.ps1 -Engine local-dotnet
```

Built-in local profiles:

| Profile | Engine ID | Default endpoint | Default storage |
|---|---|---|---|
| `local-rust` | `rust-tonic` | `http://127.0.0.1:50051` | `sqlite` |
| `local-dotnet` | `dotnet-aspnet` | `http://127.0.0.1:50052` | `memory` |

`start-qsoripper.ps1` is the local interface for these profiles.
It imports `.env`.
It builds the selected engine when necessary.
Then it starts the engine in the background from the repository root.
It records process state and output logs below `artifacts\run\`.
`-ForceRestart` restarts only the requested profile.

Thus, both local profiles can run on their default ports.

### Direct launch

From the repository root, run each engine you want in a separate terminal.

```
cargo run --manifest-path src/rust/Cargo.toml -p qsoripper-server --bin qsoripper-server
dotnet run --project src/dotnet/QsoRipper.Engine.DotNet
```

Rust listens on port 50051. .NET listens on port 50052.
Wait for the engine to start. In another terminal, start the Avalonia GUI.

```
dotnet run --project src/dotnet/QsoRipper.Gui
```

Complete first-run setup to log QSOs. Use the **Tools** menu to switch engines.

On Ubuntu, use the same GUI project to capture a fixture screenshot.
Prefix the command with `xvfb-run -a` if there is no display.

```sh
dotnet run --project src/dotnet/QsoRipper.Gui -- --capture --capture-output artifacts/ux/current/main-window.png
```

The server can now swap storage implementations at startup:

```powershell
cd src\rust
cargo run -p qsoripper-server --bin qsoripper-server -- --storage memory
cargo run -p qsoripper-server --bin qsoripper-server -- --storage sqlite --sqlite-path .\data\qsoripper.db
cargo run -p qsoripper-server --bin qsoripper-server -- --config .\config\qsoripper.toml
```

Equivalent environment variables are also supported:

```powershell
$env:QSORIPPER_STORAGE_BACKEND = "sqlite"
$env:QSORIPPER_SQLITE_PATH = ".\data\qsoripper.db"
$env:QSORIPPER_CONFIG_PATH = ".\config\qsoripper.toml"
cargo run -p qsoripper-server --bin qsoripper-server
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

The stress host listens on `127.0.0.1:50061` by default.
It supplies a developer-only gRPC control service for stress runs.
The TUI connects to this endpoint.
It shows activity for each vector, calls per second, CPU use, and memory use.
It also keeps a limited recent-event log with representative inputs.

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

The Avalonia GUI supports repeatable desktop UX inspection.
It has a fixture-backed inspection mode and a Windows automation driver.
The driver builds the GUI below `artifacts\ux\automation-bin\`.
It starts the GUI with repeatable fixture data.
Then it performs scripted UI actions.
It saves screenshots and UI tree data below `artifacts\ux\current\`.

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
  - `*.screen.png` - rendered terminal image for the visible viewport
  - `*.screen.txt` - visible viewport text
  - `*.screen.json` - viewport metadata and lines
  - `*.ansi.txt` - serialized ANSI screen content
- Every run also writes `transcript.txt` plus `report.json`.

The `sample-tui` fixture tests menu, filter, list, and detail views.

```powershell
npm run ux:drive:tui -- --action-script scripts/automation/tui-sample-smoke.json
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

**Important:** `QSORIPPER_QRZ_XML_PASSWORD` and `QSORIPPER_QRZ_LOGBOOK_API_KEY` are **not** the same value and are **not** interchangeable. The incorrect XML password causes login failures and can cause a temporary lockout.

Engine clients can also enable current space weather through the NOAA SWPC service:

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

Engine-backed CW keying uses the non-transmitting `null` backend by default.
Save station settings below `[cw_keying]` in the shared `config.toml`.
Matching `QSORIPPER_CW_*` environment variables supply startup overrides.
Use direct `winkeyer` for one client.
Use [CatHub](https://github.com/treitforge/cathub) when QsoRipper and N1MM share one physical keyer.
CatHub supplies typed and virtual COM endpoints.

CatHub has an independent installation and release.

Normal QsoRipper logging does not require CatHub.

First, configure and probe with the transmit gate off.
Enable transmission only after `qsoripper cw status` reports the correct broker, port, and firmware.
See [CatHub integration](docs/integrations/cathub-setup.md) and [CW keying setup](docs/integrations/cw-keying.md).

`SetupService` saves shared engine settings in `config.toml`.
These settings include the log path, initial station profile, QRZ settings, and rig-control defaults.
The service applies the saved values to the running engine.
Setup wizards guide the common first-run tasks.
Settings screens use the same service for the complete shared configuration.
After setup, `StationProfileService` manages additional station profiles.

It also manages active-profile selection and limited in-memory session overrides.

The Debug Host `/engine` page supplies setup and station-profile editor forms.
Thus, local setup and profile tests do not require `grpcurl`.

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

This builds the .NET engine, GUI, CLI, and DebugHost.

### Code Coverage

CI measures coverage for the Rust engine and .NET components during each run. It uploads coverage reports as workflow artifacts.

**Thresholds:**

| Surface  | Tool             | Threshold | Measured baseline |
|----------|------------------|-----------|-------------------|
| Rust     | cargo-llvm-cov   | 80% lines | ~86% lines        |
| .NET     | Coverlet         | 8% lines  | ~10% lines        |

> **Note:** Generated protobuf and gRPC stubs reduce the .NET coverage value. These stubs have no direct unit tests. Service and model code has higher coverage. Increase the threshold when tests increase and coverage excludes generated code.

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
dotnet run --project QsoRipper.Cli -- enrich --preview
dotnet run --project QsoRipper.Cli -- enrich --apply --after 2026-08-01T00:00:00Z
```

On Windows, publish and run the Native AOT CLI.

```powershell
.\build.ps1 dotnet
.\artifacts\publish\qsoripper-cli\Release\QsoRipper.Cli.exe status
```

The CLI generates client stubs from the shared proto contracts during the build.
It includes commands for status, lookup, local logbook operations, and QSO enrichment backfill.

## Documentation

- [Documentation standard](docs/documentation-style.md)
- [Engine specification](docs/architecture/engine-specification.md)
- [Data model](docs/architecture/data-model.md)
- [API documentation](docs/api/README.md)
- [Keyboard shortcuts](docs/keyboard-shortcuts.md)
- [CatHub setup](docs/integrations/cathub-setup.md)

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
<!-- merge-queue-smoke-test 1 (2026-05-21T15:32:02.0694560-07:00) -->
