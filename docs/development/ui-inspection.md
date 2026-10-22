# UI inspection and automation setup

## What this covers

QsoRipper now has three repo-local UI inspection lanes:

- **Web** capture and visual diff with Playwright
- **Avalonia desktop** deterministic screenshot export plus Windows UI automation
- **Terminal** workflow capture to GIF/transcript with a repo-local Terminalizer runtime (Windows-only today)
- **Terminal/TUI live automation** with a PTY-backed TypeScript harness and JSON action scripts

All three write artifacts under:

- `artifacts\ux\current\`
- `artifacts\ux\baseline\`
- `artifacts\ux\diff\`

## Required developer dependencies

| Dependency | Why it is needed |
|---|---|
| .NET SDK 10 | Runs the Avalonia GUI, DebugHost, CLI, and inspection entry points |
| Node.js + npm | Restores Playwright tooling and bootstraps the local Terminalizer runtime |
| PowerShell 7 | Runs `capture-avalonia.ps1`, `drive-avalonia.ps1`, `capture-tui.ps1`, and other repo scripts |
| Playwright Chromium browser | Required by the web capture/diff scripts |
| Windows | Required for `drive-avalonia.ps1` and for the current `capture-tui.ps1` implementation |
| Windows desktop session | Required only for `drive-avalonia.ps1` because it uses Windows UI Automation |

## One-time setup

From the repo root:

```powershell
npm install
npx playwright install chromium
```

Notes:

- `npm install` restores the root TypeScript/Playwright toolchain from `package.json`.
- The same install also restores the PTY and xterm packages used by `scripts\drive-tui.ts`.
- `npx playwright install chromium` installs the browser binary used by the web capture scripts.
- You do **not** need to install Terminalizer globally. `scripts\capture-tui.ps1` is currently Windows-only and bootstraps a repo-local Node 22 + Terminalizer runtime under `tools\terminalizer-bootstrap\` and `tools\terminalizer-runtime\` on first use.
- `drive-avalonia.ps1` does **not** require WinAppDriver. It uses built-in Windows UI Automation assemblies plus native user32 calls.

## Lane-specific setup and constraints

### Web capture and diff

Entry points:

```powershell
npm run ux:capture:web -- --scenario debughost-home --launch-debughost
npm run ux:diff:web -- --scenario debughost-home --launch-debughost
```

Requirements:

- `npm install` completed successfully
- `npx playwright install chromium` completed successfully
- If you use `--launch-debughost`, the script will start the local DebugHost for you

Outputs:

- PNG in `artifacts\ux\current\`
- JSON sidecar in `artifacts\ux\current\`
- Diff PNG in `artifacts\ux\diff\` when comparing against a baseline

### Avalonia deterministic capture

Entry point:

```powershell
.\scripts\capture-avalonia.ps1 -Scenario main-window
```

Requirements:

- .NET SDK 10
- No extra desktop automation dependency

Notes:

- This path uses the app's built-in capture mode and `RenderTargetBitmap`.
- It supports fixture-backed deterministic captures and named control capture.
- It is the preferred path for static inspection of the main window or hot-path controls.

### Avalonia live automation

Entry point:

```powershell
.\scripts\drive-avalonia.ps1 -ActionScript .\scripts\automation\avalonia-main-window-smoke.json
```

Requirements:

- Windows
- Interactive desktop session
- .NET SDK 10
- PowerShell 7

Notes:

- This path launches the real Avalonia window, drives it through Windows UI Automation, and captures screenshots plus UI tree dumps.
- Use it when you need real clicks, typing, menu traversal, combo-box expansion, or modal dialog inspection.
- Build outputs are written under `artifacts\ux\automation-bin\<Configuration>\<scenario>\<runId>\`.
- The current run's build output is deleted automatically when the run finishes unless you pass `-KeepBuildOutput` or `-KeepOpen`.
- Leftover old build outputs are also pruned automatically by default; the script keeps the latest 3 runs per scenario/configuration. Use `-KeepBuildRuns` to change the retention count or `-NoPruneBuildOutputs` to disable pruning for a session.

### Terminal workflow capture

Entry point:

```powershell
.\scripts\capture-tui.ps1 -Scenario cli-help
```

Requirements:

- Windows
- PowerShell 7 (`pwsh`)
- `npm` available on the machine

Notes:

- The script is currently Windows-only.
- The script bootstraps a repo-local Node 22 + Terminalizer runtime on first use.
- No global Terminalizer install is required.
- The current terminal lane targets scripted CLI workflows honestly; it can point at a future TUI binary later without changing the artifact contract.
- Output includes a GIF, transcript, YAML recording, and JSON summary.

### Terminal / TUI live automation

Entry point:

```powershell
npm run ux:drive:tui -- --action-script .\scripts\automation\tui-sample-smoke.json
```

Requirements:

- `npm install` completed successfully
- A terminal that supports PTY execution on the current platform
- Playwright Chromium installed (`npx playwright install chromium`)

Notes:

- This lane is intended for interactive terminal flows, not GIF rendering.
- It is cross-platform in principle because it uses `node-pty` plus a repo-local TypeScript harness.
- Action scripts are JSON and conceptually mirror `drive-avalonia.ps1`: scenario in, artifacts plus `report.json` out.
- The harness can drive either a structured command or the built-in deterministic `sample-tui` fixture.
- Snapshot actions write rendered `*.screen.png` files plus `*.screen.txt`, `*.screen.json`, and `*.ansi.txt` artifacts under `artifacts\ux\current\<scenario>\`.
- Keep `scripts\capture-tui.ps1` for rendered GIF review; the two paths are complementary.

## Recommended quick verification

```powershell
npm run ux:capture:web -- --scenario debughost-home --launch-debughost
.\scripts\capture-avalonia.ps1 -Scenario main-window
npm run ux:drive:tui -- --action-script .\scripts\automation\tui-sample-smoke.json
.\scripts\capture-tui.ps1 -Scenario cli-help
```

If all four commands succeed, the local UI inspection toolchain is ready.

The Terminalizer GIF capture verification command currently requires Windows.
