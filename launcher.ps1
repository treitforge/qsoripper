#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Runs the prebuilt qsoripper-launcher TUI for instant startup.

.DESCRIPTION
    Invokes the release binary at src\rust\target\release\qsoripper-launcher
    directly, bypassing cargo so there is no manifest-resolve overhead. If the
    binary is missing, the script builds it first with 'cargo build --release'.
    If -Rebuild is passed, the script runs the repository build so launchable
    artifacts such as the Win32 app are rebuilt too. After a successful rebuild
    it also stops any engine/app processes that are still running from a binary
    that was side-lined during the build (renamed *.locked-*.old). Those are
    stale pre-rebuild processes; leaving them running makes the launcher reuse
    outdated engines, which surfaces as the GUI showing stale config (for
    example an empty CAT Hub settings page after editing config.toml).

    Pass -Dev to use the debug binary and Debug artifacts. Additional arguments
    are forwarded to the launcher.

.EXAMPLE
    .\launcher.ps1

.EXAMPLE
    .\launcher.ps1 -Rebuild

.EXAMPLE
    pwsh -File launcher.ps1 --help
#>

[CmdletBinding()]
param(
    [switch]$Dev,
    [switch]$Rebuild,
    [Parameter(ValueFromRemainingArguments = $true)]
    [string[]]$Forward
)

$ErrorActionPreference = 'Stop'

& (Join-Path $PSScriptRoot 'scripts' 'Initialize-DotnetRoot.ps1')

$rustRoot = Join-Path $PSScriptRoot 'src\rust'
if (-not (Test-Path -LiteralPath $rustRoot)) {
    throw "Rust workspace not found at $rustRoot"
}
$catHubBootstrapPath = Join-Path $PSScriptRoot 'scripts\CatHubBootstrap.ps1'
if (-not (Test-Path -LiteralPath $catHubBootstrapPath -PathType Leaf)) {
    throw "CatHub bootstrap helper not found at $catHubBootstrapPath"
}
. $catHubBootstrapPath

function Stop-StalePublishedProcesses {
    <#
        Stops processes that are still running from a freshly rebuilt binary but
        whose process start time predates the binary's last-write time. Such a
        process is, by definition, executing outdated code from before this
        rebuild (the build side-lined the in-use exe and dropped a newer one in
        its place), so the next launch should spawn a fresh one instead of
        reusing it. Leaving them running makes the launcher reuse outdated
        engines, which surfaces as the GUI showing stale config (for example an
        empty CAT Hub settings page after editing config.toml).

        Scoped to images under the repo's artifacts\publish tree to avoid
        touching anything unrelated. Best-effort: never aborts the script.
    #>
    param([Parameter(Mandatory)][string]$PublishRoot)

    $rootFull = [System.IO.Path]::GetFullPath($PublishRoot)
    $sep = [System.IO.Path]::DirectorySeparatorChar
    if (-not $rootFull.EndsWith($sep)) { $rootFull += $sep }

    foreach ($proc in (Get-Process -ErrorAction SilentlyContinue)) {
        $path = $null
        try { $path = $proc.Path } catch { $path = $null }
        if (-not $path) { continue }
        if (-not $path.StartsWith($rootFull, [System.StringComparison]::OrdinalIgnoreCase)) { continue }

        $start = $null
        try { $start = $proc.StartTime } catch { $start = $null }
        if (-not $start) { continue }

        $binary = Get-Item -LiteralPath $path -ErrorAction SilentlyContinue
        if (-not $binary) { continue }
        if ($start -ge $binary.LastWriteTime) { continue }

        Write-Host "Stopping stale process (older than rebuilt binary): $($proc.ProcessName) (PID $($proc.Id))"
        try {
            Stop-Process -Id $proc.Id -Force -ErrorAction Stop
        } catch {
            Write-Warning "Could not stop stale process PID $($proc.Id): $($_.Exception.Message)"
        }
    }
}

$profileDir = if ($Dev) { 'debug' } else { 'release' }
$exeName = if ($IsWindows -or $env:OS -eq 'Windows_NT') { 'qsoripper-launcher.exe' } else { 'qsoripper-launcher' }
$exePath = Join-Path $rustRoot "target\$profileDir\$exeName"

if ($Rebuild) {
    $configuration = if ($Dev) { 'Debug' } else { 'Release' }
    $buildScript = Join-Path $PSScriptRoot 'build.ps1'
    & $buildScript build -Configuration $configuration
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    Stop-StalePublishedProcesses -PublishRoot (Join-Path $PSScriptRoot 'artifacts\publish')
}
elseif (-not (Test-Path -LiteralPath $exePath)) {
    $buildArgs = @('build', '-p', 'qsoripper-launcher')
    if (-not $Dev) { $buildArgs += '--release' }
    Push-Location $rustRoot
    try {
        & cargo @buildArgs
        if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    }
    finally {
        Pop-Location
    }
}

if (-not (Test-Path -LiteralPath $exePath)) {
    throw "Launcher binary not found at $exePath after build"
}

$catHubProfile = if ($Dev) { 'Debug' } else { 'Release' }
$null = Initialize-CatHubExecutable -QsoRipperRoot $PSScriptRoot -Profile $catHubProfile

$launcherArguments = @()
if ($Dev) {
    $launcherArguments += '--debug'
}
if ($Forward) {
    $launcherArguments += $Forward
}
& $exePath @launcherArguments
exit $LASTEXITCODE
