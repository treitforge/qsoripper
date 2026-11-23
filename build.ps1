#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Cross-platform build and quality script for QsoRipper.

.DESCRIPTION
    Orchestrates Rust, .NET, and proto builds and quality checks.
    Mirrors the CI workflows so issues are caught locally before push.

.PARAMETER Command
    The build command to run. Default: build

.PARAMETER Configuration
    Build configuration for build and .NET validation commands. Default: Release

.EXAMPLE
    ./build.ps1              # Build all projects
    ./build.ps1 -Configuration Debug
    ./build.ps1 check        # Full CI-equivalent quality check
    ./build.ps1 check-rust   # Rust quality only
    ./build.ps1 check-dotnet # .NET quality only
#>

param(
    [Parameter(Position = 0)]
    [ValidateSet('build', 'check', 'rust', 'cw-decoder', 'dotnet', 'win32', 'cathub-probe-native', 'check-rust', 'check-dotnet', 'proto', 'help')]
    [string]$Command = 'build',

    [ValidateSet('Release', 'Debug')]
    [string]$Configuration = 'Release'
)

$ErrorActionPreference = 'Stop'

$RustManifest = Join-Path $PSScriptRoot 'src' 'rust' 'Cargo.toml'
$DotnetSolution = Join-Path $PSScriptRoot 'src' 'dotnet' 'QsoRipper.slnx'
$DotnetCliProject = Join-Path $PSScriptRoot 'src' 'dotnet' 'QsoRipper.Cli' 'QsoRipper.Cli.csproj'
$DotnetGuiProject = Join-Path $PSScriptRoot 'src' 'dotnet' 'QsoRipper.Gui' 'QsoRipper.Gui.csproj'
$DotnetCliPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-cli' | Join-Path -ChildPath $Configuration
$DotnetGuiPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-gui' | Join-Path -ChildPath $Configuration
$TuiPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-tui' | Join-Path -ChildPath $Configuration
$StressTuiPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-stress-tui' | Join-Path -ChildPath $Configuration
$RustDir = Join-Path $PSScriptRoot 'src' 'rust'
$IsReleaseBuild = $Configuration -eq 'Release'
$RustTargetProfile = if ($IsReleaseBuild) { 'release' } else { 'debug' }
$TuiBinary = if ($IsWindows) { 'qsoripper-tui.exe' } else { 'qsoripper-tui' }
$StressTuiBinary = if ($IsWindows) { 'qsoripper-stress-tui.exe' } else { 'qsoripper-stress-tui' }

function Write-Step([string]$Message) {
    Write-Host "`n=== $Message ===" -ForegroundColor Cyan
}

function Get-CppcheckInstallHint {
    if ($IsWindows) {
        return 'Install with: winget install Cppcheck.Cppcheck'
    }

    if ($IsMacOS) {
        return 'Install with: brew install cppcheck'
    }

    if ($IsLinux) {
        return 'Install with: sudo apt install cppcheck'
    }

    return 'Install from https://cppcheck.sourceforge.io/'
}

function Invoke-Build([string]$Step, [string]$Command, [string[]]$Arguments) {
    Write-Step $Step
    if (-not $script:BuildTimings) { $script:BuildTimings = [System.Collections.Generic.List[object]]::new() }
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $entry = [pscustomobject]@{ Step = $Step; Status = 'RUN'; Seconds = 0.0 }
    $code = 0
    try {
        & $Command @Arguments
        $code = $LASTEXITCODE
    } finally {
        $sw.Stop()
        $entry.Seconds = [Math]::Round($sw.Elapsed.TotalSeconds, 2)
        $script:BuildTimings.Add($entry) | Out-Null
    }
    if ($code -ne 0) {
        $entry.Status = 'FAIL'
        Write-Host "FAILED: $Step" -ForegroundColor Red
        exit $code
    }
    $entry.Status = 'OK'
}

function Measure-BuildStep([string]$Step, [scriptblock]$Body) {
    Write-Step $Step
    if (-not $script:BuildTimings) { $script:BuildTimings = [System.Collections.Generic.List[object]]::new() }
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    $entry = [pscustomobject]@{ Step = $Step; Status = 'RUN'; Seconds = 0.0 }
    $code = 0
    try {
        & $Body
        $code = $LASTEXITCODE
    } finally {
        $sw.Stop()
        $entry.Seconds = [Math]::Round($sw.Elapsed.TotalSeconds, 2)
        $script:BuildTimings.Add($entry) | Out-Null
    }
    if ($code -ne 0) {
        $entry.Status = 'FAIL'
        Write-Host "FAILED: $Step" -ForegroundColor Red
        exit $code
    }
    $entry.Status = 'OK'
}

function Format-BuildSeconds([double]$Seconds) {
    if ($Seconds -ge 60) {
        $m = [Math]::Floor($Seconds / 60)
        $s = $Seconds - ($m * 60)
        return ('{0}m{1:N1}s' -f $m, $s)
    }
    return ('{0:N2}s' -f $Seconds)
}

function Write-BuildSummary {
    if (-not $script:BuildTimings -or $script:BuildTimings.Count -eq 0) { return }
    $total = ($script:BuildTimings | Measure-Object -Property Seconds -Sum).Sum
    $maxLen = ($script:BuildTimings | ForEach-Object { $_.Step.Length } | Measure-Object -Maximum).Maximum
    $maxLen = [Math]::Max([int]$maxLen, 4)
    $bar = '=' * ($maxLen + 30)
    Write-Host ''
    Write-Host $bar -ForegroundColor Cyan
    Write-Host (' BUILD TIMING SUMMARY ({0} steps)' -f $script:BuildTimings.Count) -ForegroundColor Cyan
    Write-Host $bar -ForegroundColor Cyan
    $headerFmt = '  {0,-6} {1,-' + $maxLen + '} {2,10}  {3,6}'
    Write-Host ($headerFmt -f 'STATUS', 'STEP', 'TIME', 'SHARE') -ForegroundColor Gray
    foreach ($e in $script:BuildTimings) {
        $share = if ($total -gt 0) { ('{0,5:N1}%' -f (100.0 * $e.Seconds / $total)) } else { '    -' }
        $color = if ($e.Status -eq 'OK') { 'Green' } elseif ($e.Status -eq 'FAIL') { 'Red' } else { 'Yellow' }
        Write-Host ($headerFmt -f $e.Status, $e.Step, (Format-BuildSeconds $e.Seconds), $share) -ForegroundColor $color
    }
    Write-Host $bar -ForegroundColor Cyan
    Write-Host (('  TOTAL  {0,-' + $maxLen + '} {1,10}') -f '', (Format-BuildSeconds $total)) -ForegroundColor Cyan
    Write-Host $bar -ForegroundColor Cyan
    $logDir = Join-Path $PSScriptRoot 'artifacts'
    if (-not (Test-Path $logDir)) { New-Item -ItemType Directory -Path $logDir -Force | Out-Null }
    $logFile = Join-Path $logDir 'build-timings.json'
    $payload = [pscustomobject]@{
        timestamp     = (Get-Date).ToString('o')
        command       = $Command
        configuration = $Configuration
        total_seconds = [Math]::Round($total, 2)
        steps         = $script:BuildTimings
    }
    $payload | ConvertTo-Json -Depth 5 | Set-Content -Path $logFile -Encoding utf8
    Write-Host ('  log: {0}' -f $logFile) -ForegroundColor DarkGray
}

$Win32SourceDir = Join-Path $PSScriptRoot 'src' 'c' 'qsoripper-win32'
$Win32Source = Join-Path $Win32SourceDir 'src' 'main.c'
$Win32JsonParserSource = Join-Path $Win32SourceDir 'src' 'json_parser.c'
$Win32FfiGateSource = Join-Path $Win32SourceDir 'src' 'backend_ffi_gate.c'
$Win32ResourcesDir = Join-Path $Win32SourceDir 'resources'
$Win32ResourceScript = Join-Path $Win32ResourcesDir 'app.rc'
$Win32PublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-win32' | Join-Path -ChildPath $Configuration
$ServerPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-server' | Join-Path -ChildPath $Configuration
$DotnetEnginePublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-engine-dotnet' | Join-Path -ChildPath $Configuration
$DotnetDebugHostPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-debughost' | Join-Path -ChildPath $Configuration
$CwScopeGuiPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'cw-decoder-gui' | Join-Path -ChildPath $Configuration
$DotnetEngineProject = Join-Path $PSScriptRoot 'src' 'dotnet' 'QsoRipper.Engine.DotNet' 'QsoRipper.Engine.DotNet.csproj'
$DotnetDebugHostProject = Join-Path $PSScriptRoot 'src' 'dotnet' 'QsoRipper.DebugHost' 'QsoRipper.DebugHost.csproj'
$CwScopeGuiProject = Join-Path $PSScriptRoot 'experiments' 'cw-decoder' 'gui' 'CwDecoderGui.csproj'
$CatHubNativeProbeSourceDir = Join-Path $PSScriptRoot 'experiments' 'cathub-frequency-probe-native'
$CatHubNativeProbeBuildDir = Join-Path $PSScriptRoot 'artifacts' 'build' 'cathub-frequency-probe-native'
$ServerBinary = if ($IsWindows) { 'qsoripper-server.exe' } else { 'qsoripper-server' }
$CwDecoderRustManifest = Join-Path $PSScriptRoot 'experiments' 'cw-decoder' 'Cargo.toml'
$CwDecoderRustTargetDir = Join-Path $PSScriptRoot 'experiments' 'cw-decoder' 'target' | Join-Path -ChildPath $RustTargetProfile
$CwDecoderRustBinary = if ($IsWindows) { 'cw-decoder.exe' } else { 'cw-decoder' }
$CwDecoderEvalBinary = if ($IsWindows) { 'eval.exe' } else { 'eval' }

function Copy-PublishArtifact {
    param(
        [Parameter(Mandatory)] [string] $Path,
        [Parameter(Mandatory)] [string] $DestinationDir
    )

    $fileName = Split-Path -Path $Path -Leaf
    $destination = Join-Path $DestinationDir $fileName

    try {
        Copy-Item -Path $Path -Destination $destination -Force -ErrorAction Stop
        return
    }
    catch {
        # The destination is likely locked because a published binary is still
        # running (common with launcher.ps1 -Rebuild while the app is open). On
        # Windows a running executable or loaded DLL can be renamed but not
        # overwritten, so move the locked file aside and copy the fresh build
        # into place. The running process keeps using the renamed file; the next
        # launch picks up the new binary.
        if (-not (Test-Path -LiteralPath $destination)) {
            throw
        }

        Get-ChildItem -LiteralPath $DestinationDir -Filter '*.locked-*.old' -ErrorAction SilentlyContinue |
            ForEach-Object {
                try { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction Stop } catch { }
            }

        $stamp = [DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmssfff')
        $sidelined = "$destination.locked-$stamp.old"
        Move-Item -LiteralPath $destination -Destination $sidelined -Force -ErrorAction Stop
        Copy-Item -Path $Path -Destination $destination -Force -ErrorAction Stop
        Write-Host "  (replaced in-use file; previous binary moved aside as $(Split-Path -Leaf $sidelined))" -ForegroundColor DarkYellow
    }
}

function Test-FileLocked {
    param([Parameter(Mandatory)] [string] $Path)

    if (-not (Test-Path -LiteralPath $Path)) {
        return $false
    }

    try {
        $stream = [System.IO.File]::Open($Path, 'Open', 'ReadWrite', 'None')
        $stream.Close()
        $stream.Dispose()
        return $false
    }
    catch [System.IO.IOException] {
        return $true
    }
    catch [System.UnauthorizedAccessException] {
        return $true
    }
}

function Clear-LockedPublishArtifacts {
    param([Parameter(Mandatory)] [string] $DestinationDir)

    # Before `dotnet publish` overwrites an output directory, side-line any files
    # that are still locked by a running app (common with launcher.ps1 -Rebuild
    # while the GUI/engine is open). MSBuild's own copy retries then fails hard
    # (MSB3021/MSB3027) when a published DLL/EXE is in use. On Windows a running
    # executable or loaded DLL can be renamed but not overwritten, so renaming the
    # locked file aside frees the path for the fresh publish output. The running
    # process keeps using the renamed file; the next launch picks up the new build.
    if (-not (Test-Path -LiteralPath $DestinationDir)) {
        return
    }

    Get-ChildItem -LiteralPath $DestinationDir -Recurse -Filter '*.locked-*.old' -ErrorAction SilentlyContinue |
        ForEach-Object {
            try { Remove-Item -LiteralPath $_.FullName -Force -ErrorAction Stop } catch { }
        }

    $stamp = [DateTimeOffset]::UtcNow.ToString('yyyyMMddHHmmssfff')
    Get-ChildItem -LiteralPath $DestinationDir -Recurse -File -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -notlike '*.locked-*.old' } |
        ForEach-Object {
            if (Test-FileLocked -Path $_.FullName) {
                $sidelined = "$($_.FullName).locked-$stamp.old"
                try {
                    Move-Item -LiteralPath $_.FullName -Destination $sidelined -Force -ErrorAction Stop
                    Write-Host "  (side-lined in-use file $($_.Name); previous binary moved aside)" -ForegroundColor DarkYellow
                }
                catch { }
            }
        }
}

function Build-Rust {
    $arguments = @('build', '--manifest-path', $RustManifest)
    if ($IsReleaseBuild) {
        $arguments += '--release'
    }

    Invoke-Build "Building Rust ($Configuration)" cargo $arguments

    $tuiSrc = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile $TuiBinary
    if (Test-Path $tuiSrc) {
        Write-Step "Publishing qsoripper-tui ($Configuration)"
        $null = New-Item -ItemType Directory -Force -Path $TuiPublishDir
        Copy-PublishArtifact -Path $tuiSrc -DestinationDir $TuiPublishDir
        Write-Host "  -> $TuiPublishDir"
    }

    $stressTuiSrc = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile $StressTuiBinary
    if (Test-Path $stressTuiSrc) {
        Write-Step "Publishing qsoripper-stress-tui ($Configuration)"
        $null = New-Item -ItemType Directory -Force -Path $StressTuiPublishDir
        Copy-PublishArtifact -Path $stressTuiSrc -DestinationDir $StressTuiPublishDir
        Write-Host "  -> $StressTuiPublishDir"
    }

    $serverSrc = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile $ServerBinary
    if (Test-Path $serverSrc) {
        Write-Step "Publishing qsoripper-server ($Configuration)"
        $null = New-Item -ItemType Directory -Force -Path $ServerPublishDir
        Copy-PublishArtifact -Path $serverSrc -DestinationDir $ServerPublishDir
        Write-Host "  -> $ServerPublishDir"
    }

    # Publish qsoripper-ffi DLL and import library (Windows only)
    if ($IsWindows) {
        $ffiDll = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile 'qsoripper_ffi.dll'
        $ffiLib = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile 'qsoripper_ffi.dll.lib'
        if (Test-Path $ffiDll) {
            Write-Step "Publishing qsoripper-ffi ($Configuration)"
            $ffiPublishDir = Join-Path $PSScriptRoot 'artifacts' 'publish' | Join-Path -ChildPath 'qsoripper-ffi' | Join-Path -ChildPath $Configuration
            $null = New-Item -ItemType Directory -Force -Path $ffiPublishDir
            Copy-PublishArtifact -Path $ffiDll -DestinationDir $ffiPublishDir
            if (Test-Path $ffiLib) {
                Copy-PublishArtifact -Path $ffiLib -DestinationDir $ffiPublishDir
            }
            Write-Host "  -> $ffiPublishDir"
        }
    }
}

function Find-VcVarsAll {
    $vswherePath = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio' 'Installer' 'vswhere.exe'
    if (-not (Test-Path $vswherePath)) {
        return $null
    }

    # Try vswhere first (standard detection used by ILCompiler)
    $vsPath = & $vswherePath -latest -prerelease -products * `
        -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 `
        -property installationPath 2>$null
    if ($vsPath) {
        $vcvars = Join-Path $vsPath 'VC' 'Auxiliary' 'Build' 'vcvarsall.bat'
        if (Test-Path $vcvars) {
            return $vcvars
        }
    }

    # Fallback: scan all VS installations for vcvarsall.bat
    $allPaths = & $vswherePath -all -products * -property installationPath 2>$null
    foreach ($path in $allPaths) {
        $vcvars = Join-Path $path 'VC' 'Auxiliary' 'Build' 'vcvarsall.bat'
        if (Test-Path $vcvars) {
            return $vcvars
        }
    }

    return $null
}

function Build-Dotnet {
    # Native AOT requires the MSVC linker. ILCompiler's findvcvarsall.bat uses
    # vswhere to locate it, but some VS installations (e.g., VS 18 BuildTools)
    # may not register VC.Tools correctly. When that happens, set up the VC
    # environment manually and pass IlcUseEnvironmentalTools=true.
    $vcvarsAll = Find-VcVarsAll
    $needsVcEnv = $false
    $extraPublishArgs = @()

    if ($IsWindows -and $vcvarsAll) {
        Write-Host "  Using vcvarsall.bat for Native AOT toolchain: $vcvarsAll" -ForegroundColor Yellow
        $needsVcEnv = $true
        $extraPublishArgs = @('-p:IlcUseEnvironmentalTools=true')
    }

    $publishArgs = @(
        'publish',
        $DotnetCliProject,
        '-c',
        $Configuration,
        '--use-current-runtime',
        '-o',
        $DotnetCliPublishDir
    ) + $extraPublishArgs

    if ($needsVcEnv) {
        $arch = if ([System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture -eq 'Arm64') { 'arm64' } else { 'amd64' }
        Write-Step "Publishing QsoRipper.Cli Native AOT ($Configuration)"
        Clear-LockedPublishArtifacts -DestinationDir $DotnetCliPublishDir
        cmd /c "call `"$vcvarsAll`" $arch >nul 2>&1 && dotnet $($publishArgs -join ' ')"
        if ($LASTEXITCODE -ne 0) {
            Write-Host "FAILED: Publishing QsoRipper.Cli Native AOT ($Configuration)" -ForegroundColor Red
            exit $LASTEXITCODE
        }
    }
    else {
        Clear-LockedPublishArtifacts -DestinationDir $DotnetCliPublishDir
        Invoke-Build "Publishing QsoRipper.Cli Native AOT ($Configuration)" dotnet $publishArgs
    }

    Clear-LockedPublishArtifacts -DestinationDir $DotnetGuiPublishDir
    Invoke-Build "Publishing QsoRipper.Gui ($Configuration)" dotnet @(
        'publish',
        $DotnetGuiProject,
        '-c',
        $Configuration,
        '--use-current-runtime',
        '-o',
        $DotnetGuiPublishDir
    )

    Clear-LockedPublishArtifacts -DestinationDir $DotnetEnginePublishDir
    Invoke-Build "Publishing QsoRipper.Engine.DotNet ($Configuration)" dotnet @(
        'publish',
        $DotnetEngineProject,
        '-c',
        $Configuration,
        '--use-current-runtime',
        '-o',
        $DotnetEnginePublishDir
    )

    Clear-LockedPublishArtifacts -DestinationDir $DotnetDebugHostPublishDir
    Invoke-Build "Publishing QsoRipper.DebugHost ($Configuration)" dotnet @(
        'publish',
        $DotnetDebugHostProject,
        '-c',
        $Configuration,
        '--use-current-runtime',
        '-o',
        $DotnetDebugHostPublishDir
    )

    if (Test-Path $CwScopeGuiProject) {
        Clear-LockedPublishArtifacts -DestinationDir $CwScopeGuiPublishDir
        Invoke-Build "Publishing CwDecoderGui ($Configuration)" dotnet @(
            'publish',
            $CwScopeGuiProject,
            '-c',
            $Configuration,
            '--use-current-runtime',
            '-o',
            $CwScopeGuiPublishDir
        )
    }
}

function Build-Win32 {
    if (-not (Test-Path $Win32Source)) {
        Write-Step 'Win32 GUI'
        Write-Host 'Win32 source not found, skipping.' -ForegroundColor Yellow
        return
    }

    $vcvars = Find-VcVarsAll
    if (-not $vcvars) {
        Write-Step 'Win32 GUI'
        Write-Host 'MSVC toolchain not found, skipping Win32 build. Install the C++ Desktop workload.' -ForegroundColor Yellow
        return
    }

    # Verify FFI library is available (built by Build-Rust) — optional with dynamic loading
    $ffiLibDir = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile
    $ffiDll    = Join-Path $ffiLibDir 'qsoripper_ffi.dll'
    $ffiInclude = Join-Path $Win32SourceDir 'include'

    if (-not (Test-Path $ffiDll)) {
        Write-Host "FFI DLL not found at $ffiDll — Win32 app will run in CLI-only mode." -ForegroundColor Yellow
    }

    # cppcheck static analysis — fails the build on error-severity findings
    $cppcheckExe = Get-Command cppcheck -ErrorAction SilentlyContinue
    if ($cppcheckExe) {
        Measure-BuildStep 'Win32 static analysis (cppcheck)' {
            cppcheck --enable=warning,performance,portability `
                     --error-exitcode=1 `
                     --std=c11 `
                     --suppress=missingIncludeSystem `
                     --suppress=missingInclude `
                     --inline-suppr `
                     $Win32Source `
                     $Win32JsonParserSource `
                     $Win32FfiGateSource
        }
    }
    else {
        Write-Step 'Win32 static analysis (cppcheck, optional)'
        $installHint = Get-CppcheckInstallHint
        Write-Host "cppcheck not found; continuing without optional Win32 static analysis. $installHint" -ForegroundColor Yellow
    }

    $null = New-Item -ItemType Directory -Force -Path $Win32PublishDir
    # The MSVC linker writes qsoripper-win32.exe directly into the publish dir and
    # fails with LNK1104 if a previously built instance is still running (common
    # with launcher.ps1 -Rebuild). Side-line any in-use outputs first so the link
    # can create a fresh exe; the running process keeps its renamed handle.
    Clear-LockedPublishArtifacts -DestinationDir $Win32PublishDir
    $optFlags = if ($IsReleaseBuild) { '/O2' } else { '/Od /Zi' }
    $exe = Join-Path $Win32PublishDir 'qsoripper-win32.exe'

    $arch = if ([System.Runtime.InteropServices.RuntimeInformation]::ProcessArchitecture -eq 'Arm64') { 'arm64' } else { 'amd64' }
    $win32Res = Join-Path $Win32PublishDir 'app.res'
    $buildScript = Join-Path $Win32PublishDir '_build.cmd'
    @"
@echo off
call "$vcvars" $arch >nul 2>&1
rc /nologo /I"$Win32ResourcesDir" /fo"$win32Res" "$Win32ResourceScript"
if errorlevel 1 exit /b %errorlevel%
cl /W4 /WX /analyze $optFlags /DUNICODE /D_UNICODE /I"$ffiInclude" /I"$Win32ResourcesDir" "$Win32Source" "$Win32JsonParserSource" "$Win32FfiGateSource" /Fe:"$exe" /link "$win32Res" user32.lib gdi32.lib shell32.lib comctl32.lib
"@ | Set-Content -LiteralPath $buildScript -Encoding ASCII

    Measure-BuildStep "Building qsoripper-win32 ($Configuration)" {
        Push-Location $Win32PublishDir
        try {
            cmd /c $buildScript
        }
        finally {
            Pop-Location
        }
    }

    # Copy FFI DLL alongside the win32 executable
    if (Test-Path $ffiDll) {
        Copy-PublishArtifact -Path $ffiDll -DestinationDir $Win32PublishDir
    }

    # Clean intermediate files
    Remove-Item (Join-Path $Win32PublishDir '*.obj') -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $Win32PublishDir '*.pft') -Force -ErrorAction SilentlyContinue
    Remove-Item (Join-Path $Win32PublishDir '*.res') -Force -ErrorAction SilentlyContinue
    Remove-Item $buildScript -Force -ErrorAction SilentlyContinue

    Write-Host "  -> $Win32PublishDir"
}

function Build-CwDecoderRust {
    # The CW Scope GUI (CwDecoderGui) shells out to experiments/cw-decoder's
    # Rust binaries (cw-decoder.exe and eval.exe). Those live in a separate
    # cargo workspace from src/rust/ and are NOT touched by Build-Rust, so
    # without this step runall.ps1 would happily relaunch the GUI on top of
    # a stale cw-decoder.exe and silently miss source changes (see e.g. the
    # default pitch-sweep range fix). Build them explicitly here so that
    # build.ps1 is the single source of truth for "what is on disk".
    if (-not (Test-Path -LiteralPath $CwDecoderRustManifest)) {
        Write-Step "Skipping CW decoder Rust build (manifest not found at $CwDecoderRustManifest)"
        return
    }

    $arguments = @('build', '--manifest-path', $CwDecoderRustManifest, '--bins')
    if ($IsReleaseBuild) {
        $arguments += '--release'
    }

    Invoke-Build "Building CW decoder Rust binaries ($Configuration)" cargo $arguments

    foreach ($binaryName in @($CwDecoderRustBinary, $CwDecoderEvalBinary)) {
        $binaryPath = Join-Path $CwDecoderRustTargetDir $binaryName
        if (Test-Path -LiteralPath $binaryPath) {
            Write-Host "  -> $binaryPath"
        }
        else {
            Write-Host "  Warning: expected $binaryName at $binaryPath but it was not found." -ForegroundColor Yellow
        }
    }
}

function Build-CatHubNativeProbe {
    if (-not $IsWindows) {
        Write-Step 'CatHub native frequency probe'
        Write-Host 'The native CatHub frequency probe is Win32-only; skipping on this platform.' -ForegroundColor Yellow
        return
    }

    $cmake = Get-Command cmake -ErrorAction SilentlyContinue
    if (-not $cmake) {
        Write-Step 'CatHub native frequency probe'
        Write-Host 'CMake not found, skipping native CatHub frequency probe. Install CMake and the Visual Studio C++ workload.' -ForegroundColor Yellow
        return
    }

    if (-not (Test-Path -LiteralPath (Join-Path $CatHubNativeProbeSourceDir 'CMakeLists.txt'))) {
        Write-Step 'CatHub native frequency probe'
        Write-Host "Source not found at $CatHubNativeProbeSourceDir, skipping." -ForegroundColor Yellow
        return
    }

    $selection = [pscustomobject]@{
        Generator = $null
        BuildDir  = $null
    }

    Measure-BuildStep "Configuring CatHub native frequency probe ($Configuration)" {
        $generators = @('Visual Studio 18 2026', 'Visual Studio 17 2022')
        foreach ($generator in $generators) {
            $generatorKey = ($generator.ToLowerInvariant() -replace '[^a-z0-9]+', '-').Trim('-')
            $candidateBuildDir = Join-Path $CatHubNativeProbeBuildDir $generatorKey
            Write-Host "  Trying CMake generator: $generator"
            cmake -S $CatHubNativeProbeSourceDir -B $candidateBuildDir -G $generator -A x64
            if ($LASTEXITCODE -eq 0) {
                $selection.Generator = $generator
                $selection.BuildDir = $candidateBuildDir
                break
            }
        }

        if ($null -eq $selection.Generator) {
            Write-Host 'FAILED: no supported Visual Studio CMake generator found for native CatHub frequency probe.' -ForegroundColor Red
            exit 1
        }
    }

    $catHubNativeProbeOutputDir = Join-Path $selection.BuildDir $Configuration
    Clear-LockedPublishArtifacts -DestinationDir $catHubNativeProbeOutputDir

    Invoke-Build "Building CatHub native frequency probe with $($selection.Generator) ($Configuration)" cmake @(
        '--build',
        $selection.BuildDir,
        '--config',
        $Configuration
    )

    $ffiDll = Join-Path $PSScriptRoot 'src' 'rust' 'target' $RustTargetProfile 'qsoripper_ffi.dll'
    if (Test-Path -LiteralPath $ffiDll) {
        Copy-PublishArtifact -Path $ffiDll -DestinationDir $catHubNativeProbeOutputDir
    }
    else {
        Write-Host "  Warning: qsoripper_ffi.dll not found at $ffiDll; native probe will run direct cathub reads but ENGINE SKEW will show ERR." -ForegroundColor Yellow
    }

    $exe = Join-Path $catHubNativeProbeOutputDir 'CatHubFrequencyProbeNative.exe'
    if (Test-Path -LiteralPath $exe) {
        Write-Host "  -> $exe"
    }
    else {
        Write-Host "  Warning: expected CatHubFrequencyProbeNative.exe at $exe but it was not found." -ForegroundColor Yellow
    }
}

function Build-All {
    Build-Rust
    Build-CwDecoderRust
    Build-Dotnet
    Build-Win32
}

function Check-Proto {
    $bufCmd = Get-Command buf -ErrorAction SilentlyContinue
    if (-not $bufCmd) {
        Write-Step 'buf lint'
        Write-Host 'buf not installed, skipping. Install from: https://buf.build/docs/installation' -ForegroundColor Yellow
        return
    }
    Invoke-Build 'buf lint' buf @('lint')
}

function Check-Rust {
    Invoke-Build 'Rust formatting' cargo @('fmt', '--manifest-path', $RustManifest, '--all', '--', '--check')
    Invoke-Build 'Rust clippy' cargo @('clippy', '--manifest-path', $RustManifest, '--all-targets', '--', '-D', 'warnings')

    # Coverage threshold check (matches CI's 80% minimum)
    $llvmCovCmd = Get-Command cargo-llvm-cov -ErrorAction SilentlyContinue
    if ($llvmCovCmd) {
        Invoke-Build 'Rust tests (with coverage)' cargo @(
            'llvm-cov', '--manifest-path', $RustManifest,
            '--workspace',
            '--exclude', 'qsoripper-ffi',
            '--exclude', 'qsoripper-stress',
            '--exclude', 'qsoripper-stress-tui'
        )

        Write-Step 'Rust coverage threshold'
        $summaryPath = Join-Path ([System.IO.Path]::GetTempPath()) "qsoripper-rust-cov-$PID.json"
        try {
            cargo llvm-cov report `
                --manifest-path $RustManifest `
                --ignore-filename-regex 'qsoripper-(stress|ffi)' `
                --json --summary-only `
                --output-path $summaryPath
            if ($LASTEXITCODE -ne 0) {
                Write-Host "FAILED: Generating Rust coverage report" -ForegroundColor Red
                exit $LASTEXITCODE
            }
            $summary = Get-Content $summaryPath -Raw | ConvertFrom-Json
            $lineCoverage = [double]$summary.data[0].totals.lines.percent
            $covered = $summary.data[0].totals.lines.covered
            $total = $summary.data[0].totals.lines.count
            $threshold = 80.0
            Write-Host "Rust line coverage: $([math]::Round($lineCoverage, 2))% ($covered/$total lines)  (threshold: ${threshold}%)"
            if ($lineCoverage -lt $threshold) {
                Write-Host "FAIL: Rust line coverage $([math]::Round($lineCoverage, 2))% is below the minimum threshold of ${threshold}%" -ForegroundColor Red
                exit 1
            }
            Write-Host "PASS: Rust coverage threshold met." -ForegroundColor Green
        }
        finally {
            Remove-Item $summaryPath -ErrorAction SilentlyContinue
        }
    }
    else {
        Write-Step 'Rust tests'
        Write-Host 'cargo-llvm-cov not installed, running tests without coverage. Install with: cargo install cargo-llvm-cov' -ForegroundColor Yellow
        Invoke-Build 'Rust tests (no coverage)' cargo @('test', '--manifest-path', $RustManifest)
    }

    Check-Proto

    $denyCmd = Get-Command cargo-deny -ErrorAction SilentlyContinue
    if (-not $denyCmd) {
        Write-Step 'cargo deny'
        Write-Host 'cargo-deny not installed, skipping. Install with: cargo install cargo-deny --version 0.20.2 --locked' -ForegroundColor Yellow
    }
    else {
        Write-Step 'cargo deny'
        Push-Location $RustDir
        try {
            cargo deny check
            if ($LASTEXITCODE -ne 0) {
                Write-Host "FAILED: cargo deny" -ForegroundColor Red
                exit $LASTEXITCODE
            }
        }
        finally {
            Pop-Location
        }
    }
}

function Check-Dotnet {
    Invoke-Build '.NET formatting' dotnet @('format', $DotnetSolution, '--verify-no-changes')
    Invoke-Build ".NET build ($Configuration)" dotnet @('build', $DotnetSolution, '-c', $Configuration)

    # Run tests with coverage collection (matches CI)
    $coverageDir = Join-Path $PSScriptRoot 'coverage'
    if (Test-Path $coverageDir) { Remove-Item $coverageDir -Recurse -Force }

    $runsettings = Join-Path $PSScriptRoot 'src' 'dotnet' 'CodeCoverage.runsettings'
    Invoke-Build ".NET tests with coverage ($Configuration)" dotnet @(
        'test', $DotnetSolution, '-c', $Configuration, '--no-build',
        '--collect:XPlat Code Coverage',
        "--settings:$runsettings",
        "--results-directory:$coverageDir"
    )

    # Coverage threshold check (matches CI's 50% minimum)
    Write-Step '.NET coverage threshold'
    $coberturaFiles = Get-ChildItem $coverageDir -Filter 'coverage.cobertura.xml' -Recurse -ErrorAction SilentlyContinue
    if ($coberturaFiles) {
        $totalCovered = 0
        $totalLines = 0
        foreach ($file in $coberturaFiles) {
            [xml]$cobertura = Get-Content $file.FullName
            $lineRate = [double]$cobertura.coverage.'line-rate'
            $linesValid = [int]$cobertura.coverage.'lines-valid'
            $totalCovered += [math]::Round($lineRate * $linesValid)
            $totalLines += $linesValid
        }
        $coverage = if ($totalLines -gt 0) { [math]::Round(($totalCovered / $totalLines) * 100, 1) } else { 0 }
        $threshold = 50.0
        Write-Host ".NET line coverage: ${coverage}% ($totalCovered/$totalLines lines)  (threshold: ${threshold}%)"
        if ($coverage -lt $threshold) {
            Write-Host "FAIL: .NET line coverage ${coverage}% is below the minimum threshold of ${threshold}%" -ForegroundColor Red
            exit 1
        }
        Write-Host "PASS: .NET coverage threshold met." -ForegroundColor Green
    }
    else {
        Write-Host 'WARNING: No Cobertura coverage files found. Coverage threshold not checked.' -ForegroundColor Yellow
    }

    # Vulnerable package check (matches CI)
    Write-Step 'Vulnerable package check'
    $vulnOutput = dotnet list $DotnetSolution package --vulnerable --include-transitive 2>&1 | Out-String
    Write-Host $vulnOutput
    if ($vulnOutput -match 'has the following vulnerable packages') {
        Write-Host "FAIL: Vulnerable NuGet packages detected. Review output above and update affected packages." -ForegroundColor Red
        exit 1
    }
    Write-Host "PASS: No vulnerable packages found." -ForegroundColor Green
}

function Check-All {
    Check-Rust
    Check-Dotnet
}

function Show-Help {
    Write-Host @"

QsoRipper Build Script

Usage: ./build.ps1 [command] [-Configuration Release|Debug]

Commands:
  build         Build Rust, .NET, Win32 apps, and the experiments/cw-decoder Rust binaries (default: Release)
  check         Full CI-equivalent quality check
  rust          Build Rust only (copies qsoripper-tui and qsoripper-stress-tui binaries to artifacts)
  cw-decoder    Build the experiments/cw-decoder Rust binaries (cw-decoder + eval) only
  dotnet        Publish the CLI and GUI apps only
  win32         Build the Win32 C GUI app only
  cathub-probe-native
                Build the native C++ CatHub frequency probe only
  check-rust    Rust quality: fmt, clippy, test + coverage threshold, buf lint, cargo deny
  check-dotnet  .NET quality: format, build, test + coverage threshold, vulnerable package check
  proto         Run buf lint
  help          Show this help

Examples:
  ./build.ps1                                 # Build Rust and publish the CLI and GUI apps in Release
  ./build.ps1 -Configuration Debug            # Build Rust and publish the CLI and GUI apps in Debug
  ./build.ps1 check                           # Run all quality checks before pushing
  ./build.ps1 check-dotnet -Configuration Debug

"@
}

try {
    switch ($Command) {
        'build'        { Build-All }
        'check'        { Check-All }
        'rust'         { Build-Rust }
        'cw-decoder'   { Build-CwDecoderRust }
        'dotnet'       { Build-Dotnet }
        'win32'        { Build-Win32 }
        'cathub-probe-native' { Build-CatHubNativeProbe }
        'check-rust'   { Check-Rust }
        'check-dotnet' { Check-Dotnet }
        'proto'        { Check-Proto }
        'help'         { Show-Help }
    }
} finally {
    Write-BuildSummary
}

Write-Host "`nDone." -ForegroundColor Green
