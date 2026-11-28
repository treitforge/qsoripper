#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Pester tests for build.ps1 quality gate parity with CI.
.DESCRIPTION
    Validates that local check-rust and check-dotnet functions include the
    same quality gates that CI enforces (coverage thresholds, vuln checks).
#>

$repoRoot = Split-Path -Parent $PSScriptRoot
$scriptPath = Join-Path $repoRoot 'build.ps1'
$global:BuildTestsScriptContent = Get-Content $scriptPath -Raw
$buildAndTestPath = Join-Path $repoRoot 'build-and-test.ps1'
$global:BuildTestsBuildAndTestContent = Get-Content $buildAndTestPath -Raw
$testScriptPath = Join-Path $repoRoot 'test.ps1'
$global:BuildTestsTestScriptContent = Get-Content $testScriptPath -Raw
$rustWorkflowPath = Join-Path $repoRoot '.github' 'workflows' 'rust-quality.yml'
$global:BuildTestsRustWorkflowContent = Get-Content $rustWorkflowPath -Raw
$dotnetWorkflowPath = Join-Path $repoRoot '.github' 'workflows' 'dotnet-quality.yml'
$global:BuildTestsDotnetWorkflowContent = Get-Content $dotnetWorkflowPath -Raw
$win32WorkflowPath = Join-Path $repoRoot '.github' 'workflows' 'win32-quality.yml'
$global:BuildTestsWin32WorkflowContent = Get-Content $win32WorkflowPath -Raw
$powershellWorkflowPath = Join-Path $repoRoot '.github' 'workflows' 'powershell-quality.yml'
$global:BuildTestsPowershellWorkflowContent = Get-Content $powershellWorkflowPath -Raw
$engineConformanceWorkflowPath = Join-Path $repoRoot '.github' 'workflows' 'engine-conformance.yml'
$global:BuildTestsEngineConformanceWorkflowContent = Get-Content $engineConformanceWorkflowPath -Raw
$win32MainPath = Join-Path $repoRoot 'src' 'c' 'qsoripper-win32' 'src' 'main.c'
$global:BuildTestsWin32MainContent = Get-Content $win32MainPath -Raw

# Extract function bodies for targeted checks
function Get-FunctionBody([string]$Content, [string]$FunctionName) {
    $pattern = "(?ms)function\s+$FunctionName\s*\{(.+?)^\}"
    if ($Content -match $pattern) { return $Matches[1] }
    return ''
}

$global:BuildTestsCheckRustBody = Get-FunctionBody $global:BuildTestsScriptContent 'Check-Rust'
$global:BuildTestsCheckDotnetBody = Get-FunctionBody $global:BuildTestsScriptContent 'Check-Dotnet'

function Get-CFunctionBody([string]$Content, [string]$FunctionSignature) {
    $escaped = [regex]::Escape($FunctionSignature)
    $pattern = "(?ms)$escaped\s*\{(.+?)^\}"
    if ($Content -match $pattern) { return $Matches[1] }
    return ''
}

$win32MainPath = Join-Path $repoRoot 'src' 'c' 'qsoripper-win32' 'src' 'main.c'
$global:BuildTestsWin32MainContent = Get-Content $win32MainPath -Raw
$global:BuildTestsLogQsoBody = Get-CFunctionBody $global:BuildTestsWin32MainContent 'static void LogQso(void)'

function global:Assert-BuildMatches([string]$Actual, [string]$Pattern) {
    if ($Actual -notmatch $Pattern) {
        throw "Expected text to match regex '$Pattern'."
    }
}

function global:Assert-BuildNotMatches([string]$Actual, [string]$Pattern) {
    if ($Actual -match $Pattern) {
        throw "Expected text not to match regex '$Pattern'."
    }
}

function global:Assert-BuildEquals($Actual, $Expected) {
    if ($Actual -ne $Expected) {
        throw "Expected '$Expected' but got '$Actual'."
    }
}

Describe 'Linux build and launch scripts' {
    It 'skips the Win32 cross-build when its tools are missing' -Skip:(-not $IsLinux) {
        $repo = Split-Path -Parent $PSScriptRoot
        $pwsh = (Get-Process -Id $PID).Path
        $savedPath = $env:PATH
        try {
            $env:PATH = $TestDrive
            $output = & $pwsh -NoProfile -File (Join-Path $repo 'build.ps1') win32
        }
        finally {
            $env:PATH = $savedPath
        }
        Assert-BuildEquals $LASTEXITCODE 0
        Assert-BuildMatches ($output -join "`n") 'Skipping Win32 cross-build: cmake is missing'
    }

    It 'publishes a Windows executable in the requested configuration' -Skip:(-not $IsLinux -or
        @(Get-Command cmake, make, x86_64-w64-mingw32-gcc, x86_64-w64-mingw32-windres -CommandType Application -ErrorAction SilentlyContinue).Count -lt 4) {
        $repo = Split-Path -Parent $PSScriptRoot
        $pwsh = (Get-Process -Id $PID).Path
        $sourceRoot = Join-Path $TestDrive 'src' 'c'
        $null = New-Item -ItemType Directory -Path $sourceRoot -Force
        Copy-Item (Join-Path $repo 'build.ps1') $TestDrive
        Copy-Item (Join-Path $repo 'src' 'c' 'qsoripper-win32') $sourceRoot -Recurse

        & $pwsh -NoProfile -File (Join-Path $TestDrive 'build.ps1') win32 -Configuration Debug

        Assert-BuildEquals $LASTEXITCODE 0
        $exe = Join-Path $TestDrive 'artifacts' 'publish' 'qsoripper-win32' 'Debug' 'qsoripper-win32.exe'
        $stream = [System.IO.File]::OpenRead($exe)
        try {
            Assert-BuildEquals $stream.ReadByte() 77
            Assert-BuildEquals $stream.ReadByte() 90
        }
        finally {
            $stream.Dispose()
        }
    }

    It 'selects debug artifacts and forwards arguments to the launcher' -Skip:(-not $IsLinux) {
        $repo = Split-Path -Parent $PSScriptRoot
        $pwsh = (Get-Process -Id $PID).Path
        $scripts = Join-Path $TestDrive 'scripts'
        $debug = Join-Path $TestDrive 'src' 'rust' 'target' 'debug'
        $null = New-Item -ItemType Directory -Path $scripts, $debug -Force
        Copy-Item (Join-Path $repo 'launcher.ps1') $TestDrive
        Copy-Item (Join-Path $repo 'scripts' 'Initialize-DotnetRoot.ps1') $scripts
        Copy-Item (Join-Path $repo 'scripts' 'CatHubBootstrap.ps1') $scripts
        $launcher = Join-Path $debug 'qsoripper-launcher'
        @('#!/bin/sh', 'printf ''%s\n'' "$*"') | Set-Content -LiteralPath $launcher
        & chmod +x $launcher
        Assert-BuildEquals $LASTEXITCODE 0
        $output = & $pwsh -NoProfile -File (Join-Path $TestDrive 'launcher.ps1') -Dev --help
        Assert-BuildEquals $LASTEXITCODE 0
        Assert-BuildEquals ($output -join "`n") '--debug --help'
    }

    It 'leaves the engine controls alone with NoEngines' {
        $repo = Split-Path -Parent $PSScriptRoot
        $pwsh = (Get-Process -Id $PID).Path
        $scripts = Join-Path $TestDrive 'scripts'
        $null = New-Item -ItemType Directory -Path $scripts -Force
        Copy-Item (Join-Path $repo 'runall.ps1') $TestDrive
        Copy-Item (Join-Path $repo 'scripts' 'Initialize-DotnetRoot.ps1') $scripts
        $engineStub = 'Set-Content (Join-Path $PSScriptRoot "engine-called") "Unexpected engine control"'
        Set-Content (Join-Path $TestDrive 'stop-qsoripper.ps1') $engineStub
        Set-Content (Join-Path $TestDrive 'start-qsoripper.ps1') $engineStub

        $null = & $pwsh -NoProfile -File (Join-Path $TestDrive 'runall.ps1') -SkipBuild -NoEngines -NoGui -NoDebugHost -NoCwScope

        Assert-BuildEquals $LASTEXITCODE 0
        Assert-BuildEquals (Test-Path (Join-Path $TestDrive 'engine-called')) $false
    }

    It 'does not identify a shell that mentions the Rust binary as an engine' -Skip:(-not $IsLinux) {
        $repo = Split-Path -Parent $PSScriptRoot
        $ast = [System.Management.Automation.Language.Parser]::ParseFile(
            (Join-Path $repo 'start-qsoripper.ps1'), [ref]$null, [ref]$null)
        $function = $ast.Find({
            param($node)
            $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $node.Name -eq 'Test-LinuxQsoRipperEngine'
        }, $false)
        . ([scriptblock]::Create($function.Extent.Text))
        $shell = Start-Process sh -ArgumentList '-c', "'sleep 1; : qsoripper-server'" -PassThru
        try {
            Assert-BuildEquals (Test-LinuxQsoRipperEngine ([pscustomobject]@{ EngineId = 'rust-tonic' }) $shell) $false
        }
        finally {
            $null = $shell.WaitForExit(5000)
        }
    }

    It 'finishes stopping a Rust engine when its parent has not reaped it' -Skip:(-not $IsLinux) {
        $repo = Split-Path -Parent $PSScriptRoot
        $ast = [System.Management.Automation.Language.Parser]::ParseFile(
            (Join-Path $repo 'start-qsoripper.ps1'), [ref]$null, [ref]$null)
        foreach ($definition in $ast.FindAll({
            param($node)
            $node -is [System.Management.Automation.Language.FunctionDefinitionAst] -and
                $node.Name -in @('Test-ProcessStopped', 'Stop-TrackedProcess')
        }, $false)) {
            . ([scriptblock]::Create($definition.Extent.Text))
        }

        $script:EngineProcessState = 'S'
        Mock Get-Process { [pscustomobject]@{ Id = 123 } } -ParameterFilter { $Id -eq 123 }
        Mock Get-Content { "123 (qsoripper-serve) $script:EngineProcessState 1" } -ParameterFilter {
            $LiteralPath -eq '/proc/123/stat'
        }
        Mock Stop-Process { $script:EngineProcessState = 'Z' } -ParameterFilter { $Id -eq 123 }
        Mock Start-Sleep {}
        Stop-TrackedProcess -ProcessId 123
        Stop-TrackedProcess -ProcessId 123
        Should -Invoke Stop-Process -Times 1 -Exactly
        Should -Invoke Start-Sleep -Times 1 -Exactly
    }
}

Describe 'build.ps1 Check-Rust CI parity (Bug #202)' {

    It 'runs tests with coverage via cargo-llvm-cov when available' {
        # Check-Rust must reference cargo-llvm-cov for coverage collection
        Assert-BuildMatches $global:BuildTestsCheckRustBody 'cargo-llvm-cov'
    }

    It 'checks Rust coverage against a threshold' {
        # Must reference a numeric threshold (80) for coverage validation
        Assert-BuildMatches $global:BuildTestsCheckRustBody '80'
    }

    It 'fails if Rust coverage is below threshold' {
        # Must have exit/throw logic tied to coverage check
        Assert-BuildMatches $global:BuildTestsCheckRustBody 'coverage.*threshold|threshold.*coverage|below.*threshold'
    }
}

Describe 'build.ps1 Rust coverage exclusion parity (Bug #269)' {

    It 'excludes qsoripper-ffi during local cargo llvm-cov runs' {
        Assert-BuildMatches $global:BuildTestsCheckRustBody "'--exclude', 'qsoripper-ffi'"
    }

    It 'matches CI ignore-filename-regex for stress and ffi' {
        Assert-BuildMatches $global:BuildTestsCheckRustBody "ignore-filename-regex 'qsoripper-\(stress\|ffi\)'"
    }

    It 'CI workflow still excludes qsoripper-ffi' {
        Assert-BuildMatches $global:BuildTestsRustWorkflowContent '--exclude qsoripper-ffi'
    }
}

Describe 'build.ps1 Check-Dotnet CI parity (Bug #202)' {

    It 'runs tests with coverage collection' {
        # Check-Dotnet must reference XPlat Code Coverage for coverage collection
        Assert-BuildMatches $global:BuildTestsCheckDotnetBody 'XPlat Code Coverage|Code Coverage'
    }

    It 'checks .NET coverage against a threshold' {
        # Must reference a numeric threshold (50) for coverage validation
        Assert-BuildMatches $global:BuildTestsCheckDotnetBody '50'
    }

    It 'fails if .NET coverage is below threshold' {
        Assert-BuildMatches $global:BuildTestsCheckDotnetBody 'coverage.*threshold|threshold.*coverage|below.*threshold'
    }

    It 'runs vulnerable package check' {
        # Must reference --vulnerable for package vulnerability scanning
        Assert-BuildMatches $global:BuildTestsCheckDotnetBody '--vulnerable'
    }
}

Describe '.github/workflows/dotnet-quality.yml vulnerable package gate (Bug #259)' {

    It 'fails the workflow when vulnerable packages are reported' {
        Assert-BuildMatches $global:BuildTestsDotnetWorkflowContent 'has the following vulnerable packages'
        Assert-BuildMatches $global:BuildTestsDotnetWorkflowContent 'exit 1'
    }
}

Describe 'Win32 CLI publish/discovery path contract (WIN32-BUG-2)' {

    It 'publishes CLI to artifacts\publish\qsoripper-cli\<Configuration>' {
        Assert-BuildMatches $global:BuildTestsScriptContent "'qsoripper-cli'"
    }

    It 'probes the qsoripper-cli directory from FindCliPath candidates' {
        Assert-BuildMatches $global:BuildTestsWin32MainContent 'qsoripper-cli'
        Assert-BuildNotMatches $global:BuildTestsWin32MainContent 'QsoRipper\.Cli\\\\%s\\\\(?:net10\.0\\\\)?QsoRipper\.Cli\.exe'
    }
}

Describe 'Local Visual Studio generator selection' {

    It 'prefers Visual Studio 2026 before Visual Studio 2022 for local builds' {
        Assert-BuildMatches $global:BuildTestsScriptContent "@\('Visual Studio 18 2026', 'Visual Studio 17 2022'\)"
        Assert-BuildMatches $global:BuildTestsTestScriptContent "@\('Visual Studio 18 2026', 'Visual Studio 17 2022'\)"
    }

    It 'does not pin Win32 CI to Visual Studio 2022' {
        Assert-BuildNotMatches $global:BuildTestsWin32WorkflowContent 'Visual Studio 17 2022'
    }
}

Describe 'PR test workflow coverage' {

    It 'runs Win32 CI through the shared test script' {
        Assert-BuildMatches $global:BuildTestsWin32WorkflowContent '\./test\.ps1 win32 -Configuration Release'
    }

    It 'runs Pester tests on pull requests' {
        Assert-BuildMatches $global:BuildTestsPowershellWorkflowContent 'pull_request:'
        Assert-BuildMatches $global:BuildTestsPowershellWorkflowContent '\./test\.ps1 pester'
    }

    It 'runs engine conformance on pull requests' {
        Assert-BuildMatches $global:BuildTestsEngineConformanceWorkflowContent 'pull_request:'
        Assert-BuildMatches $global:BuildTestsEngineConformanceWorkflowContent '\./tests/Run-EngineConformance\.ps1'
    }
}

Describe 'Local build-and-test CI coverage' {

    It 'runs build.ps1 check so formatting and coverage gates fail locally' {
        Assert-BuildMatches $global:BuildTestsBuildAndTestContent "build\.ps1'\) check -Configuration"
    }

    It 'runs test.ps1 all so Pester, Win32, and conformance suites fail locally' {
        Assert-BuildMatches $global:BuildTestsBuildAndTestContent "test\.ps1'\) all -Configuration"
    }
}

Describe 'Win32 LogQso shadowing regression (WIN32-BUG-1)' {

    It 'declares exactly one cmd buffer in LogQso' {
        Assert-BuildEquals ([regex]::Matches($global:BuildTestsLogQsoBody, 'char\s+cmd\s*\[\s*4096\s*\]\s*;').Count) 1
    }
}
