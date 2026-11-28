#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Starts the selected QsoRipper engine profile in the background.

.DESCRIPTION
    Builds the selected engine if needed, launches it as a background process,
    and records its PID and log paths under artifacts/run.
#>

param(
    [string]$Engine,
    [string]$ListenAddress,
    [string]$Storage,
    [Alias('SqlitePath')]
    [string]$PersistenceLocation,
    [string]$ConfigPath,
    [int]$StartupTimeoutSeconds = 30,
    [switch]$SkipBuild,
    [switch]$ForceRestart
)

$ErrorActionPreference = 'Stop'

$runtimeDirectory = Join-Path $PSScriptRoot 'artifacts' | Join-Path -ChildPath 'run'
$legacyStatePath = Join-Path $runtimeDirectory 'qsoripper-engine.json'
$statePath = $null
$dotenvPath = Join-Path $PSScriptRoot '.env'
$defaultPersistenceLocation = Join-Path (Join-Path '.' 'data') 'qsoripper.db'

function Write-Info([string]$Message) {
    Write-Host $Message -ForegroundColor Cyan
}

function Import-DotEnv([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        return
    }

    foreach ($line in Get-Content -LiteralPath $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed) -or $trimmed.StartsWith('#')) {
            continue
        }

        $parts = $line -split '=', 2
        if ($parts.Count -ne 2) {
            continue
        }

        $name = $parts[0].Trim()
        $value = $parts[1].Trim()
        if (
            ($value.StartsWith('"') -and $value.EndsWith('"')) -or
            ($value.StartsWith("'") -and $value.EndsWith("'"))
        ) {
            $value = $value.Substring(1, $value.Length - 2)
        }

        Set-Item -Path "Env:$name" -Value $value
    }
}

function Get-DefaultSharedConfigPath {
    if ($IsWindows) {
        if ([string]::IsNullOrWhiteSpace($env:APPDATA)) {
            throw 'APPDATA is not set; cannot resolve the default shared config path.'
        }

        return Join-Path (Join-Path $env:APPDATA 'qsoripper') 'config.toml'
    }

    if (-not [string]::IsNullOrWhiteSpace($env:XDG_CONFIG_HOME)) {
        return Join-Path (Join-Path $env:XDG_CONFIG_HOME 'qsoripper') 'config.toml'
    }

    if ([string]::IsNullOrWhiteSpace($env:HOME)) {
        throw 'HOME is not set; cannot resolve the default shared config path.'
    }

    return Join-Path (Join-Path (Join-Path $env:HOME '.config') 'qsoripper') 'config.toml'
}

function Get-StatePathForProfile([string]$ProfileId) {
    if ([string]::IsNullOrWhiteSpace($ProfileId)) {
        throw 'ProfileId is required.'
    }

    return Join-Path $runtimeDirectory "qsoripper-engine-$ProfileId.json"
}

function Get-State([string]$Path = $statePath) {
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return $null
    }

    return Get-Content -LiteralPath $Path -Raw | ConvertFrom-Json
}

function Remove-StateIfTrackedProcess([string]$Path, [int]$ProcessId) {
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return
    }

    $state = Get-State -Path $Path
    if ($null -eq $state -or [int]$state.pid -eq $ProcessId) {
        Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
    }
}

function Get-TrackedProcess([string]$Path = $statePath) {
    $state = Get-State -Path $Path
    if ($null -eq $state) {
        return $null
    }

    $process = Get-Process -Id $state.pid -ErrorAction SilentlyContinue
    if ($null -eq $process) {
        Remove-Item -LiteralPath $Path -Force -ErrorAction SilentlyContinue
        return $null
    }

    [pscustomobject]@{
        State = $state
        Process = $process
    }
}

function Get-ProbeTargets([string]$Address) {
    if ($Address -match '^\[(?<host>.+)\]:(?<port>\d+)$') {
        $probeHost = $Matches.host
        $probePort = [int]$Matches.port
    }
    elseif ($Address -match '^(?<host>[^:]+):(?<port>\d+)$') {
        $probeHost = $Matches.host
        $probePort = [int]$Matches.port
    }
    else {
        throw "Unsupported listen address format: $Address"
    }

    $probeHosts = switch ($probeHost) {
        '0.0.0.0' { @('127.0.0.1') }
        '::' { @('::1', '127.0.0.1') }
        '[::]' { @('::1', '127.0.0.1') }
        '*' { @('127.0.0.1') }
        '+' { @('127.0.0.1') }
        default { @($probeHost) }
    }

    return @(
        $probeHosts |
            Select-Object -Unique |
            ForEach-Object {
                [pscustomobject]@{
                    Host = $_
                    Port = $probePort
                }
            }
    )
}

function Format-HttpEndpoint([string]$TargetHost, [int]$Port) {
    if ($TargetHost.Contains(':') -and -not $TargetHost.StartsWith('[')) {
        return "http://[${TargetHost}]:${Port}"
    }

    return "http://${TargetHost}:${Port}"
}

function Test-TcpEndpoint([string]$TargetHost, [int]$Port) {
    $client = [System.Net.Sockets.TcpClient]::new()
    try {
        $connectTask = $client.ConnectAsync($TargetHost, $Port)
        if (-not $connectTask.Wait([TimeSpan]::FromMilliseconds(500))) {
            return $false
        }

        return $client.Connected
    }
    catch {
        return $false
    }
    finally {
        $client.Dispose()
    }
}

function Get-LogTail([string]$Path) {
    if (-not (Test-Path -LiteralPath $Path)) {
        return @()
    }

    return Get-Content -LiteralPath $Path -Tail 20
}

function Test-ProcessStopped([int]$ProcessId) {
    if ($null -eq (Get-Process -Id $ProcessId -ErrorAction SilentlyContinue)) {
        return $true
    }

    if ($IsLinux) {
        # An exited process can keep its PID until its parent reaps it.
        $status = Get-Content -LiteralPath "/proc/$ProcessId/stat" -Raw -ErrorAction SilentlyContinue
        return $status -match '(?s)^\d+ \(.*\) Z '
    }

    return $false
}

function Stop-TrackedProcess([int]$ProcessId) {
    if (Test-ProcessStopped -ProcessId $ProcessId) {
        return
    }

    Stop-Process -Id $ProcessId

    for ($attempt = 0; $attempt -lt 50; $attempt++) {
        Start-Sleep -Milliseconds 200
        if (Test-ProcessStopped -ProcessId $ProcessId) {
            return
        }
    }

    throw "Timed out waiting for process $ProcessId to stop."
}

function Get-ListeningProcessIdsForPort([int]$Port) {
    if ($Port -le 0) {
        return @()
    }

    if ($IsWindows) {
        try {
            $listeners = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction Stop
            return @(
                $listeners |
                    Select-Object -ExpandProperty OwningProcess -Unique |
                    Where-Object { $_ -gt 0 }
            )
        }
        catch {
            # Fall back to netstat parsing if Get-NetTCPConnection is unavailable.
            $matches = netstat -ano |
                Select-String -Pattern "LISTENING\s+(?<pid>\d+)\s*$" |
                ForEach-Object {
                    $line = $_.Line
                    if ($line -match ":(?<port>\d+)\s+\S+\s+LISTENING\s+(?<pid>\d+)\s*$" -and [int]$Matches.port -eq $Port) {
                        [int]$Matches.pid
                    }
                } |
                Where-Object { $_ -gt 0 } |
                Select-Object -Unique

            return @($matches)
        }
    }

    foreach ($netstatPath in @('/bin/netstat', '/usr/bin/netstat')) {
        if (-not (Test-Path -LiteralPath $netstatPath)) {
            continue
        }

        $matches = & $netstatPath -tunlp 2>$null |
            Select-String -Pattern ":(?<port>\d+)\s+.*LISTEN" |
            ForEach-Object {
                $line = $_.Line
                if ($line -match ":(?<port>\d+)\s+.*LISTEN\s+(?<pid>\d+)/" -and [int]$Matches.port -eq $Port -and [int]$Matches.pid -gt 0) {
                    [int]$Matches.pid
                }
            } |
            Where-Object { $_ -gt 0 } |
            Select-Object -Unique

        return @($matches)
    }

    return @()
}

function Get-ListeningProcessIds([object[]]$Targets) {
    $ports = $Targets |
        Select-Object -ExpandProperty Port -Unique |
        Where-Object { $_ -gt 0 }

    $processIds = [System.Collections.Generic.List[int]]::new()
    foreach ($port in $ports) {
        foreach ($processId in Get-ListeningProcessIdsForPort -Port $port) {
            if ($processId -gt 0) {
                $processIds.Add($processId)
            }
        }
    }

    return @($processIds | Select-Object -Unique)
}

function Test-LikelyQsoRipperProcess([pscustomobject]$Profile, [int]$ProcessId) {
    if ($IsLinux) {
        $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
        return $null -ne $process -and (Test-LinuxQsoRipperEngine -Profile $Profile -Process $process)
    }

    $commandLine = Get-ProcessCommandLine -ProcessId $ProcessId
    if ([string]::IsNullOrWhiteSpace($commandLine)) {
        return $false
    }

    if (
        $commandLine.Contains('qsoripper', [System.StringComparison]::OrdinalIgnoreCase) -or
        $commandLine.Contains('QsoRipper.Engine.DotNet', [System.StringComparison]::OrdinalIgnoreCase)
    ) {
        return $true
    }

    if (
        -not [string]::IsNullOrWhiteSpace($Profile.LaunchFilePath) -and
        $Profile.LaunchFilePath -notin @('cargo', 'dotnet') -and
        $commandLine.Contains($Profile.LaunchFilePath, [System.StringComparison]::OrdinalIgnoreCase)
    ) {
        return $true
    }

    foreach ($argument in $Profile.LaunchArguments) {
        if ([string]::IsNullOrWhiteSpace($argument) -or $argument -match '^\{.+\}$') {
            continue
        }

        if ($commandLine.Contains($argument, [System.StringComparison]::OrdinalIgnoreCase)) {
            return $true
        }
    }

    return $false
}

function Test-LinuxQsoRipperEngine([pscustomobject]$Profile, [System.Diagnostics.Process]$Process) {
    try {
        $executable = [System.IO.Path]::GetFileName($Process.Path)
        if ($Profile.EngineId -eq 'rust-tonic') {
            return $executable -ceq 'qsoripper-server'
        }
        if ($executable -ceq 'QsoRipper.Engine.DotNet') {
            return $true
        }
        if ($executable -cne 'dotnet') {
            return $false
        }

        # Match the assembly argument, never text embedded in a shell command.
        $arguments = [System.IO.File]::ReadAllText("/proc/$($Process.Id)/cmdline").Split("`0")
        return [System.IO.Path]::GetFileName($arguments[1]) -ceq 'QsoRipper.Engine.DotNet.dll'
    }
    catch {
        return $false
    }
}

function Get-ProcessCommandLine([int]$ProcessId) {
    if ($IsWindows) {
        return (Get-CimInstance Win32_Process -Filter "ProcessId = $ProcessId" -ErrorAction SilentlyContinue).CommandLine
    }

    $procCommandLinePath = "/proc/$ProcessId/cmdline"
    if (Test-Path -LiteralPath $procCommandLinePath) {
        $raw = [System.IO.File]::ReadAllText($procCommandLinePath)
        return ($raw -replace "`0", ' ').Trim()
    }

    foreach ($psPath in @('/bin/ps', '/usr/bin/ps')) {
        if (Test-Path -LiteralPath $psPath) {
            $commandLine = & $psPath -o command= -p $ProcessId 2>$null | Select-Object -First 1
            if (-not [string]::IsNullOrWhiteSpace($commandLine)) {
                return $commandLine.Trim()
            }
        }
    }

    return $null
}

function Get-UntrackedEngineProcesses {
    param(
        [pscustomobject]$Profile,
        [int]$TrackedProcessId = 0
    )

    if ($IsLinux) {
        return @(Get-Process -ErrorAction SilentlyContinue | Where-Object {
            $_.Id -ne $TrackedProcessId -and (Test-LinuxQsoRipperEngine -Profile $Profile -Process $_)
        } | Sort-Object Id)
    }

    $fragments = [System.Collections.Generic.List[string]]::new()
    if (-not [string]::IsNullOrWhiteSpace($Profile.LaunchFilePath) -and $Profile.LaunchFilePath -notin @('cargo', 'dotnet')) {
        $fragments.Add($Profile.LaunchFilePath)
        $fragments.Add([System.IO.Path]::GetFileName($Profile.LaunchFilePath))
    }

    foreach ($argument in $Profile.LaunchArguments) {
        if ([string]::IsNullOrWhiteSpace($argument) -or $argument -match '^\{.+\}$' -or $argument.StartsWith('-')) {
            continue
        }

        if (Test-Path -LiteralPath $argument) {
            $fragments.Add($argument)
            $fragments.Add([System.IO.Path]::GetFileName($argument))
            continue
        }

        if (
            $argument.EndsWith('.dll', [System.StringComparison]::OrdinalIgnoreCase) -or
            $argument.EndsWith('.exe', [System.StringComparison]::OrdinalIgnoreCase) -or
            $argument.Contains('qsoripper', [System.StringComparison]::OrdinalIgnoreCase)
        ) {
            $fragments.Add($argument)
        }
    }

    $uniqueFragments = @($fragments.ToArray() | Select-Object -Unique)
    if ($uniqueFragments.Count -eq 0) {
        return @()
    }

    $matches = [System.Collections.Generic.List[pscustomobject]]::new()
    if ($IsWindows) {
        $candidates = Get-CimInstance Win32_Process -ErrorAction SilentlyContinue |
            Where-Object {
                $_.ProcessId -gt 0 -and
                ($TrackedProcessId -le 0 -or $_.ProcessId -ne $TrackedProcessId) -and
                -not [string]::IsNullOrWhiteSpace($_.CommandLine)
            }

        foreach ($candidate in $candidates) {
            foreach ($fragment in $uniqueFragments) {
                if ($candidate.CommandLine.Contains($fragment, [System.StringComparison]::OrdinalIgnoreCase)) {
                    $matches.Add([pscustomobject]@{ Id = [int]$candidate.ProcessId })
                    break
                }
            }
        }
    }
    else {
        foreach ($candidate in Get-Process -ErrorAction SilentlyContinue) {
            if ($TrackedProcessId -gt 0 -and $candidate.Id -eq $TrackedProcessId) {
                continue
            }

            $commandLine = Get-ProcessCommandLine -ProcessId $candidate.Id
            if ([string]::IsNullOrWhiteSpace($commandLine)) {
                continue
            }

            foreach ($fragment in $uniqueFragments) {
                if ($commandLine.Contains($fragment, [System.StringComparison]::OrdinalIgnoreCase)) {
                    $matches.Add([pscustomobject]@{ Id = [int]$candidate.Id })
                    break
                }
            }
        }
    }

    return @($matches | Sort-Object Id -Unique)
}

function Resolve-TemplateValue([string]$Template, [hashtable]$Tokens) {
    if ([string]::IsNullOrWhiteSpace($Template)) {
        return ''
    }

    $resolved = $Template
    foreach ($token in $Tokens.GetEnumerator()) {
        $resolved = $resolved.Replace("{$($token.Key)}", [string]$token.Value)
    }

    return $resolved
}

function Resolve-TemplateList([string[]]$Templates, [hashtable]$Tokens) {
    $values = @()
    foreach ($template in $Templates) {
        $resolved = Resolve-TemplateValue -Template $template -Tokens $Tokens
        if (-not [string]::IsNullOrWhiteSpace($resolved)) {
            $values += $resolved
        }
    }

    return $values
}

function Invoke-WithTemporaryEnvironment([hashtable]$EnvironmentOverrides, [scriptblock]$Action) {
    $originalValues = @{}
    try {
        foreach ($entry in $EnvironmentOverrides.GetEnumerator()) {
            $name = $entry.Key
            $existing = [System.Environment]::GetEnvironmentVariable($name)
            $originalValues[$name] = $existing

            if ([string]::IsNullOrWhiteSpace($entry.Value)) {
                Remove-Item -Path "Env:$name" -ErrorAction SilentlyContinue
            }
            else {
                Set-Item -Path "Env:$name" -Value $entry.Value
            }
        }

        & $Action
    }
    finally {
        foreach ($entry in $originalValues.GetEnumerator()) {
            if ($null -eq $entry.Value) {
                Remove-Item -Path "Env:$($entry.Key)" -ErrorAction SilentlyContinue
            }
            else {
                Set-Item -Path "Env:$($entry.Key)" -Value $entry.Value
            }
        }
    }
}

function Get-EngineProfiles {
    $defaultConfigPath = Get-DefaultSharedConfigPath
    $rustManifestPath = Join-Path $PSScriptRoot 'src' | Join-Path -ChildPath 'rust' | Join-Path -ChildPath 'Cargo.toml'
    $dotnetProjectPath = Join-Path $PSScriptRoot 'src' | Join-Path -ChildPath 'dotnet' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet.csproj'
    $dotnetDebugDllPath = Join-Path $PSScriptRoot 'src' | Join-Path -ChildPath 'dotnet' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet' | Join-Path -ChildPath 'bin' | Join-Path -ChildPath 'Debug' | Join-Path -ChildPath 'net10.0' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet.dll'
    $dotnetReleaseDllPath = Join-Path $PSScriptRoot 'src' | Join-Path -ChildPath 'dotnet' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet' | Join-Path -ChildPath 'bin' | Join-Path -ChildPath 'Release' | Join-Path -ChildPath 'net10.0' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet.dll'
    $dotnetDllPath = if (Test-Path -LiteralPath $dotnetDebugDllPath) {
        $dotnetDebugDllPath
    }
    elseif (Test-Path -LiteralPath $dotnetReleaseDllPath) {
        $dotnetReleaseDllPath
    }
    else {
        $dotnetDebugDllPath
    }
    $binaryName = if ($IsWindows) { 'qsoripper-server.exe' } else { 'qsoripper-server' }
    $rustReleaseArtifactPath = Join-Path $PSScriptRoot 'artifacts' | Join-Path -ChildPath 'publish' | Join-Path -ChildPath 'qsoripper-server' | Join-Path -ChildPath 'Release' | Join-Path -ChildPath $binaryName
    $rustReleaseTargetPath = Join-Path $PSScriptRoot 'src' | Join-Path -ChildPath 'rust' | Join-Path -ChildPath 'target' | Join-Path -ChildPath 'release' | Join-Path -ChildPath $binaryName
    $rustDebugTargetPath = Join-Path $PSScriptRoot 'src' | Join-Path -ChildPath 'rust' | Join-Path -ChildPath 'target' | Join-Path -ChildPath 'debug' | Join-Path -ChildPath $binaryName
    # Pick the freshest available binary so -SkipBuild after PR merges doesn't
    # silently re-launch a stale debug build (which then 404s on new RPCs).
    $rustCandidates = @(@($rustReleaseArtifactPath, $rustReleaseTargetPath, $rustDebugTargetPath) | Where-Object { Test-Path -LiteralPath $_ })
    $rustBinaryPath = if ($rustCandidates.Count -gt 0) {
        ($rustCandidates | Sort-Object { (Get-Item -LiteralPath $_).LastWriteTimeUtc } -Descending | Select-Object -First 1)
    } else {
        $rustDebugTargetPath
    }

    $dotnetReleasePublishedDllPath = Join-Path $PSScriptRoot 'artifacts' | Join-Path -ChildPath 'publish' | Join-Path -ChildPath 'qsoripper-engine-dotnet' | Join-Path -ChildPath 'Release' | Join-Path -ChildPath 'QsoRipper.Engine.DotNet.dll'
    $dotnetCandidates = @(@($dotnetReleasePublishedDllPath, $dotnetReleaseDllPath, $dotnetDebugDllPath) | Where-Object { Test-Path -LiteralPath $_ })
    if ($dotnetCandidates.Count -gt 0) {
        $dotnetDllPath = ($dotnetCandidates | Sort-Object { (Get-Item -LiteralPath $_).LastWriteTimeUtc } -Descending | Select-Object -First 1)
    }

    return @(
        [pscustomobject]@{
            ProfileId = 'local-rust'
            EngineId = 'rust-tonic'
            DisplayName = 'QsoRipper Rust Engine'
            Aliases = @('local-rust', 'rust', 'rust-tonic')
            DefaultListenAddress = '127.0.0.1:50051'
            DefaultStorage = 'sqlite'
            DefaultPersistenceLocation = $defaultPersistenceLocation
            DefaultConfigPath = $defaultConfigPath
            EnvironmentTemplates = @{
                QSORIPPER_STORAGE_BACKEND = '{storageBackend}'
                QSORIPPER_SQLITE_PATH = '{persistenceLocation}'
            }
            BuildFilePath = 'cargo'
            BuildArguments = @('build', '--manifest-path', $rustManifestPath)
            LaunchFilePath = $rustBinaryPath
            LaunchArguments = @('--listen', '{listenAddress}', '--config', '{configPath}')
            SupportsStorageSession = $true
        },
        [pscustomobject]@{
            ProfileId = 'local-dotnet'
            EngineId = 'dotnet-aspnet'
            DisplayName = 'QsoRipper .NET Engine'
            Aliases = @('local-dotnet', 'dotnet', 'dotnet-aspnet', 'managed')
            DefaultListenAddress = '127.0.0.1:50052'
            DefaultStorage = 'memory'
            DefaultPersistenceLocation = $defaultPersistenceLocation
            DefaultConfigPath = $defaultConfigPath
            EnvironmentTemplates = @{
                QSORIPPER_STORAGE_BACKEND = '{storageBackend}'
                QSORIPPER_STORAGE_PATH = '{persistenceLocation}'
            }
            BuildFilePath = 'dotnet'
            BuildArguments = @('build', $dotnetProjectPath, '-c', 'Debug')
            LaunchFilePath = 'dotnet'
            LaunchArguments = @(
                $dotnetDllPath,
                '--listen',
                '{listenAddress}',
                '--config',
                '{configPath}'
            )
            SupportsStorageSession = $true
        }
    )
}

function Resolve-EngineProfile([string]$RequestedEngine, [object[]]$Profiles) {
    foreach ($profile in $Profiles) {
        if (
            $RequestedEngine -ieq $profile.ProfileId -or
            $RequestedEngine -ieq $profile.EngineId -or
            ($profile.Aliases | Where-Object { $_ -ieq $RequestedEngine })
        ) {
            return $profile
        }
    }

    $knownProfiles = $Profiles |
        ForEach-Object { @($_.ProfileId) + $_.Aliases } |
        Select-Object -Unique

    throw "Unknown engine profile '$RequestedEngine'. Known values: $($knownProfiles -join ', ')."
}

New-Item -ItemType Directory -Path $runtimeDirectory -Force | Out-Null
Import-DotEnv -Path $dotenvPath

if ([string]::IsNullOrWhiteSpace($Engine)) {
    $Engine = if ([string]::IsNullOrWhiteSpace($env:QSORIPPER_ENGINE)) {
        'rust'
    }
    else {
        $env:QSORIPPER_ENGINE
    }
}

$profiles = Get-EngineProfiles
$profile = Resolve-EngineProfile -RequestedEngine $Engine -Profiles $profiles
$statePath = Get-StatePathForProfile -ProfileId $profile.ProfileId
$stdoutPath = Join-Path $runtimeDirectory "qsoripper-$($profile.ProfileId).stdout.log"
$stderrPath = Join-Path $runtimeDirectory "qsoripper-$($profile.ProfileId).stderr.log"

if ([string]::IsNullOrWhiteSpace($ListenAddress)) {
    $ListenAddress = $profile.DefaultListenAddress
}

$resolvedConfigPath = if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $profile.DefaultConfigPath
}
else {
    $ConfigPath
}

$storageSpecified = $PSBoundParameters.ContainsKey('Storage')
$persistenceSpecified = $PSBoundParameters.ContainsKey('PersistenceLocation') -or $PSBoundParameters.ContainsKey('SqlitePath')

if (-not $storageSpecified -and $persistenceSpecified) {
    $Storage = 'sqlite'
    $storageSpecified = $true
}

if ($storageSpecified) {
    if ([string]::IsNullOrWhiteSpace($Storage)) {
        $Storage = $profile.DefaultStorage
    }

    if (-not $profile.SupportsStorageSession -and $Storage -ne $profile.DefaultStorage) {
        throw "$($profile.DisplayName) only supports its default storage backend '$($profile.DefaultStorage)' through the launcher helper."
    }

    if ($Storage -ne 'memory' -and [string]::IsNullOrWhiteSpace($PersistenceLocation)) {
        $PersistenceLocation = $profile.DefaultPersistenceLocation
    }
}
elseif (-not (Test-Path -LiteralPath $resolvedConfigPath)) {
    $Storage = $profile.DefaultStorage
    if ($Storage -ne 'memory' -and [string]::IsNullOrWhiteSpace($PersistenceLocation)) {
        $PersistenceLocation = $profile.DefaultPersistenceLocation
    }
}
else {
    $Storage = ''
    $PersistenceLocation = ''
}

if ([string]::IsNullOrWhiteSpace($ConfigPath)) {
    $ConfigPath = $resolvedConfigPath
}

$existing = Get-TrackedProcess -Path $statePath
if ($null -ne $existing) {
    if (-not $ForceRestart) {
        Write-Host "QsoRipper is already running (PID $($existing.Process.Id)) at $($existing.State.listenAddress)." -ForegroundColor Yellow
        Write-Host "Stop it first with .\stop-qsoripper.ps1 or rerun with -ForceRestart." -ForegroundColor Yellow
        exit 0
    }

    Write-Info "Stopping existing QsoRipper process $($existing.Process.Id)."
    Stop-TrackedProcess -ProcessId $existing.Process.Id
    Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
    Remove-StateIfTrackedProcess -Path $legacyStatePath -ProcessId $existing.Process.Id
}
else {
    $otherEngine = Get-TrackedProcess -Path $legacyStatePath
    if ($null -ne $otherEngine -and $otherEngine.State.engine -ine $profile.ProfileId) {
        Write-Info "Another tracked engine is running (PID $($otherEngine.Process.Id), $($otherEngine.State.engine)); it will be left running."
    }
}

if ($ForceRestart) {
    $trackedProcessId = if ($null -ne $existing) { $existing.Process.Id } else { 0 }
    Write-Info "Checking for untracked $($profile.DisplayName) processes."
    $orphans = Get-UntrackedEngineProcesses -Profile $profile -TrackedProcessId $trackedProcessId
    foreach ($orphan in $orphans) {
        Write-Info "Stopping untracked $($profile.DisplayName) process $($orphan.Id)."
        Stop-TrackedProcess -ProcessId $orphan.Id
    }

    $launchArtifactPath = if ($profile.LaunchFilePath -eq 'dotnet') {
        $profile.LaunchArguments | Select-Object -First 1
    }
    else {
        $profile.LaunchFilePath
    }

    if ($IsWindows -and $orphans -and -not [string]::IsNullOrWhiteSpace($launchArtifactPath) -and (Test-Path -LiteralPath $launchArtifactPath)) {
        for ($lockAttempt = 0; $lockAttempt -lt 20; $lockAttempt++) {
            try {
                [System.IO.File]::Open($launchArtifactPath, 'Open', 'ReadWrite', 'None').Dispose()
                break
            }
            catch {
                Start-Sleep -Milliseconds 250
            }
        }
    }
}

$probeTargets = Get-ProbeTargets -Address $ListenAddress
Write-Info "Checking endpoint availability for $ListenAddress."
$blockingProcessIds = Get-ListeningProcessIds -Targets $probeTargets |
    Where-Object { $_ -ne $PID }
if ($null -ne $existing) {
    $blockingProcessIds = $blockingProcessIds | Where-Object { $_ -ne $existing.Process.Id }
}
$blockingProcessIds = @($blockingProcessIds)

if ($ForceRestart -and @($blockingProcessIds).Count -gt 0) {
    foreach ($blockingProcessId in $blockingProcessIds) {
        if (-not (Test-LikelyQsoRipperProcess -Profile $profile -ProcessId $blockingProcessId)) {
            continue
        }

        Write-Info "Stopping endpoint owner process $blockingProcessId on $ListenAddress."
        Stop-TrackedProcess -ProcessId $blockingProcessId
    }

    $blockingProcessIds = Get-ListeningProcessIds -Targets $probeTargets |
        Where-Object { $_ -ne $PID }
    if ($null -ne $existing) {
        $blockingProcessIds = $blockingProcessIds | Where-Object { $_ -ne $existing.Process.Id }
    }
    $blockingProcessIds = @($blockingProcessIds)
}

if (@($blockingProcessIds).Count -gt 0) {
    $processLabels = $blockingProcessIds |
        ForEach-Object {
            $processId = $_
            $process = Get-Process -Id $processId -ErrorAction SilentlyContinue
            if ($null -ne $process) {
                "$processId ($($process.ProcessName))"
            }
            else {
                "$processId"
            }
        }
    $endpoints = @($probeTargets | ForEach-Object { Format-HttpEndpoint -TargetHost $_.Host -Port $_.Port }) -join ', '
    throw "Endpoint already in use: $endpoints (PID(s): $($processLabels -join ', ')). Stop the process or rerun with -ForceRestart."
}

if (-not $SkipBuild) {
    Write-Info "Building $($profile.DisplayName)."
    & $profile.BuildFilePath @($profile.BuildArguments)
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed with exit code $LASTEXITCODE."
    }
}

$tokens = @{
    configPath = $ConfigPath
    listenAddress = $ListenAddress
    persistenceLocation = if ($Storage -eq 'memory' -or [string]::IsNullOrWhiteSpace($Storage)) { '' } else { $PersistenceLocation }
    storageBackend = $Storage
}

$filePath = Resolve-TemplateValue -Template $profile.LaunchFilePath -Tokens $tokens
$argumentList = Resolve-TemplateList -Templates $profile.LaunchArguments -Tokens $tokens
$environmentOverrides = @{}
foreach ($entry in $profile.EnvironmentTemplates.GetEnumerator()) {
    $resolvedValue = Resolve-TemplateValue -Template $entry.Value -Tokens $tokens
    if (-not [string]::IsNullOrWhiteSpace($resolvedValue)) {
        $environmentOverrides[$entry.Key] = $resolvedValue
    }
}

if (-not (Test-Path -LiteralPath $filePath) -and $filePath -notin @('cargo', 'dotnet')) {
    throw "Launch target not found at $filePath."
}

if ($filePath -eq 'dotnet' -and $argumentList.Count -gt 0 -and -not (Test-Path -LiteralPath $argumentList[0])) {
    throw "Launch target not found at $($argumentList[0])."
}

Write-Info "Starting $($profile.DisplayName) on $ListenAddress."
$startProcessParameters = @{
    FilePath = $filePath
    ArgumentList = $argumentList
    WorkingDirectory = $PSScriptRoot
    RedirectStandardOutput = $stdoutPath
    RedirectStandardError = $stderrPath
    PassThru = $true
}

if ($IsWindows) {
    $startProcessParameters.WindowStyle = 'Hidden'
}

$process = $null
Invoke-WithTemporaryEnvironment -EnvironmentOverrides $environmentOverrides -Action {
    $script:process = Start-Process @startProcessParameters
}

$state = [pscustomobject]@{
    configPath = if ([string]::IsNullOrWhiteSpace($ConfigPath)) { $null } else { $ConfigPath }
    displayName = $profile.DisplayName
    engine = $profile.ProfileId
    engineId = $profile.EngineId
    listenAddress = $ListenAddress
    pid = $process.Id
    persistenceLocation = if ($Storage -eq 'memory' -or [string]::IsNullOrWhiteSpace($PersistenceLocation)) { $null } else { $PersistenceLocation }
    startedAtUtc = [DateTime]::UtcNow.ToString('O')
    stderrPath = $stderrPath
    stdoutPath = $stdoutPath
    storage = if ([string]::IsNullOrWhiteSpace($Storage)) { $null } else { $Storage }
}
$state | ConvertTo-Json | Set-Content -LiteralPath $statePath
$state | ConvertTo-Json | Set-Content -LiteralPath $legacyStatePath

$deadline = [DateTime]::UtcNow.AddSeconds($StartupTimeoutSeconds)
$startupErrorPattern = '(?i)address already in use|only one usage of each socket address'

while ([DateTime]::UtcNow -lt $deadline) {
    $runningProcess = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
    if ($null -eq $runningProcess) {
        $stderrTail = Get-LogTail -Path $stderrPath
        $stdoutTail = Get-LogTail -Path $stdoutPath
        $details = @($stderrTail + $stdoutTail) -join [Environment]::NewLine
        throw "QsoRipper exited during startup.`n$details"
    }

    foreach ($probeTarget in $probeTargets) {
        if (Test-TcpEndpoint -TargetHost $probeTarget.Host -Port $probeTarget.Port) {
            Write-Host "$($profile.DisplayName) started in the background (PID $($process.Id))." -ForegroundColor Green
            Write-Host "Endpoint: $(Format-HttpEndpoint -TargetHost $probeTarget.Host -Port $probeTarget.Port)" -ForegroundColor Green
            if ($Storage -ne 'memory' -and -not [string]::IsNullOrWhiteSpace($PersistenceLocation)) {
                Write-Host "Persistence location: $PersistenceLocation" -ForegroundColor Green
            }
            if (-not [string]::IsNullOrWhiteSpace($ConfigPath)) {
                Write-Host "Config: $ConfigPath" -ForegroundColor Green
            }
            Write-Host "Logs: $stdoutPath" -ForegroundColor Green
            exit 0
        }
    }

    $stderrTail = Get-LogTail -Path $stderrPath
    if (($stderrTail -join [Environment]::NewLine) -match $startupErrorPattern) {
        Stop-TrackedProcess -ProcessId $process.Id
        Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
        Remove-StateIfTrackedProcess -Path $legacyStatePath -ProcessId $process.Id
        throw "QsoRipper failed to start because the endpoint is already in use.`n$($stderrTail -join [Environment]::NewLine)"
    }

    Start-Sleep -Milliseconds 250
}

Stop-TrackedProcess -ProcessId $process.Id
Remove-Item -LiteralPath $statePath -Force -ErrorAction SilentlyContinue
Remove-StateIfTrackedProcess -Path $legacyStatePath -ProcessId $process.Id
throw "QsoRipper did not open any expected endpoint ($((@($probeTargets | ForEach-Object { Format-HttpEndpoint -TargetHost $_.Host -Port $_.Port })) -join ', ')) within $StartupTimeoutSeconds seconds."
