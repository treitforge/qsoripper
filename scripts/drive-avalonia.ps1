[CmdletBinding()]
param(
    [string]$ActionScript = "",
    [string]$Fixture,
    [ValidateSet("MainWindow", "Settings", "Wizard")]
    [string]$Surface = "",
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Debug",
    [ValidateSet("Default", "Dark", "Light")]
    [string]$Theme = "Dark",
    [string]$OutputRoot,
    [ValidateRange(1, 100)]
    [int]$KeepBuildRuns = 3,
    [switch]$NoPruneBuildOutputs,
    [switch]$KeepBuildOutput,
    [switch]$KeepOpen
)

$ErrorActionPreference = "Stop"

if (-not $IsWindows) {
    throw "drive-avalonia.ps1 requires Windows."
}

$repoRoot = Split-Path -Parent $PSScriptRoot
$automationRoot = Join-Path $PSScriptRoot "automation"
if ([string]::IsNullOrWhiteSpace($ActionScript)) {
    $ActionScript = Join-Path $automationRoot "avalonia-main-window-smoke.json"
}

$ActionScript = (Resolve-Path -LiteralPath $ActionScript).Path
if ([string]::IsNullOrWhiteSpace($Fixture)) {
    $defaultFixture = Join-Path $PSScriptRoot "fixtures\ux-main-window.fixture.json"
    if (Test-Path -LiteralPath $defaultFixture) {
        $Fixture = $defaultFixture
    }
}

if (-not [string]::IsNullOrWhiteSpace($Fixture)) {
    $Fixture = (Resolve-Path -LiteralPath $Fixture).Path
}

$scenarioName = [System.IO.Path]::GetFileNameWithoutExtension($ActionScript)
if ([string]::IsNullOrWhiteSpace($OutputRoot)) {
    $OutputRoot = Join-Path $repoRoot ("artifacts\ux\current\" + $scenarioName)
}

$OutputRoot = [System.IO.Path]::GetFullPath($OutputRoot)
$runId = [DateTimeOffset]::UtcNow.ToString("yyyyMMddHHmmssfff")
$scenarioBuildRoot = Join-Path $repoRoot ("artifacts\ux\automation-bin\" + $Configuration + "\" + $scenarioName)
$buildOutputRoot = Join-Path $scenarioBuildRoot ($runId + "\")
New-Item -ItemType Directory -Force -Path $OutputRoot, $buildOutputRoot | Out-Null

function Prune-BuildOutputs {
    param(
        [Parameter(Mandatory)]
        [string]$ScenarioBuildRoot,
        [Parameter(Mandatory)]
        [int]$KeepRuns
    )

    if (-not (Test-Path -LiteralPath $ScenarioBuildRoot)) {
        return
    }

    $runDirectories = Get-ChildItem -LiteralPath $ScenarioBuildRoot -Directory |
        Where-Object { $_.Name -match '^\d{17}$' } |
        Sort-Object Name -Descending

    if ($runDirectories.Count -le $KeepRuns) {
        return
    }

    $pruned = 0
    foreach ($directory in ($runDirectories | Select-Object -Skip $KeepRuns)) {
        Remove-Item -LiteralPath $directory.FullName -Recurse -Force
        $pruned++
    }

    if ($pruned -gt 0) {
        Write-Host "Pruned $pruned old automation build run(s) from $ScenarioBuildRoot"
    }
}

if (-not $NoPruneBuildOutputs) {
    Prune-BuildOutputs -ScenarioBuildRoot $scenarioBuildRoot -KeepRuns $KeepBuildRuns
}

$actionSpec = Get-Content -LiteralPath $ActionScript -Raw | ConvertFrom-Json -Depth 20
$inspectSurface = if (-not [string]::IsNullOrWhiteSpace($Surface)) {
    $Surface
}
elseif ($null -ne $actionSpec.PSObject.Properties["inspectSurface"] -and -not [string]::IsNullOrWhiteSpace($actionSpec.inspectSurface)) {
    [string]$actionSpec.inspectSurface
}
else {
    "MainWindow"
}

$mainWindowTitle = switch ($inspectSurface) {
    "Settings" { "Settings" }
    default { "QsoRipper" }
}

if ($null -ne $actionSpec.window -and -not [string]::IsNullOrWhiteSpace($actionSpec.window.title)) {
    $mainWindowTitle = [string]$actionSpec.window.title
}

Add-Type -AssemblyName UIAutomationClient
Add-Type -AssemblyName UIAutomationTypes
Add-Type -AssemblyName System.Drawing.Common
Add-Type -AssemblyName System.Windows.Forms
Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class NativeUi
{
    [StructLayout(LayoutKind.Sequential)]
    public struct RECT
    {
        public int Left;
        public int Top;
        public int Right;
        public int Bottom;
    }

    [DllImport("user32.dll")]
    public static extern bool SetForegroundWindow(IntPtr hWnd);

    [DllImport("user32.dll")]
    public static extern bool SetWindowPos(IntPtr hWnd, IntPtr hWndInsertAfter, int X, int Y, int cx, int cy, uint uFlags);

    [DllImport("user32.dll")]
    public static extern bool GetWindowRect(IntPtr hWnd, out RECT lpRect);

    [DllImport("user32.dll")]
    public static extern bool SetCursorPos(int X, int Y);

    [DllImport("user32.dll")]
    public static extern void mouse_event(uint dwFlags, uint dx, uint dy, uint dwData, UIntPtr dwExtraInfo);
}
'@

$script:ProcessId = $null
$script:MainWindowTitle = $mainWindowTitle
$script:Artifacts = New-Object System.Collections.Generic.List[string]

function Get-ActionValue {
    param(
        [Parameter(Mandatory)]
        [object]$Action,
        [Parameter(Mandatory)]
        [string]$Name,
        $Default = $null
    )

    $property = $Action.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $Default
    }

    return $property.Value
}

function Resolve-ControlType {
    param([string]$Name)

    if ([string]::IsNullOrWhiteSpace($Name)) {
        return $null
    }

    $field = [System.Windows.Automation.ControlType].GetFields([System.Reflection.BindingFlags]::Public -bor [System.Reflection.BindingFlags]::Static) |
        Where-Object { $_.FieldType -eq [System.Windows.Automation.ControlType] -and $_.Name -ieq $Name } |
        Select-Object -First 1

    if ($null -eq $field) {
        throw "Unsupported control type '$Name'."
    }

    return [System.Windows.Automation.ControlType]$field.GetValue($null)
}

function Get-TopLevelWindow {
    param(
        [Parameter(Mandatory)]
        [int]$ProcessId,
        [string]$WindowTitle = $script:MainWindowTitle,
        [int]$TimeoutMs = 10000
    )

    $deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)
    $root = [System.Windows.Automation.AutomationElement]::RootElement
    $condition = New-Object System.Windows.Automation.PropertyCondition(
        [System.Windows.Automation.AutomationElement]::ProcessIdProperty,
        $ProcessId)

    while ([DateTime]::UtcNow -lt $deadline) {
        $windows = $root.FindAll([System.Windows.Automation.TreeScope]::Children, $condition)
        foreach ($window in $windows) {
            if ([string]::IsNullOrWhiteSpace($WindowTitle) -or $window.Current.Name -ieq $WindowTitle -or $window.Current.Name.StartsWith($WindowTitle, [StringComparison]::OrdinalIgnoreCase)) {
                return $window
            }
        }

        Start-Sleep -Milliseconds 100
    }

    throw "Could not find top-level window '$WindowTitle' for process $ProcessId."
}

function Get-NativeWindowHandle {
    param([System.Windows.Automation.AutomationElement]$Window)

    return [IntPtr][int]$Window.GetCurrentPropertyValue(
        [System.Windows.Automation.AutomationElement]::NativeWindowHandleProperty)
}

function Activate-Window {
    param([System.Windows.Automation.AutomationElement]$Window)

    $handle = Get-NativeWindowHandle -Window $Window
    if ($handle -eq [IntPtr]::Zero) {
        return
    }

    [NativeUi]::SetForegroundWindow($handle) | Out-Null
    Start-Sleep -Milliseconds 100
}

function Matches-Selector {
    param(
        [System.Windows.Automation.AutomationElement]$Element,
        [object]$Selector
    )

    $automationId = Get-ActionValue -Action $Selector -Name "automationId"
    if (-not [string]::IsNullOrWhiteSpace($automationId) -and $Element.Current.AutomationId -cne [string]$automationId) {
        return $false
    }

    $name = Get-ActionValue -Action $Selector -Name "name"
    if (-not [string]::IsNullOrWhiteSpace($name) -and $Element.Current.Name -cne [string]$name) {
        return $false
    }

    $controlTypeName = Get-ActionValue -Action $Selector -Name "controlType"
    if (-not [string]::IsNullOrWhiteSpace($controlTypeName)) {
        $expectedControlType = Resolve-ControlType -Name $controlTypeName
        if ($Element.Current.ControlType -ne $expectedControlType) {
            return $false
        }
    }

    return $true
}

function Find-UiElement {
    param(
        [Parameter(Mandatory)]
        [int]$ProcessId,
        [Parameter(Mandatory)]
        [object]$Selector,
        [int]$TimeoutMs = 10000
    )

    $windowTitle = Get-ActionValue -Action $Selector -Name "windowTitle" -Default $script:MainWindowTitle
    $scopeName = Get-ActionValue -Action $Selector -Name "scope" -Default "Descendants"
    $scope = if ([string]$scopeName -ieq "Children") {
        [System.Windows.Automation.TreeScope]::Children
    }
    else {
        [System.Windows.Automation.TreeScope]::Descendants
    }

    $matchIndex = [int](Get-ActionValue -Action $Selector -Name "index" -Default 0)
    $deadline = [DateTime]::UtcNow.AddMilliseconds($TimeoutMs)

    while ([DateTime]::UtcNow -lt $deadline) {
        $window = Get-TopLevelWindow -ProcessId $ProcessId -WindowTitle $windowTitle -TimeoutMs ([Math]::Max(500, $TimeoutMs))
        $elements = $window.FindAll($scope, [System.Windows.Automation.Condition]::TrueCondition)
        $matches = New-Object System.Collections.Generic.List[System.Windows.Automation.AutomationElement]

        foreach ($element in $elements) {
            if (Matches-Selector -Element $element -Selector $Selector) {
                $matches.Add($element)
            }
        }

        if ($matches.Count -gt $matchIndex) {
            return $matches[$matchIndex]
        }

        Start-Sleep -Milliseconds 100
    }

    $selectorSummary = @()
    foreach ($property in "automationId", "name", "controlType", "windowTitle") {
        $value = Get-ActionValue -Action $Selector -Name $property
        if (-not [string]::IsNullOrWhiteSpace($value)) {
            $selectorSummary += ($property + "=" + $value)
        }
    }

    throw "Could not find UI element ($($selectorSummary -join ', '))."
}

function Get-ElementPoint {
    param(
        [System.Windows.Automation.AutomationElement]$Element,
        [double]$RelativeX = 0.5,
        [double]$RelativeY = 0.5,
        [int]$OffsetX = 0,
        [int]$OffsetY = 0
    )

    $rect = $Element.Current.BoundingRectangle
    if ($rect.Width -le 0 -or $rect.Height -le 0) {
        throw "Element has no visible bounds."
    }

    return [pscustomobject]@{
        X = [int][Math]::Round($rect.Left + ($rect.Width * $RelativeX) + $OffsetX)
        Y = [int][Math]::Round($rect.Top + ($rect.Height * $RelativeY) + $OffsetY)
    }
}

function Send-LeftClick {
    param([int]$X, [int]$Y)

    [NativeUi]::SetCursorPos($X, $Y) | Out-Null
    Start-Sleep -Milliseconds 40
    [NativeUi]::mouse_event(0x0002, [uint32]$X, [uint32]$Y, 0, [UIntPtr]::Zero)
    Start-Sleep -Milliseconds 40
    [NativeUi]::mouse_event(0x0004, [uint32]$X, [uint32]$Y, 0, [UIntPtr]::Zero)
    Start-Sleep -Milliseconds 80
}

function Invoke-Element {
    param([System.Windows.Automation.AutomationElement]$Element)

    $invokePattern = $null
    if ($Element.TryGetCurrentPattern([System.Windows.Automation.InvokePattern]::Pattern, [ref]$invokePattern)) {
        $invokePattern.Invoke()
        return
    }

    $selectionPattern = $null
    if ($Element.TryGetCurrentPattern([System.Windows.Automation.SelectionItemPattern]::Pattern, [ref]$selectionPattern)) {
        $selectionPattern.Select()
        return
    }

    $point = Get-ElementPoint -Element $Element
    Send-LeftClick -X $point.X -Y $point.Y
}

function Toggle-Element {
    param([System.Windows.Automation.AutomationElement]$Element)

    $togglePattern = $null
    if ($Element.TryGetCurrentPattern([System.Windows.Automation.TogglePattern]::Pattern, [ref]$togglePattern)) {
        $togglePattern.Toggle()
        return
    }

    $point = Get-ElementPoint -Element $Element
    Send-LeftClick -X $point.X -Y $point.Y
}

function Set-ExpandCollapseState {
    param(
        [System.Windows.Automation.AutomationElement]$Element,
        [System.Windows.Automation.ExpandCollapseState]$DesiredState
    )

    $pattern = $null
    if ($Element.TryGetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern, [ref]$pattern)) {
        if ($DesiredState -eq [System.Windows.Automation.ExpandCollapseState]::Expanded) {
            $pattern.Expand()
        }
        else {
            $pattern.Collapse()
        }

        return
    }

    $point = Get-ElementPoint -Element $Element
    Send-LeftClick -X $point.X -Y $point.Y
}

function Set-ElementText {
    param(
        [System.Windows.Automation.AutomationElement]$Element,
        [string]$Value
    )

    $valuePattern = $null
    if ($Element.TryGetCurrentPattern([System.Windows.Automation.ValuePattern]::Pattern, [ref]$valuePattern)) {
        $valuePattern.SetValue($Value)
        return
    }

    $point = Get-ElementPoint -Element $Element
    Send-LeftClick -X $point.X -Y $point.Y
    Start-Sleep -Milliseconds 80
    [System.Windows.Forms.SendKeys]::SendWait("^a")
    Start-Sleep -Milliseconds 40
    [System.Windows.Forms.SendKeys]::SendWait($Value)
}

function Select-ComboItem {
    param(
        [int]$ProcessId,
        [object]$Action
    )

    $combo = Find-UiElement -ProcessId $ProcessId -Selector $Action -TimeoutMs ([int](Get-ActionValue -Action $Action -Name "timeoutMs" -Default 10000))
    $expandPattern = $null
    if ($combo.TryGetCurrentPattern([System.Windows.Automation.ExpandCollapsePattern]::Pattern, [ref]$expandPattern)) {
        $expandPattern.Expand()
    }
    else {
        $point = Get-ElementPoint -Element $combo
        Send-LeftClick -X $point.X -Y $point.Y
    }

    Start-Sleep -Milliseconds 150

    $itemSelector = [pscustomobject]@{
        windowTitle = (Get-ActionValue -Action $Action -Name "windowTitle" -Default $script:MainWindowTitle)
        name = (Get-ActionValue -Action $Action -Name "itemName")
        controlType = "ListItem"
        index = [int](Get-ActionValue -Action $Action -Name "itemIndex" -Default 0)
    }

    $item = Find-UiElement -ProcessId $ProcessId -Selector $itemSelector -TimeoutMs 5000
    Invoke-Element -Element $item
}

function Resolve-ArtifactPath {
    param([string]$Path)

    if ([System.IO.Path]::IsPathRooted($Path)) {
        return $Path
    }

    return Join-Path $OutputRoot $Path
}

function Capture-WindowScreenshot {
    param([System.Windows.Automation.AutomationElement]$Window, [string]$Path)

    $resolvedPath = Resolve-ArtifactPath -Path $Path
    $directory = Split-Path -Parent $resolvedPath
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }

    Activate-Window -Window $Window

    $handle = Get-NativeWindowHandle -Window $Window
    if ($handle -eq [IntPtr]::Zero) {
        throw "Window has no native handle."
    }

    $rect = New-Object NativeUi+RECT
    [NativeUi]::GetWindowRect($handle, [ref]$rect) | Out-Null
    $width = $rect.Right - $rect.Left
    $height = $rect.Bottom - $rect.Top
    $bitmap = New-Object System.Drawing.Bitmap($width, $height)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    $graphics.CopyFromScreen($rect.Left, $rect.Top, 0, 0, (New-Object System.Drawing.Size($width, $height)))
    $bitmap.Save($resolvedPath, [System.Drawing.Imaging.ImageFormat]::Png)
    $graphics.Dispose()
    $bitmap.Dispose()
    $script:Artifacts.Add($resolvedPath)
    return $resolvedPath
}

function Capture-ElementScreenshot {
    param(
        [System.Windows.Automation.AutomationElement]$Element,
        [System.Windows.Automation.AutomationElement]$Window,
        [string]$Path
    )

    $resolvedPath = Resolve-ArtifactPath -Path $Path
    $directory = Split-Path -Parent $resolvedPath
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }

    Activate-Window -Window $Window

    $rect = $Element.Current.BoundingRectangle
    $left = [int][Math]::Floor($rect.Left)
    $top = [int][Math]::Floor($rect.Top)
    $width = [int][Math]::Ceiling($rect.Width)
    $height = [int][Math]::Ceiling($rect.Height)

    if ($width -le 0 -or $height -le 0) {
        throw "Element has no visible bounds for screenshot capture."
    }

    $bitmap = New-Object System.Drawing.Bitmap($width, $height)
    $graphics = [System.Drawing.Graphics]::FromImage($bitmap)
    $graphics.CopyFromScreen($left, $top, 0, 0, (New-Object System.Drawing.Size($width, $height)))
    $bitmap.Save($resolvedPath, [System.Drawing.Imaging.ImageFormat]::Png)
    $graphics.Dispose()
    $bitmap.Dispose()
    $script:Artifacts.Add($resolvedPath)
    return $resolvedPath
}

function Dump-WindowTree {
    param([System.Windows.Automation.AutomationElement]$Window, [string]$Path)

    $resolvedPath = Resolve-ArtifactPath -Path $Path
    $directory = Split-Path -Parent $resolvedPath
    if (-not [string]::IsNullOrWhiteSpace($directory)) {
        New-Item -ItemType Directory -Force -Path $directory | Out-Null
    }

    $elements = $Window.FindAll([System.Windows.Automation.TreeScope]::Descendants, [System.Windows.Automation.Condition]::TrueCondition)
    $records = foreach ($element in $elements) {
        $rect = $element.Current.BoundingRectangle
        [pscustomobject]@{
            name = $element.Current.Name
            automationId = $element.Current.AutomationId
            controlType = $element.Current.ControlType.ProgrammaticName
            className = $element.Current.ClassName
            isEnabled = $element.Current.IsEnabled
            isOffscreen = $element.Current.IsOffscreen
            left = $rect.Left
            top = $rect.Top
            width = $rect.Width
            height = $rect.Height
        }
    }

    [pscustomobject]@{
        windowTitle = $Window.Current.Name
        processId = $Window.Current.ProcessId
        capturedAtUtc = [DateTimeOffset]::UtcNow.ToString("O")
        elements = $records
    } | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $resolvedPath

    $script:Artifacts.Add($resolvedPath)
    return $resolvedPath
}

function Resize-Window {
    param(
        [System.Windows.Automation.AutomationElement]$Window,
        [int]$X,
        [int]$Y,
        [int]$Width,
        [int]$Height
    )

    $handle = Get-NativeWindowHandle -Window $Window
    if ($handle -eq [IntPtr]::Zero) {
        throw "Window has no native handle."
    }

    [NativeUi]::SetWindowPos($handle, [IntPtr]::Zero, $X, $Y, $Width, $Height, 0x0040) | Out-Null
    Start-Sleep -Milliseconds 150
}

function Perform-Drag {
    param(
        [System.Windows.Automation.AutomationElement]$Element,
        [double]$RelativeX,
        [double]$RelativeY,
        [int]$DeltaX,
        [int]$DeltaY
    )

    $start = Get-ElementPoint -Element $Element -RelativeX $RelativeX -RelativeY $RelativeY
    $endX = $start.X + $DeltaX
    $endY = $start.Y + $DeltaY

    [NativeUi]::SetCursorPos($start.X, $start.Y) | Out-Null
    Start-Sleep -Milliseconds 40
    [NativeUi]::mouse_event(0x0002, [uint32]$start.X, [uint32]$start.Y, 0, [UIntPtr]::Zero)
    Start-Sleep -Milliseconds 60
    [NativeUi]::SetCursorPos($endX, $endY) | Out-Null
    Start-Sleep -Milliseconds 80
    [NativeUi]::mouse_event(0x0004, [uint32]$endX, [uint32]$endY, 0, [UIntPtr]::Zero)
    Start-Sleep -Milliseconds 120
}

function Run-Actions {
    param([System.Diagnostics.Process]$Process)

    if ($null -ne $actionSpec.window) {
        $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $script:MainWindowTitle -TimeoutMs 20000
        $width = Get-ActionValue -Action $actionSpec.window -Name "width" -Default 1280
        $height = Get-ActionValue -Action $actionSpec.window -Name "height" -Default 760
        $x = Get-ActionValue -Action $actionSpec.window -Name "x" -Default 80
        $y = Get-ActionValue -Action $actionSpec.window -Name "y" -Default 80
        Resize-Window -Window $window -X ([int]$x) -Y ([int]$y) -Width ([int]$width) -Height ([int]$height)
    }

    foreach ($action in $actionSpec.actions) {
        $type = [string](Get-ActionValue -Action $action -Name "type")
        switch ($type) {
            "wait" {
                Start-Sleep -Milliseconds ([int](Get-ActionValue -Action $action -Name "milliseconds" -Default 250))
            }

            "wait-window" {
                $windowTitle = Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle
                $timeoutMs = [int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000)
                $null = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $windowTitle -TimeoutMs $timeoutMs
            }

            "screenshot" {
                $windowTitle = Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $windowTitle -TimeoutMs 10000
                $null = Capture-WindowScreenshot -Window $window -Path ([string](Get-ActionValue -Action $action -Name "path" -Default ($type + ".png")))
            }

            "screenshot-element" {
                $windowTitle = Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $windowTitle -TimeoutMs 10000
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                $null = Capture-ElementScreenshot -Element $element -Window $window -Path ([string](Get-ActionValue -Action $action -Name "path" -Default "element.png"))
            }

            "dump-tree" {
                $windowTitle = Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $windowTitle -TimeoutMs 10000
                $null = Dump-WindowTree -Window $window -Path ([string](Get-ActionValue -Action $action -Name "path" -Default "ui-tree.json"))
            }

            "resize-window" {
                $windowTitle = Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $windowTitle -TimeoutMs 10000
                Resize-Window `
                    -Window $window `
                    -X ([int](Get-ActionValue -Action $action -Name "x" -Default 80)) `
                    -Y ([int](Get-ActionValue -Action $action -Name "y" -Default 80)) `
                    -Width ([int](Get-ActionValue -Action $action -Name "width" -Default 1280)) `
                    -Height ([int](Get-ActionValue -Action $action -Name "height" -Default 760))
            }

            "click" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle (Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle) -TimeoutMs 10000
                Activate-Window -Window $window
                $point = Get-ElementPoint -Element $element
                Send-LeftClick -X $point.X -Y $point.Y
            }

            "invoke" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                Invoke-Element -Element $element
                Start-Sleep -Milliseconds 120
            }

            "toggle" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                Toggle-Element -Element $element
                Start-Sleep -Milliseconds 120
            }

            "expand" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                Set-ExpandCollapseState -Element $element -DesiredState ([System.Windows.Automation.ExpandCollapseState]::Expanded)
                Start-Sleep -Milliseconds 120
            }

            "collapse" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                Set-ExpandCollapseState -Element $element -DesiredState ([System.Windows.Automation.ExpandCollapseState]::Collapsed)
                Start-Sleep -Milliseconds 120
            }

            "set-text" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                Set-ElementText -Element $element -Value ([string](Get-ActionValue -Action $action -Name "value" -Default ""))
                Start-Sleep -Milliseconds 120
            }

            "send-keys" {
                $windowTitle = Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle $windowTitle -TimeoutMs 10000
                Activate-Window -Window $window
                $automationId = Get-ActionValue -Action $action -Name "automationId"
                $name = Get-ActionValue -Action $action -Name "name"
                if (-not [string]::IsNullOrWhiteSpace($automationId) -or -not [string]::IsNullOrWhiteSpace($name)) {
                    $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                    $point = Get-ElementPoint -Element $element
                    Send-LeftClick -X $point.X -Y $point.Y
                }

                [System.Windows.Forms.SendKeys]::SendWait([string](Get-ActionValue -Action $action -Name "keys" -Default ""))
                Start-Sleep -Milliseconds 120
            }

            "select" {
                Select-ComboItem -ProcessId $Process.Id -Action $action
                Start-Sleep -Milliseconds 120
            }

            "drag" {
                $element = Find-UiElement -ProcessId $Process.Id -Selector $action -TimeoutMs ([int](Get-ActionValue -Action $action -Name "timeoutMs" -Default 10000))
                $window = Get-TopLevelWindow -ProcessId $Process.Id -WindowTitle (Get-ActionValue -Action $action -Name "windowTitle" -Default $script:MainWindowTitle) -TimeoutMs 10000
                Activate-Window -Window $window
                Perform-Drag `
                    -Element $element `
                    -RelativeX ([double](Get-ActionValue -Action $action -Name "relativeX" -Default 0.5)) `
                    -RelativeY ([double](Get-ActionValue -Action $action -Name "relativeY" -Default 0.5)) `
                    -DeltaX ([int](Get-ActionValue -Action $action -Name "deltaX" -Default 0)) `
                    -DeltaY ([int](Get-ActionValue -Action $action -Name "deltaY" -Default 0))
            }

            default {
                throw "Unsupported action type '$type'."
            }
        }
    }
}

$process = $null

Push-Location $repoRoot
try {
    & dotnet build src\dotnet\QsoRipper.Gui\QsoRipper.Gui.csproj -c $Configuration -p:OutputPath=$buildOutputRoot -v minimal
    if ($LASTEXITCODE -ne 0) {
        throw "GUI build failed with exit code $LASTEXITCODE."
    }

    $exePath = Join-Path $buildOutputRoot "QsoRipper.Gui.exe"
    if (-not (Test-Path -LiteralPath $exePath)) {
        throw "Built GUI executable not found at $exePath."
    }

    $arguments = @("--inspect", "--inspect-theme", $Theme)
    if (-not [string]::IsNullOrWhiteSpace($inspectSurface)) {
        $arguments += @("--inspect-surface", $inspectSurface)
    }
    if (-not [string]::IsNullOrWhiteSpace($Fixture)) {
        $arguments += @("--inspect-fixture", $Fixture)
    }

    $process = Start-Process -FilePath $exePath -ArgumentList $arguments -WorkingDirectory $repoRoot -PassThru
    $script:ProcessId = $process.Id

    $null = Get-TopLevelWindow -ProcessId $process.Id -WindowTitle $script:MainWindowTitle -TimeoutMs 20000
    Run-Actions -Process $process

    $summary = [pscustomobject]@{
        surface = "avalonia"
        scenario = $scenarioName
        actionScript = $ActionScript
        fixturePath = $Fixture
        inspectSurface = $inspectSurface
        outputRoot = $OutputRoot
        buildOutputRoot = $buildOutputRoot
        buildOutputRetained = ($KeepOpen -or $KeepBuildOutput)
        processId = $process.Id
        artifacts = $script:Artifacts
        capturedAtUtc = [DateTimeOffset]::UtcNow.ToString("O")
    }

    $summaryPath = Join-Path $OutputRoot "report.json"
    $summary | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $summaryPath
    Write-Host "Saved Avalonia automation artifacts to $OutputRoot"
    Write-Host "Summary: $summaryPath"
}
finally {
    if (-not $KeepOpen -and $null -ne $process) {
        $running = Get-Process -Id $process.Id -ErrorAction SilentlyContinue
        if ($null -ne $running) {
            Stop-Process -Id $process.Id
        }
    }

    if (-not $KeepOpen -and -not $KeepBuildOutput -and (Test-Path -LiteralPath $buildOutputRoot)) {
        Remove-Item -LiteralPath $buildOutputRoot -Recurse -Force
    }

    Pop-Location
}
