if ($IsLinux -and -not $env:DOTNET_ROOT) {
    # Apphosts cannot find a user-local runtime through PATH.
    $dotnet = Get-Command dotnet -CommandType Application -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($dotnet) {
        $launcher = Get-Item -LiteralPath $dotnet.Path
        $dotnetExecutable = $launcher.ResolveLinkTarget($true) ?? $launcher
        $env:DOTNET_ROOT = $dotnetExecutable.DirectoryName
    }
}
