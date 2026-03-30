param(
    [switch]$SkipInstall,
    [switch]$LeaveRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scripts = @(
    "Run-Smoke.ps1",
    "Run-JoinProjection.ps1",
    "Run-FailoverRejoin.ps1",
    "Run-DropCleanup.ps1"
)

foreach ($script in $scripts) {
    $scriptPath = Join-Path $PSScriptRoot $script
    Write-Host "Running $script ..."

    $arguments = @(
        "-ExecutionPolicy", "Bypass",
        "-File", $scriptPath
    )
    if ($SkipInstall) {
        $arguments += "-SkipInstall"
    }
    if ($LeaveRunning -and $script -eq $scripts[-1]) {
        $arguments += "-LeaveRunning"
    }

    & powershell @arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$script failed."
    }
}

Write-Host "All MiniSQL E2E scripts passed."
