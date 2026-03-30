param(
    [switch]$SkipInstall,
    [switch]$LeaveRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "MiniSqlE2E.ps1")

$projectRoot = Get-ProjectRoot
$sqlPath = Join-Path $projectRoot "tests\e2e\sql\smoke_setup.sql"

try {
    Stop-Cluster
    Start-Cluster -SkipInstall:$SkipInstall

    $output = Invoke-SqlFile -Path $sqlPath
    Assert-Contains -Text $output -Expected "| 10    | A    | 1  |" -Message "Smoke setup should return row 1."
    Assert-Contains -Text $output -Expected "| 20    | B    | 2  |" -Message "Smoke setup should return row 2."
    Assert-Contains -Text $output -Expected "(2 rows)" -Message "Smoke setup should finish with two rows."

    Write-Host "Smoke E2E passed."
} finally {
    if (-not $LeaveRunning) {
        Stop-Cluster
    }
}
