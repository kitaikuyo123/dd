param(
    [switch]$SkipInstall,
    [switch]$LeaveRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "MiniSqlE2E.ps1")

$projectRoot = Get-ProjectRoot
$sqlPath = Join-Path $projectRoot "tests\e2e\sql\join_projection.sql"

try {
    Stop-Cluster
    Start-Cluster -SkipInstall:$SkipInstall

    $output = Invoke-SqlFile -Path $sqlPath
    Assert-Contains -Text $output -Expected "| Alice | 150    |" -Message "Join projection query should keep Alice's paid order."
    Assert-Contains -Text $output -Expected "| Bob   | 220    |" -Message "Join projection query should keep Bob's paid order."
    Assert-Contains -Text $output -Expected "| Alice | 230.0 |" -Message "Join aggregation should return Alice total."
    Assert-Contains -Text $output -Expected "| Bob   | 220.0 |" -Message "Join aggregation should return Bob total."

    Write-Host "Join projection E2E passed."
} finally {
    if (-not $LeaveRunning) {
        Stop-Cluster
    }
}
