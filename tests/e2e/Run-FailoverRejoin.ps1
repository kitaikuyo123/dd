param(
    [switch]$SkipInstall,
    [switch]$LeaveRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "MiniSqlE2E.ps1")

$projectRoot = Get-ProjectRoot
$setupSql = Join-Path $projectRoot "tests\e2e\sql\smoke_setup.sql"
$postFailoverSql = Join-Path $projectRoot "tests\e2e\sql\post_failover_insert.sql"

try {
    Stop-Cluster
    Start-Cluster -SkipInstall:$SkipInstall

    $setupOutput = Invoke-SqlFile -Path $setupSql
    Assert-Contains -Text $setupOutput -Expected "(2 rows)" -Message "Setup should create two product rows."

    $regionId = Get-OnlyRegionId -TableName "products"
    $primaryAddress = Get-PrimaryAddress -RegionId $regionId
    $primaryPort = [int]($primaryAddress.Split(":")[1])

    Stop-PortProcess -Port $primaryPort
    Start-Sleep -Seconds 20

    $afterFailover = Invoke-SqlText -SqlText "SELECT * FROM products ORDER BY id;"
    Assert-Contains -Text $afterFailover -Expected "| 10    | A    | 1  |" -Message "Failover read should keep row 1."
    Assert-Contains -Text $afterFailover -Expected "| 20    | B    | 2  |" -Message "Failover read should keep row 2."

    Restart-RegionServerByPort -Port $primaryPort
    Start-Sleep -Seconds 15

    $afterRejoin = Invoke-SqlFile -Path $postFailoverSql
    Assert-Contains -Text $afterRejoin -Expected "| 20    | C    | 3  |" -Message "Post-rejoin write should replicate row 3."
    Assert-Contains -Text $afterRejoin -Expected "(3 rows)" -Message "Post-rejoin query should return three rows."

    Write-Host "Failover/Rejoin E2E passed."
} finally {
    if (-not $LeaveRunning) {
        Stop-Cluster
    }
}
