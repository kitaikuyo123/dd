param(
    [switch]$SkipInstall,
    [switch]$LeaveRunning
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "MiniSqlE2E.ps1")

$projectRoot = Get-ProjectRoot
$setupSql = Join-Path $projectRoot "tests\e2e\sql\smoke_setup.sql"
$dropSql = Join-Path $projectRoot "tests\e2e\sql\drop_products.sql"

try {
    Stop-Cluster
    Start-Cluster -SkipInstall:$SkipInstall

    $null = Invoke-SqlFile -Path $setupSql
    $regionId = Get-OnlyRegionId -TableName "products"

    $dropOutput = Invoke-SqlFile -Path $dropSql
    Assert-NotContains -Text $dropOutput -Unexpected "| products" -Message "Dropped table should not appear in SHOW TABLES."

    $tableRegionsOutput = Invoke-ZkCli -Commands @("ls /minisql/tables")
    Assert-NotContains -Text $tableRegionsOutput -Unexpected "products" -Message "Dropped table should be removed from /minisql/tables."

    $regionsOutput = Invoke-ZkCli -Commands @("ls /minisql/regions")
    Assert-NotContains -Text $regionsOutput -Unexpected $regionId -Message "Dropped region should be removed from /minisql/regions."

    Write-Host "Drop cleanup E2E passed."
} finally {
    if (-not $LeaveRunning) {
        Stop-Cluster
    }
}
