param(
    [string]$MasterHost = "localhost",
    [int]$MasterPort = 16000
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$grpcurl = Get-Command grpcurl -ErrorAction SilentlyContinue
if (-not $grpcurl) {
    Write-Error "grpcurl not found in PATH. Install grpcurl first, or wait for scheduler-based balancing."
    exit 1
}

$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$protoDir = Join-Path $projectRoot "common\src\main\proto"
$payload = '{"force":true}'
$target = "$MasterHost`:$MasterPort"

Write-Host "Triggering balance on $target ..."
& grpcurl -plaintext `
    -import-path $protoDir `
    -proto master.proto `
    -d $payload `
    $target `
    "minisql.MasterService/TriggerBalance"

if ($LASTEXITCODE -ne 0) {
    Write-Error "TriggerBalance call failed."
    exit $LASTEXITCODE
}

Write-Host "TriggerBalance call finished."
