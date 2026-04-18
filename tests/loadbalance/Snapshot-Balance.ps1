param(
    [string]$MonitorHost = "localhost",
    [int]$MonitorPort = 16010
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$base = "http://$MonitorHost`:$MonitorPort/monitor/api"

try {
    $servers = Invoke-RestMethod "$base/servers"
    $regions = Invoke-RestMethod "$base/regions"
    $events = Invoke-RestMethod "$base/events?limit=20"
} catch {
    Write-Error "Failed to query monitor API at $base. $_"
    exit 1
}

Write-Host ""
Write-Host "=== Server Summary ==="
$servers |
    Sort-Object serverId |
    Select-Object serverId, regionCount, readRequests, writeRequests, cpuUsage, memoryUsage |
    Format-Table -AutoSize

Write-Host ""
Write-Host "=== Region Roles (top 30) ==="
$regions |
    Sort-Object regionId, role, serverId |
    Select-Object -First 30 regionId, tableName, role, serverId, readRequests, writeRequests |
    Format-Table -AutoSize

Write-Host ""
Write-Host "=== Recent Events ==="
$events |
    Select-Object timestamp, type, severity, regionId, sourceServer, targetServer, message |
    Format-Table -AutoSize
