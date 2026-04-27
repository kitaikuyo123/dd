param(
    [int]$Port,
    [string]$Name
)

Write-Host "[$Name] port $Port ..."

$pids = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
    Select-Object -ExpandProperty OwningProcess -Unique

if (-not $pids) {
    Write-Host "  No process listening on port $Port, skipping."
    return
}

foreach ($p in $pids) {
    Write-Host "  Stopping PID $p..."
    # Graceful first: WM_CLOSE triggers JVM shutdown hook -> RegionServer.stop()
    Stop-Process -Id $p -Force:$false -ErrorAction SilentlyContinue
    # Wait for graceful shutdown (gRPC drain + flush + RocksDB close)
    Start-Sleep -Seconds 3

    $alive = Get-Process -Id $p -ErrorAction SilentlyContinue
    if ($alive) {
        Write-Host "  [WARN] PID $p still alive, force killing..."
        Stop-Process -Id $p -Force:$true -ErrorAction SilentlyContinue
    }
    Write-Host "  [$Name] stopped."
}
