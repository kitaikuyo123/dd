@echo off
setlocal EnableDelayedExpansion

echo ==========================================
echo   MiniSQL Cluster Stop Script
echo ==========================================
echo.

echo Shutting down RegionServers first, then Master...
echo Graceful: WM_CLOSE triggers JVM shutdown hook, 3s wait, then force-kill if needed.
echo.

call :stop_port_graceful 16022 RegionServer-3
call :stop_port_graceful 16021 RegionServer-2
call :stop_port_graceful 16020 RegionServer-1
call :stop_port_graceful 16000 Master

echo.
echo Stopping CLI helper windows...
taskkill /F /FI "WINDOWTITLE eq MiniSQL CLI" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq MiniSQL CLI*" >nul 2>nul

echo.
echo MiniSQL stop sequence completed.
echo.
pause
exit /b 0

:stop_port_graceful
set "PORT=%~1"
set "NAME=%~2"
set "SCRIPT_DIR=%~dp0"
powershell -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%stop-port.ps1" -Port %PORT% -Name "%NAME%"
exit /b 0
