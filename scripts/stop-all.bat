@echo off
setlocal

echo ==========================================
echo   MiniSQL Cluster Stop Script
echo ==========================================
echo.

echo Stopping MiniSQL service ports...
call :stop_port 16000 Master
call :stop_port 16020 RegionServer-1
call :stop_port 16021 RegionServer-2
call :stop_port 16022 RegionServer-3

echo.
echo Stopping CLI helper windows...
taskkill /F /FI "WINDOWTITLE eq MiniSQL CLI" >nul 2>nul
taskkill /F /FI "WINDOWTITLE eq MiniSQL CLI*" >nul 2>nul

echo.
echo MiniSQL stop sequence completed.
echo.
pause
exit /b 0

:stop_port
set "PORT=%~1"
set "NAME=%~2"

echo Checking %NAME% on port %PORT%...
for /f "usebackq delims=" %%P in (`powershell -NoProfile -Command "$c = Get-NetTCPConnection -State Listen -LocalPort %PORT% -ErrorAction SilentlyContinue | Select-Object -ExpandProperty OwningProcess -Unique; if ($c) { $c | ForEach-Object { $_ } }"`) do (
    echo   Stopping PID %%P for %NAME%...
    taskkill /F /PID %%P >nul 2>nul
)

exit /b 0
