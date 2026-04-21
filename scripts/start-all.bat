@echo off
setlocal

echo ==========================================
echo   MiniSQL Cluster Start Script
echo ==========================================
echo.

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"

set "RS1_CONFIG=%PROJECT_ROOT%\regionserver\src\main\resources\regionserver-1.properties"
set "RS2_CONFIG=%PROJECT_ROOT%\regionserver\src\main\resources\regionserver-2.properties"
set "RS3_CONFIG=%PROJECT_ROOT%\regionserver\src\main\resources\regionserver-3.properties"
set "MASTER_SCRIPT=%PROJECT_ROOT%\scripts\start-master.bat"
set "RS_SCRIPT=%PROJECT_ROOT%\scripts\start-regionserver.bat"

call :check_java
if errorlevel 1 exit /b 1
call :check_file "%MASTER_SCRIPT%" "master start script"
if errorlevel 1 exit /b 1
call :check_file "%RS_SCRIPT%" "regionserver start script"
if errorlevel 1 exit /b 1
call :check_file "%RS1_CONFIG%" "regionserver-1 config"
if errorlevel 1 exit /b 1
call :check_file "%RS2_CONFIG%" "regionserver-2 config"
if errorlevel 1 exit /b 1
call :check_file "%RS3_CONFIG%" "regionserver-3 config"
if errorlevel 1 exit /b 1

pushd "%PROJECT_ROOT%" >nul

echo [%TIME%] Installing project artifacts...
call mvn -q -DskipTests install
if errorlevel 1 (
    echo [ERROR] Maven install failed.
    popd >nul
    exit /b 1
)

echo [%TIME%] Starting Master...
start "MiniSQL-Master" /D "%PROJECT_ROOT%" cmd /k call scripts\start-master.bat --skip-compile
timeout /t 3 /nobreak >nul

echo [%TIME%] Starting RegionServer 1...
start "MiniSQL-RS-1" /D "%PROJECT_ROOT%" cmd /k call scripts\start-regionserver.bat 1 --skip-compile
timeout /t 2 /nobreak >nul

echo [%TIME%] Starting RegionServer 2...
start "MiniSQL-RS-2" /D "%PROJECT_ROOT%" cmd /k call scripts\start-regionserver.bat 2 --skip-compile
timeout /t 2 /nobreak >nul

echo [%TIME%] Starting RegionServer 3...
start "MiniSQL-RS-3" /D "%PROJECT_ROOT%" cmd /k call scripts\start-regionserver.bat 3 --skip-compile
timeout /t 2 /nobreak >nul

popd >nul

echo.
echo Cluster start commands issued.
echo.
echo Prerequisites:
echo   1. ZooKeeper must already be running on localhost:2181
echo.
echo Endpoints:
echo   Master:        localhost:16000
echo   RegionServer1: localhost:16020
echo   RegionServer2: localhost:16021
echo   RegionServer3: localhost:16022
echo.
echo Start the CLI with: scripts\start-cli.bat
echo.
pause
exit /b 0

:check_java
if defined JAVA_HOME (
    if exist "%JAVA_HOME%\bin\java.exe" exit /b 0
)
where java >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Java not found. Set JAVA_HOME or add java to PATH.
    exit /b 1
)
exit /b 0

:check_file
if exist "%~1" exit /b 0
echo [ERROR] Missing %~2: %~1
exit /b 1
