@echo off
setlocal

set "NUM=%~1"
if "%NUM%"=="" (
    echo Usage: start-regionserver.bat [1^|2^|3]
    exit /b 1
)
set "SKIP_COMPILE=0"
if /I "%~2"=="--skip-compile" (
    set "SKIP_COMPILE=1"
)

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"
set "CONFIG_FILE=regionserver\src\main\resources\regionserver-%NUM%.properties"
set "CONFIG_FILE_ABS=%PROJECT_ROOT%\%CONFIG_FILE%"

call :check_java || exit /b 1
if not exist "%CONFIG_FILE_ABS%" (
    echo [ERROR] Config file not found: %CONFIG_FILE_ABS%
    exit /b 1
)

pushd "%PROJECT_ROOT%" >nul
if "%SKIP_COMPILE%"=="0" (
    call mvn -q -DskipTests -pl regionserver -am install
    if errorlevel 1 (
        echo [ERROR] Maven install failed.
        popd >nul
        exit /b 1
    )
)

echo ========================================
echo Starting RegionServer %NUM%
echo Config: %CONFIG_FILE_ABS%
echo ========================================
echo.

mvn -f regionserver\pom.xml exec:java -Dexec.mainClass=com.minisql.regionserver.RegionServerMain -Dexec.args=%CONFIG_FILE%
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul
exit /b %EXIT_CODE%

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
