@echo off
setlocal

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"
set "MASTER_CONFIG=%PROJECT_ROOT%\master\src\main\resources\master.properties"
set "SKIP_COMPILE=0"

if /I "%~1"=="--skip-compile" (
    set "SKIP_COMPILE=1"
) else if not "%~1"=="" (
    set "MASTER_CONFIG=%PROJECT_ROOT%\master\src\main\resources\%~1"
    if /I "%~2"=="--skip-compile" set "SKIP_COMPILE=1"
)

call :check_java || exit /b 1
if not exist "%MASTER_CONFIG%" (
    echo [ERROR] Master config not found: %MASTER_CONFIG%
    exit /b 1
)

pushd "%PROJECT_ROOT%" >nul
if "%SKIP_COMPILE%"=="0" (
    call mvn -q -DskipTests -pl master -am install
    if errorlevel 1 (
        echo [ERROR] Maven install failed.
        popd >nul
        exit /b 1
    )
)

echo ========================================
echo Starting Master
echo Config: %MASTER_CONFIG%
echo ========================================
echo.

mvn -f master\pom.xml exec:java -Dexec.mainClass=com.minisql.master.rpc.MasterMain -Dexec.args=%MASTER_CONFIG%
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
