@echo off
setlocal

echo ==========================================
echo   MiniSQL CLI
echo ==========================================
echo.

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"

call :check_java || exit /b 1

pushd "%PROJECT_ROOT%" >nul
start "MiniSQL CLI" /D "%PROJECT_ROOT%" cmd /k mvn exec:java -pl minisql-client -Dexec.mainClass=com.minisql.client.cli.SqlCli
popd >nul
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
