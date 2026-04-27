@echo off
setlocal

echo ==========================================
echo   Starting ZooKeeper
echo ==========================================
echo.

set "SCRIPT_DIR=%~dp0"
set "PROJECT_ROOT=%SCRIPT_DIR%.."
for %%I in ("%PROJECT_ROOT%") do set "PROJECT_ROOT=%%~fI"

set "JAVA_HOME=C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot"
set "ZK_HOME=D:\apache-zookeeper-3.9.4-bin"

if not exist "%JAVA_HOME%\bin\java.exe" (
    echo [ERROR] JAVA_HOME not found: %JAVA_HOME%
    echo Set JAVA_HOME to a valid JDK 11+ installation.
    pause
    exit /b 1
)

if not exist "%ZK_HOME%\bin\zkServer.cmd" (
    echo [ERROR] ZooKeeper not found at: %ZK_HOME%
    echo Expected ZooKeeper at parent-level: apache-zookeeper-3.9.4-bin
    pause
    exit /b 1
)

cd /d "%ZK_HOME%\bin"
echo ZooKeeper home: %ZK_HOME%
echo JAVA_HOME:    %JAVA_HOME%
echo.

call .\zkServer.cmd

endlocal
