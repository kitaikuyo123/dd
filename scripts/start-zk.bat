@echo off
setlocal EnableDelayedExpansion

echo ==========================================
echo   Starting ZooKeeper
echo ==========================================
echo.

:: JAVA_HOME: use existing env var, or default JDK path
if not defined JAVA_HOME (
    set "JAVA_HOME=C:\Program Files\Microsoft\jdk-11.0.16.101-hotspot"
)

:: ZK_HOME: check env var, then default path, then sibling dirs
if not defined ZK_HOME (
    if exist "D:\apache-zookeeper-3.9.4-bin\bin\zkServer.cmd" (
        set "ZK_HOME=D:\apache-zookeeper-3.9.4-bin"
    )
)
if not defined ZK_HOME (
    if exist "..\apache-zookeeper-3.9.4-bin\bin\zkServer.cmd" (
        set "ZK_HOME=..\apache-zookeeper-3.9.4-bin"
    )
)
if not defined ZK_HOME (
    for /d %%D in (..\apache-zookeeper-*) do (
        if exist "%%D\bin\zkServer.cmd" (
            set "ZK_HOME=%%D"
        )
    )
)

if not exist "%JAVA_HOME%\bin\java.exe" (
    echo [ERROR] JAVA_HOME not found: %JAVA_HOME%
    echo Set JAVA_HOME env var or edit this script.
    pause
    exit /b 1
)

if not defined ZK_HOME (
    echo [ERROR] ZooKeeper not found.
    echo Options:
    echo   1. Set ZK_HOME env var
    echo   2. Put apache-zookeeper-*-bin next to the project
    echo   3. Put it at D:\apache-zookeeper-3.9.4-bin
    pause
    exit /b 1
)
if not exist "%ZK_HOME%\bin\zkServer.cmd" (
    echo [ERROR] zkServer.cmd not found in %ZK_HOME%\bin
    pause
    exit /b 1
)

cd /d "%ZK_HOME%\bin"
echo ZooKeeper home: %ZK_HOME%
echo JAVA_HOME:    %JAVA_HOME%
echo.

call .\zkServer.cmd

endlocal
