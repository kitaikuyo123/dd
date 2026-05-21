param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [string]$UsersTable = "perf_join_users",
    [string]$OrdersTable = "perf_join_orders",
    [int]$Users = 100,
    [int]$Orders = 1000,
    [int]$Repeats = 10,
    [switch]$SkipLoad,
    [switch]$IncludeAggregateJoin,
    [switch]$KeepSqlFile
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outRoot = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null
if ($Users -le 0) { throw "Users must be greater than 0." }
if ($Orders -le 0) { throw "Orders must be greater than 0." }
if ($Repeats -le 0) { throw "Repeats must be greater than 0." }

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outRoot "join-pressure-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$sqlFile = Join-Path $runDir "join.sql"
$inputFile = Join-Path $runDir "join.input"
$logFile = Join-Path $runDir "join.log"
$summaryFile = Join-Path $runDir "summary.txt"

function New-Utf8File([string]$Path, [string[]]$Lines) {
    [System.IO.File]::WriteAllLines($Path, $Lines, [System.Text.UTF8Encoding]::new($false))
}

$lines = New-Object System.Collections.Generic.List[string]
if (-not $SkipLoad) {
    $lines.Add("CREATE TABLE $UsersTable (id INT PRIMARY KEY, name STRING, level INT);")
    $lines.Add("CREATE TABLE $OrdersTable (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);")
    for ($i = 1; $i -le $Users; $i++) {
        $level = ($i % 5) + 1
        $lines.Add("INSERT INTO $UsersTable (id, name, level) VALUES ($i, 'user$i', $level);")
    }
    for ($i = 1; $i -le $Orders; $i++) {
        $userId = (($i - 1) % $Users) + 1
        $amount = [Math]::Round((($i % 1000) + 1) * 1.0, 2)
        $status = if ($i % 2 -eq 0) { "paid" } else { "pending" }
        $lines.Add("INSERT INTO $OrdersTable (id, user_id, amount, status) VALUES ($i, $userId, $amount, '$status');")
    }
}
for ($i = 0; $i -lt $Repeats; $i++) {
    $lines.Add("SELECT u.name, o.amount FROM $UsersTable u JOIN $OrdersTable o ON u.id = o.user_id LIMIT 20;")
    if ($IncludeAggregateJoin) {
        $lines.Add("SELECT u.name, SUM(o.amount) FROM $UsersTable u JOIN $OrdersTable o ON u.id = o.user_id GROUP BY u.name;")
    }
}
New-Utf8File $sqlFile $lines
New-Utf8File $inputFile @("source $sqlFile", "exit")

$args = "/c chcp 65001 >nul && mvn -q -pl client exec:java -Dexec.mainClass=com.minisql.client.cli.SqlCli -Dexec.args=""--host $ZkHost --port $ZkPort"" < ""$inputFile"" > ""$logFile"" 2>&1"
$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = "cmd.exe"
$psi.Arguments = $args
$psi.WorkingDirectory = $projectRoot
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true
$psi.EnvironmentVariables["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 " + $psi.EnvironmentVariables["MAVEN_OPTS"]

$timer = [System.Diagnostics.Stopwatch]::StartNew()
$p = [System.Diagnostics.Process]::Start($psi)
$p.WaitForExit()
$timer.Stop()

$text = if (Test-Path $logFile) { [System.IO.File]::ReadAllText($logFile, [System.Text.Encoding]::UTF8) } else { "" }
$ok = ([regex]::Matches($text, "OK \(")).Count
$resultSets = ([regex]::Matches($text, "\([0-9]+ row[s]?\)")).Count
$errors = @($text -split "(`r`n|`n|`r)" | Where-Object {
    $_ -match "(?i)(SQL 错误|SQL error|ERROR|Exception|Failed|无法获取)" -and
    $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains|Table already exists)"
}).Count
$joinQueries = if ($IncludeAggregateJoin) { $Repeats * 2 } else { $Repeats }
$elapsed = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)

$summary = @(
    "MiniSQL JOIN pressure test summary",
    "Time                  : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper             : $ZkHost`:$ZkPort",
    "Users table           : $UsersTable",
    "Orders table          : $OrdersTable",
    "Users loaded          : $Users",
    "Orders loaded         : $Orders",
    "Skip load             : $SkipLoad",
    "Repeats               : $Repeats",
    "Include aggregate JOIN: $IncludeAggregateJoin",
    "JOIN queries requested: $joinQueries",
    "Elapsed seconds       : $([Math]::Round($elapsed, 3))",
    "OK lines              : $ok",
    "Result sets           : $resultSets",
    "Counted errors        : $errors",
    "Run directory         : $runDir",
    "Log file              : $logFile"
)
New-Utf8File $summaryFile $summary
if (-not $KeepSqlFile) { Remove-Item -LiteralPath $sqlFile,$inputFile -Force }
$summary | ForEach-Object { Write-Host $_ }
if ($p.ExitCode -ne 0 -or $errors -gt 0) { exit 1 }
