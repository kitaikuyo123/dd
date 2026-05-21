param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [string]$TableName = "perf_orders_write_02",
    [int]$Repeats = 20,
    [switch]$KeepSqlFile
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outRoot = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null
if ($Repeats -le 0) { throw "Repeats must be greater than 0." }

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outRoot "aggregation-pressure-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$sqlFile = Join-Path $runDir "aggregation.sql"
$inputFile = Join-Path $runDir "aggregation.input"
$logFile = Join-Path $runDir "aggregation.log"
$summaryFile = Join-Path $runDir "summary.txt"

function New-Utf8File([string]$Path, [string[]]$Lines) {
    [System.IO.File]::WriteAllLines($Path, $Lines, [System.Text.UTF8Encoding]::new($false))
}

$lines = New-Object System.Collections.Generic.List[string]
for ($i = 0; $i -lt $Repeats; $i++) {
    $lines.Add("SELECT COUNT(*) FROM $TableName;")
    $lines.Add("SELECT SUM(amount) FROM $TableName;")
    $lines.Add("SELECT AVG(amount) FROM $TableName;")
    $lines.Add("SELECT MAX(amount) FROM $TableName;")
    $lines.Add("SELECT MIN(amount) FROM $TableName;")
    $lines.Add("SELECT user_id, SUM(amount) FROM $TableName GROUP BY user_id;")
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
$resultSets = ([regex]::Matches($text, "\([0-9]+ row[s]?\)")).Count
$errors = @($text -split "(`r`n|`n|`r)" | Where-Object {
    $_ -match "(?i)(SQL 错误|SQL error|ERROR|Exception|Failed|无法获取)" -and
    $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)"
}).Count
$totalQueries = $Repeats * 6
$elapsed = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)

$summary = @(
    "MiniSQL aggregation pressure test summary",
    "Time              : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper         : $ZkHost`:$ZkPort",
    "Table             : $TableName",
    "Repeats           : $Repeats",
    "Queries requested : $totalQueries",
    "Elapsed seconds   : $([Math]::Round($elapsed, 3))",
    "Queries per second: $([Math]::Round($totalQueries / $elapsed, 2))",
    "Result sets       : $resultSets",
    "Counted errors    : $errors",
    "Run directory     : $runDir",
    "Log file          : $logFile"
)
New-Utf8File $summaryFile $summary
if (-not $KeepSqlFile) { Remove-Item -LiteralPath $sqlFile,$inputFile -Force }
$summary | ForEach-Object { Write-Host $_ }
if ($p.ExitCode -ne 0 -or $errors -gt 0) { exit 1 }
