param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [string]$TableName = "perf_orders_scan",
    [int]$Rows = 2000,
    [int]$StartId = 0,
    [int]$PreviewLimit = 100,
    [switch]$SkipLoad,
    [switch]$FullScan,
    [switch]$KeepSqlFile
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outRoot = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null

if ($Rows -le 0) { throw "Rows must be greater than 0." }
if ($StartId -le 0) { $StartId = 700000000 + [int]([DateTimeOffset]::Now.ToUnixTimeMilliseconds() % 1000000000) }

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outRoot "large-table-scan-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$sqlFile = Join-Path $runDir "scan.sql"
$inputFile = Join-Path $runDir "scan.input"
$logFile = Join-Path $runDir "scan.log"
$summaryFile = Join-Path $runDir "summary.txt"

function New-Utf8File([string]$Path, [string[]]$Lines) {
    [System.IO.File]::WriteAllLines($Path, $Lines, [System.Text.UTF8Encoding]::new($false))
}

$lines = New-Object System.Collections.Generic.List[string]
if (-not $SkipLoad) {
    $lines.Add("CREATE TABLE $TableName (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);")
    for ($i = 0; $i -lt $Rows; $i++) {
        $id = $StartId + $i
        $userId = ($i % 100) + 1
        $amount = [Math]::Round((($i % 1000) + 1) * 1.0, 2)
        $status = if ($i % 2 -eq 0) { "paid" } else { "pending" }
        $lines.Add("INSERT INTO $TableName (id, user_id, amount, status) VALUES ($id, $userId, $amount, '$status');")
    }
}
$lines.Add("SELECT COUNT(*) FROM $TableName;")
$lines.Add("SELECT * FROM $TableName WHERE status = 'paid' LIMIT $PreviewLimit;")
$lines.Add("SELECT * FROM $TableName WHERE user_id = 1 LIMIT $PreviewLimit;")
if ($FullScan) {
    $lines.Add("SELECT * FROM $TableName;")
} else {
    $lines.Add("SELECT * FROM $TableName LIMIT $PreviewLimit;")
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
$elapsed = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)

$summary = @(
    "MiniSQL large table scan test summary",
    "Time            : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper       : $ZkHost`:$ZkPort",
    "Table           : $TableName",
    "Rows requested  : $Rows",
    "Skip load       : $SkipLoad",
    "Full scan       : $FullScan",
    "Preview limit   : $PreviewLimit",
    "Elapsed seconds : $([Math]::Round($elapsed, 3))",
    "OK lines        : $ok",
    "Result sets     : $resultSets",
    "Counted errors  : $errors",
    "Run directory   : $runDir",
    "Log file        : $logFile"
)
New-Utf8File $summaryFile $summary
if (-not $KeepSqlFile) { Remove-Item -LiteralPath $sqlFile,$inputFile -Force }
$summary | ForEach-Object { Write-Host $_ }
if ($p.ExitCode -ne 0 -or $errors -gt 0) { exit 1 }
