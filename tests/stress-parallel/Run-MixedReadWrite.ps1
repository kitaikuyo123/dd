param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [string]$TableName = "perf_orders_mix",
    [int]$Writers = 1,
    [int]$Readers = 2,
    [int]$RowsPerWriter = 300,
    [int]$QueriesPerReader = 50,
    [int]$BaseId = 0,
    [switch]$SkipCreateTable,
    [switch]$KeepSqlFiles
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outRoot = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null

if ($Writers -le 0) { throw "Writers must be greater than 0." }
if ($Readers -le 0) { throw "Readers must be greater than 0." }
if ($RowsPerWriter -le 0) { throw "RowsPerWriter must be greater than 0." }
if ($QueriesPerReader -le 0) { throw "QueriesPerReader must be greater than 0." }
if ($BaseId -le 0) { $BaseId = 500000000 + [int]([DateTimeOffset]::Now.ToUnixTimeMilliseconds() % 1000000000) }

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outRoot "mixed-read-write-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

function New-Utf8File([string]$Path, [string[]]$Lines) {
    [System.IO.File]::WriteAllLines($Path, $Lines, [System.Text.UTF8Encoding]::new($false))
}

function Start-CliSource([string]$SqlFile, [string]$LogFile, [string]$InputFile) {
    New-Utf8File $InputFile @("source $SqlFile", "exit")
    $args = "/c chcp 65001 >nul && mvn -q -pl client exec:java -Dexec.mainClass=com.minisql.client.cli.SqlCli -Dexec.args=""--host $ZkHost --port $ZkPort"" < ""$InputFile"" > ""$LogFile"" 2>&1"
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "cmd.exe"
    $psi.Arguments = $args
    $psi.WorkingDirectory = $projectRoot
    $psi.UseShellExecute = $false
    $psi.CreateNoWindow = $true
    $psi.EnvironmentVariables["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 " + $psi.EnvironmentVariables["MAVEN_OPTS"]
    $p = New-Object System.Diagnostics.Process
    $p.StartInfo = $psi
    $null = $p.Start()
    return $p
}

function Count-Errors([string]$Text) {
    $lines = $Text -split "(`r`n|`n|`r)" | Where-Object { $_ -match "(?i)(SQL 错误|SQL error|ERROR|Exception|Failed|无法获取)" }
    $counted = $lines | Where-Object {
        $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)" -and
        $_ -notmatch "(?i)(Create table failed: Table already exists|Table already exists)"
    }
    return @($counted).Count
}

if (-not $SkipCreateTable) {
    $setupSql = Join-Path $runDir "setup.sql"
    $setupLog = Join-Path $runDir "setup.log"
    $setupInput = Join-Path $runDir "setup.input"
    New-Utf8File $setupSql @("CREATE TABLE $TableName (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);")
    $setupProcess = Start-CliSource $setupSql $setupLog $setupInput
    $setupProcess.WaitForExit()
}

$jobs = New-Object System.Collections.Generic.List[object]
$timer = [System.Diagnostics.Stopwatch]::StartNew()

for ($w = 1; $w -le $Writers; $w++) {
    $startId = $BaseId + (($w - 1) * ($RowsPerWriter + 10000))
    $lastId = $startId + $RowsPerWriter - 1
    $sql = Join-Path $runDir ("writer-{0:D2}.sql" -f $w)
    $log = Join-Path $runDir ("writer-{0:D2}.log" -f $w)
    $input = Join-Path $runDir ("writer-{0:D2}.input" -f $w)
    $lines = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $RowsPerWriter; $i++) {
        $id = $startId + $i
        $userId = (($w * 1000 + $i) % 100) + 1
        $amount = [Math]::Round((($i % 1000) + 1) * 1.0, 2)
        $status = if (($i + $w) % 2 -eq 0) { "paid" } else { "pending" }
        $lines.Add("INSERT INTO $TableName (id, user_id, amount, status) VALUES ($id, $userId, $amount, '$status');")
    }
    $lines.Add("SELECT COUNT(*) FROM $TableName WHERE id >= $startId AND id <= $lastId;")
    New-Utf8File $sql $lines
    $jobs.Add([pscustomobject]@{ Kind="writer"; Name=("writer-{0:D2}" -f $w); Process=(Start-CliSource $sql $log $input); Log=$log }) | Out-Null
}

for ($r = 1; $r -le $Readers; $r++) {
    $sql = Join-Path $runDir ("reader-{0:D2}.sql" -f $r)
    $log = Join-Path $runDir ("reader-{0:D2}.log" -f $r)
    $input = Join-Path $runDir ("reader-{0:D2}.input" -f $r)
    $lines = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $QueriesPerReader; $i++) {
        $userId = (($r * 19 + $i) % 100) + 1
        switch ($i % 4) {
            0 { $lines.Add("SELECT COUNT(*) FROM $TableName;") }
            1 { $lines.Add("SELECT SUM(amount) FROM $TableName;") }
            2 { $lines.Add("SELECT * FROM $TableName WHERE user_id = $userId LIMIT 5;") }
            default { $lines.Add("SELECT * FROM $TableName WHERE status = 'paid' LIMIT 5;") }
        }
    }
    New-Utf8File $sql $lines
    $jobs.Add([pscustomobject]@{ Kind="reader"; Name=("reader-{0:D2}" -f $r); Process=(Start-CliSource $sql $log $input); Log=$log }) | Out-Null
}

foreach ($job in $jobs) { $job.Process.WaitForExit() }
$timer.Stop()

$totalOk = 0
$totalErrors = 0
$details = New-Object System.Collections.Generic.List[string]
foreach ($job in $jobs) {
    $text = if (Test-Path $job.Log) { [System.IO.File]::ReadAllText($job.Log, [System.Text.Encoding]::UTF8) } else { "" }
    $ok = ([regex]::Matches($text, "OK \(")).Count
    $results = ([regex]::Matches($text, "\([0-9]+ row[s]?\)")).Count
    $errors = Count-Errors $text
    $totalOk += $ok
    $totalErrors += $errors
    $details.Add(("{0}: exit={1}, OK={2}, result sets={3}, errors={4}, log={5}" -f $job.Name, $job.Process.ExitCode, $ok, $results, $errors, $job.Log))
}

$elapsed = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)
$summary = @(
    "MiniSQL mixed read/write test summary",
    "Time              : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper         : $ZkHost`:$ZkPort",
    "Table             : $TableName",
    "Writers           : $Writers",
    "Readers           : $Readers",
    "Rows per writer   : $RowsPerWriter",
    "Queries per reader: $QueriesPerReader",
    "Elapsed seconds   : $([Math]::Round($elapsed, 3))",
    "Write OK lines    : $totalOk",
    "Counted errors    : $totalErrors",
    "Run directory     : $runDir",
    ""
) + $details
New-Utf8File (Join-Path $runDir "summary.txt") $summary

if (-not $KeepSqlFiles) {
    Get-ChildItem -LiteralPath $runDir -Filter "*.sql" | Remove-Item -Force
    Get-ChildItem -LiteralPath $runDir -Filter "*.input" | Remove-Item -Force
}

$summary | ForEach-Object { Write-Host $_ }
if ($totalErrors -gt 0) { exit 1 }
