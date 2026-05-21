param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [string]$TableName = "perf_orders_scan",
    [int]$Clients = 4,
    [int]$RowsPerClient = 100,
    [int]$BaseId = 0,
    [switch]$KeepSqlFiles
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outRoot = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null

if ($Clients -le 0) { throw "Clients must be greater than 0." }
if ($RowsPerClient -le 0) { throw "RowsPerClient must be greater than 0." }
if ($BaseId -le 0) {
    $BaseId = 800000000 + [int]([DateTimeOffset]::Now.ToUnixTimeMilliseconds() % 100000000)
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outRoot "cons07-concurrent-write-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$runTag = "cons07_$timestamp"
$expectedRows = $Clients * $RowsPerClient
$lastGlobalId = $BaseId + $expectedRows - 1

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
    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi
    $null = $process.Start()
    return $process
}

function Count-Errors([string]$Text) {
    $lines = $Text -split "(`r`n|`n|`r)" | Where-Object {
        $_ -match "(?i)(SQL 错误|SQL error|ERROR|Exception|Failed|无法获取)"
    }
    $counted = $lines | Where-Object {
        $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)"
    }
    return @($counted).Count
}

Write-Host "MiniSQL CONS-07 concurrent write consistency test"
Write-Host "ZooKeeper       : $ZkHost`:$ZkPort"
Write-Host "Table           : $TableName"
Write-Host "Clients         : $Clients"
Write-Host "Rows per client : $RowsPerClient"
Write-Host "Base ID         : $BaseId"
Write-Host "Run tag         : $runTag"
Write-Host "Run directory   : $runDir"
Write-Host ""

$jobs = New-Object System.Collections.Generic.List[object]
$timer = [System.Diagnostics.Stopwatch]::StartNew()

for ($client = 1; $client -le $Clients; $client++) {
    $startId = $BaseId + (($client - 1) * $RowsPerClient)
    $lastId = $startId + $RowsPerClient - 1
    $sql = Join-Path $runDir ("client-{0:D2}.sql" -f $client)
    $input = Join-Path $runDir ("client-{0:D2}.input" -f $client)
    $log = Join-Path $runDir ("client-{0:D2}.log" -f $client)

    $lines = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $RowsPerClient; $i++) {
        $id = $startId + $i
        $userId = (($client * 1000 + $i) % 100) + 1
        $amount = [Math]::Round((($client * 100000) + $i) * 1.0, 2)
        $lines.Add("INSERT INTO $TableName (id, user_id, amount, status) VALUES ($id, $userId, $amount, '$runTag');")
    }
    New-Utf8File $sql $lines

    $jobs.Add([pscustomobject]@{
        Client = $client
        Process = Start-CliSource $sql $log $input
        StartId = $startId
        LastId = $lastId
        Log = $log
        Sql = $sql
        Input = $input
    }) | Out-Null
}

foreach ($job in $jobs) {
    $job.Process.WaitForExit()
}

$verifySql = Join-Path $runDir "verify.sql"
$verifyInput = Join-Path $runDir "verify.input"
$verifyLog = Join-Path $runDir "verify.log"
New-Utf8File $verifySql @(
    "SELECT COUNT(*) FROM $TableName WHERE id >= $BaseId AND id <= $lastGlobalId;",
    "SELECT COUNT(*) FROM $TableName WHERE status = '$runTag';",
    "SELECT * FROM $TableName WHERE id = $BaseId;",
    "SELECT * FROM $TableName WHERE id = $lastGlobalId;"
)
$verifyProcess = Start-CliSource $verifySql $verifyLog $verifyInput
$verifyProcess.WaitForExit()
$timer.Stop()

$totalOk = 0
$totalErrors = 0
$details = New-Object System.Collections.Generic.List[string]
foreach ($job in $jobs) {
    $text = if (Test-Path $job.Log) { [System.IO.File]::ReadAllText($job.Log, [System.Text.Encoding]::UTF8) } else { "" }
    $ok = ([regex]::Matches($text, "OK \(")).Count
    $errors = Count-Errors $text
    $totalOk += $ok
    $totalErrors += $errors
    $details.Add(("Client {0:D2}: exit={1}, OK={2}, errors={3}, id range={4}-{5}, log={6}" -f
        $job.Client, $job.Process.ExitCode, $ok, $errors, $job.StartId, $job.LastId, $job.Log))
}

$verifyText = if (Test-Path $verifyLog) { [System.IO.File]::ReadAllText($verifyLog, [System.Text.Encoding]::UTF8) } else { "" }
$verifyResultSets = ([regex]::Matches($verifyText, "\([0-9]+ row[s]?\)")).Count
$verifyErrors = Count-Errors $verifyText
$totalErrors += $verifyErrors
$elapsed = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)

$summary = @(
    "MiniSQL CONS-07 concurrent write consistency test summary",
    "Time              : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper         : $ZkHost`:$ZkPort",
    "Table             : $TableName",
    "Run tag           : $runTag",
    "Clients           : $Clients",
    "Rows per client   : $RowsPerClient",
    "Expected new rows : $expectedRows",
    "ID range          : $BaseId-$lastGlobalId",
    "Elapsed seconds   : $([Math]::Round($elapsed, 3))",
    "Insert OK lines   : $totalOk",
    "Verifier results  : $verifyResultSets",
    "Counted errors    : $totalErrors",
    "Verifier log      : $verifyLog",
    "Run directory     : $runDir",
    "",
    "Expected verification:",
    "1. COUNT by ID range should equal $expectedRows.",
    "2. COUNT by run tag should equal $expectedRows.",
    "3. First and last inserted rows should be visible.",
    ""
) + $details

New-Utf8File (Join-Path $runDir "summary.txt") $summary
if (-not $KeepSqlFiles) {
    Get-ChildItem -LiteralPath $runDir -Filter "*.sql" | Remove-Item -Force
    Get-ChildItem -LiteralPath $runDir -Filter "*.input" | Remove-Item -Force
}

$summary | ForEach-Object { Write-Host $_ }
$nonZeroExit = @($jobs | Where-Object { $_.Process.ExitCode -ne 0 }).Count
if ($nonZeroExit -gt 0 -or $verifyProcess.ExitCode -ne 0 -or $totalErrors -gt 0) {
    exit 1
}
