param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [int]$Clients = 3,
    [int]$RowsPerClient = 500,
    [int]$BaseId = 0,
    [string]$TableName = "perf_orders",
    [switch]$SkipCreateTable,
    [switch]$KeepSqlFiles
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outDir = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if ($Clients -le 0) { throw "Clients must be greater than 0." }
if ($RowsPerClient -le 0) { throw "RowsPerClient must be greater than 0." }

if ($BaseId -le 0) {
    $BaseId = 100000000 + [int]([DateTimeOffset]::Now.ToUnixTimeMilliseconds() % 1000000000)
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outDir "multi-client-write-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null

function New-Utf8File {
    param([string]$Path, [string[]]$Lines)
    [System.IO.File]::WriteAllLines($Path, $Lines, [System.Text.UTF8Encoding]::new($false))
}

function Start-CliSource {
    param(
        [string]$SqlFile,
        [string]$LogFile,
        [string]$InputFile
    )

    New-Utf8File -Path $InputFile -Lines @("source $SqlFile", "exit")

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

function Get-CountedErrors {
    param([string]$Text)

    $candidateErrorLines = $Text -split "(`r`n|`n|`r)" | Where-Object {
        $_ -match "(?i)(SQL 错误|SQL error|ERROR|Exception|Failed|无法获取)"
    }
    $counted = $candidateErrorLines | Where-Object {
        $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)" -and
        $_ -notmatch "(?i)(Create table failed: Table already exists|Table already exists)"
    }
    return @($counted).Count
}

Write-Host "MiniSQL multi-client concurrent write test"
Write-Host "Project root    : $projectRoot"
Write-Host "ZooKeeper       : $ZkHost`:$ZkPort"
Write-Host "Table           : $TableName"
Write-Host "Clients         : $Clients"
Write-Host "Rows per client : $RowsPerClient"
Write-Host "Run directory   : $runDir"
Write-Host ""

if (-not $SkipCreateTable) {
    $setupSql = Join-Path $runDir "setup.sql"
    $setupLog = Join-Path $runDir "setup.log"
    $setupInput = Join-Path $runDir "setup.input"
    New-Utf8File -Path $setupSql -Lines @(
        "CREATE TABLE $TableName (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);"
    )
    $setupProcess = Start-CliSource -SqlFile $setupSql -LogFile $setupLog -InputFile $setupInput
    $setupProcess.WaitForExit()
}

$timer = [System.Diagnostics.Stopwatch]::StartNew()
$jobs = New-Object System.Collections.Generic.List[object]
$spacing = $RowsPerClient + 10000

for ($client = 1; $client -le $Clients; $client++) {
    $startId = $BaseId + (($client - 1) * $spacing)
    $lastId = $startId + $RowsPerClient - 1
    $sqlFile = Join-Path $runDir ("client-{0:D2}.sql" -f $client)
    $inputFile = Join-Path $runDir ("client-{0:D2}.input" -f $client)
    $logFile = Join-Path $runDir ("client-{0:D2}.log" -f $client)

    $lines = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $RowsPerClient; $i++) {
        $id = $startId + $i
        $userId = (($client * 1000 + $i) % 100) + 1
        $amount = [Math]::Round((($i % 1000) + 1) * 1.0, 2)
        $status = if (($i + $client) % 2 -eq 0) { "paid" } else { "pending" }
        $lines.Add("INSERT INTO $TableName (id, user_id, amount, status) VALUES ($id, $userId, $amount, '$status');")
    }
    $lines.Add("SELECT COUNT(*) FROM $TableName WHERE id >= $startId AND id <= $lastId;")
    $lines.Add("SELECT * FROM $TableName WHERE id = $startId;")
    $lines.Add("SELECT * FROM $TableName WHERE id = $lastId;")
    New-Utf8File -Path $sqlFile -Lines $lines

    $process = Start-CliSource -SqlFile $sqlFile -LogFile $logFile -InputFile $inputFile
    $jobs.Add([pscustomobject]@{
        Client = $client
        Process = $process
        StartId = $startId
        LastId = $lastId
        SqlFile = $sqlFile
        InputFile = $inputFile
        LogFile = $logFile
    }) | Out-Null
}

foreach ($job in $jobs) {
    $job.Process.WaitForExit()
}
$timer.Stop()

$totalOk = 0
$totalErrors = 0
$clientSummaries = New-Object System.Collections.Generic.List[string]

foreach ($job in $jobs) {
    $text = if (Test-Path $job.LogFile) {
        [System.IO.File]::ReadAllText($job.LogFile, [System.Text.Encoding]::UTF8)
    } else {
        ""
    }
    $okCount = ([regex]::Matches($text, "OK \(")).Count
    $errorCount = Get-CountedErrors -Text $text
    $totalOk += $okCount
    $totalErrors += $errorCount
    $clientSummaries.Add(("Client {0:D2}: exit={1}, insert OK={2}, counted errors={3}, id range={4}-{5}, log={6}" -f
        $job.Client, $job.Process.ExitCode, $okCount, $errorCount, $job.StartId, $job.LastId, $job.LogFile))
}

$totalRows = $Clients * $RowsPerClient
$elapsedSeconds = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)
$rowsPerSecond = [Math]::Round($totalRows / $elapsedSeconds, 2)
$summaryFile = Join-Path $runDir "summary.txt"

$summary = @(
    "MiniSQL multi-client concurrent write test summary",
    "Time              : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper         : $ZkHost`:$ZkPort",
    "Table             : $TableName",
    "Clients           : $Clients",
    "Rows per client   : $RowsPerClient",
    "Rows requested    : $totalRows",
    "Base ID           : $BaseId",
    "Elapsed seconds   : $([Math]::Round($elapsedSeconds, 3))",
    "Rows per second   : $rowsPerSecond",
    "Total insert OK   : $totalOk",
    "Counted errors    : $totalErrors",
    "Run directory     : $runDir",
    ""
) + $clientSummaries

New-Utf8File -Path $summaryFile -Lines $summary

if (-not $KeepSqlFiles) {
    Get-ChildItem -LiteralPath $runDir -Filter "*.sql" | Remove-Item -Force
    Get-ChildItem -LiteralPath $runDir -Filter "*.input" | Remove-Item -Force
}

Write-Host ""
$summary | ForEach-Object { Write-Host $_ }

$nonZeroExit = @($jobs | Where-Object { $_.Process.ExitCode -ne 0 }).Count
if ($nonZeroExit -gt 0 -or $totalErrors -gt 0) {
    Write-Host ""
    Write-Host "Test finished with errors. Check logs in:"
    Write-Host $runDir
    exit 1
}

Write-Host ""
Write-Host "Test finished successfully."
