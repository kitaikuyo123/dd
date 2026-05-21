param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [int]$Clients = 3,
    [int]$QueriesPerClient = 50,
    [string]$TableName = "perf_orders",
    [int]$ProbeId = 0,
    [switch]$KeepSqlFiles
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outDir = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if ($Clients -le 0) { throw "Clients must be greater than 0." }
if ($QueriesPerClient -le 0) { throw "QueriesPerClient must be greater than 0." }

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outDir "multi-client-read-$timestamp"
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
        $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)"
    }
    return @($counted).Count
}

function New-ReadSql {
    param([int]$Client, [int]$Index)

    $userId = (($Client * 17 + $Index) % 100) + 1
    $selector = $Index % 5
    if ($ProbeId -gt 0 -and $selector -eq 4) {
        return "SELECT * FROM $TableName WHERE id = $ProbeId;"
    }
    switch ($selector) {
        0 { return "SELECT COUNT(*) FROM $TableName;" }
        1 { return "SELECT SUM(amount) FROM $TableName;" }
        2 { return "SELECT * FROM $TableName WHERE user_id = $userId LIMIT 5;" }
        3 { return "SELECT * FROM $TableName WHERE status = 'paid' LIMIT 5;" }
        default { return "SELECT AVG(amount) FROM $TableName;" }
    }
}

Write-Host "MiniSQL multi-client concurrent read test"
Write-Host "Project root        : $projectRoot"
Write-Host "ZooKeeper           : $ZkHost`:$ZkPort"
Write-Host "Table               : $TableName"
Write-Host "Clients             : $Clients"
Write-Host "Queries per client  : $QueriesPerClient"
Write-Host "Run directory       : $runDir"
Write-Host ""

$timer = [System.Diagnostics.Stopwatch]::StartNew()
$jobs = New-Object System.Collections.Generic.List[object]

for ($client = 1; $client -le $Clients; $client++) {
    $sqlFile = Join-Path $runDir ("client-{0:D2}.sql" -f $client)
    $inputFile = Join-Path $runDir ("client-{0:D2}.input" -f $client)
    $logFile = Join-Path $runDir ("client-{0:D2}.log" -f $client)

    $lines = New-Object System.Collections.Generic.List[string]
    for ($i = 0; $i -lt $QueriesPerClient; $i++) {
        $lines.Add((New-ReadSql -Client $client -Index $i))
    }
    New-Utf8File -Path $sqlFile -Lines $lines

    $process = Start-CliSource -SqlFile $sqlFile -LogFile $logFile -InputFile $inputFile
    $jobs.Add([pscustomobject]@{
        Client = $client
        Process = $process
        SqlFile = $sqlFile
        InputFile = $inputFile
        LogFile = $logFile
    }) | Out-Null
}

foreach ($job in $jobs) {
    $job.Process.WaitForExit()
}
$timer.Stop()

$totalErrors = 0
$clientSummaries = New-Object System.Collections.Generic.List[string]

foreach ($job in $jobs) {
    $text = if (Test-Path $job.LogFile) {
        [System.IO.File]::ReadAllText($job.LogFile, [System.Text.Encoding]::UTF8)
    } else {
        ""
    }
    $errorCount = Get-CountedErrors -Text $text
    $resultSetCount = ([regex]::Matches($text, "\([0-9]+ row[s]?\)")).Count
    $totalErrors += $errorCount
    $clientSummaries.Add(("Client {0:D2}: exit={1}, result sets={2}, counted errors={3}, log={4}" -f
        $job.Client, $job.Process.ExitCode, $resultSetCount, $errorCount, $job.LogFile))
}

$totalQueries = $Clients * $QueriesPerClient
$elapsedSeconds = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)
$queriesPerSecond = [Math]::Round($totalQueries / $elapsedSeconds, 2)
$summaryFile = Join-Path $runDir "summary.txt"

$summary = @(
    "MiniSQL multi-client concurrent read test summary",
    "Time              : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper         : $ZkHost`:$ZkPort",
    "Table             : $TableName",
    "Clients           : $Clients",
    "Queries per client: $QueriesPerClient",
    "Queries requested : $totalQueries",
    "Probe ID          : $ProbeId",
    "Elapsed seconds   : $([Math]::Round($elapsedSeconds, 3))",
    "Queries per second: $queriesPerSecond",
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
