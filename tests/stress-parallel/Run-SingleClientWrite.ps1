param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [int]$Rows = 1000,
    [int]$StartId = 0,
    [string]$TableName = "perf_orders",
    [switch]$SkipCreateTable,
    [switch]$KeepSqlFile
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outDir = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

if ($Rows -le 0) {
    throw "Rows must be greater than 0."
}

if ($StartId -le 0) {
    $StartId = [int]([DateTimeOffset]::Now.ToUnixTimeSeconds() % 2000000000)
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$sqlFile = Join-Path $outDir "single-client-write-$timestamp.sql"
$logFile = Join-Path $outDir "single-client-write-$timestamp.log"
$summaryFile = Join-Path $outDir "single-client-write-$timestamp.summary.txt"

$sqlLines = New-Object System.Collections.Generic.List[string]
if (-not $SkipCreateTable) {
    $sqlLines.Add("CREATE TABLE $TableName (id INT PRIMARY KEY, user_id INT, amount DOUBLE, status STRING);")
}

for ($i = 0; $i -lt $Rows; $i++) {
    $id = $StartId + $i
    $userId = ($i % 100) + 1
    $amount = [Math]::Round((($i % 1000) + 1) * 1.0, 2)
    $status = if (($i % 2) -eq 0) { "paid" } else { "pending" }
    $sqlLines.Add("INSERT INTO $TableName (id, user_id, amount, status) VALUES ($id, $userId, $amount, '$status');")
}

$lastId = $StartId + $Rows - 1
$sqlLines.Add("SELECT COUNT(*) FROM $TableName;")
$sqlLines.Add("SELECT * FROM $TableName WHERE id = $StartId;")
$sqlLines.Add("SELECT * FROM $TableName WHERE id = $lastId;")

[System.IO.File]::WriteAllLines($sqlFile, $sqlLines, [System.Text.UTF8Encoding]::new($false))

Write-Host "MiniSQL single-client continuous write test"
Write-Host "Project root : $projectRoot"
Write-Host "ZooKeeper    : $ZkHost`:$ZkPort"
Write-Host "Table        : $TableName"
Write-Host "Rows         : $Rows"
Write-Host "ID range     : $StartId - $lastId"
Write-Host "SQL file     : $sqlFile"
Write-Host "Log file     : $logFile"
Write-Host ""

$cliInput = "source $sqlFile`nexit`n"
$arguments = "/c chcp 65001 >nul && mvn -q -pl client exec:java -Dexec.mainClass=com.minisql.client.cli.SqlCli -Dexec.args=""--host $ZkHost --port $ZkPort"""

$psi = New-Object System.Diagnostics.ProcessStartInfo
$psi.FileName = "cmd.exe"
$psi.Arguments = $arguments
$psi.WorkingDirectory = $projectRoot
$psi.RedirectStandardInput = $true
$psi.RedirectStandardOutput = $true
$psi.RedirectStandardError = $true
$psi.StandardOutputEncoding = [System.Text.Encoding]::UTF8
$psi.StandardErrorEncoding = [System.Text.Encoding]::UTF8
$psi.UseShellExecute = $false
$psi.CreateNoWindow = $true
$psi.EnvironmentVariables["MAVEN_OPTS"] = "-Dfile.encoding=UTF-8 -Dsun.stdout.encoding=UTF-8 -Dsun.stderr.encoding=UTF-8 " + $psi.EnvironmentVariables["MAVEN_OPTS"]

$process = New-Object System.Diagnostics.Process
$process.StartInfo = $psi

$timer = [System.Diagnostics.Stopwatch]::StartNew()
$null = $process.Start()
$process.StandardInput.Write($cliInput)
$process.StandardInput.Close()
$stdout = $process.StandardOutput.ReadToEnd()
$stderr = $process.StandardError.ReadToEnd()
$process.WaitForExit()
$timer.Stop()

$combined = @()
$combined += "===== STDOUT ====="
$combined += $stdout
$combined += "===== STDERR ====="
$combined += $stderr
[System.IO.File]::WriteAllLines($logFile, $combined, [System.Text.UTF8Encoding]::new($false))

$okCount = ([regex]::Matches($stdout, "OK \(")).Count
$combinedText = $stdout + "`n" + $stderr
$candidateErrorLines = $combinedText -split "(`r`n|`n|`r)" | Where-Object {
    $_ -match "(?i)(SQL 错误|SQL error|ERROR|Exception|Failed|无法获取)"
}
$ignoredErrorLines = $candidateErrorLines | Where-Object {
    $_ -match "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)"
}
$rawSqlErrorLines = $candidateErrorLines | Where-Object {
    $_ -notmatch "(?i)(SLF4J|StatusLogger|log4j-core|logging implementation|codes\.html|Ignoring binding|Class path contains)"
}
$createTableAlreadyExists = (-not $SkipCreateTable) -and
    ($combinedText -match "(?i)(already exists|exists|duplicate|table.*exist)")
$countedErrorLines = $rawSqlErrorLines | Where-Object {
    -not ($createTableAlreadyExists -and $_ -match "(?i)(Create table failed: Table already exists|Table already exists)")
}
$rawErrorCount = @($candidateErrorLines).Count
$ignoredErrorCount = @($ignoredErrorLines).Count
$sqlErrorCount = @($countedErrorLines).Count
$insertOkCount = if ($SkipCreateTable -or $createTableAlreadyExists) { $okCount } else { [Math]::Max(0, $okCount - 1) }
$elapsedSeconds = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)
$rowsPerSecond = [Math]::Round($Rows / $elapsedSeconds, 2)

$summary = @(
    "MiniSQL single-client continuous write test summary",
    "Time              : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper         : $ZkHost`:$ZkPort",
    "Table             : $TableName",
    "Rows requested    : $Rows",
    "ID range          : $StartId - $lastId",
    "Process exit code : $($process.ExitCode)",
    "Elapsed seconds   : $([Math]::Round($elapsedSeconds, 3))",
    "Rows per second   : $rowsPerSecond",
    "OK lines          : $okCount",
    "Approx insert OK  : $insertOkCount",
    "Raw errors        : $rawErrorCount",
    "Ignored warnings  : $ignoredErrorCount",
    "Counted errors    : $sqlErrorCount",
    "Create existed    : $createTableAlreadyExists",
    "SQL file          : $sqlFile",
    "Log file          : $logFile"
)

[System.IO.File]::WriteAllLines($summaryFile, $summary, [System.Text.UTF8Encoding]::new($false))

Write-Host ""
$summary | ForEach-Object { Write-Host $_ }

if (-not $KeepSqlFile) {
    Remove-Item -LiteralPath $sqlFile -Force
    Write-Host "SQL file removed. Use -KeepSqlFile to keep generated SQL."
}

if ($process.ExitCode -ne 0 -or $sqlErrorCount -gt 0) {
    Write-Host ""
    Write-Host "Test finished with errors. Check log file for details:"
    Write-Host $logFile
    exit 1
}

Write-Host ""
Write-Host "Test finished successfully."
