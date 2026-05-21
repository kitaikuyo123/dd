param(
    [string]$ZkHost = "localhost",
    [int]$ZkPort = 2181,
    [string]$TableName = "perf_orders_scan",
    [int]$Clients = 4,
    [int]$UpdatesPerClient = 50,
    [int]$TargetId = 0,
    [switch]$KeepSqlFiles
)

$ErrorActionPreference = "Stop"
[Console]::OutputEncoding = [System.Text.Encoding]::UTF8

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectRoot = Resolve-Path (Join-Path $scriptDir "..\..")
$outRoot = Join-Path $scriptDir "out"
New-Item -ItemType Directory -Force -Path $outRoot | Out-Null

if ($Clients -le 0) { throw "Clients must be greater than 0." }
if ($UpdatesPerClient -le 0) { throw "UpdatesPerClient must be greater than 0." }
if ($TargetId -le 0) {
    $TargetId = 900000000 + [int]([DateTimeOffset]::Now.ToUnixTimeMilliseconds() % 100000000)
}

$timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
$runDir = Join-Path $outRoot "cons08-concurrent-update-$timestamp"
New-Item -ItemType Directory -Force -Path $runDir | Out-Null
$runTag = "cons08_$timestamp"
$expectedUpdates = $Clients * $UpdatesPerClient

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

Write-Host "MiniSQL CONS-08 concurrent update consistency test"
Write-Host "ZooKeeper          : $ZkHost`:$ZkPort"
Write-Host "Table              : $TableName"
Write-Host "Target ID          : $TargetId"
Write-Host "Clients            : $Clients"
Write-Host "Updates per client : $UpdatesPerClient"
Write-Host "Run tag            : $runTag"
Write-Host "Run directory      : $runDir"
Write-Host ""

$setupSql = Join-Path $runDir "setup.sql"
$setupInput = Join-Path $runDir "setup.input"
$setupLog = Join-Path $runDir "setup.log"
New-Utf8File $setupSql @(
    "INSERT INTO $TableName (id, user_id, amount, status) VALUES ($TargetId, 999, 0.0, '$runTag');",
    "SELECT * FROM $TableName WHERE id = $TargetId;"
)
$setupProcess = Start-CliSource $setupSql $setupLog $setupInput
$setupProcess.WaitForExit()

$jobs = New-Object System.Collections.Generic.List[object]
$timer = [System.Diagnostics.Stopwatch]::StartNew()
for ($client = 1; $client -le $Clients; $client++) {
    $sql = Join-Path $runDir ("client-{0:D2}.sql" -f $client)
    $input = Join-Path $runDir ("client-{0:D2}.input" -f $client)
    $log = Join-Path $runDir ("client-{0:D2}.log" -f $client)
    $lines = New-Object System.Collections.Generic.List[string]
    for ($i = 1; $i -le $UpdatesPerClient; $i++) {
        $amount = ($client * 100000) + $i
        $status = ("c{0:D2}u{1:D3}" -f $client, $i)
        $lines.Add("UPDATE $TableName SET amount = $amount, status = '$status' WHERE id = $TargetId;")
    }
    New-Utf8File $sql $lines
    $jobs.Add([pscustomobject]@{
        Client = $client
        Process = Start-CliSource $sql $log $input
        Log = $log
        LastAmount = (($client * 100000) + $UpdatesPerClient)
        LastStatus = ("c{0:D2}u{1:D3}" -f $client, $UpdatesPerClient)
    }) | Out-Null
}

foreach ($job in $jobs) {
    $job.Process.WaitForExit()
}

$verifySql = Join-Path $runDir "verify.sql"
$verifyInput = Join-Path $runDir "verify.input"
$verifyLog = Join-Path $runDir "verify.log"
New-Utf8File $verifySql @(
    "SELECT COUNT(*) FROM $TableName WHERE id = $TargetId;",
    "SELECT * FROM $TableName WHERE id = $TargetId;"
)
$verifyProcess = Start-CliSource $verifySql $verifyLog $verifyInput
$verifyProcess.WaitForExit()
$timer.Stop()

$setupText = if (Test-Path $setupLog) { [System.IO.File]::ReadAllText($setupLog, [System.Text.Encoding]::UTF8) } else { "" }
$setupOk = ([regex]::Matches($setupText, "OK \(")).Count
$totalUpdateOk = 0
$totalErrors = Count-Errors $setupText
$details = New-Object System.Collections.Generic.List[string]
foreach ($job in $jobs) {
    $text = if (Test-Path $job.Log) { [System.IO.File]::ReadAllText($job.Log, [System.Text.Encoding]::UTF8) } else { "" }
    $ok = ([regex]::Matches($text, "OK \(")).Count
    $errors = Count-Errors $text
    $totalUpdateOk += $ok
    $totalErrors += $errors
    $details.Add(("Client {0:D2}: exit={1}, update OK={2}, errors={3}, final candidate amount={4}, status={5}, log={6}" -f
        $job.Client, $job.Process.ExitCode, $ok, $errors, $job.LastAmount, $job.LastStatus, $job.Log))
}

$verifyText = if (Test-Path $verifyLog) { [System.IO.File]::ReadAllText($verifyLog, [System.Text.Encoding]::UTF8) } else { "" }
$verifyResultSets = ([regex]::Matches($verifyText, "\([0-9]+ row[s]?\)")).Count
$verifyErrors = Count-Errors $verifyText
$totalErrors += $verifyErrors
$finalRowMatch = [regex]::Match($verifyText, "\|\s*$TargetId\s*\|\s*999\s*\|\s*([0-9.]+)\s*\|\s*([^|]+?)\s*\|")
$finalAmount = if ($finalRowMatch.Success) { $finalRowMatch.Groups[1].Value.Trim() } else { "" }
$finalStatus = if ($finalRowMatch.Success) { $finalRowMatch.Groups[2].Value.Trim() } else { "" }
$candidateStatuses = @($jobs | ForEach-Object { $_.LastStatus })
$finalStatusIsCandidate = $candidateStatuses -contains $finalStatus
$elapsed = [Math]::Max($timer.Elapsed.TotalSeconds, 0.001)

$summary = @(
    "MiniSQL CONS-08 concurrent update consistency test summary",
    "Time                 : $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')",
    "ZooKeeper            : $ZkHost`:$ZkPort",
    "Table                : $TableName",
    "Target ID            : $TargetId",
    "Run tag              : $runTag",
    "Clients              : $Clients",
    "Updates per client   : $UpdatesPerClient",
    "Expected updates     : $expectedUpdates",
    "Elapsed seconds      : $([Math]::Round($elapsed, 3))",
    "Setup OK lines       : $setupOk",
    "Update OK lines      : $totalUpdateOk",
    "Verifier results     : $verifyResultSets",
    "Final amount         : $finalAmount",
    "Final status         : $finalStatus",
    "Final status valid   : $finalStatusIsCandidate",
    "Counted errors       : $totalErrors",
    "Verifier log         : $verifyLog",
    "Run directory        : $runDir",
    "",
    "Expected verification:",
    "1. COUNT for target ID should be 1.",
    "2. Final status should be one of the client final candidates.",
    "3. Counted errors should be 0.",
    ""
) + $details

New-Utf8File (Join-Path $runDir "summary.txt") $summary
if (-not $KeepSqlFiles) {
    Get-ChildItem -LiteralPath $runDir -Filter "*.sql" | Remove-Item -Force
    Get-ChildItem -LiteralPath $runDir -Filter "*.input" | Remove-Item -Force
}

$summary | ForEach-Object { Write-Host $_ }
$nonZeroExit = @($jobs | Where-Object { $_.Process.ExitCode -ne 0 }).Count
if ($setupProcess.ExitCode -ne 0 -or $nonZeroExit -gt 0 -or $verifyProcess.ExitCode -ne 0 -or $totalErrors -gt 0 -or -not $finalStatusIsCandidate) {
    exit 1
}
