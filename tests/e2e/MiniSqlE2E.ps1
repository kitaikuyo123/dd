Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-ProjectRoot {
    return (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
}

function Invoke-Maven {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Arguments,
        [string]$WorkingDirectory = (Get-ProjectRoot)
    )

    & mvn @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Maven command failed: mvn $($Arguments -join ' ')"
    }
}

function Wait-Port {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port,
        [int]$TimeoutSeconds = 60
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $conn = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue
        if ($conn) {
            return
        }
        Start-Sleep -Seconds 1
    }

    throw "Timed out waiting for port $Port to listen."
}

function Stop-PortProcess {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $connections = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -ExpandProperty OwningProcess -Unique
    foreach ($processId in $connections) {
        Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
    }
}

function Stop-Cluster {
    foreach ($port in 16000, 16020, 16021, 16022) {
        Stop-PortProcess -Port $port
    }
}

function Start-Cluster {
    param(
        [switch]$SkipInstall
    )

    $projectRoot = Get-ProjectRoot

    if (-not $SkipInstall) {
        Invoke-Maven -Arguments @("-q", "-DskipTests", "install") -WorkingDirectory $projectRoot
    }

    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "call scripts\start-master.bat --skip-compile" `
        -WorkingDirectory $projectRoot | Out-Null
    Start-Sleep -Seconds 3

    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "call scripts\start-regionserver.bat 1 --skip-compile" `
        -WorkingDirectory $projectRoot | Out-Null
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "call scripts\start-regionserver.bat 2 --skip-compile" `
        -WorkingDirectory $projectRoot | Out-Null
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "call scripts\start-regionserver.bat 3 --skip-compile" `
        -WorkingDirectory $projectRoot | Out-Null

    Wait-Port -Port 16000
    Wait-Port -Port 16020
    Wait-Port -Port 16021
    Wait-Port -Port 16022
    Start-Sleep -Seconds 10
}

function Invoke-SqlText {
    param(
        [Parameter(Mandatory = $true)]
        [string]$SqlText
    )

    $projectRoot = Get-ProjectRoot
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "cmd.exe"
    $psi.Arguments = "/c mvn -q -pl minisql-client exec:java -Dexec.mainClass=com.minisql.client.cli.SqlCli"
    $psi.WorkingDirectory = $projectRoot
    $psi.UseShellExecute = $false
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi
    $null = $process.Start()
    $process.StandardInput.WriteLine($SqlText)
    $process.StandardInput.WriteLine("quit")
    $process.StandardInput.Close()

    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    return ($stdout + [Environment]::NewLine + $stderr)
}

function Invoke-SqlFile {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Path
    )

    $sqlText = Get-Content -Path $Path -Raw
    return Invoke-SqlText -SqlText $sqlText
}

function Get-ZkCliPath {
    $fromPath = Get-Command "zkCli.cmd" -ErrorAction SilentlyContinue
    if ($fromPath) {
        return $fromPath.Source
    }

    if ($env:ZOOKEEPER_HOME) {
        $candidate = Join-Path $env:ZOOKEEPER_HOME "bin\zkCli.cmd"
        if (Test-Path $candidate) {
            return $candidate
        }
    }

    throw "zkCli.cmd not found in PATH or ZOOKEEPER_HOME."
}

function Invoke-ZkCli {
    param(
        [Parameter(Mandatory = $true)]
        [string[]]$Commands
    )

    $zkCli = Get-ZkCliPath
    $psi = New-Object System.Diagnostics.ProcessStartInfo
    $psi.FileName = "cmd.exe"
    $psi.Arguments = "/c `"$zkCli`" -server localhost:2181"
    $psi.WorkingDirectory = (Get-ProjectRoot)
    $psi.UseShellExecute = $false
    $psi.RedirectStandardInput = $true
    $psi.RedirectStandardOutput = $true
    $psi.RedirectStandardError = $true

    $process = New-Object System.Diagnostics.Process
    $process.StartInfo = $psi
    $null = $process.Start()
    foreach ($command in $Commands) {
        $process.StandardInput.WriteLine($command)
    }
    $process.StandardInput.WriteLine("quit")
    $process.StandardInput.Close()

    $stdout = $process.StandardOutput.ReadToEnd()
    $stderr = $process.StandardError.ReadToEnd()
    $process.WaitForExit()

    return ($stdout + [Environment]::NewLine + $stderr)
}

function Get-OnlyRegionId {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TableName
    )

    $output = Invoke-ZkCli -Commands @("ls /minisql/tables/$TableName/regions")
    $match = [regex]::Match($output, "\[(?<items>[^\]]*)\]")
    if (-not $match.Success) {
        throw "Could not read region list for table '$TableName'. Output:`n$output"
    }

    $items = $match.Groups["items"].Value.Split(",", [System.StringSplitOptions]::RemoveEmptyEntries) |
        ForEach-Object { $_.Trim() }
    if ($items.Count -ne 1) {
        throw "Expected exactly one region for table '$TableName', got: $($items -join ', ')"
    }

    return $items[0]
}

function Get-PrimaryAddress {
    param(
        [Parameter(Mandatory = $true)]
        [string]$TableName,
        [Parameter(Mandatory = $true)]
        [string]$RegionId
    )

    $output = Invoke-ZkCli -Commands @("get /minisql/tables/$TableName/regions/$RegionId/primary")
    $match = [regex]::Match($output, "(?<addr>localhost:\d+)")
    if (-not $match.Success) {
        throw "Could not read primary address for region '$RegionId'. Output:`n$output"
    }

    return $match.Groups["addr"].Value
}

function Get-RegionServerIndexFromPort {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    switch ($Port) {
        16020 { return 1 }
        16021 { return 2 }
        16022 { return 3 }
        default { throw "Unsupported RegionServer port: $Port" }
    }
}

function Restart-RegionServerByPort {
    param(
        [Parameter(Mandatory = $true)]
        [int]$Port
    )

    $projectRoot = Get-ProjectRoot
    $index = Get-RegionServerIndexFromPort -Port $Port
    Start-Process -FilePath "cmd.exe" `
        -ArgumentList "/c", "call scripts\start-regionserver.bat $index --skip-compile" `
        -WorkingDirectory $projectRoot | Out-Null
    Wait-Port -Port $Port
    Start-Sleep -Seconds 10
}

function Assert-Contains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [string]$Expected,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if ($Text -notmatch [regex]::Escape($Expected)) {
        throw "$Message`nExpected to find: $Expected`nActual output:`n$Text"
    }
}

function Assert-NotContains {
    param(
        [Parameter(Mandatory = $true)]
        [string]$Text,
        [Parameter(Mandatory = $true)]
        [string]$Unexpected,
        [Parameter(Mandatory = $true)]
        [string]$Message
    )

    if ($Text -match [regex]::Escape($Unexpected)) {
        throw "$Message`nUnexpected text: $Unexpected`nActual output:`n$Text"
    }
}
