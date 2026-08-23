[CmdletBinding(SupportsShouldProcess = $true, ConfirmImpact = "High")]
param(
    [ValidateSet(
        "List",
        "All",
        "RedisUnavailable",
        "PostgresUnavailable",
        "ImagePullFailure",
        "CommandExitOne",
        "TaskTimeout",
        "WorkerDeath",
        "ApiRestart",
        "DuplicateEnqueue",
        "StaleWorkerResult",
        "CancelRunning"
    )]
    [string]$Case = "List",
    [string]$BaseUrl = "http://localhost:8000",
    [int]$WaitTimeoutSeconds = 240,
    [int]$LeaseWaitSeconds = 40,
    [string]$PostgresUser = "task",
    [string]$PostgresDatabase = "task_platform"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$script:TerminalStatuses = @("succeeded", "failed", "cancelled", "timed_out")
$script:RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$script:UseWsl = $null -eq (Get-Command docker -ErrorAction SilentlyContinue)
$script:WslRepoRoot = $null

if ($script:UseWsl) {
    if ($null -eq (Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
        throw "docker is not on PATH and wsl.exe is unavailable"
    }
    $root = [System.IO.Path]::GetPathRoot($script:RepoRoot)
    if ([string]::IsNullOrWhiteSpace($root) -or $root.Length -lt 2) {
        throw "cannot translate repository path to WSL: $($script:RepoRoot)"
    }
    $drive = $root.Substring(0, 1).ToLowerInvariant()
    $relative = $script:RepoRoot.Substring($root.Length).Replace("\", "/")
    $script:WslRepoRoot = "/mnt/$drive/$relative"
}

function Invoke-Docker {
    param([Parameter(Mandatory)][string[]]$ArgumentList)

    if ($script:UseWsl) {
        $output = & wsl.exe --cd $script:WslRepoRoot -- docker @ArgumentList 2>&1
    }
    else {
        $output = & docker @ArgumentList 2>&1
    }
    if ($LASTEXITCODE -ne 0) {
        $rendered = $output -join [Environment]::NewLine
        $message = "docker $($ArgumentList -join ' ') failed with exit code $LASTEXITCODE"
        throw ($message + [Environment]::NewLine + $rendered)
    }
    return $output
}

function Invoke-Compose {
    param([Parameter(Mandatory)][string[]]$ArgumentList)
    return Invoke-Docker -ArgumentList (@("compose") + $ArgumentList)
}

function Invoke-Api {
    param(
        [Parameter(Mandatory)][string]$Method,
        [Parameter(Mandatory)][string]$Path,
        [object]$Body,
        [hashtable]$Headers
    )

    $parameters = @{
        Uri = "$($BaseUrl.TrimEnd('/'))$Path"
        Method = $Method
        TimeoutSec = 30
    }
    if ($PSBoundParameters.ContainsKey("Body")) {
        $parameters.ContentType = "application/json"
        $parameters.Body = $Body | ConvertTo-Json -Depth 20 -Compress
    }
    if ($null -ne $Headers) {
        $parameters.Headers = $Headers
    }
    return Invoke-RestMethod @parameters
}

function Assert-Condition {
    param(
        [Parameter(Mandatory)][bool]$Condition,
        [Parameter(Mandatory)][string]$Message
    )
    if (-not $Condition) {
        throw $Message
    }
}

function Wait-Health {
    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($WaitTimeoutSeconds)
    do {
        try {
            $health = Invoke-Api -Method "GET" -Path "/health"
            if ($health.status -eq "ok") {
                return $health
            }
        }
        catch {
            Write-Verbose "health request failed: $($_.Exception.Message)"
        }
        Start-Sleep -Seconds 1
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "platform did not become healthy within $WaitTimeoutSeconds seconds"
}

function Wait-Task {
    param(
        [Parameter(Mandatory)][string]$TaskId,
        [Parameter(Mandatory)][string[]]$Statuses
    )

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds($WaitTimeoutSeconds)
    $lastStatus = $null
    do {
        try {
            $task = Invoke-Api -Method "GET" -Path "/api/v1/tasks/$TaskId"
            $lastStatus = [string]$task.status
            if ($Statuses -contains $lastStatus) {
                return $task
            }
        }
        catch {
            Write-Verbose "task poll failed: $($_.Exception.Message)"
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "task $TaskId did not reach [$($Statuses -join ', ')]; last status=$lastStatus"
}

function Wait-TerminalTask {
    param([Parameter(Mandatory)][string]$TaskId)
    return Wait-Task -TaskId $TaskId -Statuses $script:TerminalStatuses
}

function New-FaultTask {
    param(
        [Parameter(Mandatory)][string]$Image,
        [Parameter(Mandatory)][string[]]$Command,
        [int]$TimeoutSeconds = 60,
        [int]$MaxRetries = 0
    )

    $parameters = @{
        Method = "POST"
        Path = "/api/v1/tasks"
        Headers = @{ "Idempotency-Key" = "fault-$([guid]::NewGuid().ToString('N'))" }
        Body = @{
            image = $Image
            command = $Command
            timeout_seconds = $TimeoutSeconds
            max_retries = $MaxRetries
            cpu_limit = 0.5
            memory_limit_mb = 128
        }
    }
    return Invoke-Api @parameters
}

function New-SleepTask {
    param(
        [int]$SleepSeconds,
        [int]$TimeoutSeconds
    )
    $code = "import time; print('fault-start', flush=True); " +
        "time.sleep($SleepSeconds); print('fault-end', flush=True)"
    $parameters = @{
        Image = "python:3.12-slim"
        Command = @("python", "-c", $code)
        TimeoutSeconds = $TimeoutSeconds
    }
    return New-FaultTask @parameters
}

function Find-TaskContainers {
    param([Parameter(Mandatory)][string]$TaskId)
    return @(
        Invoke-Docker -ArgumentList @(
            "ps",
            "-aq",
            "--filter",
            "label=mini-docker-cloud.task_id=$TaskId"
        )
    ) | Where-Object { -not [string]::IsNullOrWhiteSpace([string]$_) }
}

function Remove-TaskContainers {
    param([Parameter(Mandatory)][string]$TaskId)
    foreach ($containerId in @(Find-TaskContainers -TaskId $TaskId)) {
        Invoke-Docker -ArgumentList @("rm", "-f", [string]$containerId) | Out-Host
    }
}

function Wait-NoTaskContainers {
    param([Parameter(Mandatory)][string]$TaskId)

    $deadline = [DateTimeOffset]::UtcNow.AddSeconds(30)
    do {
        if (@(Find-TaskContainers -TaskId $TaskId).Count -eq 0) {
            return
        }
        Start-Sleep -Milliseconds 500
    } while ([DateTimeOffset]::UtcNow -lt $deadline)
    throw "task container for $TaskId still exists after cancellation"
}

function Invoke-PostgresSql {
    param([Parameter(Mandatory)][string]$Sql)
    return Invoke-Compose -ArgumentList @(
        "exec",
        "-T",
        "postgres",
        "psql",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        $PostgresUser,
        "-d",
        $PostgresDatabase,
        "-c",
        $Sql
    )
}

function Invoke-PythonInApi {
    param([Parameter(Mandatory)][string]$Code)

    $encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($Code))
    $runner = "import base64; exec(base64.b64decode('$encoded'))"
    return Invoke-Compose -ArgumentList @("exec", "-T", "api", "python", "-c", $runner)
}

function Test-RedisUnavailable {
    Write-Host "[Case 1] Redis temporarily unavailable"
    Invoke-Compose -ArgumentList @("stop", "redis") | Out-Host
    try {
        $task = New-SleepTask -SleepSeconds 1 -TimeoutSeconds 30
        $result = Wait-TerminalTask -TaskId ([string]$task.id)
        Assert-Condition ($result.status -eq "succeeded") (
            "PostgreSQL fallback did not complete the task: $($result.status)"
        )
        Write-Host "PASS task=$($task.id) status=$($result.status)"
    }
    finally {
        Invoke-Compose -ArgumentList @("start", "redis") | Out-Host
        Wait-Health | Out-Null
    }
}

function Test-PostgresUnavailable {
    Write-Host "[Case 2] PostgreSQL temporarily unavailable"
    Invoke-Compose -ArgumentList @("stop", "postgres") | Out-Host
    try {
        Start-Sleep -Seconds 2
        $requestFailed = $false
        try {
            Invoke-Api -Method "GET" -Path "/api/v1/tasks?limit=1" | Out-Null
        }
        catch {
            $requestFailed = $true
            Write-Host "Observed expected API failure: $($_.Exception.Message)"
        }
        Assert-Condition $requestFailed (
            "task query unexpectedly succeeded while PostgreSQL was down"
        )
        $healthResponse = Invoke-WebRequest `
            -Uri "$($BaseUrl.TrimEnd('/'))/health" `
            -Method Get `
            -TimeoutSec 30 `
            -SkipHttpErrorCheck
        Assert-Condition ($healthResponse.StatusCode -eq 503) (
            "degraded health endpoint did not return HTTP 503"
        )
        $health = $healthResponse.Content | ConvertFrom-Json
        Assert-Condition ($health.status -eq "degraded") "health endpoint did not report degraded"
        Write-Host "PASS API rejected DB-backed work and health reported degraded"
    }
    finally {
        Invoke-Compose -ArgumentList @("start", "postgres") | Out-Host
        Wait-Health | Out-Null
    }
}

function Test-ImagePullFailure {
    Write-Host "[Case 3] Docker image pull failure"
    $parameters = @{
        Image = "python:mini-docker-cloud-image-does-not-exist"
        Command = @("python", "-c", "print('unreachable')")
    }
    $task = New-FaultTask @parameters
    $result = Wait-TerminalTask -TaskId ([string]$task.id)
    Assert-Condition ($result.status -eq "failed") "image pull task ended as $($result.status)"
    Assert-Condition (-not [string]::IsNullOrWhiteSpace([string]$result.error_message)) (
        "image pull failure did not persist an error message"
    )
    Write-Host "PASS task=$($task.id) error=$($result.error_message)"
}

function Test-CommandExitOne {
    Write-Host "[Case 4] Task command exits with code 1"
    $parameters = @{
        Image = "python:3.12-slim"
        Command = @("python", "-c", "import sys; sys.exit(1)")
    }
    $task = New-FaultTask @parameters
    $result = Wait-TerminalTask -TaskId ([string]$task.id)
    Assert-Condition ($result.status -eq "failed") "exit-one task ended as $($result.status)"
    Assert-Condition ([int]$result.exit_code -eq 1) "expected exit_code=1, got $($result.exit_code)"
    Write-Host "PASS task=$($task.id) status=failed exit_code=1"
}

function Test-TaskTimeout {
    Write-Host "[Case 5] Running task timeout"
    Invoke-Docker -ArgumentList @("pull", "python:3.12-slim") | Out-Host
    $task = New-SleepTask -SleepSeconds 60 -TimeoutSeconds 2
    $result = Wait-TerminalTask -TaskId ([string]$task.id)
    Assert-Condition ($result.status -eq "timed_out") "timeout task ended as $($result.status)"
    Wait-NoTaskContainers -TaskId ([string]$task.id)
    Write-Host "PASS task=$($task.id) status=timed_out and container removed"
}

function Test-WorkerDeath {
    Write-Host "[Case 6] Worker dies during execution"
    Invoke-Docker -ArgumentList @("pull", "python:3.12-slim") | Out-Host
    $task = New-SleepTask -SleepSeconds 20 -TimeoutSeconds 120
    Wait-Task -TaskId ([string]$task.id) -Statuses @("running") | Out-Null
    Invoke-Compose -ArgumentList @("kill", "-s", "SIGKILL", "worker") | Out-Host
    Invoke-Compose -ArgumentList @("stop", "worker") | Out-Host
    Invoke-Compose -ArgumentList @("rm", "-f", "worker") | Out-Host
    try {
        Start-Sleep -Seconds $LeaseWaitSeconds
        Invoke-Compose -ArgumentList @("up", "-d", "worker") | Out-Host
        $result = Wait-TerminalTask -TaskId ([string]$task.id)
        Assert-Condition ($result.status -eq "succeeded") (
            "recovered worker task ended as $($result.status)"
        )
        Assert-Condition ([int]$result.recovery_count -ge 1) (
            "task completed without recording lease recovery"
        )
        Write-Host "PASS task=$($task.id) recovery_count=$($result.recovery_count)"
    }
    finally {
        Invoke-Compose -ArgumentList @("up", "-d", "worker") | Out-Host
        Remove-TaskContainers -TaskId ([string]$task.id)
    }
}

function Test-ApiRestart {
    Write-Host "[Case 7] API server restarts while a task runs"
    $task = New-SleepTask -SleepSeconds 5 -TimeoutSeconds 60
    Wait-Task -TaskId ([string]$task.id) -Statuses @("running") | Out-Null
    Invoke-Compose -ArgumentList @("restart", "api") | Out-Host
    Wait-Health | Out-Null
    $result = Wait-TerminalTask -TaskId ([string]$task.id)
    Assert-Condition ($result.status -eq "succeeded") "task ended as $($result.status)"
    Write-Host "PASS task=$($task.id) survived API restart"
}

function Test-DuplicateEnqueue {
    Write-Host "[Case 8] Duplicate Redis enqueue"
    $task = New-SleepTask -SleepSeconds 5 -TimeoutSeconds 60
    Wait-Task -TaskId ([string]$task.id) -Statuses @("running") | Out-Null
    $payload = @{ task_id = [string]$task.id } | ConvertTo-Json -Compress
    foreach ($index in 1..2) {
        $streamId = "$([DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds())-$index"
        $xaddResult = Invoke-Compose -ArgumentList @(
            "exec",
            "-T",
            "redis",
            "redis-cli",
            "XADD",
            "tasks:ready",
            $streamId,
            "event_id",
            "manual-$([guid]::NewGuid())",
            "event_type",
            "task.ready",
            "payload",
            $payload,
            "task_id",
            [string]$task.id
        )
        $xaddResult | Out-Host
        Assert-Condition (-not (($xaddResult | Out-String) -match "ERR")) (
            "duplicate XADD failed: $xaddResult"
        )
    }
    $result = Wait-TerminalTask -TaskId ([string]$task.id)
    Assert-Condition ($result.status -eq "succeeded") (
        "duplicate enqueue task ended as $($result.status)"
    )
    $logs = Invoke-Api -Method "GET" -Path "/api/v1/tasks/$($task.id)/logs?limit=5000"
    $starts = @($logs.logs | Where-Object { $_.content -match "^container .* started$" })
    Assert-Condition ($starts.Count -eq 1) (
        "duplicate queue messages produced $($starts.Count) container starts"
    )
    Write-Host "PASS task=$($task.id) had one accepted execution"
}

function Test-StaleWorkerResult {
    Write-Host "[Case 9] Stale Worker result"
    Invoke-Docker -ArgumentList @("pull", "python:3.12-slim") | Out-Host
    $task = New-SleepTask -SleepSeconds 20 -TimeoutSeconds 120
    $old = Wait-Task -TaskId ([string]$task.id) -Statuses @("running")
    $oldExecution = [string]$old.execution_id
    $oldWorker = [string]$old.worker_id

    Invoke-Compose -ArgumentList @("kill", "-s", "SIGKILL", "worker") | Out-Host
    Invoke-Compose -ArgumentList @("stop", "worker") | Out-Host
    Invoke-Compose -ArgumentList @("rm", "-f", "worker") | Out-Host
    try {
        $sql = "UPDATE tasks SET lease_expires_at = NOW() - INTERVAL '1 second' " +
            "WHERE id = '$($task.id)';"
        Invoke-PostgresSql $sql | Out-Host
        Start-Sleep -Seconds 7
        Invoke-Compose -ArgumentList @("up", "-d", "worker") | Out-Host

        $deadline = [DateTimeOffset]::UtcNow.AddSeconds($WaitTimeoutSeconds)
        $current = $null
        do {
            $candidate = Invoke-Api -Method "GET" -Path "/api/v1/tasks/$($task.id)"
            if (
                -not [string]::IsNullOrWhiteSpace([string]$candidate.execution_id) -and
                [string]$candidate.execution_id -ne $oldExecution
            ) {
                $current = $candidate
                break
            }
            Start-Sleep -Milliseconds 500
        } while ([DateTimeOffset]::UtcNow -lt $deadline)
        Assert-Condition ($null -ne $current) "task never received a new execution_id"

        $workerJson = $oldWorker | ConvertTo-Json -Compress
        $python = @"
import asyncio
import json
import uuid
from core.config import get_settings
from core.database import Database
from core.enums import TaskStatus
from repositories.tasks import TaskRepository

async def check():
    database = Database(get_settings().database_url)
    try:
        async with database.session() as session, session.begin():
            result = await TaskRepository.finish_execution(
                session,
                task_id=uuid.UUID("$($task.id)"),
                worker_id=$workerJson,
                execution_id=uuid.UUID("$oldExecution"),
                target=TaskStatus.SUCCEEDED,
                exit_code=0,
                error_message=None,
                retry_max_backoff_seconds=60.0,
                cpu_price_per_hour=0.05,
                gpu_price_per_hour=1.0,
            )
        print(json.dumps({"accepted": result.accepted, "status": result.status}))
    finally:
        await database.dispose()

asyncio.run(check())
"@
        $output = @(Invoke-PythonInApi -Code $python)
        $fencing = $output[-1] | ConvertFrom-Json
        Assert-Condition (-not [bool]$fencing.accepted) "stale execution result was accepted"
        Write-Host (
            "PASS task=$($task.id) old_execution=$oldExecution " +
            "current_execution=$($current.execution_id)"
        )

        try {
            Invoke-Api -Method "POST" -Path "/api/v1/tasks/$($task.id)/cancel" | Out-Null
        }
        catch {
            Write-Verbose "task became terminal before cleanup cancellation"
        }
        Wait-TerminalTask -TaskId ([string]$task.id) | Out-Null
    }
    finally {
        Invoke-Compose -ArgumentList @("up", "-d", "worker") | Out-Host
        Remove-TaskContainers -TaskId ([string]$task.id)
    }
}

function Test-CancelRunning {
    Write-Host "[Case 10] User cancels a running task"
    Invoke-Docker -ArgumentList @("pull", "python:3.12-slim") | Out-Host
    $task = New-SleepTask -SleepSeconds 60 -TimeoutSeconds 120
    Wait-Task -TaskId ([string]$task.id) -Statuses @("running") | Out-Null
    Invoke-Api -Method "POST" -Path "/api/v1/tasks/$($task.id)/cancel" | Out-Null
    $result = Wait-TerminalTask -TaskId ([string]$task.id)
    Assert-Condition ($result.status -eq "cancelled") (
        "cancelled task ended as $($result.status)"
    )
    Wait-NoTaskContainers -TaskId ([string]$task.id)
    Write-Host "PASS task=$($task.id) status=cancelled and container removed"
}

function Invoke-FaultCase {
    param([Parameter(Mandatory)][string]$Name)
    switch ($Name) {
        "RedisUnavailable" { Test-RedisUnavailable }
        "PostgresUnavailable" { Test-PostgresUnavailable }
        "ImagePullFailure" { Test-ImagePullFailure }
        "CommandExitOne" { Test-CommandExitOne }
        "TaskTimeout" { Test-TaskTimeout }
        "WorkerDeath" { Test-WorkerDeath }
        "ApiRestart" { Test-ApiRestart }
        "DuplicateEnqueue" { Test-DuplicateEnqueue }
        "StaleWorkerResult" { Test-StaleWorkerResult }
        "CancelRunning" { Test-CancelRunning }
        default { throw "unknown fault case: $Name" }
    }
}

$caseNames = @(
    "RedisUnavailable",
    "PostgresUnavailable",
    "ImagePullFailure",
    "CommandExitOne",
    "TaskTimeout",
    "WorkerDeath",
    "ApiRestart",
    "DuplicateEnqueue",
    "StaleWorkerResult",
    "CancelRunning"
)

if ($Case -eq "List") {
    $caseNames | ForEach-Object { Write-Output $_ }
    return
}

$target = if ($script:UseWsl) {
    "Compose project mini-docker-cloud through WSL"
}
else {
    "Compose project mini-docker-cloud"
}
if (-not $PSCmdlet.ShouldProcess($target, "run destructive fault injection case $Case")) {
    return
}

Push-Location $script:RepoRoot
try {
    Invoke-Compose -ArgumentList @("config", "--quiet") | Out-Null
    Wait-Health | Out-Null
    $selected = if ($Case -eq "All") { $caseNames } else { @($Case) }
    foreach ($name in $selected) {
        Invoke-FaultCase -Name $name
    }
}
finally {
    Pop-Location
}
