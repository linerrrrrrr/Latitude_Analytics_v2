param(
    [Parameter(Mandatory = $true)]
    [string]$RunRoot,

    [ValidateRange(1, 3600)]
    [int]$RefreshSeconds = 2,

    [switch]$Once
)

$ErrorActionPreference = 'Stop'
$resolvedRunRoot = [System.IO.Path]::GetFullPath($RunRoot)
$statusPath = Join-Path $resolvedRunRoot 'status.json'
$monitorPidPath = Join-Path $resolvedRunRoot 'monitor.pid'

# -Once is a strictly read-only renderer for diagnostics and tests.  Only the
# interactive monitor creates the run directory and publishes its PID.
if (-not $Once) {
    New-Item -ItemType Directory -Path $resolvedRunRoot -Force | Out-Null
    [System.IO.File]::WriteAllText($monitorPidPath, ([string]$PID + "`n"))
    try {
        $host.UI.RawUI.WindowTitle = 'Latitude formal collection monitor'
    }
    catch {
        # A host without a mutable title is still a user-visible terminal.
    }
}

while ($true) {
    if (-not $Once) {
        Clear-Host
    }
    Write-Host 'Latitude 正式采集监控' -ForegroundColor Cyan
    Write-Host ('Run root : ' + $resolvedRunRoot)
    if (-not $Once) {
        Write-Host ('Monitor  : PID ' + $PID)
    }
    Write-Host ''

    if (-not (Test-Path -LiteralPath $statusPath -PathType Leaf)) {
        Write-Host '状态     : 等待 worker 创建 status.json' -ForegroundColor Yellow
        if ($Once) { return }
        Start-Sleep -Seconds $RefreshSeconds
        continue
    }

    try {
        # This sharing mode is mandatory: the worker must remain able to
        # atomically replace status.json while the monitor reads it.
        $share = [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
        $stream = [System.IO.File]::Open(
            $statusPath,
            [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            $share
        )
        $reader = [System.IO.StreamReader]::new(
            $stream,
            [System.Text.UTF8Encoding]::new($false),
            $true,
            4096,
            $false
        )
        try {
            $statusText = $reader.ReadToEnd()
        }
        finally {
            $reader.Dispose()
        }
        $status = $statusText | ConvertFrom-Json
    }
    catch {
        Write-Host ('状态读取失败: ' + $_.Exception.Message) -ForegroundColor Red
        if ($Once) { return }
        Start-Sleep -Seconds $RefreshSeconds
        continue
    }

    $propertyNames = @($status.PSObject.Properties.Name)
    $protocolVersion = 'legacy'
    if ($propertyNames -contains 'protocol_version') {
        $protocolVersion = [string]$status.protocol_version
    }
    $operationName = 'legacy_collection_batch'
    if ($propertyNames -contains 'operation_name') {
        $operationName = [string]$status.operation_name
    }
    elseif ($propertyNames -contains 'mode') {
        $operationName = 'legacy_' + [string]$status.mode + '_collection_batch'
    }
    $phase = 'legacy'
    if ($propertyNames -contains 'phase') {
        $phase = [string]$status.phase
    }
    elseif ($status.state -eq 'running') {
        $phase = 'business'
    }
    elseif ($null -ne $status.state) {
        $phase = [string]$status.state
    }

    $now = [DateTimeOffset]::UtcNow
    $elapsedText = 'unknown'
    if ($null -ne $status.started_at) {
        try {
            $started = [DateTimeOffset]::Parse([string]$status.started_at)
            $elapsed = $now - $started
            $elapsedText = '{0:00}:{1:00}:{2:00}' -f `
                [int]$elapsed.TotalHours, $elapsed.Minutes, $elapsed.Seconds
        }
        catch {
            $elapsedText = 'invalid timestamp'
        }
    }
    $heartbeatAge = $null
    if ($null -ne $status.heartbeat_at) {
        try {
            $heartbeat = [DateTimeOffset]::Parse([string]$status.heartbeat_at)
            $heartbeatAge = [Math]::Max(
                0,
                [Math]::Round(($now - $heartbeat).TotalSeconds, 1)
            )
        }
        catch {
            $heartbeatAge = $null
        }
    }

    $workerAlive = $false
    if ($null -ne $status.worker_pid) {
        $workerAlive = $null -ne (
            Get-Process -Id $status.worker_pid -ErrorAction SilentlyContinue
        )
    }
    $childAlive = $false
    if ($null -ne $status.child_pid) {
        $childAlive = $null -ne (
            Get-Process -Id $status.child_pid -ErrorAction SilentlyContinue
        )
    }

    $stateColor = 'Green'
    if ($status.state -in @('failed', 'quota_stopped', 'interrupted')) {
        $stateColor = 'Red'
    }
    elseif ($status.state -in @('starting', 'waiting_for_monitor', 'running')) {
        $stateColor = 'Yellow'
    }

    Write-Host ('协议     : ' + $protocolVersion)
    Write-Host ('操作     : ' + $operationName)
    Write-Host ('状态     : ' + $status.state) -ForegroundColor $stateColor
    Write-Host ('Phase    : ' + $phase)
    Write-Host (
        '阶段     : ' + $status.stage_index + '/' + $status.stage_total +
        '  ' + $status.stage_name
    )
    Write-Host ('进度     : ' + $status.progress)
    Write-Host ('耗时     : ' + $elapsedText)
    if ($null -eq $heartbeatAge) {
        Write-Host '心跳     : unknown' -ForegroundColor Red
    }
    elseif ($heartbeatAge -le 10) {
        Write-Host ('心跳     : ' + $heartbeatAge + ' 秒前') -ForegroundColor Green
    }
    else {
        Write-Host ('心跳     : ' + $heartbeatAge + ' 秒前（过期）') -ForegroundColor Red
    }
    Write-Host ('Worker   : PID ' + $status.worker_pid + ' alive=' + $workerAlive)
    Write-Host ('Child    : PID ' + $status.child_pid + ' alive=' + $childAlive)
    Write-Host ''
    Write-Host '最近业务输出:' -ForegroundColor Cyan
    foreach ($line in @($status.recent_lines)) {
        Write-Host ('  ' + $line)
    }

    if ($null -ne $status.error) {
        Write-Host ''
        Write-Host ('故障     : ' + $status.error) -ForegroundColor Red
    }

    if ($Once) { return }
    if ($status.state -in @('succeeded', 'failed', 'quota_stopped', 'interrupted')) {
        Write-Host ''
        Write-Host '任务已经进入终态；现场已保留。请按 Enter 关闭窗口。' -ForegroundColor Cyan
        Read-Host | Out-Null
        return
    }

    Start-Sleep -Seconds $RefreshSeconds
}

