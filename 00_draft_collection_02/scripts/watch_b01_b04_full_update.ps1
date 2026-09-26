param(
    [Parameter(Mandatory = $true)]
    [string]$RunRoot,

    [int]$RefreshSeconds = 2,

    [switch]$Once
)

$ErrorActionPreference = 'Stop'
$resolvedRunRoot = [System.IO.Path]::GetFullPath($RunRoot)
$statusPath = Join-Path $resolvedRunRoot 'status.json'
$monitorPidPath = Join-Path $resolvedRunRoot 'monitor.pid'

New-Item -ItemType Directory -Path $resolvedRunRoot -Force | Out-Null
[System.IO.File]::WriteAllText($monitorPidPath, [string]$PID)
$host.UI.RawUI.WindowTitle = 'Latitude data update monitor'

while ($true) {
    # 正常监控刷新可见窗口；-Once 用于无副作用的渲染测试，不清屏。
    if (-not $Once) {
        Clear-Host
    }
    Write-Host 'Latitude 数据更新' -ForegroundColor Cyan
    Write-Host ('Run root : ' + $resolvedRunRoot)
    Write-Host ('Monitor  : PID ' + $PID)
    Write-Host ''

    if (-not (Test-Path -LiteralPath $statusPath)) {
        Write-Host '状态     : 等待 worker 创建 status.json' -ForegroundColor Yellow
        if ($Once) { return }
        Start-Sleep -Seconds $RefreshSeconds
        continue
    }

    try {
        # 允许 worker 在 monitor 读取期间原子替换 status.json。
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

    $now = [DateTimeOffset]::UtcNow
    $started = [DateTimeOffset]::Parse($status.started_at)
    $heartbeat = [DateTimeOffset]::Parse($status.heartbeat_at)
    $elapsed = $now - $started
    $heartbeatAge = [Math]::Max(0, [Math]::Round(($now - $heartbeat).TotalSeconds, 1))
    $workerAlive = $null -ne (Get-Process -Id $status.worker_pid -ErrorAction SilentlyContinue)
    $childAlive = $false
    if ($null -ne $status.child_pid) {
        $childAlive = $null -ne (Get-Process -Id $status.child_pid -ErrorAction SilentlyContinue)
    }

    $stateColor = 'Green'
    if ($status.state -in @('failed', 'quota_stopped')) { $stateColor = 'Red' }
    elseif ($status.state -in @('starting', 'waiting_for_monitor', 'running')) { $stateColor = 'Yellow' }

    Write-Host ('状态     : ' + $status.state) -ForegroundColor $stateColor
    Write-Host ('阶段     : ' + $status.stage_index + '/' + $status.stage_total + '  ' + $status.stage_name)
    Write-Host ('进度     : ' + $status.progress)
    Write-Host ('耗时     : {0:00}:{1:00}:{2:00}' -f [int]$elapsed.TotalHours, $elapsed.Minutes, $elapsed.Seconds)
    if ($heartbeatAge -le 10) {
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
    if ($status.state -in @('succeeded', 'failed', 'quota_stopped')) {
        Write-Host ''
        Write-Host '任务已经进入终态；现场已保留。请按 Enter 关闭窗口。' -ForegroundColor Cyan
        Read-Host | Out-Null
        return
    }

    Start-Sleep -Seconds $RefreshSeconds
}
