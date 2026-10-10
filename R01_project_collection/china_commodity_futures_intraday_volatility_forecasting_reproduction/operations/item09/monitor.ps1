param(
    [Parameter(Mandatory = $true)]
    [string]$StatusPath,
    [Parameter(Mandatory = $true)]
    [string]$MonitorPidPath
)

$ErrorActionPreference = "Stop"
[System.IO.File]::WriteAllText(
    $MonitorPidPath,
    [string]$PID,
    [System.Text.UTF8Encoding]::new($false)
)

function Read-SharedStatus {
    if (-not (Test-Path -LiteralPath $StatusPath)) {
        return $null
    }
    $fileStream = [System.IO.File]::Open(
        $StatusPath,
        [System.IO.FileMode]::Open,
        [System.IO.FileAccess]::Read,
        [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete
    )
    try {
        $reader = [System.IO.StreamReader]::new(
            $fileStream,
            [System.Text.UTF8Encoding]::new($false),
            $true
        )
        try {
            return $reader.ReadToEnd() | ConvertFrom-Json
        }
        finally {
            $reader.Dispose()
        }
    }
    finally {
        $fileStream.Dispose()
    }
}

while ($true) {
    try {
        $status = Read-SharedStatus
        Clear-Host
        Write-Host "Item 09 - Rolling Variance Forecasts" -ForegroundColor Cyan
        Write-Host "Status: $StatusPath"
        Write-Host "Monitor PID: $PID"
        Write-Host ""
        if ($null -eq $status) {
            Write-Host "Waiting for worker status..."
        }
        else {
            $elapsed = [TimeSpan]::FromSeconds([double]$status.elapsed_seconds)
            $streamPercent = if ([int]$status.total_streams -gt 0) {
                100.0 * [int]$status.completed_streams / [int]$status.total_streams
            }
            else { 0.0 }
            $fitPercent = if ([int]$status.total_selected_fits -gt 0) {
                100.0 * [int]$status.completed_selected_fits / [int]$status.total_selected_fits
            }
            else { 0.0 }
            Write-Host ("Phase: {0}" -f $status.phase)
            Write-Host ("Notebook cell index: {0}" -f $status.current_cell_index)
            Write-Host ("Streams: {0}/{1} ({2:N2}%)" -f $status.completed_streams, $status.total_streams, $streamPercent)
            Write-Host ("Persisted this run: reused={0}; new commits={1}" -f $status.reused_streams, $status.newly_committed_streams)
            Write-Host ("Selected fits: {0}/{1} ({2:N2}%)" -f $status.completed_selected_fits, $status.total_selected_fits, $fitPercent)
            Write-Host ("Last completed: {0} / {1}" -f $status.last_series_id, $status.last_model_id)
            Write-Host ("Elapsed: {0:dd\.hh\:mm\:ss}" -f $elapsed)
            Write-Host ("Heartbeat UTC: {0}" -f $status.heartbeat_at)
            Write-Host ("Worker PID: {0}" -f $status.worker_pid)
            if ($null -ne $status.failure) {
                Write-Host ""
                Write-Host "Failure:" -ForegroundColor Red
                Write-Host $status.failure -ForegroundColor Red
            }
            if ($status.phase -in @("succeeded", "failed", "monitor_failed")) {
                Write-Host ""
                Write-Host "Terminal state reached. This window will remain for 60 seconds." -ForegroundColor Yellow
                Start-Sleep -Seconds 60
                break
            }
        }
    }
    catch {
        Write-Host "Monitor read error: $($_.Exception.Message)" -ForegroundColor Red
    }
    Start-Sleep -Seconds 2
}
