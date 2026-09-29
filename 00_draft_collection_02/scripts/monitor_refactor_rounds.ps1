param([Parameter(Mandatory=$true)][string]$RunDir)
$ErrorActionPreference = 'Stop'
$Host.UI.RawUI.WindowTitle = 'Codex sequential prompt queue'
$statusPath = Join-Path $RunDir 'status.json'
$heartbeatPath = Join-Path $RunDir 'monitor.heartbeat'
try {
    while ($true) {
        [System.IO.File]::WriteAllText($heartbeatPath, [DateTime]::UtcNow.ToString('o'))
        $stream = [System.IO.File]::Open($statusPath, [System.IO.FileMode]::Open,
            [System.IO.FileAccess]::Read,
            [System.IO.FileShare]::ReadWrite -bor [System.IO.FileShare]::Delete)
        try {
            $reader = [System.IO.StreamReader]::new($stream, [System.Text.Encoding]::UTF8)
            try { $status = $reader.ReadToEnd() | ConvertFrom-Json }
            finally { $reader.Dispose() }
        } finally { $stream.Dispose() }
        $now = [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() / 1000.0
        Clear-Host
        Write-Host 'Codex: bounded sequential prompt queue'
        Write-Host ('State: {0} | Message: {1}/{3} | Completed: {2}/{3}' -f $status.state, $status.round, $status.completed_rounds, $status.total_rounds)
        if ($status.target) { Write-Host ('Script: {0} | Stage: {1}/5' -f $status.target, $status.script_round) }
        Write-Host ('Phase: {0} | Elapsed: {1:N0}s | Heartbeat age: {2:N1}s' -f $status.phase, ($now - $status.started_at), ($now - $status.heartbeat_at))
        Write-Host ('Worker: {0} | Transport child: {1} | Event: {2}' -f $status.worker_pid, $status.child_pid, $status.last_event)
        Write-Host ('Evidence: {0}' -f $RunDir)
        Write-Host 'Close this window to stop sending further prompts. A desktop turn already sent may continue.'
        if ($status.summary) { Write-Host $status.summary }
        if ($status.error) { Write-Host $status.error -ForegroundColor Red }
        if ($status.state -in @('completed', 'failed')) { break }
        if (($now - $status.heartbeat_at) -gt 20) { throw 'Worker heartbeat expired.' }
        Start-Sleep -Seconds 2
    }
} catch { Write-Host $_ -ForegroundColor Red }
Read-Host 'Batch stopped. Press Enter to close'
