# =============================================================
# schedule_windows.ps1 — Windows Task Scheduler Setup
#
# Run this ONCE to register the automated weekly pipeline.
# Open PowerShell as Administrator and run:
#   powershell -ExecutionPolicy Bypass -File scripts\schedule_windows.ps1
#
# This creates two scheduled tasks:
#   1. HoldingsReportAgent       — Sunday 18:00 IST (12:30 UTC) — full pipeline
#   2. HoldingsReportAgentCheck  — Sunday 12:00 IST (06:30 UTC) — pre-run validation
# =============================================================

$ErrorActionPreference = "Stop"

# ── Configuration ─────────────────────────────────────────────────────────────
$ProjectRoot = Split-Path -Parent $PSScriptRoot
$PythonExe   = (Get-Command python -ErrorAction SilentlyContinue).Source
$MainScript  = Join-Path $ProjectRoot "main.py"
$LogDir      = Join-Path $ProjectRoot "logs"
$TaskLogFile = Join-Path $LogDir "task_scheduler.log"

# ── Validation ────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=========================================="
Write-Host "  Holdings Report Agent — Task Scheduler"
Write-Host "=========================================="
Write-Host ""

if (-not $PythonExe) {
    Write-Error "Python not found in PATH. Install Python 3.11+ and try again."
    exit 1
}
Write-Host "  Python   : $PythonExe"
Write-Host "  Project  : $ProjectRoot"
Write-Host "  Log file : $TaskLogFile"
Write-Host ""

# Ensure log directory exists
New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

# ── Task 1: Main Pipeline — Every Sunday at 18:00 IST (12:30 UTC) ─────────────
# Note: IST = UTC+5:30, so 18:00 IST = 12:30 UTC
# Windows Task Scheduler uses LOCAL time. This is set to 18:00 India time.
# If your machine runs on a different timezone, adjust accordingly.

$TaskName1   = "HoldingsReportAgent"
$TaskDesc1   = "Weekly portfolio digest — fetches holdings, enriches with news and LLM, sends email."

$Action1 = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "`"$MainScript`" --scheduled" `
    -WorkingDirectory $ProjectRoot

# Every Sunday at 18:00 (local time — set your PC timezone to IST)
$Trigger1 = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Sunday `
    -At "18:00"

$Settings1 = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 15) `
    -RestartCount 2 `
    -RestartInterval (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable `
    -WakeToRun $false

# Run as current user (no password needed for personal use)
$Principal1 = New-ScheduledTaskPrincipal `
    -UserId ([System.Security.Principal.WindowsIdentity]::GetCurrent().Name) `
    -LogonType Interactive `
    -RunLevel Highest

# Remove existing task if present
Unregister-ScheduledTask -TaskName $TaskName1 -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $TaskName1 `
    -Description $TaskDesc1 `
    -Action $Action1 `
    -Trigger $Trigger1 `
    -Settings $Settings1 `
    -Principal $Principal1 | Out-Null

Write-Host "  [OK] Task registered: $TaskName1"
Write-Host "       Runs: Every Sunday at 18:00 (local time)"

# ── Task 2: Pre-run Check — Sunday at 12:00 IST ───────────────────────────────
# Validates environment and sends a warning email if anything is broken.
# Gives you 6 hours to fix issues before the main pipeline runs.

$CheckScript = Join-Path $ProjectRoot "scripts\pre_run_check.py"
$TaskName2   = "HoldingsReportAgentCheck"
$TaskDesc2   = "Pre-pipeline check — validates API keys and environment are ready."

$Action2 = New-ScheduledTaskAction `
    -Execute $PythonExe `
    -Argument "`"$CheckScript`"" `
    -WorkingDirectory $ProjectRoot

$Trigger2 = New-ScheduledTaskTrigger `
    -Weekly `
    -DaysOfWeek Sunday `
    -At "12:00"

$Settings2 = New-ScheduledTaskSettingsSet `
    -ExecutionTimeLimit (New-TimeSpan -Minutes 5) `
    -StartWhenAvailable `
    -RunOnlyIfNetworkAvailable

Unregister-ScheduledTask -TaskName $TaskName2 -Confirm:$false -ErrorAction SilentlyContinue

Register-ScheduledTask `
    -TaskName $TaskName2 `
    -Description $TaskDesc2 `
    -Action $Action2 `
    -Trigger $Trigger2 `
    -Settings $Settings2 `
    -Principal $Principal1 | Out-Null

Write-Host "  [OK] Task registered: $TaskName2"
Write-Host "       Runs: Every Sunday at 12:00 (pre-run health check)"

# ── Summary ───────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "=========================================="
Write-Host "  Setup complete!"
Write-Host ""
Write-Host "  Upcoming runs:"
Write-Host "    12:00 IST Sunday — Environment check"
Write-Host "    18:00 IST Sunday — Full pipeline + email"
Write-Host ""
Write-Host "  To verify:"
Write-Host "    Get-ScheduledTask -TaskName 'HoldingsReportAgent'"
Write-Host ""
Write-Host "  To run immediately (test):"
Write-Host "    Start-ScheduledTask -TaskName 'HoldingsReportAgent'"
Write-Host ""
Write-Host "  To remove:"
Write-Host "    Unregister-ScheduledTask -TaskName 'HoldingsReportAgent'"
Write-Host "    Unregister-ScheduledTask -TaskName 'HoldingsReportAgentCheck'"
Write-Host "=========================================="
Write-Host ""

# ── IMPORTANT: Timezone note ──────────────────────────────────────────────────
$tz = [System.TimeZoneInfo]::Local.Id
Write-Host "  NOTE: Your machine timezone is: $tz"
if ($tz -notlike "*India*" -and $tz -notlike "*Kolkata*") {
    Write-Host ""
    Write-Host "  WARNING: Your machine is NOT set to IST."
    Write-Host "  The tasks above run at LOCAL time 18:00."
    Write-Host "  If you want 18:00 IST, either:"
    Write-Host "    a) Change your PC timezone to India Standard Time, OR"
    Write-Host "    b) Edit the trigger times above to your local equivalent"
    Write-Host "       (18:00 IST = 12:30 UTC = 07:30 EST = 04:30 PST)"
}
Write-Host ""
