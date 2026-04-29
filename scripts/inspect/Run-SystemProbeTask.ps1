param(
    [string]$TaskName = "TMDSSystemProbe",
    [string]$ScriptPath = "C:\Users\Ji\.codex\memories\Run-SqlAsSystem.cmd",
    [string]$OutputPath = "D:\NCTD TMDS Decoder\exports\system_sql_probe.txt"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

& schtasks.exe /Create /TN $TaskName /SC ONCE /ST 23:59 /RU SYSTEM /RL HIGHEST /TR "cmd /c `"$ScriptPath`"" /F | Out-Null
& schtasks.exe /Run /TN $TaskName | Out-Null
Start-Sleep -Seconds 10

if (Test-Path $OutputPath) {
    Get-Content $OutputPath
} else {
    throw "Probe output file was not created: $OutputPath"
}
