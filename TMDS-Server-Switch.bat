@echo off
setlocal
cd /d "%~dp0"
set "ROOT=%CD%"

powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$ErrorActionPreference = 'Stop';" ^
  "$root = $env:ROOT;" ^
  "$currentPid = $PID;" ^
  "$parentPid = (Get-CimInstance Win32_Process -Filter ('ProcessId=' + $PID)).ParentProcessId;" ^
  "$names = \"Name = 'electron.exe' OR Name = 'node.exe' OR Name = 'cmd.exe' OR Name = 'powershell.exe' OR Name = 'wscript.exe'\";" ^
  "$serverProcesses = @(Get-CimInstance Win32_Process -Filter $names | Where-Object {" ^
  "  $_.ProcessId -ne $currentPid -and $_.ProcessId -ne $parentPid -and $_.CommandLine -and $_.CommandLine -like ('*' + $root + '*') -and" ^
  "  ($_.CommandLine -match 'start-hosted-webapp|start-webapp|node_modules\\.bin\\electron|node_modules\\electron\\dist\\electron|dist\\main\\main\.js')" ^
  "});" ^
  "if ($serverProcesses.Count -gt 0) {" ^
  "  Write-Host 'TMDS server is ON. Turning it OFF...';" ^
  "  foreach ($process in $serverProcesses) {" ^
  "    Write-Host ('Stopping ' + $process.Name + ' (PID ' + $process.ProcessId + ')');" ^
  "    Start-Process -FilePath 'taskkill.exe' -ArgumentList @('/F','/T','/PID',[string]$process.ProcessId) -Wait -WindowStyle Hidden;" ^
  "  }" ^
  "  Write-Host ''; Write-Host 'TMDS server is now OFF.';" ^
  "} else {" ^
  "  Write-Host 'TMDS server is OFF. Turning it ON in the background (this also pulls the latest update)...';" ^
  "  $launcher = Join-Path $root 'scripts\\hide-launcher.vbs';" ^
  "  $target = Join-Path $root 'scripts\\start-hosted-webapp.ps1';" ^
  "  Start-Process -FilePath 'wscript.exe' -ArgumentList ('\"' + $launcher + '\" \"' + $target + '\"') -WindowStyle Hidden;" ^
  "  Write-Host ''; Write-Host 'TMDS server is starting. Use this same switch again to turn it OFF.';" ^
  "}"

echo.
ping -n 6 127.0.0.1 >nul
