param(
    [string]$OutputPath = (Join-Path $PSScriptRoot "..\..\exports\system_sql_probe.txt")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$sqlcmd = (Get-Command sqlcmd -ErrorAction Stop).Source
$lines = @()
$lines += "WHOAMI: $(whoami)"

try {
    $result = & $sqlcmd -S localhost -E -Q "SET NOCOUNT ON; SELECT SUSER_SNAME() AS login_name, SYSTEM_USER AS system_user, @@SERVERNAME AS server_name, DB_NAME() AS db_name;"
    $lines += "SQLCMD_OK"
    $lines += ($result | Out-String)
} catch {
    $lines += "SQLCMD_FAIL"
    $lines += $_.Exception.Message
}

$dir = Split-Path -Parent $OutputPath
if (-not (Test-Path $dir)) {
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
}

$lines | Set-Content -Encoding UTF8 -Path $OutputPath
Write-Host $OutputPath
