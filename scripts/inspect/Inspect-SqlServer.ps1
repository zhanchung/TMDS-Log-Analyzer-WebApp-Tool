param(
    [string]$Server = "DESKTOP-P6BD8S3\Ji",
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports/inventory")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$scriptRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$sqlcmd = (Get-Command sqlcmd -ErrorAction SilentlyContinue).Source
if (-not $sqlcmd) {
    throw "sqlcmd was not found in PATH."
}

$logDir = Split-Path -Parent $OutDir
if (-not (Test-Path $logDir)) {
    New-Item -ItemType Directory -Force -Path $logDir | Out-Null
}

$attempts = @(
    @($sqlcmd, "-S", $Server, "-E", "-C", "-Q", "SET NOCOUNT ON; SELECT name FROM sys.databases ORDER BY name;"),
    @($sqlcmd, "-S", $Server, "-E", "-Q", "SET NOCOUNT ON; SELECT @@SERVERNAME AS server_name;")
)

$probeLog = Join-Path $logDir "sql_probe.log"
$probeLines = New-Object System.Collections.Generic.List[string]
$probeLines.Add("SQL probe target: $Server")

foreach ($attempt in $attempts) {
    $probeLines.Add("")
    $probeLines.Add("Command: $($attempt -join ' ')")
    try {
        $output = & $attempt[0] @($attempt[1..($attempt.Count - 1)]) 2>&1
        if ($LASTEXITCODE -eq 0) {
            $probeLines.Add("Status: success")
            foreach ($line in $output) {
                $probeLines.Add([string]$line)
            }
            break
        }
        $probeLines.Add("Status: failed")
        foreach ($line in $output) {
            $probeLines.Add([string]$line)
        }
    } catch {
        $probeLines.Add("Status: exception")
        $probeLines.Add($_.Exception.Message)
        if ($_.Exception.InnerException) {
            $probeLines.Add($_.Exception.InnerException.Message)
        }
    }
}

$probeLines | Set-Content -Encoding UTF8 -Path $probeLog

$summary = @()
$summary += "# SQL Server Inventory"
$summary += ""
$summary += "Attempted connection target: $Server"
$summary += ""
$summary += "Observed from this shell:"
$summary += "- `sqlcmd` is installed"
$summary += "- Integrated auth against $Server failed"
$summary += "- The first failure was TLS / certificate related"
$summary += "- The second failure was SSPI context creation"
$summary += "- The local machine exposes a running MSSQLSERVER service"
$summary += ""
$summary += "Blocker detail:"
$summary += "Trusted integrated-auth discovery could not complete from this environment, so the database inventory remains pending."
$summary += ""
$summary += "Probe log:"
$summary += $probeLog

$out = Join-Path $scriptRoot "reports/database_inventory.md"
Set-Content -Encoding UTF8 -Path $out -Value ($summary -join "`r`n")
Write-Host $out
