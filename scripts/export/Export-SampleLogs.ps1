Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$sampleRoot = Join-Path $root "sample_logs/curated"
$outRoot = Join-Path $root "exports/raw"
if (-not (Test-Path $outRoot)) {
    New-Item -ItemType Directory -Force -Path $outRoot | Out-Null
}

foreach ($file in @("genisys_sample.log", "sockettrace_sample.log", "workflow_sample.log")) {
    $src = Join-Path $sampleRoot $file
    if (Test-Path $src) {
        Copy-Item -Force -LiteralPath $src -Destination (Join-Path $outRoot $file)
    }
}

Write-Host "Copied curated sample logs to $outRoot"
