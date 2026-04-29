Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$out = Join-Path $root "exports/mappings/base_mappings.json"

$mappings = [pscustomobject]@{
    sources = @{
        genisys_manuals = "03_GenisysManuals.zip"
        icd_manuals = "ICD PDF FOR PTC.zip"
        workflow_memo = "TMDS Technical Memo - Near Side Signal Control.pdf"
    }
    runtime_hints = @{
        keepalive = "transport_heartbeat"
        alive = "transport_reply"
        indication = "field_state_update"
        recall = "state_refresh_anchor"
        control = "office_command"
        codestation = "control_point_translation_anchor"
    }
    unresolved_policy = "No meaning is assigned unless the source document or verified database row defines it."
}

$mappings | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $out
Write-Host $out
