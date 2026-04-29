Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
. (Join-Path $root "scripts/utils/SourceInventory.ps1")

$rows = Get-SourceInventoryFromMusic

$csv = Join-Path $root "exports/inventory/music_sources.csv"
$json = Join-Path $root "exports/inventory/music_sources.json"
$md = Join-Path $root "reports/manual_inventory.md"

Ensure-ParentDirectory -Path $csv
Write-InventoryCsv -Rows $rows -OutPath $csv
Write-InventoryJson -Rows $rows -OutPath $json

$summary = @()
$summary += "# Manual Inventory"
$summary += ""
$summary += "## Source Used"
$summary += "- `C:\\Users\\Ji\\Music` filename and archive-structure reconnaissance"
$summary += ""
$summary += "## Source Families"
$summary += ""
foreach ($group in ($rows | Group-Object kind | Sort-Object Name)) {
    $summary += "- $($group.Name): $($group.Count)"
}
$summary += ""
$summary += "## What Was Learned"
$summary += "- Genisys manuals were found in the folder wrapper 03_GenisysManuals.zip, which contains Genisys Trace Analysis.pdf, P2346F Genisys Code System.pdf, and Reading Genisys Data-DRAFT 1.docx."
$summary += "- ICD PDFs were found in `ICD PDF FOR PTC.zip`."
$summary += "- Additional operational source wrappers were found in `More Data`, `More Stuff`, and `MORE BOC`; these contain versioned report PDFs and large log bundles, not additional ICD manuals by filename."
$summary += "- TMDS workflow evidence exists in the TMDS memo and TCOS training materials."
$summary += "- The Music folder also contains extensive real-log bundles and CodeServer build outputs."
$summary += ""
$summary += "## Confidence"
$summary += "- High for source-family discovery."
$summary += "- Low for exact manual semantics until a document text extractor is available."
$summary += ""
$summary += "## Unknowns"
$summary += "- Page-level references are not extracted yet."
$summary += "- Bit, byte, and field meanings remain unresolved."
$summary += "- Some source bundles are folders named with .zip, so inventory code must treat archive folders and files differently."

Set-Content -Encoding UTF8 -Path $md -Value ($summary -join "`r`n")

Write-Host "Wrote:"
Write-Host " - $csv"
Write-Host " - $json"
Write-Host " - $md"
