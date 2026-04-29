Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$music = Join-Path $env:USERPROFILE "Music\Code Debugger Project"

$out = Join-Path $root "reports/old_project_reuse_plan.md"

$lines = @(
    "# Old Project Reuse Plan",
    "",
    "Source inspected: $music",
    "",
    "Reusable items:",
    "- dark theme and color language",
    "- drag/drop intake overlay and file intake pipeline",
    "- login bootstrap and first-time password setup behavior",
    "- admin account scaffolding",
    "- window resize and persisted window state",
    "- socket-trace and Genisys seed lexicon",
    "",
    "Non-reusable items:",
    "- cramped table-oriented result UI",
    "- speculative translation logic",
    "- any unverified manual-derived meaning",
    "",
    "Risk note: the old project is source material only; the new app should use its behavior selectively, not copy the old layout."
)

Set-Content -Encoding UTF8 -Path $out -Value ($lines -join "`r`n")
Write-Host $out
