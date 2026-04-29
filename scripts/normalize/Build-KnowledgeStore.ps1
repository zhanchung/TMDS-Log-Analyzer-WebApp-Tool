Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$out = Join-Path $root "exports/normalized/knowledge_model_seed.json"

$payload = [pscustomobject]@{
    generated_at = (Get-Date).ToString("o")
    entities = @(
        "source_record",
        "database_definition",
        "database_schema_definition",
        "database_table_definition",
        "database_column_definition",
        "database_key_definition",
        "database_relation_definition",
        "database_row_sample",
        "database_lookup_definition",
        "workflow_definition",
        "workflow_relation",
        "genisys_bit_definition",
        "genisys_byte_definition",
        "office_header_definition",
        "field_header_definition",
        "icd_version_definition",
        "icd_message_definition",
        "translation_template",
        "unresolved_token"
    )
    fallback_policy = "Preserve raw evidence when source meaning is unavailable."
}

$payload | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $out
Write-Host $out
