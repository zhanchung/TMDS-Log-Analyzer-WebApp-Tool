param(
    [string]$Server = "172.20.20.35",
    [string[]]$Databases = @("tmdsDatabaseDynamic", "tmdsDatabaseStatic"),
    [string]$UserId,
    [string]$Password,
    [int]$SampleRows = 20,
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports/normalized/sql_snapshot")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path (Split-Path -Parent $PSScriptRoot) "utils/SqlHelpers.ps1")

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

foreach ($db in $Databases) {
    $cs = New-ConnectionString -ServerName $Server -DatabaseName $db -SqlUserId $UserId -SqlPassword $Password
    $schemaSql = @"
SET NOCOUNT ON;
SELECT s.name AS schema_name, o.name AS object_name, o.type_desc AS object_type, SUM(COALESCE(p.rows, 0)) AS row_count
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
LEFT JOIN sys.partitions p ON o.object_id = p.object_id AND p.index_id IN (0, 1)
WHERE o.type IN ('U', 'V')
GROUP BY s.name, o.name, o.type_desc
ORDER BY s.name, o.name;
"@
    try {
        $tables = Invoke-Query -ConnectionString $cs -Sql $schemaSql
    } catch {
        Write-Host "FAILED $db $($_.Exception.Message)"
        continue
    }

    foreach ($t in (Convert-DataTableToObjects -Table $tables)) {
        if ([int]$t.row_count -le 0) {
            continue
        }

        $query = "SELECT TOP ($SampleRows) * FROM [$($t.schema_name)].[$($t.object_name)]"
        try {
            $sample = Invoke-Query -ConnectionString $cs -Sql $query
            $samplePath = Join-Path $OutDir "$($db).$($t.schema_name).$($t.object_name).json"
            (Convert-DataTableToObjects -Table $sample) | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $samplePath
        } catch {
            $errorPath = Join-Path $OutDir "$($db).$($t.schema_name).$($t.object_name).error.txt"
            $_.Exception.Message | Set-Content -Encoding UTF8 -Path $errorPath
        }
    }
}
