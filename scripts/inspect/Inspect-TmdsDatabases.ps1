param(
    [string[]]$ServerCandidates = @("localhost", "DESKTOP-P6BD8S3", "DESKTOP-P6BD8S3W", ".", "lpc:localhost", "172.20.20.35"),
    [string[]]$Databases = @("tmdsDatabaseDynamic", "tmdsDatabaseStatic"),
    [string]$UserId,
    [string]$Password,
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports/inventory")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

. (Join-Path (Split-Path -Parent $PSScriptRoot) "utils/SqlHelpers.ps1")

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

$results = New-Object System.Collections.Generic.List[object]

foreach ($db in $Databases) {
    $tableRows = $null
    $lastError = $null
    foreach ($server in $ServerCandidates) {
        $cs = New-ConnectionString -ServerName $server -DatabaseName $db -SqlUserId $UserId -SqlPassword $Password
        $schemaSql = @"
SET NOCOUNT ON;
SELECT
    DB_NAME() AS database_name,
    s.name AS schema_name,
    o.name AS object_name,
    o.type_desc AS object_type,
    SUM(COALESCE(p.rows, 0)) AS row_count
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
LEFT JOIN sys.partitions p ON o.object_id = p.object_id AND p.index_id IN (0, 1)
WHERE o.type IN ('U', 'V')
GROUP BY s.name, o.name, o.type_desc
ORDER BY s.name, o.name;
"@
        try {
            $tableRows = Invoke-Query -ConnectionString $cs -Sql $schemaSql
            $results.Add([pscustomobject]@{
                database = $db
                server = $server
                status = "connected"
            })
            break
        } catch {
            $lastError = $_.Exception.Message
        }
    }

    if (-not $tableRows) {
        $results.Add([pscustomobject]@{
            database = $db
            status = "failed"
            error = $lastError
        })
        continue
    }

    foreach ($table in (Convert-DataTableToObjects -Table $tableRows)) {
        $results.Add([pscustomobject]@{
            database = $table.database_name
            schema = $table.schema_name
            object = $table.object_name
            type = $table.object_type
            row_count = [int]$table.row_count
            status = "discovered"
        })
    }
}

$jsonPath = Join-Path $OutDir "tmds_database_inventory.json"
$csvPath = Join-Path $OutDir "tmds_database_inventory.csv"
$results | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
$results | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

Write-Host $jsonPath
Write-Host $csvPath
