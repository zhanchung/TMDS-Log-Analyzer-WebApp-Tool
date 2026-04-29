param(
    [string]$Server = "localhost",
    [string[]]$Databases = @("tmdsDatabaseDynamic", "tmdsDatabaseStatic"),
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports\inventory")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Data

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

$rows = New-Object System.Collections.Generic.List[object]

foreach ($database in $Databases) {
    $connectionString = "Data Source=$Server;Initial Catalog=$database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex TMDS Schema Probe`";Connect Timeout=5"
    try {
        $connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
        $connection.Open()
        try {
            $command = $connection.CreateCommand()
            $command.CommandText = @"
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
            $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
            $table = New-Object System.Data.DataTable
            [void]$adapter.Fill($table)

            $rows.Add([pscustomobject]@{
                database = $database
                status = "connected"
                server = $Server
            })

            foreach ($row in $table.Rows) {
                $rows.Add([pscustomobject]@{
                    database = $row["database_name"]
                    schema = $row["schema_name"]
                    object = $row["object_name"]
                    type = $row["object_type"]
                    row_count = [int64]$row["row_count"]
                    status = "discovered"
                })
            }
        } finally {
            $connection.Close()
        }
    } catch {
        $rows.Add([pscustomobject]@{
            database = $database
            status = "failed"
            error = $_.Exception.Message
        })
    }
}

$jsonPath = Join-Path $OutDir "tmds_schema_inventory.json"
$csvPath = Join-Path $OutDir "tmds_schema_inventory.csv"
$rows | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
$rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath
Write-Host $jsonPath
Write-Host $csvPath
