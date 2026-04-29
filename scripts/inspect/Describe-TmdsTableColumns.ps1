param(
    [string]$Server = "localhost",
    [string]$Database,
    [string[]]$Tables,
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports\inventory")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $Database) {
    throw "Database is required."
}

if (-not $Tables -or $Tables.Count -eq 0) {
    throw "At least one table name is required."
}

Add-Type -AssemblyName System.Data

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

$connectionString = "Data Source=$Server;Initial Catalog=$Database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex Column Probe`";Connect Timeout=5"

$results = New-Object System.Collections.Generic.List[object]
$inParams = @()

for ($i = 0; $i -lt $Tables.Count; $i++) {
    $inParams += "@t$i"
}

$sql = @"
SET NOCOUNT ON;
SELECT
    TABLE_CATALOG AS database_name,
    TABLE_SCHEMA AS schema_name,
    TABLE_NAME AS table_name,
    COLUMN_NAME AS column_name,
    ORDINAL_POSITION AS ordinal_position,
    DATA_TYPE AS data_type,
    IS_NULLABLE AS is_nullable
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME IN ($(($inParams -join ", ")))
ORDER BY TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION;
"@

$connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
$connection.Open()
try {
    $command = $connection.CreateCommand()
    $command.CommandText = $sql
    for ($i = 0; $i -lt $Tables.Count; $i++) {
        [void]$command.Parameters.AddWithValue("@t$i", $Tables[$i])
    }
    $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
    $table = New-Object System.Data.DataTable
    [void]$adapter.Fill($table)

    foreach ($row in $table.Rows) {
        $results.Add([pscustomobject]@{
            database = $row["database_name"]
            schema = $row["schema_name"]
            table = $row["table_name"]
            column = $row["column_name"]
            ordinal = [int]$row["ordinal_position"]
            data_type = $row["data_type"]
            is_nullable = $row["is_nullable"]
        })
    }
} finally {
    $connection.Close()
}

$safeDb = $Database -replace '[^A-Za-z0-9_-]', '_'
$safeTables = (($Tables -join "_") -replace '[^A-Za-z0-9_-]', '_')
if ($safeTables.Length -gt 80) {
    $safeTables = $safeTables.Substring(0, 80)
}
$jsonPath = Join-Path $OutDir "$safeDb.$safeTables.table_columns.json"
$csvPath = Join-Path $OutDir "$safeDb.$safeTables.table_columns.csv"
$results | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
$results | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath
Write-Host $jsonPath
Write-Host $csvPath
