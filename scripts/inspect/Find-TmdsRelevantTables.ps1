param(
    [string]$Server = "localhost",
    [string[]]$Databases = @("tmdsDatabaseDynamic", "tmdsDatabaseStatic"),
    [string[]]$Keywords = @(
        "code", "line", "bit", "signal", "route", "control", "indication",
        "station", "cp", "mile", "location", "alias", "lookup", "enum",
        "message", "field", "dispatch", "train", "socket", "cad",
        "genisys", "bos", "boc", "mdm", "icd"
    ),
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports\inventory")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Data

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

$likeClauses = @()
for ($i = 0; $i -lt $Keywords.Count; $i++) {
    $likeClauses += "LOWER(t.name) LIKE @p$i OR LOWER(c.name) LIKE @p$i"
}
$predicate = [string]::Join(" OR ", $likeClauses)

$sql = @"
SET NOCOUNT ON;
SELECT DISTINCT
    DB_NAME() AS database_name,
    s.name AS schema_name,
    t.name AS table_name,
    SUM(COALESCE(p.rows, 0)) OVER (PARTITION BY t.object_id) AS row_count
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
LEFT JOIN sys.columns c ON t.object_id = c.object_id
LEFT JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
WHERE $predicate
ORDER BY s.name, t.name;
"@

$results = New-Object System.Collections.Generic.List[object]

foreach ($database in $Databases) {
    $connectionString = "Data Source=$Server;Initial Catalog=$database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex Relevant Table Probe`";Connect Timeout=5"
    try {
        $connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
        $connection.Open()
        try {
            $command = $connection.CreateCommand()
            $command.CommandText = $sql
            for ($i = 0; $i -lt $Keywords.Count; $i++) {
                [void]$command.Parameters.AddWithValue("@p$i", "%$($Keywords[$i].ToLower())%")
            }
            $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
            $table = New-Object System.Data.DataTable
            [void]$adapter.Fill($table)

            foreach ($row in $table.Rows) {
                $results.Add([pscustomobject]@{
                    database = $row["database_name"]
                    schema = $row["schema_name"]
                    table = $row["table_name"]
                    row_count = [int64]$row["row_count"]
                })
            }
        } finally {
            $connection.Close()
        }
    } catch {
        $results.Add([pscustomobject]@{
            database = $database
            schema = $null
            table = $null
            row_count = $null
            error = $_.Exception.Message
        })
    }
}

$jsonPath = Join-Path $OutDir "tmds_relevant_tables.json"
$csvPath = Join-Path $OutDir "tmds_relevant_tables.csv"
$results | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
$results | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath
Write-Host $jsonPath
Write-Host $csvPath
