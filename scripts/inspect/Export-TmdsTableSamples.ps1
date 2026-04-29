param(
    [string]$Server = "localhost",
    [string]$Database,
    [string[]]$Tables,
    [int]$Top = 25,
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

function Get-SafeName {
    param([string]$Value)
    return ($Value -replace "[^A-Za-z0-9._-]", "_")
}

function Get-OrderByColumns {
    param(
        [System.Data.SqlClient.SqlConnection]$Connection,
        [string]$SchemaName,
        [string]$TableName
    )

    $metadataSql = @"
SET NOCOUNT ON;
WITH primary_key_columns AS (
    SELECT
        c.name AS column_name,
        ic.key_ordinal,
        0 AS priority_group
    FROM sys.tables t
    JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    JOIN sys.indexes i
        ON t.object_id = i.object_id
       AND i.is_primary_key = 1
    JOIN sys.index_columns ic
        ON i.object_id = ic.object_id
       AND i.index_id = ic.index_id
    JOIN sys.columns c
        ON ic.object_id = c.object_id
       AND ic.column_id = c.column_id
    WHERE s.name = @schema_name
      AND t.name = @table_name
),
fallback_columns AS (
    SELECT
        c.name AS column_name,
        c.column_id AS key_ordinal,
        1 AS priority_group
    FROM sys.tables t
    JOIN sys.schemas s
        ON t.schema_id = s.schema_id
    JOIN sys.columns c
        ON t.object_id = c.object_id
    JOIN sys.types ty
        ON c.user_type_id = ty.user_type_id
    WHERE s.name = @schema_name
      AND t.name = @table_name
      AND ty.name NOT IN ('text', 'ntext', 'image', 'xml', 'sql_variant', 'geometry', 'geography', 'hierarchyid')
)
SELECT TOP (4)
    column_name
FROM (
    SELECT column_name, key_ordinal, priority_group FROM primary_key_columns
    UNION ALL
    SELECT column_name, key_ordinal, priority_group FROM fallback_columns
) ranked
ORDER BY priority_group, key_ordinal;
"@

    $command = $Connection.CreateCommand()
    $command.CommandText = $metadataSql
    [void]$command.Parameters.AddWithValue("@schema_name", $SchemaName)
    [void]$command.Parameters.AddWithValue("@table_name", $TableName)
    $reader = $command.ExecuteReader()
    $columns = New-Object System.Collections.Generic.List[string]
    try {
        while ($reader.Read()) {
            $columnName = [string]$reader["column_name"]
            if (-not [string]::IsNullOrWhiteSpace($columnName) -and -not $columns.Contains($columnName)) {
                $columns.Add($columnName)
            }
        }
    } finally {
        $reader.Close()
    }

    if ($columns.Count -eq 0) {
        throw "No sortable columns found for [$SchemaName].[$TableName]."
    }

    return $columns
}

$connectionString = "Data Source=$Server;Initial Catalog=$Database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex TMDS Sample Export`";Connect Timeout=5"
$connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
$connection.Open()
try {
    $manifest = New-Object System.Collections.Generic.List[object]

    foreach ($tableSpec in $Tables) {
        $parts = $tableSpec.Split(".", 2)
        if ($parts.Count -eq 2) {
            $schemaName = $parts[0]
            $tableName = $parts[1]
        } else {
            $schemaName = "dbo"
            $tableName = $tableSpec
        }

        if ($schemaName -notmatch "^[A-Za-z0-9_]+$" -or $tableName -notmatch "^[A-Za-z0-9_]+$") {
            throw "Unsafe schema/table name: $tableSpec"
        }

        $orderByColumns = Get-OrderByColumns -Connection $connection -SchemaName $schemaName -TableName $tableName
        $orderByClause = ($orderByColumns | ForEach-Object { "[{0}] ASC" -f $_ }) -join ", "
        $qualifiedName = "[{0}].[{1}]" -f $schemaName, $tableName
        $sampleSql = "SELECT TOP ($Top) * FROM $qualifiedName ORDER BY $orderByClause;"

        $command = $connection.CreateCommand()
        $command.CommandText = $sampleSql
        $adapter = New-Object System.Data.SqlClient.SqlDataAdapter $command
        $dataTable = New-Object System.Data.DataTable
        [void]$adapter.Fill($dataTable)

        $rows = New-Object System.Collections.Generic.List[object]
        foreach ($row in $dataTable.Rows) {
            $entry = [ordered]@{}
            foreach ($column in $dataTable.Columns) {
                $value = $row[$column.ColumnName]
                if ($value -is [System.DBNull]) {
                    $entry[$column.ColumnName] = $null
                } else {
                    $entry[$column.ColumnName] = $value
                }
            }
            $rows.Add([pscustomobject]$entry)
        }

        $safeBase = Get-SafeName "$Database.$schemaName.$tableName.sample_rows"
        $jsonPath = Join-Path $OutDir "$safeBase.json"
        $csvPath = Join-Path $OutDir "$safeBase.csv"
        $metaPath = Join-Path $OutDir "$safeBase.meta.json"

        $rows | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
        $rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

        $metadata = [pscustomobject]@{
            database = $Database
            schema = $schemaName
            table = $tableName
            top = $Top
            order_by = $orderByColumns
            row_count_exported = $rows.Count
        }
        $metadata | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -Path $metaPath

        $manifest.Add([pscustomobject]@{
            database = $Database
            schema = $schemaName
            table = $tableName
            top = $Top
            order_by = ($orderByColumns -join ", ")
            row_count_exported = $rows.Count
            json_path = $jsonPath
            csv_path = $csvPath
            meta_path = $metaPath
        })
    }

    $manifestPath = Join-Path $OutDir "$(Get-SafeName "$Database.sample_manifest").json"
    $manifestMap = @{}

    if (Test-Path $manifestPath) {
        $existingManifest = Get-Content -Raw -Path $manifestPath | ConvertFrom-Json
        if ($existingManifest) {
            if ($existingManifest -is [System.Array]) {
                foreach ($entry in $existingManifest) {
                    $key = "{0}.{1}.{2}" -f $entry.database, $entry.schema, $entry.table
                    $manifestMap[$key] = [pscustomobject]@{
                        database = $entry.database
                        schema = $entry.schema
                        table = $entry.table
                        top = $entry.top
                        order_by = $entry.order_by
                        row_count_exported = $entry.row_count_exported
                        json_path = $entry.json_path
                        csv_path = $entry.csv_path
                        meta_path = $entry.meta_path
                    }
                }
            } else {
                $key = "{0}.{1}.{2}" -f $existingManifest.database, $existingManifest.schema, $existingManifest.table
                $manifestMap[$key] = [pscustomobject]@{
                    database = $existingManifest.database
                    schema = $existingManifest.schema
                    table = $existingManifest.table
                    top = $existingManifest.top
                    order_by = $existingManifest.order_by
                    row_count_exported = $existingManifest.row_count_exported
                    json_path = $existingManifest.json_path
                    csv_path = $existingManifest.csv_path
                    meta_path = $existingManifest.meta_path
                }
            }
        }
    }

    foreach ($entry in $manifest) {
        $key = "{0}.{1}.{2}" -f $entry.database, $entry.schema, $entry.table
        $manifestMap[$key] = $entry
    }

    $mergedManifest = $manifestMap.GetEnumerator() |
        Sort-Object Name |
        ForEach-Object { $_.Value }

    $mergedManifest | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $manifestPath
    Write-Host $manifestPath
} finally {
    $connection.Close()
}
