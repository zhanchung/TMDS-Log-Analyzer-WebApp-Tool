param(
    [string]$Server = "localhost",
    [string]$Database,
    [string]$Table,
    [string]$SchemaName = "dbo",
    [string]$FilterJson,
    [string[]]$Filters,
    [int]$Top = 25,
    [string]$OutDir = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports\inventory")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not $Database) {
    throw "Database is required."
}

if (-not $Table) {
    throw "Table is required."
}

if (-not $FilterJson -and (-not $Filters -or $Filters.Count -eq 0)) {
    throw "FilterJson or Filters is required."
}

if ($SchemaName -notmatch "^[A-Za-z0-9_]+$" -or $Table -notmatch "^[A-Za-z0-9_]+$") {
    throw "Unsafe schema/table name."
}

Add-Type -AssemblyName System.Data

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

function Get-SafeName {
    param([string]$Value)
    return ($Value -replace "[^A-Za-z0-9._-]", "_")
}

$filterTable = @{}

if ($FilterJson) {
    $filterObject = $FilterJson | ConvertFrom-Json
    foreach ($property in $filterObject.PSObject.Properties) {
        $filterTable[$property.Name] = $property.Value
    }
} else {
    foreach ($filter in $Filters) {
        if ($filter -notmatch "=") {
            throw "Invalid filter format. Use Column=Value."
        }

        $parts = $filter.Split("=", 2)
        $columnName = $parts[0].Trim()
        $rawValue = $parts[1]
        if (-not $columnName) {
            throw "Invalid filter format. Column name is required."
        }

        if ($rawValue -eq "<NULL>") {
            $filterTable[$columnName] = $null
        } elseif ($rawValue -match '^-?\d+$') {
            $filterTable[$columnName] = [int64]$rawValue
        } elseif ($rawValue -match '^(?i:true|false)$') {
            $filterTable[$columnName] = [bool]::Parse($rawValue)
        } else {
            $filterTable[$columnName] = $rawValue
        }
    }
    $filterObject = [pscustomobject]$filterTable
}

$filterProperties = @($filterObject.PSObject.Properties)
if ($filterProperties.Count -eq 0) {
    throw "FilterJson did not contain any properties."
}

$whereClauses = New-Object System.Collections.Generic.List[string]
$parameterList = New-Object System.Collections.Generic.List[object]
$index = 0
foreach ($property in $filterProperties) {
    if ($property.Name -notmatch "^[A-Za-z0-9_]+$") {
        throw "Unsafe column name in filter: $($property.Name)"
    }

    $paramName = "@p$index"
    if ($null -eq $property.Value) {
        $whereClauses.Add("[$($property.Name)] IS NULL")
    } else {
        $whereClauses.Add("[$($property.Name)] = $paramName")
        $parameterList.Add([pscustomobject]@{
            Name = $paramName
            Value = $property.Value
        })
        $index++
    }
}

$whereClause = [string]::Join(" AND ", $whereClauses)
$connectionString = "Data Source=$Server;Initial Catalog=$Database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex TMDS Filtered Export`";Connect Timeout=5"
$qualifiedName = "[{0}].[{1}]" -f $SchemaName, $Table
$sql = "SELECT TOP ($Top) * FROM $qualifiedName WHERE $whereClause;"

$connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
$connection.Open()
try {
    $command = $connection.CreateCommand()
    $command.CommandText = $sql
    foreach ($parameter in $parameterList) {
        [void]$command.Parameters.AddWithValue($parameter.Name, $parameter.Value)
    }

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

    $filterSuffix = ($filterProperties | ForEach-Object {
        "{0}-{1}" -f $_.Name, (Get-SafeName ([string]$_.Value))
    }) -join "__"

    $safeBase = Get-SafeName "$Database.$SchemaName.$Table.filtered.$filterSuffix"
    $jsonPath = Join-Path $OutDir "$safeBase.json"
    $csvPath = Join-Path $OutDir "$safeBase.csv"
    $metaPath = Join-Path $OutDir "$safeBase.meta.json"

    $rows | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $jsonPath
    $rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $csvPath

    $metadata = [pscustomobject]@{
        database = $Database
        schema = $SchemaName
        table = $Table
        top = $Top
        where_clause = $whereClause
        filter = $filterObject
        row_count_exported = $rows.Count
        sql = $sql
    }
    $metadata | ConvertTo-Json -Depth 8 | Set-Content -Encoding UTF8 -Path $metaPath

    Write-Host $jsonPath
    Write-Host $csvPath
    Write-Host $metaPath
} finally {
    $connection.Close()
}
