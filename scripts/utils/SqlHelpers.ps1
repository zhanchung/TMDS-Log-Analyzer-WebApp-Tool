Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function New-ConnectionString {
    param(
        [Parameter(Mandatory = $true)][string]$ServerName,
        [Parameter(Mandatory = $true)][string]$DatabaseName,
        [string]$SqlUserId,
        [string]$SqlPassword
    )

    $builder = New-Object System.Data.SqlClient.SqlConnectionStringBuilder
    $builder["Data Source"] = $ServerName
    $builder["Initial Catalog"] = $DatabaseName
    $builder["Encrypt"] = $false
    $builder["TrustServerCertificate"] = $true
    $builder["Connect Timeout"] = 5
    if ($SqlUserId -and $SqlPassword) {
        $builder["Integrated Security"] = $false
        $builder["User ID"] = $SqlUserId
        $builder["Password"] = $SqlPassword
    } else {
        $builder["Integrated Security"] = $true
    }
    return $builder.ConnectionString
}

function Invoke-Query {
    param(
        [Parameter(Mandatory = $true)][string]$ConnectionString,
        [Parameter(Mandatory = $true)][string]$Sql
    )

    Add-Type -AssemblyName System.Data
    $conn = New-Object System.Data.SqlClient.SqlConnection $ConnectionString
    $conn.Open()
    try {
        $cmd = $conn.CreateCommand()
        $cmd.CommandText = $Sql
        $dt = New-Object System.Data.DataTable
        $reader = $cmd.ExecuteReader()
        try {
            $dt.Load($reader)
        } finally {
            $reader.Close()
        }
        return $dt
    } finally {
        $conn.Close()
    }
}

function Convert-DataTableToObjects {
    param([Parameter(Mandatory = $true)][System.Data.DataTable]$Table)

    foreach ($row in $Table.Rows) {
        $obj = [ordered]@{}
        foreach ($col in $Table.Columns) {
            $value = $row[$col.ColumnName]
            if ($null -eq $value -or $value -is [DBNull]) {
                $obj[$col.ColumnName] = $null
            } else {
                $obj[$col.ColumnName] = $value
            }
        }
        [pscustomobject]$obj
    }
}
