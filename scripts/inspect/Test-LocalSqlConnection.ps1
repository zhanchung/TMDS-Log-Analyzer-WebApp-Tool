param(
    [string]$Server = "localhost",
    [string]$Database = "master",
    [string]$OutputPath = (Join-Path (Split-Path -Parent (Split-Path -Parent $PSScriptRoot)) "exports\inventory\local_sql_connection_test.txt")
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

Add-Type -AssemblyName System.Data

$connectionString = "Data Source=$Server;Initial Catalog=$Database;Integrated Security=True;Persist Security Info=False;Pooling=False;MultipleActiveResultSets=False;Encrypt=False;TrustServerCertificate=True;Application Name=`"Codex SQL Probe`";Connect Timeout=5"

$lines = New-Object System.Collections.Generic.List[string]
$lines.Add("WHOAMI: $(whoami)")
$lines.Add("CONNECTION_STRING: $connectionString")

try {
    $connection = New-Object System.Data.SqlClient.SqlConnection $connectionString
    $connection.Open()
    try {
        $command = $connection.CreateCommand()
        $command.CommandText = "SELECT SUSER_SNAME() AS login_name, SYSTEM_USER AS current_system_user, @@SERVERNAME AS server_name, DB_NAME() AS db_name;"
        $reader = $command.ExecuteReader()
        try {
            while ($reader.Read()) {
                $lines.Add("LOGIN_NAME: $($reader['login_name'])")
                $lines.Add("SYSTEM_USER: $($reader['current_system_user'])")
                $lines.Add("SERVER_NAME: $($reader['server_name'])")
                $lines.Add("DATABASE_NAME: $($reader['db_name'])")
            }
        } finally {
            $reader.Close()
        }
    } finally {
        $connection.Close()
    }
    $lines.Add("STATUS: connected")
} catch {
    $lines.Add("STATUS: failed")
    $lines.Add("ERROR: $($_.Exception.Message)")
}

$outDir = Split-Path -Parent $OutputPath
if (-not (Test-Path $outDir)) {
    New-Item -ItemType Directory -Force -Path $outDir | Out-Null
}

$lines | Set-Content -Encoding UTF8 -Path $OutputPath
Write-Host $OutputPath
