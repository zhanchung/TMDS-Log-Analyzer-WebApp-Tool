param(
  [string]$UpdateSource = $env:TMDS_UPDATE_SOURCE,
  [string]$WebHost = $env:TMDS_WEB_HOST,
  [string]$WebPort = $env:TMDS_WEB_PORT,
  [switch]$NoUpdate
)

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $Root

function Get-NpmCommand {
  $npmCmd = Get-Command npm.cmd -ErrorAction SilentlyContinue
  if ($npmCmd) {
    return $npmCmd.Source
  }

  $npm = Get-Command npm -ErrorAction SilentlyContinue
  if ($npm) {
    return $npm.Source
  }

  throw "Node.js/npm was not found. Install Node.js LTS, then run this launcher again."
}

function Get-ElectronCommand {
  $electronCommand = Join-Path $Root "node_modules\.bin\electron.cmd"
  if (Test-Path -LiteralPath $electronCommand) {
    return $electronCommand
  }

  throw "Electron was not found in node_modules. Run npm install, then run this launcher again."
}

function Invoke-NativeCommand {
  param(
    [string]$FilePath,
    [string[]]$Arguments,
    [string]$FailureMessage
  )

  & $FilePath @Arguments
  if ($LASTEXITCODE -ne 0) {
    throw "$FailureMessage (exit code $LASTEXITCODE)"
  }
}

function Get-ConfiguredUpdateSource {
  if ($NoUpdate) {
    return ""
  }

  if ($UpdateSource) {
    return $UpdateSource.Trim()
  }

  $configPath = Join-Path $Root ".tmds-update-source"
  if (Test-Path -LiteralPath $configPath) {
    return (Get-Content -LiteralPath $configPath -Raw).Trim()
  }

  return ""
}

function Get-ConfiguredWebHost {
  if ($WebHost) {
    return $WebHost.Trim()
  }

  $configPath = Join-Path $Root ".tmds-web-host"
  if (Test-Path -LiteralPath $configPath) {
    return (Get-Content -LiteralPath $configPath -Raw).Trim()
  }

  return ""
}

function Get-ConfiguredWebPort {
  if ($WebPort) {
    return $WebPort.Trim()
  }

  $configPath = Join-Path $Root ".tmds-web-port"
  if (Test-Path -LiteralPath $configPath) {
    return (Get-Content -LiteralPath $configPath -Raw).Trim()
  }

  return ""
}

function Sync-ProjectSource {
  param([string]$Source)

  if (-not $Source) {
    return
  }

  try {
    $sourcePath = (Resolve-Path -LiteralPath $Source).Path
  } catch {
    throw "Configured update source was not found: $Source"
  }

  if ($sourcePath.TrimEnd("\", "/") -ieq $Root.TrimEnd("\", "/")) {
    return
  }

  Write-Host "Updating TMDS webapp source from $sourcePath"
  $items = @(
    "app",
    "exports",
    "reference_assets",
    "sample_logs",
    "scripts",
    "AGENTS.md",
    "package.json",
    "package-lock.json",
    "README.md",
    "TMDS-Server-Switch.bat",
    "Open-TMDS-Webapp.html",
    ".tmds-update-source.example",
    ".tmds-web-host.example",
    ".tmds-web-port.example",
    "tsconfig.json",
    "tsconfig.node.json",
    "vite.config.cjs"
  )

  foreach ($item in $items) {
    $from = Join-Path $sourcePath $item
    if (-not (Test-Path -LiteralPath $from)) {
      continue
    }
    $to = Join-Path $Root $item
    if (Test-Path -LiteralPath $from -PathType Container) {
      if (-not (Test-Path -LiteralPath $to)) {
        New-Item -ItemType Directory -Path $to | Out-Null
      }
      Get-ChildItem -LiteralPath $from -Force | Copy-Item -Destination $to -Recurse -Force
    } else {
      Copy-Item -LiteralPath $from -Destination $to -Force
    }
  }
}

function Get-DependencyFingerprint {
  $packageLockPath = Join-Path $Root "package-lock.json"
  if (Test-Path -LiteralPath $packageLockPath) {
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $packageLockPath).Hash.ToLowerInvariant()
  }

  $packageJsonPath = Join-Path $Root "package.json"
  if (Test-Path -LiteralPath $packageJsonPath) {
    return (Get-FileHash -Algorithm SHA256 -LiteralPath $packageJsonPath).Hash.ToLowerInvariant()
  }

  return ""
}

function Get-DependencyStampPath {
  return (Join-Path $Root ".tmds-node-modules.stamp")
}

function Test-DependenciesRequireInstall {
  $nodeModulesPath = Join-Path $Root "node_modules"
  if (-not (Test-Path -LiteralPath $nodeModulesPath -PathType Container)) {
    return $true
  }

  $electronCommand = Join-Path $Root "node_modules\.bin\electron.cmd"
  if (-not (Test-Path -LiteralPath $electronCommand -PathType Leaf)) {
    return $true
  }

  $expectedFingerprint = Get-DependencyFingerprint
  if (-not $expectedFingerprint) {
    return $false
  }

  $stampPath = Get-DependencyStampPath
  if (-not (Test-Path -LiteralPath $stampPath -PathType Leaf)) {
    return $true
  }

  $installedFingerprint = (Get-Content -LiteralPath $stampPath -Raw).Trim().ToLowerInvariant()
  return $installedFingerprint -ne $expectedFingerprint
}

function Update-DependencyStamp {
  $fingerprint = Get-DependencyFingerprint
  if (-not $fingerprint) {
    return
  }

  Set-Content -LiteralPath (Get-DependencyStampPath) -Value $fingerprint -NoNewline
}

$configuredUpdateSource = Get-ConfiguredUpdateSource
Sync-ProjectSource -Source $configuredUpdateSource

$configuredWebHost = Get-ConfiguredWebHost
if ($configuredWebHost) {
  $env:TMDS_WEB_HOST = $configuredWebHost
}

$configuredWebPort = Get-ConfiguredWebPort
if ($configuredWebPort) {
  $env:TMDS_WEB_PORT = $configuredWebPort
}

$npm = Get-NpmCommand
if (Test-DependenciesRequireInstall) {
  Write-Host "Installing dependencies..."
  Invoke-NativeCommand -FilePath $npm -Arguments @("install") -FailureMessage "npm install failed"
  Update-DependencyStamp
}

Write-Host "Building TMDS webapp..."
Invoke-NativeCommand -FilePath $npm -Arguments @("run", "build") -FailureMessage "npm run build failed"

if ($env:TMDS_WEB_HOST) {
  $displayPort = if ($env:TMDS_WEB_PORT) { $env:TMDS_WEB_PORT } else { "4173" }
  Write-Host "Starting TMDS webapp on host $($env:TMDS_WEB_HOST) port $displayPort"
} else {
  Write-Host "Starting TMDS webapp at http://127.0.0.1:4173/"
}
$electron = Get-ElectronCommand
Invoke-NativeCommand -FilePath $electron -Arguments @(".") -FailureMessage "Electron failed to start"
