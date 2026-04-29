param()

$ErrorActionPreference = "Stop"
$Root = (Resolve-Path -LiteralPath (Join-Path $PSScriptRoot "..")).Path
Set-Location -LiteralPath $Root

function Get-ConfiguredValue {
  param(
    [string]$Path,
    [string]$Fallback = ""
  )

  if (Test-Path -LiteralPath $Path) {
    return (Get-Content -LiteralPath $Path -Raw).Trim()
  }

  return $Fallback
}

function Get-PrimaryIpv4Address {
  try {
    $ip = Get-NetIPAddress -AddressFamily IPv4 -ErrorAction Stop |
      Where-Object {
        $_.IPAddress -notlike "169.254.*" -and
        $_.IPAddress -ne "127.0.0.1" -and
        $_.PrefixOrigin -ne "WellKnown"
      } |
      Sort-Object InterfaceMetric, SkipAsSource |
      Select-Object -First 1 -ExpandProperty IPAddress
    if ($null -eq $ip) {
      return ""
    }
    return ([string]$ip).Trim()
  } catch {
    return ""
  }
}

function Get-TailscaleExe {
  $candidates = @(
    "C:\Program Files\Tailscale\tailscale.exe",
    "C:\Program Files (x86)\Tailscale\tailscale.exe"
  )
  foreach ($candidate in $candidates) {
    if (Test-Path -LiteralPath $candidate) {
      return $candidate
    }
  }
  $cmd = Get-Command tailscale.exe -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  return ""
}

function Test-TailscaleFunnel {
  param([string]$Exe, [string]$Port)
  if (-not $Exe) { return $false }
  try {
    $output = & $Exe funnel status 2>&1 | Out-String
    return ($output -match [regex]::Escape("127.0.0.1:$Port")) -or ($output -match [regex]::Escape("localhost:$Port"))
  } catch {
    return $false
  }
}

$publicUrl = Get-ConfiguredValue -Path (Join-Path $Root "TMDS-Shared-URL.txt") -Fallback "https://desktop-p6bd8s3.tailf80859.ts.net/"
$configuredPort = Get-ConfiguredValue -Path (Join-Path $Root ".tmds-web-port") -Fallback "4173"
$localIp = Get-PrimaryIpv4Address
$tailscale = Get-TailscaleExe

Write-Host ""
Write-Host "TMDS hosted mode (Tailscale Funnel)"
Write-Host "Public URL: $publicUrl"
if ($localIp) {
  Write-Host "LAN URL:    http://${localIp}:$configuredPort/"
}
Write-Host "Host bind:  0.0.0.0:$configuredPort"
Write-Host ""

if ($tailscale) {
  if (Test-TailscaleFunnel -Exe $tailscale -Port $configuredPort) {
    Write-Host "Tailscale Funnel: ACTIVE on port $configuredPort"
  } else {
    Write-Host "Tailscale Funnel: NOT detected on port $configuredPort"
    Write-Host "  To enable, run (once): `"$tailscale`" funnel --bg $configuredPort"
  }
} else {
  Write-Host "Tailscale not found. Install from https://tailscale.com/download/windows"
}
Write-Host ""
Write-Host "Keep this window/app running on the home PC for other users."
Write-Host ""

& (Join-Path $PSScriptRoot "start-webapp.ps1") -WebHost "0.0.0.0" -WebPort $configuredPort
