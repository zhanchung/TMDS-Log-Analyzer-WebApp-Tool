Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Get-InventoryRow {
    param(
        [Parameter(Mandatory = $true)][string]$Kind,
        [Parameter(Mandatory = $true)][string]$Title,
        [Parameter(Mandatory = $true)][string]$Path,
        [string]$Version,
        [string]$Section,
        [string]$Page,
        [string]$Notes
    )

    [pscustomobject]@{
        kind = $Kind
        title = $Title
        path = $Path
        version = $Version
        section = $Section
        page = $Page
        notes = $Notes
    }
}

function Test-TextExtension {
    param([Parameter(Mandatory = $true)][string]$Path)
    $ext = [System.IO.Path]::GetExtension($Path).ToLowerInvariant()
    return $ext -in @(".txt", ".log", ".csv", ".json", ".xml", ".md", ".tsv", ".ini", ".cfg")
}

function Get-SourceInventoryFromMusic {
    param([string]$MusicRoot = (Join-Path $env:USERPROFILE "Music"))

    $rows = New-Object System.Collections.Generic.List[object]

    $rows.Add((Get-InventoryRow -Kind "manual" -Title "Genisys manuals archive" -Path (Join-Path $MusicRoot "03_GenisysManuals.zip") -Notes "Expanded folder with Genisys trace and code-system manuals."))
    $rows.Add((Get-InventoryRow -Kind "icd" -Title "ICD PDF for PTC archive" -Path (Join-Path $MusicRoot "ICD PDF FOR PTC.zip") -Notes "Archive with three ICD PDFs and revision variants."))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "More Data operational reports" -Path (Join-Path $MusicRoot "More Data") -Notes "Folder wrapper with braking, locomotive fault, onboard config, and back-office event report bundles. Contains operational version context such as ICD 3.0 exports." ))
    $rows.Add((Get-InventoryRow -Kind "sample_log" -Title "More Stuff server logs" -Path (Join-Path $MusicRoot "More Stuff") -Notes "Folder wrapper with CodeServer, CommServer, ControlServer, and MDM log bundles plus version strings." ))
    $rows.Add((Get-InventoryRow -Kind "sample_log" -Title "MORE BOC back-office control logs" -Path (Join-Path $MusicRoot "MORE BOC") -Notes "Folder wrapper with BackOfficeControl event logs and related operational traces." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "TMDS Technical Memo - Near Side Signal Control" -Path (Join-Path $MusicRoot "TMDS Technical Memo - Near Side Signal Control.pdf") -Notes "Workflow/context source for side-panel explanation."))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "TCOS Flow" -Path (Join-Path $MusicRoot "TCOS TRAINING\\TCOS Flow.pptx") -Notes "System overview deck for TMDS component relationships and operational workflow." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "CAD Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\CAD Training.pptx") -Notes "Dispatcher workflow deck for sign-in, territory loading, control functions, and near-side signal controls." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "BOS System Administrator Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\NCTD Training Materials-System Administrator - Instructor's Guide BOS Presentation.pptx") -Notes "Back Office Server training deck covering PTC trains, raw logs, menu functions, and configuration surfaces." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "Code Server Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\Training Materials-System Administrator - Instructor's Guide CODE Presentation - QC.pptx") -Notes "Code Server training deck covering code lines, packet switch emulation, logging, and function assignments." ))
    $rows.Add((Get-InventoryRow -Kind "sample_log" -Title "01_CodeServerBuild archive" -Path (Join-Path $MusicRoot "01_CodeServerBuild.zip") -Notes "Code Server build wrapper containing code-line event logs, socket traces, and runtime binaries." ))
    $rows.Add((Get-InventoryRow -Kind "sample_log" -Title "CodeServer backup event logs" -Path (Join-Path $MusicRoot "CodeServer-BackupEventLogs-03-02-26.zip") -Notes "Code Server backup bundle with indication, recall, control, and service-signal samples." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "Track Editor Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\Training Materials-System Administrator - Instructor's Guide TED Presentation.pptx") -Notes "Track Editor training deck covering territories, control points, devices, and code station configuration." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "MDM Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\MDM Training.pptx") -Notes "Mobile Device Manager training deck covering download/upload workflows, versioning, and HA behavior." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "Admin Client Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\Admin Client Training Presentation.pptx") -Notes "Admin Client training deck covering database-facing maintenance and management functions." ))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "WebApp Training" -Path (Join-Path $MusicRoot "TCOS TRAINING\\WebApp Training Presentation.pptx") -Notes "WebApp training deck covering web-based administration, configuration, roles, users, and data maintenance." ))
    $rows.Add((Get-InventoryRow -Kind "manual" -Title "ITCM merged manuals" -Path (Join-Path $MusicRoot "ITCM Merged All Manuals.pdf") -Notes "Likely broad support reference; text extraction still blocked."))
    $rows.Add((Get-InventoryRow -Kind "manual" -Title "VHLC ATCS Code System Emulation 100227-008" -Path (Join-Path $MusicRoot "VHLC ATCS Code System Emulation 100227-008.pdf") -Notes "Code-system reference from Music root."))
    $rows.Add((Get-InventoryRow -Kind "workflow" -Title "TCOS TRAINING workflow chart" -Path (Join-Path $MusicRoot "TCOS TRAINING\WORKFLOW CHART.png") -Notes "Visual workflow reference for the explanation-first tab."))

    Get-ChildItem -Path $MusicRoot -Recurse -File -ErrorAction SilentlyContinue |
        Where-Object { Test-TextExtension $_.FullName } |
        ForEach-Object {
            $name = $_.Name
            if ($name -match '(?i)sockettrace|eventlog|exceptionlog|statistical|genisys|icd|workflow|memo') {
                $kind = if ($name -match '(?i)genisys') { "manual" } elseif ($name -match '(?i)icd') { "icd" } elseif ($name -match '(?i)workflow|memo') { "workflow" } else { "sample_log" }
                $rows.Add((Get-InventoryRow -Kind $kind -Title $name -Path $_.FullName -Notes "Discovered by filename sweep."))
            }
        }

    return $rows
}

function Write-InventoryCsv {
    param(
        [Parameter(Mandatory = $true)]$Rows,
        [Parameter(Mandatory = $true)][string]$OutPath
    )

    $Rows | Export-Csv -NoTypeInformation -Encoding UTF8 -Path $OutPath
}

function Write-InventoryJson {
    param(
        [Parameter(Mandatory = $true)]$Rows,
        [Parameter(Mandatory = $true)][string]$OutPath
    )

    $Rows | ConvertTo-Json -Depth 6 | Set-Content -Encoding UTF8 -Path $OutPath
}

function Ensure-ParentDirectory {
    param([Parameter(Mandatory = $true)][string]$Path)
    $parent = Split-Path -Parent $Path
    if ($parent -and -not (Test-Path $parent)) {
        New-Item -ItemType Directory -Force -Path $parent | Out-Null
    }
}
