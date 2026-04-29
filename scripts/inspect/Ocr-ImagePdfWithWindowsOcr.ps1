param(
    [Parameter(Mandatory = $true)]
    [string]$InputPdf,
    [Parameter(Mandatory = $true)]
    [string]$OutDir,
    [string]$LanguageTag = "ko"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if (-not (Test-Path $OutDir)) {
    New-Item -ItemType Directory -Force -Path $OutDir | Out-Null
}

$inputFile = Get-Item -LiteralPath $InputPdf
$safeStem = ($inputFile.BaseName -replace "[^A-Za-z0-9._-]", "_")
$imageDir = Join-Path $OutDir ($safeStem + "_pages")
$ocrJsonPath = Join-Path $OutDir ($safeStem + ".ocr.pages.json")
$ocrTextPath = Join-Path $OutDir ($safeStem + ".ocr.txt")

if (-not (Test-Path $imageDir)) {
    New-Item -ItemType Directory -Force -Path $imageDir | Out-Null
}

$pythonScript = @'
from pathlib import Path
from pypdf import PdfReader
import sys

input_pdf = Path(sys.argv[1])
image_dir = Path(sys.argv[2])
image_dir.mkdir(parents=True, exist_ok=True)

reader = PdfReader(str(input_pdf))
for page_index, page in enumerate(reader.pages, start=1):
    xobjects = page["/Resources"].get("/XObject")
    if not xobjects:
        continue
    image_written = False
    for _, ref in xobjects.items():
        obj = ref.get_object()
        if obj.get("/Subtype") != "/Image":
            continue
        image_path = image_dir / f"page_{page_index:03d}.jpg"
        image_path.write_bytes(obj.get_data())
        image_written = True
        break
    if not image_written:
        print(f"NO_IMAGE\t{page_index}")

print(f"PAGES\t{len(list(image_dir.glob('page_*.jpg')))}")
'@

@"
$pythonScript
"@ | python - $InputPdf $imageDir | Out-Host

Add-Type -AssemblyName System.Runtime.WindowsRuntime
$null = [Windows.Storage.StorageFile, Windows.Storage, ContentType=WindowsRuntime]
$null = [Windows.Storage.Streams.IRandomAccessStream, Windows.Storage.Streams, ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.BitmapDecoder, Windows.Graphics.Imaging, ContentType=WindowsRuntime]
$null = [Windows.Graphics.Imaging.SoftwareBitmap, Windows.Graphics.Imaging, ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrEngine, Windows.Foundation, ContentType=WindowsRuntime]
$null = [Windows.Media.Ocr.OcrResult, Windows.Foundation, ContentType=WindowsRuntime]
$null = [Windows.Globalization.Language, Windows.Globalization, ContentType=WindowsRuntime]

function Await-WinRT {
    param(
        [Parameter(Mandatory = $true)]$AsyncOperation,
        [Parameter(Mandatory = $true)][Type]$ResultType
    )

    $asTaskMethod = [System.WindowsRuntimeSystemExtensions].GetMethods() |
        Where-Object {
            $_.Name -eq "AsTask" -and
            $_.IsGenericMethod -and
            $_.GetParameters().Count -eq 1 -and
            $_.ReturnType.Name -eq 'Task`1' -and
            $_.GetParameters()[0].ParameterType.Name -eq 'IAsyncOperation`1'
        } |
        Select-Object -First 1

    if (-not $asTaskMethod) {
        throw "Unable to locate Windows Runtime AsTask<TResult>(IAsyncOperation<TResult>) overload."
    }

    $asTask = $asTaskMethod.MakeGenericMethod($ResultType)

    $task = $asTask.Invoke($null, @($AsyncOperation))
    $task.Wait(-1) | Out-Null
    return $task.Result
}

$language = New-Object Windows.Globalization.Language($LanguageTag)
$ocrEngine = [Windows.Media.Ocr.OcrEngine]::TryCreateFromLanguage($language)
if (-not $ocrEngine) {
    throw "OCR engine unavailable for language tag '$LanguageTag'."
}

$pageResults = New-Object System.Collections.Generic.List[object]
foreach ($file in Get-ChildItem -LiteralPath $imageDir -Filter "page_*.jpg" | Sort-Object Name) {
    $storageFile = Await-WinRT ([Windows.Storage.StorageFile]::GetFileFromPathAsync($file.FullName)) ([Windows.Storage.StorageFile])
    $stream = Await-WinRT ($storageFile.OpenAsync([Windows.Storage.FileAccessMode]::Read)) ([Windows.Storage.Streams.IRandomAccessStream])
    try {
        $decoder = Await-WinRT ([Windows.Graphics.Imaging.BitmapDecoder]::CreateAsync($stream)) ([Windows.Graphics.Imaging.BitmapDecoder])
        $bitmap = Await-WinRT ($decoder.GetSoftwareBitmapAsync()) ([Windows.Graphics.Imaging.SoftwareBitmap])
        $ocrResult = Await-WinRT ($ocrEngine.RecognizeAsync($bitmap)) ([Windows.Media.Ocr.OcrResult])
        $pageNumber = [int]([System.Text.RegularExpressions.Regex]::Match($file.BaseName, "\d+").Value)
        $pageResults.Add(
            [pscustomobject]@{
                page_number = $pageNumber
                image_path = $file.FullName
                char_count = $ocrResult.Text.Length
                text = $ocrResult.Text
            }
        ) | Out-Null
    } finally {
        $stream.Dispose()
    }
}

$pageResults |
    ConvertTo-Json -Depth 4 |
    Set-Content -Encoding UTF8 -Path $ocrJsonPath

(
    $pageResults |
    Sort-Object page_number |
    ForEach-Object { $_.text }
) -join ([Environment]::NewLine + [Environment]::NewLine + [Environment]::NewLine) |
    Set-Content -Encoding UTF8 -Path $ocrTextPath

Write-Host ("JSON`t" + $ocrJsonPath)
Write-Host ("TXT`t" + $ocrTextPath)
Write-Host ("PAGES`t" + $pageResults.Count)
Write-Host ("CHARS`t" + (($pageResults | Measure-Object char_count -Sum).Sum))
