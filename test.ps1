param(
    [string[]]$Profiles = @(0..9 | ForEach-Object { '0.5.{0}' -f $_ }),
    [string]$RequestPath = 'C:\data3\request.json',
    [string]$BlockStoreRoot = 'C:\data3\block-store',
    [string]$OutputRoot = 'C:\data3\v05x-quality',
    [int]$TraversalWidth = 16
)

$ErrorActionPreference = 'Stop'

Set-Location $PSScriptRoot

$exe = Join-Path $PSScriptRoot 'target\release\lexonarchivebuilder-indexer.exe'
if (-not (Test-Path -Path $exe)) { throw "Indexer binary not found at '$exe'. Build with: cargo build --release -p lexonarchivebuilder-indexer" }
$logDir = Join-Path $OutputRoot 'logs'
$resultsCsv = Join-Path $OutputRoot ("results-tw{0}.csv" -f $TraversalWidth)

if ($Profiles.Count -eq 0) {
    throw 'Provide at least one profile version.'
}

$baselineProfile = $Profiles[0]
$comparisonCsv = Join-Path $OutputRoot ("comparison-vs-v{0}-tw{1}.csv" -f $baselineProfile, $TraversalWidth)

New-Item -ItemType Directory -Force -Path $OutputRoot, $logDir | Out-Null

$rows = [System.Collections.ArrayList]::new()

foreach ($profile in $Profiles) {
    $summaryPath = Join-Path $OutputRoot ("summary-v{0}.json" -f $profile)
    $qualityPath = Join-Path $OutputRoot ("quality-v{0}-tw{1}.json" -f $profile, $TraversalWidth)
    $runLog = Join-Path $logDir ("run-v{0}.log" -f $profile)
    $qualityLog = Join-Path $logDir ("quality-v{0}-tw{1}.log" -f $profile, $TraversalWidth)

    Remove-Item $summaryPath, $qualityPath, $runLog, $qualityLog -Force -ErrorAction SilentlyContinue

    Write-Host "=== START $profile BUILD ==="
    & $exe run `
        --request $RequestPath `
        --stage clustering-and-block-assembly `
        --profile-version $profile `
        --summary-out $summaryPath *>> $runLog
    if ($LASTEXITCODE -ne 0) {
        throw "Build failed for $profile with exit code $LASTEXITCODE"
    }

    $root = (Get-Content $summaryPath -Raw | ConvertFrom-Json).root_id

    Write-Host "=== START $profile QUALITY root=$root tw=$TraversalWidth ==="
    & $exe quality `
        --root-id $root `
        --block-store-root $BlockStoreRoot `
        --traversal-width $TraversalWidth `
        --json-out $qualityPath *>> $qualityLog
    if ($LASTEXITCODE -ne 0) {
        throw "Quality failed for $profile with exit code $LASTEXITCODE"
    }

    $report = Get-Content $qualityPath -Raw | ConvertFrom-Json
    $tnn1 = [double](($report.corpus_tnn_recall.recall_at | Where-Object { $_.k -eq 1 }).mean_recall)
    $tnn5 = [double](($report.corpus_tnn_recall.recall_at | Where-Object { $_.k -eq 5 }).mean_recall)
    $tnn10 = [double](($report.corpus_tnn_recall.recall_at | Where-Object { $_.k -eq 10 }).mean_recall)

    [void]$rows.Add([pscustomobject]@{
        profile      = $profile
        root_id      = $root
        tnn1         = $tnn1
        tnn5         = $tnn5
        tnn10        = $tnn10
        summary_path = $summaryPath
        quality_path = $qualityPath
    })

    $rows | Sort-Object @{ Expression = { [version]$_.profile } } | Export-Csv -Path $resultsCsv -NoTypeInformation

    Write-Host ("=== DONE {0} TNN@1={1:N4} TNN@5={2:N4} TNN@10={3:N4} ===" -f $profile, $tnn1, $tnn5, $tnn10)
}

$baseline = $rows | Where-Object { $_.profile -eq $baselineProfile } | Select-Object -First 1
if ($null -ne $baseline) {
    $comparison = foreach ($row in ($rows | Sort-Object @{ Expression = { [version]$_.profile } })) {
        [pscustomobject]@{
            profile     = $row.profile
            tnn1        = [double]$row.tnn1
            delta_tnn1  = [double]$row.tnn1 - [double]$baseline.tnn1
            tnn5        = [double]$row.tnn5
            delta_tnn5  = [double]$row.tnn5 - [double]$baseline.tnn5
            tnn10       = [double]$row.tnn10
            delta_tnn10 = [double]$row.tnn10 - [double]$baseline.tnn10
            root_id     = $row.root_id
        }
    }
    $comparison | Export-Csv -Path $comparisonCsv -NoTypeInformation
}

Write-Host '=== ALL PROFILES COMPLETE ==='
Write-Host "Results: $resultsCsv"
Write-Host "Comparison: $comparisonCsv"
