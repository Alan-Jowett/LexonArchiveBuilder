# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

param(
    [string[]]$Profiles = @(0..9 | ForEach-Object { '0.6.{0}' -f $_ }),
    [string]$RequestPath = 'C:\data3\request.json',
    [string]$BlockStoreRoot = 'C:\data3\block-store',
    [string]$OutputRoot = 'C:\data3\v06x-quality',
    [int]$TraversalWidth = 16,
    [ValidateSet('profile-sweep', 'v070-ladder-plan', 'v070-ladder')]
    [string]$Experiment = 'profile-sweep',
    [string]$LadderProfile = '0.7.0',
    [int]$LadderBudget = 1024,
    [string[]]$LadderRungs = @('4x256', '8x128', '16x64', '32x32', '64x16')
)

$ErrorActionPreference = 'Stop'

Set-Location $PSScriptRoot

$exe = Join-Path $PSScriptRoot 'target\release\lexonarchivebuilder-indexer.exe'
if (-not (Test-Path -Path $exe)) { throw "Indexer binary not found at '$exe'. Build with: cargo build --release -p lexonarchivebuilder-indexer" }

function Parse-LadderRung {
    param(
        [string]$Rung,
        [int]$ExpectedBudget
    )

    if ($Rung -notmatch '^(\d+)x(\d+)$') {
        throw "Invalid ladder rung '$Rung'. Expected format '<beam_width>x<cluster_count>'."
    }

    $beamWidth = [int]$Matches[1]
    $clusterCount = [int]$Matches[2]
    if (($beamWidth * $clusterCount) -ne $ExpectedBudget) {
        throw "Invalid ladder rung '$Rung'. Expected beam width * cluster count = $ExpectedBudget."
    }

    [pscustomobject]@{
        rung          = $Rung
        beam_width    = $beamWidth
        cluster_count = $clusterCount
    }
}

function Join-CommandLine {
    param([string[]]$Arguments)

    if ($Arguments.Count -eq 0) {
        return '&'
    }

    $quotedArguments = $Arguments | ForEach-Object {
        "'" + ($_ -replace "'", "''") + "'"
    }

    '& ' + ($quotedArguments -join ' ')
}

function Get-V070LadderEntries {
    param(
        [string]$ExecutablePath,
        [string]$RequestPath,
        [string]$BlockStoreRoot,
        [string]$OutputRoot,
        [string]$Profile,
        [int]$Budget,
        [string[]]$Rungs
    )

    $logDir = Join-Path $OutputRoot 'logs'

    if ($Profile -eq '0.7.0') {
        throw "Published profile 0.7.0 now runs through the streaming-indexer v2 path, which rejects --local-testing-cluster-count. The repository-local ladder automation needs an updated upstream-supported override path before it can run."
    }

    foreach ($rung in $Rungs) {
        $parsed = Parse-LadderRung -Rung $rung -ExpectedBudget $Budget
        $validateLog = Join-Path $logDir ("validate-v{0}-tw{1}-cc{2}.log" -f $Profile, $parsed.beam_width, $parsed.cluster_count)
        $runSummaryPath = Join-Path $OutputRoot ("summary-v{0}-tw{1}-cc{2}.json" -f $Profile, $parsed.beam_width, $parsed.cluster_count)
        $qualityPath = Join-Path $OutputRoot ("quality-v{0}-tw{1}-cc{2}.json" -f $Profile, $parsed.beam_width, $parsed.cluster_count)
        $runLog = Join-Path $logDir ("run-v{0}-tw{1}-cc{2}.log" -f $Profile, $parsed.beam_width, $parsed.cluster_count)
        $qualityLog = Join-Path $logDir ("quality-v{0}-tw{1}-cc{2}.log" -f $Profile, $parsed.beam_width, $parsed.cluster_count)

        $validateArgs = @(
            $ExecutablePath, 'run',
            '--request', $RequestPath,
            '--stage', 'clustering-and-block-assembly',
            '--profile-version', $Profile,
            '--local-testing-cluster-count', $parsed.cluster_count,
            '--validate-only'
        )
        $runArgs = @(
            $ExecutablePath, 'run',
            '--request', $RequestPath,
            '--stage', 'clustering-and-block-assembly',
            '--profile-version', $Profile,
            '--local-testing-cluster-count', $parsed.cluster_count,
            '--summary-out', $runSummaryPath
        )
        $qualityArgs = @(
            $ExecutablePath, 'quality',
            '--root-id', '<root-from-summary>',
            '--block-store-root', $BlockStoreRoot,
            '--traversal-width', $parsed.beam_width,
            '--json-out', $qualityPath
        )

        [pscustomobject]@{
            rung               = $parsed.rung
            beam_width         = $parsed.beam_width
            cluster_count      = $parsed.cluster_count
            budget             = $Budget
            validate_log       = $validateLog
            summary_path       = $runSummaryPath
            quality_path       = $qualityPath
            run_log            = $runLog
            quality_log        = $qualityLog
            validate_command   = Join-CommandLine $validateArgs
            run_command        = Join-CommandLine $runArgs
            quality_command    = Join-CommandLine $qualityArgs
        }
    }
}

function Write-V070LadderPlan {
    param(
        [string]$ExecutablePath,
        [string]$RequestPath,
        [string]$BlockStoreRoot,
        [string]$OutputRoot,
        [string]$Profile,
        [int]$Budget,
        [string[]]$Rungs
    )

    $logDir = Join-Path $OutputRoot 'logs'
    New-Item -ItemType Directory -Force -Path $OutputRoot, $logDir | Out-Null

    $parsedRungs = @(Get-V070LadderEntries `
        -ExecutablePath $ExecutablePath `
        -RequestPath $RequestPath `
        -BlockStoreRoot $BlockStoreRoot `
        -OutputRoot $OutputRoot `
        -Profile $Profile `
        -Budget $Budget `
        -Rungs $Rungs)
    $planPath = Join-Path $OutputRoot ("ladder-plan-v{0}-budget{1}.json" -f $Profile, $Budget)
    $comparisonCsv = Join-Path $OutputRoot ("comparison-v{0}-budget{1}.csv" -f $Profile, $Budget)
    $plan = [pscustomobject]@{
        experiment = 'v070-fixed-budget-ladder'
        status = 'ready-to-run'
        profile = $Profile
        fixed_budget = $Budget
        request_path = $RequestPath
        block_store_root = $BlockStoreRoot
        output_root = $OutputRoot
        comparison_csv = $comparisonCsv
        execution_order = @(
            'Run validate-only preflight for each rung in listed order.',
            'Run clustering-and-block-assembly for each rung after its preflight passes.',
            'Run rooted quality with traversal width equal to the rung beam width.',
            'Compare TNN recall, access-cost metrics, and post-hoc block statistics across rungs.'
        )
        rungs = @($parsedRungs)
    }

    $plan | ConvertTo-Json -Depth 6 | Set-Content -Path $planPath -Encoding utf8

    Write-Host '=== V0.7.0 FIXED-BUDGET LADDER PLAN ==='
    Write-Host ("Profile: {0}" -f $Profile)
    Write-Host ("Fixed budget: {0}" -f $Budget)
    foreach ($rung in $parsedRungs) {
        Write-Host ("- rung {0}: beam width {1}, cluster_count {2}" -f $rung.rung, $rung.beam_width, $rung.cluster_count)
    }
    Write-Host ("Plan: {0}" -f $planPath)
    Write-Host ("Comparison target: {0}" -f $comparisonCsv)
}

function Get-RecallMean {
    param(
        [object]$Report,
        [int]$K
    )

    $entry = $Report.corpus_tnn_recall.recall_at | Where-Object { $_.k -eq $K } | Select-Object -First 1
    if ($null -eq $entry) {
        throw "Quality report did not contain recall@${K}."
    }
    [double]$entry.mean_recall
}

function Invoke-V070Ladder {
    param(
        [string]$ExecutablePath,
        [string]$RequestPath,
        [string]$BlockStoreRoot,
        [string]$OutputRoot,
        [string]$Profile,
        [int]$Budget,
        [string[]]$Rungs
    )

    $logDir = Join-Path $OutputRoot 'logs'
    $resultsCsv = Join-Path $OutputRoot ("results-v{0}-budget{1}.csv" -f $Profile, $Budget)
    $comparisonCsv = Join-Path $OutputRoot ("comparison-v{0}-budget{1}.csv" -f $Profile, $Budget)
    New-Item -ItemType Directory -Force -Path $OutputRoot, $logDir | Out-Null

    $entries = @(Get-V070LadderEntries `
        -ExecutablePath $ExecutablePath `
        -RequestPath $RequestPath `
        -BlockStoreRoot $BlockStoreRoot `
        -OutputRoot $OutputRoot `
        -Profile $Profile `
        -Budget $Budget `
        -Rungs $Rungs)
    $rows = [System.Collections.ArrayList]::new()
    $baselineRung = if ($Rungs -contains '16x64') { '16x64' } else { $entries[0].rung }

    foreach ($entry in $entries) {
        Remove-Item `
            $entry.validate_log, $entry.summary_path, $entry.quality_path, $entry.run_log, $entry.quality_log `
            -Force -ErrorAction SilentlyContinue

        Write-Host ("=== PREFLIGHT {0} tw={1} cc={2} ===" -f $Profile, $entry.beam_width, $entry.cluster_count)
        & $ExecutablePath run `
            --request $RequestPath `
            --stage clustering-and-block-assembly `
            --profile-version $Profile `
            --local-testing-cluster-count $entry.cluster_count `
            --validate-only *>> $entry.validate_log
        if ($LASTEXITCODE -ne 0) {
            throw "Validate-only preflight failed for rung $($entry.rung) with exit code $LASTEXITCODE"
        }

        Write-Host ("=== START {0} BUILD tw={1} cc={2} ===" -f $Profile, $entry.beam_width, $entry.cluster_count)
        & $ExecutablePath run `
            --request $RequestPath `
            --stage clustering-and-block-assembly `
            --profile-version $Profile `
            --local-testing-cluster-count $entry.cluster_count `
            --summary-out $entry.summary_path *>> $entry.run_log
        if ($LASTEXITCODE -ne 0) {
            throw "Build failed for rung $($entry.rung) with exit code $LASTEXITCODE"
        }

        $root = (Get-Content $entry.summary_path -Raw | ConvertFrom-Json).root_id

        Write-Host ("=== START {0} QUALITY root={1} tw={2} cc={3} ===" -f $Profile, $root, $entry.beam_width, $entry.cluster_count)
        & $ExecutablePath quality `
            --root-id $root `
            --block-store-root $BlockStoreRoot `
            --traversal-width $entry.beam_width `
            --json-out $entry.quality_path *>> $entry.quality_log
        if ($LASTEXITCODE -ne 0) {
            throw "Quality failed for rung $($entry.rung) with exit code $LASTEXITCODE"
        }

        $report = Get-Content $entry.quality_path -Raw | ConvertFrom-Json
        $access = $report.corpus_tnn_recall.access_summary
        $tnn1 = Get-RecallMean -Report $report -K 1
        $tnn5 = Get-RecallMean -Report $report -K 5
        $tnn10 = Get-RecallMean -Report $report -K 10

        [void]$rows.Add([pscustomobject]@{
            rung                = $entry.rung
            beam_width          = $entry.beam_width
            cluster_count       = $entry.cluster_count
            budget              = $Budget
            root_id             = $root
            tnn1                = $tnn1
            tnn5                = $tnn5
            tnn10               = $tnn10
            touched_block_count = [int]$access.touched_block_count
            bytes_read          = [int64]$access.bytes_read
            estimated_rtts      = [int]$access.estimated_rtts
            summary_path        = $entry.summary_path
            quality_path        = $entry.quality_path
        })

        $rows | Export-Csv -Path $resultsCsv -NoTypeInformation
        Write-Host ("=== DONE {0} rung={1} TNN@1={2:N4} TNN@5={3:N4} TNN@10={4:N4} blocks={5} bytes={6} rtts={7} ===" -f $Profile, $entry.rung, $tnn1, $tnn5, $tnn10, $access.touched_block_count, $access.bytes_read, $access.estimated_rtts)
    }

    $baseline = $rows | Where-Object { $_.rung -eq $baselineRung } | Select-Object -First 1
    if ($null -eq $baseline) {
        throw "Baseline rung '$baselineRung' was not found in ladder results."
    }

    $comparison = foreach ($row in $rows) {
        [pscustomobject]@{
            rung                      = $row.rung
            beam_width                = [int]$row.beam_width
            cluster_count             = [int]$row.cluster_count
            budget                    = [int]$row.budget
            tnn1                      = [double]$row.tnn1
            delta_tnn1                = [double]$row.tnn1 - [double]$baseline.tnn1
            tnn5                      = [double]$row.tnn5
            delta_tnn5                = [double]$row.tnn5 - [double]$baseline.tnn5
            tnn10                     = [double]$row.tnn10
            delta_tnn10               = [double]$row.tnn10 - [double]$baseline.tnn10
            touched_block_count       = [int]$row.touched_block_count
            delta_touched_block_count = [int]$row.touched_block_count - [int]$baseline.touched_block_count
            bytes_read                = [int64]$row.bytes_read
            delta_bytes_read          = [int64]$row.bytes_read - [int64]$baseline.bytes_read
            estimated_rtts            = [int]$row.estimated_rtts
            delta_estimated_rtts      = [int]$row.estimated_rtts - [int]$baseline.estimated_rtts
            root_id                   = $row.root_id
        }
    }
    $comparison | Export-Csv -Path $comparisonCsv -NoTypeInformation

    Write-Host '=== V0.7.0 LADDER COMPLETE ==='
    Write-Host ("Baseline rung: {0}" -f $baselineRung)
    Write-Host ("Results: {0}" -f $resultsCsv)
    Write-Host ("Comparison: {0}" -f $comparisonCsv)
}

if ($Experiment -eq 'v070-ladder-plan') {
    if ($OutputRoot -eq 'C:\data3\v06x-quality') {
        $OutputRoot = 'C:\data3\v070-ladder'
    }
    Write-V070LadderPlan `
        -ExecutablePath $exe `
        -RequestPath $RequestPath `
        -BlockStoreRoot $BlockStoreRoot `
        -OutputRoot $OutputRoot `
        -Profile $LadderProfile `
        -Budget $LadderBudget `
        -Rungs $LadderRungs
    return
}

if ($Experiment -eq 'v070-ladder') {
    if ($OutputRoot -eq 'C:\data3\v06x-quality') {
        $OutputRoot = 'C:\data3\v070-ladder'
    }
    Invoke-V070Ladder `
        -ExecutablePath $exe `
        -RequestPath $RequestPath `
        -BlockStoreRoot $BlockStoreRoot `
        -OutputRoot $OutputRoot `
        -Profile $LadderProfile `
        -Budget $LadderBudget `
        -Rungs $LadderRungs
    return
}

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
