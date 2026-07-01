# SPDX-License-Identifier: MIT
# Copyright (c) 2026 LexonArchiveBuilder contributors

param(
    [string]$Root = $PSScriptRoot
)

$ErrorActionPreference = 'Stop'

$requiredFiles = @(
    'main.bicep',
    'network.bicep',
    'storage.bicep',
    'cdn.bicep',
    'keyvault.bicep',
    'vm-indexer.bicep',
    'vm-embedder.bicep',
    'main.parameters.example.json'
)

foreach ($relativePath in $requiredFiles) {
    $fullPath = Join-Path $Root $relativePath
    if (-not (Test-Path -LiteralPath $fullPath)) {
        throw "Missing required deployment artifact: $relativePath"
    }
}

$mainBicep = Get-Content -LiteralPath (Join-Path $Root 'main.bicep') -Raw
$cdnBicep = Get-Content -LiteralPath (Join-Path $Root 'cdn.bicep') -Raw
$storageBicep = Get-Content -LiteralPath (Join-Path $Root 'storage.bicep') -Raw
$parameters = Get-Content -LiteralPath (Join-Path $Root 'main.parameters.example.json') -Raw | ConvertFrom-Json

$requiredMainPatterns = @(
    "module\s+storage\s+'storage\.bicep'",
    "module\s+cdn\s+'cdn\.bicep'",
    "module\s+keyVault\s+'keyvault\.bicep'",
    "module\s+indexerVm\s+'vm-indexer\.bicep'",
    "module\s+embedderVm\s+'vm-embedder\.bicep'",
    'output\s+postDeployOriginConfiguration\s+object',
    '@maxLength\(32000\)\s*param\s+indexerRequestJson\s+string'
)

foreach ($pattern in $requiredMainPatterns) {
    if (-not [regex]::IsMatch($mainBicep, $pattern)) {
        throw "main.bicep is missing expected content matching pattern: $pattern"
    }
}

if (-not $cdnBicep.Contains("Standard_Akamai")) {
    throw 'cdn.bicep is missing the Standard_Akamai SKU selection.'
}

if (-not [regex]::IsMatch($cdnBicep, 'output\s+originQueryString\s+string')) {
    throw 'cdn.bicep is missing the originQueryString output for the post-deploy Akamai step.'
}

if (-not $storageBicep.Contains("allowBlobPublicAccess: false")) {
    throw 'storage.bicep must disable anonymous blob access.'
}

$requiredParameterNames = @(
    'storageAccountName',
    'containerName',
    'sasExpiry',
    'cdnProfileName',
    'cdnEndpointName',
    'indexerImageReference',
    'embedderImageReference',
    'indexerRequestJson'
)

foreach ($parameterName in $requiredParameterNames) {
    if (-not ($parameters.parameters.PSObject.Properties.Name -contains $parameterName)) {
        throw "main.parameters.example.json is missing required parameter: $parameterName"
    }
}

Write-Host 'Azure MVP deployment package verification passed.'
