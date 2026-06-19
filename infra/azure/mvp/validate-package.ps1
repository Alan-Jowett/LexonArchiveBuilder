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

$requiredMainSnippets = @(
    "module storage 'storage.bicep'",
    "module cdn 'cdn.bicep'",
    "module indexerVm 'vm-indexer.bicep'",
    "module embedderVm 'vm-embedder.bicep'",
    'output postDeployOriginConfiguration object'
)

foreach ($snippet in $requiredMainSnippets) {
    if (-not $mainBicep.Contains($snippet)) {
        throw "main.bicep is missing expected content: $snippet"
    }
}

if (-not $cdnBicep.Contains("Standard_Akamai")) {
    throw 'cdn.bicep is missing the Standard_Akamai SKU selection.'
}

if (-not $cdnBicep.Contains('originQueryString')) {
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
