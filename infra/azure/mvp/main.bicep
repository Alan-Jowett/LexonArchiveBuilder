targetScope = 'resourceGroup'

@description('Location for the Azure MVP deployment.')
param location string = resourceGroup().location

@description('Tags applied to deployed Azure resources.')
param tags object = {}

@description('Name of the virtual network.')
param vnetName string = 'lab-mvp-vnet'

@description('Address space for the virtual network.')
param vnetAddressPrefixes array = [
  '10.30.0.0/16'
]

@description('Address prefix for the shared VM subnet.')
param vmSubnetPrefix string = '10.30.0.0/24'

@description('Address prefix for the private-endpoint subnet.')
param privateEndpointSubnetPrefix string = '10.30.1.0/24'

@description('Name of the storage account.')
param storageAccountName string

@description('Name of the private blob container used as the CDN origin.')
param containerName string

@description('Whether public network access remains enabled on the storage account. The Akamai hidden-origin compromise requires Enabled.')
@allowed([
  'Enabled'
  'Disabled'
])
param storagePublicNetworkAccess string = 'Enabled'

@description('IP ranges allowed through the storage firewall. Provide Azure CDN POP ranges and any operator-required public IPs.')
param storageAllowedIpCidrs array = []

@description('Whether the VM subnet should also be allowed to reach the storage account public endpoint through service endpoints.')
param allowVmSubnetStorageAccess bool = true

@description('Whether to create a storage private endpoint for VM-side access.')
param enableStoragePrivateEndpoint bool = false

@description('SAS expiry timestamp in UTC for origin access.')
param sasExpiry string

@description('Whether the generated container SAS should include list permission in addition to read.')
param includeSasListPermission bool = false

@description('Whether to emit the generated origin SAS token as a deployment output.')
param outputSasToken bool = false

@description('Whether to deploy an Azure Key Vault for origin secrets.')
param enableKeyVault bool = true

@description('Name of the Azure Key Vault when enabled.')
param keyVaultName string = 'lab-mvp-kv'

@description('Name of the CDN profile.')
param cdnProfileName string = 'lab-mvp-cdn'

@description('Name of the CDN endpoint.')
param cdnEndpointName string = 'lab-mvp-endpoint'

@description('CDN SKU. This implementation targets Azure CDN Standard from Akamai.')
@allowed([
  'Standard_Akamai'
])
param cdnSkuName string = 'Standard_Akamai'

@description('Global cache duration for CDN content.')
param cdnCacheDuration string = '7.00:00:00'

@description('Query string caching behavior for the CDN endpoint.')
@allowed([
  'IgnoreQueryString'
  'BypassCaching'
  'UseQueryString'
])
param cdnQueryStringCachingBehavior string = 'IgnoreQueryString'

@description('Whether to emit a rewrite rule that prepends the blob container path.')
param enableUrlRewrite bool = true

@description('Optional custom domain hostname for the CDN endpoint.')
param cdnCustomDomainHostName string = ''

@description('Name of the indexing VM.')
param indexerVmName string = 'lab-indexer-vm'

@description('VM size for the indexing VM.')
param indexerVmSize string = 'Standard_F1s'

@description('Name of the embedding VM.')
param embedderVmName string = 'lab-embedder-vm'

@description('VM size for the embedding VM.')
param embedderVmSize string = 'Standard_B1s'

@description('Admin username for both Linux VMs.')
param adminUsername string = 'azureuser'

@description('SSH public key placed on the Linux VMs.')
param sshPublicKey string

@description('Whether to assign a public IP address to the indexing VM.')
param enableIndexerPublicIp bool = false

@description('Whether to assign a public IP address to the embedding VM.')
param enableEmbedderPublicIp bool = false

@description('Whether to allow inbound SSH on the VM subnet NSG.')
param enableSshAccess bool = false

@description('Source prefixes allowed to use SSH when enabled.')
param sshSourcePrefixes array = []

@description('Port exposed by the embedding API on the embedding VM.')
param embeddingPort int = 8080

@description('Whether the VM subnet NSG should allow inbound embedding API traffic.')
param enableEmbedderPublicIngress bool = false

@description('Source prefixes allowed to reach the embedding API when public ingress is enabled.')
param embedderIngressSourcePrefixes array = []

@description('The full GHCR image reference for the indexing container.')
param indexerImageReference string

@description('The full GHCR image reference for the embedding container.')
param embedderImageReference string

@description('Directory on the indexing VM host that receives summary output.')
param indexOutputPath string = '/opt/lexonarchivebuilder/indexer/output'

@description('JSON request payload written to the indexing VM before the indexer container runs.')
param indexerRequestJson string

@description('Storage-access mode hint exported to the indexing VM bootstrap.')
param indexerStorageAccessMode string = 'sas'

@description('Storage-access mode hint exported to the embedding VM bootstrap.')
param embedderStorageAccessConfiguration string = ''

module network 'network.bicep' = {
  name: 'lab-mvp-network'
  params: {
    location: location
    tags: tags
    vnetName: vnetName
    vnetAddressPrefixes: vnetAddressPrefixes
    vmSubnetPrefix: vmSubnetPrefix
    privateEndpointSubnetPrefix: privateEndpointSubnetPrefix
    enableSshAccess: enableSshAccess
    sshSourcePrefixes: sshSourcePrefixes
    enableStoragePrivateEndpoint: enableStoragePrivateEndpoint
  }
}

module storage 'storage.bicep' = {
  name: 'lab-mvp-storage'
  params: {
    location: location
    tags: tags
    storageAccountName: storageAccountName
    containerName: containerName
    publicNetworkAccess: storagePublicNetworkAccess
    allowedIpCidrs: storageAllowedIpCidrs
    allowedSubnetIds: allowVmSubnetStorageAccess ? [
      network.outputs.vmSubnetId
    ] : []
    enablePrivateEndpoint: enableStoragePrivateEndpoint
    privateEndpointSubnetId: network.outputs.privateEndpointSubnetId
    privateDnsZoneId: network.outputs.privateDnsZoneId
    sasExpiry: sasExpiry
    includeSasListPermission: includeSasListPermission
    outputSasToken: outputSasToken
  }
}

module cdn 'cdn.bicep' = {
  name: 'lab-mvp-cdn'
  params: {
    location: location
    tags: tags
    profileName: cdnProfileName
    endpointName: cdnEndpointName
    skuName: cdnSkuName
    storageAccountName: storage.outputs.storageAccountName
    containerName: storage.outputs.containerName
    storageBlobHostName: storage.outputs.blobHostName
    sasExpiry: sasExpiry
    includeSasListPermission: includeSasListPermission
    cacheDuration: cdnCacheDuration
    queryStringCachingBehavior: cdnQueryStringCachingBehavior
    enableUrlRewrite: enableUrlRewrite
    customDomainHostName: cdnCustomDomainHostName
    outputOriginQueryString: outputSasToken
  }
}

module keyVault 'keyvault.bicep' = if (enableKeyVault) {
  name: 'lab-mvp-keyvault'
  params: {
    location: location
    tags: tags
    keyVaultName: keyVaultName
    storageAccountName: storage.outputs.storageAccountName
    containerName: storage.outputs.containerName
    sasExpiry: sasExpiry
    includeSasListPermission: includeSasListPermission
  }
}

module indexerVm 'vm-indexer.bicep' = {
  name: 'lab-mvp-indexer-vm'
  params: {
    location: location
    tags: tags
    vmName: indexerVmName
    vmSize: indexerVmSize
    subnetId: network.outputs.vmSubnetId
    adminUsername: adminUsername
    sshPublicKey: sshPublicKey
    enablePublicIp: enableIndexerPublicIp
    imageReference: indexerImageReference
    requestJson: indexerRequestJson
    indexOutputPath: indexOutputPath
    storageAccessMode: indexerStorageAccessMode
  }
}

module embedderVm 'vm-embedder.bicep' = {
  name: 'lab-mvp-embedder-vm'
  params: {
    location: location
    tags: tags
    vmName: embedderVmName
    vmSize: embedderVmSize
    subnetId: network.outputs.vmSubnetId
    adminUsername: adminUsername
    sshPublicKey: sshPublicKey
    enablePublicIp: enableEmbedderPublicIp
    imageReference: embedderImageReference
    embeddingPort: embeddingPort
    storageAccessConfiguration: embedderStorageAccessConfiguration
    enablePublicIngress: enableEmbedderPublicIngress
    ingressSourcePrefixes: embedderIngressSourcePrefixes
  }
}

output cdnPublicUrl string = 'https://${cdn.outputs.hostName}'
output embeddingVmPublicUrl string = enableEmbedderPublicIp && !empty(embedderVm.outputs.publicIpAddress) ? 'http://${embedderVm.outputs.publicIpAddress}:${embeddingPort}' : ''
output originSasToken string = outputSasToken ? storage.outputs.sasToken : ''
output keyVaultUri string = enableKeyVault ? keyVault.outputs.vaultUri : ''
output postDeployOriginConfiguration object = {
  profileName: cdn.outputs.profileName
  endpointName: cdn.outputs.endpointName
  storageBlobHostName: storage.outputs.blobHostName
  originPath: '/${containerName}'
  originQueryString: outputSasToken ? cdn.outputs.originQueryString : ''
  originQueryStringSource: outputSasToken ? 'deployment output' : (enableKeyVault ? 'Key Vault secret cdn-origin-sas' : 'generate manually from the storage account')
  note: 'Azure CDN Standard (Akamai) requires a post-deploy origin-query-string step to attach the hidden-origin SAS because this package does not use Azure Front Door Private Link.'
}
