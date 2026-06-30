@description('Location for storage resources.')
param location string

@description('Tags applied to storage resources.')
param tags object = {}

@description('Name of the storage account.')
param storageAccountName string

@description('Name of the blob container.')
param containerName string

@description('SAS expiry timestamp in UTC.')
param sasExpiry string

@description('Service SAS permissions for the experiment container.')
param sasPermissions string = 'racwl'

var blobHostName = '${storageAccountName}.blob.${environment().suffixes.storage}'
var containerSasToken = storage.listServiceSas(storage.apiVersion, {
  canonicalizedResource: '/blob/${storageAccountName}/${containerName}'
  signedResource: 'c'
  signedProtocol: 'https'
  signedPermission: sasPermissions
  signedExpiry: sasExpiry
  keyToSign: 'key1'
}).serviceSasToken

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' = {
  name: storageAccountName
  location: location
  tags: tags
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    accessTier: 'Hot'
    minimumTlsVersion: 'TLS1_2'
    publicNetworkAccess: 'Enabled'
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: true
    isHnsEnabled: false
  }
}

resource blobService 'Microsoft.Storage/storageAccounts/blobServices@2023-05-01' = {
  name: 'default'
  parent: storage
}

resource container 'Microsoft.Storage/storageAccounts/blobServices/containers@2023-05-01' = {
  name: containerName
  parent: blobService
  properties: {
    publicAccess: 'None'
  }
}

output storageAccountName string = storage.name
output containerName string = container.name
output blobEndpoint string = storage.properties.primaryEndpoints.blob
output containerSasToken string = containerSasToken
output containerSasUrl string = '${storage.properties.primaryEndpoints.blob}${containerName}?${containerSasToken}'
