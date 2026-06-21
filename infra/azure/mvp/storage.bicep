@description('Location for storage resources.')
param location string

@description('Tags applied to storage resources.')
param tags object = {}

@description('Name of the storage account.')
param storageAccountName string

@description('Name of the private blob container.')
param containerName string

@description('Public network access mode for the storage account.')
@allowed([
  'Enabled'
  'Disabled'
])
param publicNetworkAccess string = 'Enabled'

@description('IP ranges allowed by the storage firewall.')
param allowedIpCidrs array = []

@description('Subnet resource IDs allowed by the storage firewall.')
param allowedSubnetIds array = []

@description('Whether to create a private endpoint for VM-side access.')
param enablePrivateEndpoint bool = false

@description('Subnet resource ID for the storage private endpoint.')
param privateEndpointSubnetId string = ''

@description('Private DNS zone ID for storage private endpoints.')
param privateDnsZoneId string = ''

@description('SAS expiry timestamp in UTC.')
param sasExpiry string

@description('Whether the generated SAS should include list permission.')
param includeSasListPermission bool = false

@description('Whether to emit the generated SAS as a module output.')
param outputSasToken bool = false

var sasPermissions = includeSasListPermission ? 'rl' : 'r'
var blobHostName = '${storageAccountName}.blob.${environment().suffixes.storage}'
var sasConfig = {
  canonicalizedResource: '/blob/${storageAccountName}/${containerName}'
  signedResource: 'c'
  signedProtocol: 'https'
  signedPermission: sasPermissions
  signedExpiry: sasExpiry
  keyToSign: 'key1'
}

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
    publicNetworkAccess: publicNetworkAccess
    supportsHttpsTrafficOnly: true
    allowBlobPublicAccess: false
    allowSharedKeyAccess: true
    isHnsEnabled: false
    networkAcls: {
      bypass: 'None'
      defaultAction: 'Deny'
      ipRules: [
        for cidr in allowedIpCidrs: {
          action: 'Allow'
          value: cidr
        }
      ]
      virtualNetworkRules: [
        for subnetId in allowedSubnetIds: {
          action: 'Allow'
          id: subnetId
        }
      ]
    }
    encryption: {
      keySource: 'Microsoft.Storage'
      services: {
        blob: {
          enabled: true
          keyType: 'Account'
        }
        file: {
          enabled: true
          keyType: 'Account'
        }
      }
    }
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

resource privateEndpoint 'Microsoft.Network/privateEndpoints@2023-09-01' = if (enablePrivateEndpoint) {
  name: '${storageAccountName}-blob-pe'
  location: location
  tags: tags
  properties: {
    subnet: {
      id: privateEndpointSubnetId
    }
    privateLinkServiceConnections: [
      {
        name: '${storageAccountName}-blob-connection'
        properties: {
          privateLinkServiceId: storage.id
          groupIds: [
            'blob'
          ]
        }
      }
    ]
  }
}

resource privateDnsZoneGroup 'Microsoft.Network/privateEndpoints/privateDnsZoneGroups@2023-09-01' = if (enablePrivateEndpoint) {
  name: 'default'
  parent: privateEndpoint
  properties: {
    privateDnsZoneConfigs: [
      {
        name: 'blob-zone'
        properties: {
          privateDnsZoneId: privateDnsZoneId
        }
      }
    ]
  }
}

output storageAccountName string = storage.name
output containerName string = container.name
output blobHostName string = blobHostName
output blobEndpoint string = storage.properties.primaryEndpoints.blob
output sasToken string = outputSasToken ? storage.listServiceSas(storage.apiVersion, sasConfig).serviceSasToken : ''

