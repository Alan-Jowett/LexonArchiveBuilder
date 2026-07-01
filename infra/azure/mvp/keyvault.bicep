// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

@description('Location for the Key Vault.')
param location string

@description('Tags applied to Key Vault resources.')
param tags object = {}

@description('Name of the Azure Key Vault.')
param keyVaultName string

@description('Storage account name used to generate origin secrets.')
param storageAccountName string

@description('Blob container name used to generate the container SAS.')
param containerName string

@description('SAS expiry timestamp in UTC.')
param sasExpiry string

@description('Whether the SAS should include list permission.')
param includeSasListPermission bool = false

var sasPermissions = includeSasListPermission ? 'rl' : 'r'
var sasConfig = {
  canonicalizedResource: '/blob/${storageAccountName}/${containerName}'
  signedResource: 'c'
  signedProtocol: 'https'
  signedPermission: sasPermissions
  signedExpiry: sasExpiry
  keyToSign: 'key1'
}

resource storage 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storageAccountName
}

resource vault 'Microsoft.KeyVault/vaults@2023-07-01' = {
  name: keyVaultName
  location: location
  tags: tags
  properties: {
    tenantId: subscription().tenantId
    enableRbacAuthorization: true
    sku: {
      family: 'A'
      name: 'standard'
    }
    softDeleteRetentionInDays: 90
    enablePurgeProtection: true
  }
}

resource sasSecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  name: 'cdn-origin-sas'
  parent: vault
  properties: {
    value: storage.listServiceSas(storage.apiVersion, sasConfig).serviceSasToken
  }
}

resource storageKeySecret 'Microsoft.KeyVault/vaults/secrets@2023-07-01' = {
  name: 'cdn-origin-storage-key'
  parent: vault
  properties: {
    value: storage.listKeys().keys[0].value
  }
}

output vaultUri string = vault.properties.vaultUri
output sasSecretUri string = sasSecret.properties.secretUriWithVersion
output storageKeySecretUri string = storageKeySecret.properties.secretUriWithVersion

