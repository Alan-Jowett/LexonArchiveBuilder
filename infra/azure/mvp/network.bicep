// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

@description('Location for network resources.')
param location string

@description('Tags applied to network resources.')
param tags object = {}

@description('Name of the virtual network.')
param vnetName string

@description('Address space for the virtual network.')
param vnetAddressPrefixes array

@description('Address prefix for the shared VM subnet.')
param vmSubnetPrefix string

@description('Address prefix for the private-endpoint subnet.')
param privateEndpointSubnetPrefix string

@description('Whether to allow inbound SSH on the VM subnet.')
param enableSshAccess bool = false

@description('Allowed source prefixes for SSH.')
param sshSourcePrefixes array = []

@description('Whether to create the private DNS zone for storage private endpoints.')
param enableStoragePrivateEndpoint bool = false

resource vmSubnetNsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${vnetName}-vm-nsg'
  location: location
  tags: tags
  properties: {
    securityRules: enableSshAccess ? [
      for (prefix, index) in sshSourcePrefixes: {
        name: 'allow-ssh-${index}'
        properties: {
          access: 'Allow'
          direction: 'Inbound'
          priority: 100 + index
          protocol: 'Tcp'
          sourceAddressPrefix: prefix
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: '22'
        }
      }
    ] : []
  }
}

resource privateEndpointSubnetNsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${vnetName}-pe-nsg'
  location: location
  tags: tags
  properties: {
    securityRules: []
  }
}

resource vnet 'Microsoft.Network/virtualNetworks@2023-09-01' = {
  name: vnetName
  location: location
  tags: tags
  properties: {
    addressSpace: {
      addressPrefixes: vnetAddressPrefixes
    }
    subnets: [
      {
        name: 'vm-subnet'
        properties: {
          addressPrefix: vmSubnetPrefix
          networkSecurityGroup: {
            id: vmSubnetNsg.id
          }
          serviceEndpoints: [
            {
              service: 'Microsoft.Storage'
            }
          ]
        }
      }
      {
        name: 'private-endpoint-subnet'
        properties: {
          addressPrefix: privateEndpointSubnetPrefix
          networkSecurityGroup: {
            id: privateEndpointSubnetNsg.id
          }
          privateEndpointNetworkPolicies: 'Disabled'
          privateLinkServiceNetworkPolicies: 'Disabled'
        }
      }
    ]
  }
}

resource privateDnsZone 'Microsoft.Network/privateDnsZones@2020-06-01' = if (enableStoragePrivateEndpoint) {
  name: 'privatelink.blob.core.windows.net'
  location: 'global'
  tags: tags
}

resource privateDnsLink 'Microsoft.Network/privateDnsZones/virtualNetworkLinks@2020-06-01' = if (enableStoragePrivateEndpoint) {
  parent: privateDnsZone
  name: '${vnetName}-link'
  location: 'global'
  properties: {
    virtualNetwork: {
      id: vnet.id
    }
    registrationEnabled: false
  }
}

output vnetId string = vnet.id
output vmSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', vnet.name, 'vm-subnet')
output privateEndpointSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', vnet.name, 'private-endpoint-subnet')
output privateDnsZoneId string = enableStoragePrivateEndpoint ? privateDnsZone.id : ''
