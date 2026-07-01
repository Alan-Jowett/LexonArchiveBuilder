@description('Location for network resources.')
param location string

@description('Tags applied to network resources.')
param tags object = {}

@description('Name of the virtual network.')
param vnetName string

@description('Address space for the virtual network.')
param vnetAddressPrefixes array = [
  '10.40.0.0/16'
]

@description('Address prefix for the VM subnet.')
param vmSubnetPrefix string = '10.40.0.0/24'

@description('Whether to allow inbound SSH on the VM subnet.')
param enableSshAccess bool = false

@description('Allowed source prefixes for SSH.')
param sshSourcePrefixes array = []

var sshSecurityRules = [
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
]

resource vmSubnetNsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = {
  name: '${vnetName}-vm-nsg'
  location: location
  tags: tags
  properties: {
    securityRules: enableSshAccess ? sshSecurityRules : []
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
        }
      }
    ]
  }
}

output vmSubnetId string = resourceId('Microsoft.Network/virtualNetworks/subnets', vnet.name, 'vm-subnet')
