// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

@description('Location for the embedding VM.')
param location string

@description('Tags applied to the embedding VM resources.')
param tags object = {}

@description('Name of the embedding VM.')
param vmName string

@description('Size of the embedding VM.')
param vmSize string

@description('Subnet resource ID for the embedding VM.')
param subnetId string

@description('Admin username for the embedding VM.')
param adminUsername string

@description('SSH public key for the embedding VM.')
param sshPublicKey string

@description('Whether to assign a public IP address.')
param enablePublicIp bool = false

@description('Full GHCR image reference for the embedding container.')
param imageReference string

@description('Host port exposed by the embedding container.')
param embeddingPort int = 8080

@description('Optional storage-access configuration hint exported to the embedding container.')
param storageAccessConfiguration string = ''

@description('Whether to allow inbound embedding API traffic directly to this VM NIC.')
param enablePublicIngress bool = false

@description('Source prefixes allowed to reach the embedding API when public ingress is enabled.')
param ingressSourcePrefixes array = []

var nicName = '${vmName}-nic'
var publicIpName = '${vmName}-pip'
var ingressNsgName = '${vmName}-ingress-nsg'
var cloudInit = '''
#cloud-config
package_update: true
runcmd:
  - |
    set -euxo pipefail
    apt-get update
    apt-get install -y --no-install-recommends ca-certificates curl docker.io docker-compose-plugin
    systemctl enable docker
    systemctl start docker
    mkdir -p /opt/lexonarchivebuilder/embedder
    cat > /opt/lexonarchivebuilder/embedder/docker-compose.yml <<'EOF'
    services:
      embedder:
        image: ${imageReference}
        restart: always
        environment:
          LAB_STORAGE_ACCESS_CONFIGURATION: '${replace(storageAccessConfiguration, '''', '''''')}'
        ports:
          - "${embeddingPort}:8080"
    EOF
    docker compose -f /opt/lexonarchivebuilder/embedder/docker-compose.yml pull
    docker compose -f /opt/lexonarchivebuilder/embedder/docker-compose.yml up -d
'''

resource publicIp 'Microsoft.Network/publicIPAddresses@2023-09-01' = if (enablePublicIp) {
  name: publicIpName
  location: location
  tags: tags
  sku: {
    name: 'Standard'
  }
  properties: {
    publicIPAllocationMethod: 'Static'
  }
}

resource ingressNsg 'Microsoft.Network/networkSecurityGroups@2023-09-01' = if (enablePublicIngress) {
  name: ingressNsgName
  location: location
  tags: tags
  properties: {
    securityRules: [
      for (prefix, index) in ingressSourcePrefixes: {
        name: 'allow-embedder-api-${index}'
        properties: {
          access: 'Allow'
          direction: 'Inbound'
          priority: 100 + index
          protocol: 'Tcp'
          sourceAddressPrefix: prefix
          sourcePortRange: '*'
          destinationAddressPrefix: '*'
          destinationPortRange: string(embeddingPort)
        }
      }
    ]
  }
}

resource nic 'Microsoft.Network/networkInterfaces@2023-09-01' = {
  name: nicName
  location: location
  tags: tags
  properties: union(
    {
      ipConfigurations: [
        {
          name: 'ipconfig1'
          properties: {
            privateIPAllocationMethod: 'Dynamic'
            subnet: {
              id: subnetId
            }
            publicIPAddress: enablePublicIp ? {
              id: publicIp.id
            } : null
          }
        }
      ]
    },
    enablePublicIngress ? {
      networkSecurityGroup: {
        id: ingressNsg.id
      }
    } : {}
  )
}

resource vm 'Microsoft.Compute/virtualMachines@2023-09-01' = {
  name: vmName
  location: location
  tags: tags
  properties: {
    hardwareProfile: {
      vmSize: vmSize
    }
    osProfile: {
      computerName: vmName
      adminUsername: adminUsername
      customData: base64(cloudInit)
      linuxConfiguration: {
        disablePasswordAuthentication: true
        ssh: {
          publicKeys: [
            {
              path: '/home/${adminUsername}/.ssh/authorized_keys'
              keyData: sshPublicKey
            }
          ]
        }
      }
    }
    storageProfile: {
      imageReference: {
        publisher: 'Canonical'
        offer: '0001-com-ubuntu-server-jammy'
        sku: '22_04-lts-gen2'
        version: 'latest'
      }
      osDisk: {
        createOption: 'FromImage'
        managedDisk: {
          storageAccountType: 'Standard_LRS'
        }
      }
    }
    networkProfile: {
      networkInterfaces: [
        {
          id: nic.id
        }
      ]
    }
  }
}

output vmId string = vm.id
output publicIpAddress string = enablePublicIp ? publicIp.properties.ipAddress : ''
