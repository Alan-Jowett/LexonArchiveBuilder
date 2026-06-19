@description('Location for the indexing VM.')
param location string

@description('Tags applied to the indexing VM resources.')
param tags object = {}

@description('Name of the indexing VM.')
param vmName string

@description('Size of the indexing VM.')
param vmSize string

@description('Subnet resource ID for the indexing VM.')
param subnetId string

@description('Admin username for the indexing VM.')
param adminUsername string

@description('SSH public key for the indexing VM.')
param sshPublicKey string

@description('Whether to assign a public IP address.')
param enablePublicIp bool = false

@description('Full GHCR image reference for the indexer container.')
param imageReference string

@description('JSON request payload written to the indexing VM.')
param requestJson string

@description('Host directory used for indexer summary output.')
param indexOutputPath string

@description('Storage-access mode hint exported to the container environment.')
param storageAccessMode string = 'sas'

var nicName = '${vmName}-nic'
var publicIpName = '${vmName}-pip'
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
    mkdir -p /opt/lexonarchivebuilder/indexer
    mkdir -p ${indexOutputPath}
    cat > /opt/lexonarchivebuilder/indexer/docker-compose.yml <<'EOF'
    services:
      indexer:
        image: ${imageReference}
        restart: "no"
        environment:
          LAB_STORAGE_ACCESS_MODE: ${storageAccessMode}
        volumes:
          - /opt/lexonarchivebuilder/indexer/request.json:/workspace/request.json:ro
          - ${indexOutputPath}:/workspace/output
        command:
          - run
          - --request
          - /workspace/request.json
          - --summary-out
          - /workspace/output/summary.json
    EOF
    cat > /opt/lexonarchivebuilder/indexer/request.json <<'EOF'
    ${requestJson}
    EOF
    docker compose -f /opt/lexonarchivebuilder/indexer/docker-compose.yml pull
    set +e
    docker compose -f /opt/lexonarchivebuilder/indexer/docker-compose.yml up --abort-on-container-exit --exit-code-from indexer
    EXIT_CODE=$?
    set -e
    echo "${EXIT_CODE}" > /opt/lexonarchivebuilder/indexer/last-exit-code
    shutdown -h now
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

resource nic 'Microsoft.Network/networkInterfaces@2023-09-01' = {
  name: nicName
  location: location
  tags: tags
  properties: {
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
  }
}

resource vm 'Microsoft.Compute/virtualMachines@2023-09-01' = {
  name: vmName
  location: location
  tags: tags
  identity: {
    type: 'SystemAssigned'
  }
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

