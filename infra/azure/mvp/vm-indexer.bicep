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

@description('Subscription ID used when the VM deallocates itself after an indexing run.')
param azureSubscriptionId string

@description('Resource group name used when the VM deallocates itself after an indexing run.')
param azureResourceGroupName string

@description('Full GHCR image reference for the indexer container.')
param imageReference string

@description('JSON request payload written to the indexing VM.')
@maxLength(32000)
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
    apt-get install -y --no-install-recommends ca-certificates curl docker.io docker-compose-plugin python3
    systemctl enable docker
    systemctl start docker
    mkdir -p /opt/lexonarchivebuilder/indexer
    mkdir -p "${indexOutputPath}"
    cat > /opt/lexonarchivebuilder/indexer/docker-compose.yml <<'EOF'
    services:
      indexer:
        image: ${imageReference}
        restart: "no"
        environment:
          LAB_STORAGE_ACCESS_MODE: '${replace(storageAccessMode, '''', '''''')}'
        volumes:
          - /opt/lexonarchivebuilder/indexer/request.json:/workspace/request.json:ro
          - "${indexOutputPath}:/workspace/output"
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
    cat > /usr/local/bin/lexonarchivebuilder-run-indexer.sh <<'EOF'
    #!/usr/bin/env bash
    set -euo pipefail
    docker compose -f /opt/lexonarchivebuilder/indexer/docker-compose.yml pull
    set +e
    docker compose -f /opt/lexonarchivebuilder/indexer/docker-compose.yml up --abort-on-container-exit --exit-code-from indexer
    EXIT_CODE=$?
    set -e
    echo "${EXIT_CODE}" > /opt/lexonarchivebuilder/indexer/last-exit-code
    ARM_TOKEN=''
    TOKEN_EXIT_CODE=0
    set +e
    ARM_TOKEN="$(curl --fail -sS -H Metadata:true 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/' | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")"
    TOKEN_EXIT_CODE=$?
    set -e
    echo "${TOKEN_EXIT_CODE}" > /opt/lexonarchivebuilder/indexer/last-token-exit-code
    DEALLOCATE_EXIT_CODE=0
    if [ "${TOKEN_EXIT_CODE}" -eq 0 ]; then
      set +e
      curl --fail -sS -X POST -H "Authorization: Bearer ${ARM_TOKEN}" -H 'Content-Length: 0' "https://management.azure.com/subscriptions/${azureSubscriptionId}/resourceGroups/${azureResourceGroupName}/providers/Microsoft.Compute/virtualMachines/${vmName}/deallocate?api-version=2023-09-01"
      DEALLOCATE_EXIT_CODE=$?
      set -e
    else
      DEALLOCATE_EXIT_CODE="${TOKEN_EXIT_CODE}"
    fi
    echo "${DEALLOCATE_EXIT_CODE}" > /opt/lexonarchivebuilder/indexer/last-deallocate-exit-code
    SHUTDOWN_EXIT_CODE=0
    if [ "${DEALLOCATE_EXIT_CODE}" -ne 0 ]; then
      set +e
      systemctl poweroff --no-block
      SHUTDOWN_EXIT_CODE=$?
      set -e
      echo "${SHUTDOWN_EXIT_CODE}" > /opt/lexonarchivebuilder/indexer/last-shutdown-exit-code
    fi
    if [ "${EXIT_CODE}" -ne 0 ]; then
      exit "${EXIT_CODE}"
    fi
    if [ "${DEALLOCATE_EXIT_CODE}" -ne 0 ]; then
      if [ "${SHUTDOWN_EXIT_CODE}" -ne 0 ]; then
        exit "${SHUTDOWN_EXIT_CODE}"
      fi
      exit "${DEALLOCATE_EXIT_CODE}"
    fi
    exit 0
    EOF
    chmod 0755 /usr/local/bin/lexonarchivebuilder-run-indexer.sh
    cat > /etc/systemd/system/lexonarchivebuilder-indexer.service <<'EOF'
    [Unit]
    Description=LexonArchiveBuilder one-shot indexing run
    Wants=network-online.target docker.service
    After=network-online.target docker.service

    [Service]
    Type=oneshot
    ExecStart=/usr/local/bin/lexonarchivebuilder-run-indexer.sh

    [Install]
    WantedBy=multi-user.target
    EOF
    systemctl daemon-reload
    systemctl enable lexonarchivebuilder-indexer.service
    systemctl start lexonarchivebuilder-indexer.service
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

resource selfDeallocateRoleAssignment 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(vm.id, 'self-deallocate-role-assignment')
  scope: vm
  properties: {
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', '9980e02c-c2be-4d73-94e8-173b1dc7cf3c')
    principalId: vm.identity.principalId
    principalType: 'ServicePrincipal'
  }
}

output vmId string = vm.id
output publicIpAddress string = enablePublicIp ? publicIp.properties.ipAddress : ''
