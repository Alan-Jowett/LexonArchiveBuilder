// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

@description('Location for the runner VM.')
param location string

@description('Tags applied to runner resources.')
param tags object = {}

@description('Name of the runner VM.')
param vmName string

@description('Size of the runner VM.')
param vmSize string = 'Standard_F1s'

@description('Subnet resource ID for the runner VM.')
param subnetId string

@description('Admin username for the runner VM.')
param adminUsername string = 'azureuser'

@description('SSH public key for the runner VM.')
param sshPublicKey string

@description('Whether to assign a public IP address to the runner VM.')
param enablePublicIp bool = false

@description('Subscription ID used when the VM deallocates itself after a run.')
param azureSubscriptionId string

@description('Resource group name used when the VM deallocates itself after a run.')
param azureResourceGroupName string

@description('Container SAS URL used by the hosted workload.')
param containerSasUrl string

@description('Storage account name used by the hosted workload.')
param storageAccountName string

@description('Blob container name used by the hosted workload.')
param containerName string

@description('Environment file content written before the workload script runs.')
@maxLength(32000)
param workloadEnvironmentFile string

@description('Workload script content executed by the runner VM.')
@maxLength(32000)
param workloadScript string

var nicName = '${vmName}-nic'
var publicIpName = '${vmName}-pip'
var containerSasUrlBase64 = base64(containerSasUrl)
var storageAccountNameBase64 = base64(storageAccountName)
var containerNameBase64 = base64(containerName)
var workloadEnvironmentFileBase64 = base64(workloadEnvironmentFile)
var workloadScriptBase64 = base64(workloadScript)
var renderWorkloadStorageEnvScript = loadTextContent('render-workload-storage-env.py')
var cloudInitTemplate = '''
#cloud-config
package_update: true
runcmd:
  - |
    set -eu
    apt-get update
    apt-get install -y --no-install-recommends ca-certificates curl docker.io python3
    systemctl enable docker
    systemctl start docker
    mkdir -p /opt/lexonarchivebuilder/runner
    cat > /opt/lexonarchivebuilder/runner/render_workload_storage_env.py <<'PY'
    __RENDER_WORKLOAD_STORAGE_ENV_PY__
    PY
    printf '%s' '__WORKLOAD_ENVIRONMENT_FILE_BASE64__' | base64 -d > /opt/lexonarchivebuilder/runner/workload.env
    printf '\n' >> /opt/lexonarchivebuilder/runner/workload.env
    python3 - '__CONTAINER_SAS_URL_BASE64__' '__STORAGE_ACCOUNT_NAME_BASE64__' '__CONTAINER_NAME_BASE64__' >> /opt/lexonarchivebuilder/runner/workload.env <<'PY'
    import base64
    import sys

    sys.path.insert(0, '/opt/lexonarchivebuilder/runner')
    from render_workload_storage_env import main

    container_sas_url_b64, storage_account_name_b64, container_name_b64 = sys.argv[1:4]
    raise SystemExit(
        main(
            base64.b64decode(container_sas_url_b64).decode('utf-8'),
            base64.b64decode(storage_account_name_b64).decode('utf-8'),
            base64.b64decode(container_name_b64).decode('utf-8'),
        )
    )
    PY
    printf '%s' '__WORKLOAD_SCRIPT_BASE64__' | base64 -d > /opt/lexonarchivebuilder/runner/workload.sh
    chmod 0600 /opt/lexonarchivebuilder/runner/workload.env
    chmod 0644 /opt/lexonarchivebuilder/runner/render_workload_storage_env.py
    chmod 0755 /opt/lexonarchivebuilder/runner/workload.sh
    cat > /usr/local/bin/lexonarchivebuilder-runner-wrapper.sh <<'EOF'
    #!/usr/bin/env bash
    set -euo pipefail
    source /opt/lexonarchivebuilder/runner/workload.env
    set +e
    /opt/lexonarchivebuilder/runner/workload.sh
    WORKLOAD_EXIT_CODE=$?
    set -e
    echo "${WORKLOAD_EXIT_CODE}" > /opt/lexonarchivebuilder/runner/last-exit-code
    if [ "${WORKLOAD_EXIT_CODE}" -ne 0 ] && [ "${DEBUG_RETAIN_ON_FAILURE:-false}" = "true" ]; then
      echo "debug-retained" > /opt/lexonarchivebuilder/runner/last-deallocate-exit-code
      exit "${WORKLOAD_EXIT_CODE}"
    fi
    ARM_TOKEN=''
    TOKEN_EXIT_CODE=0
    set +e
    ARM_TOKEN="$(curl --fail -sS --connect-timeout 5 --max-time 20 -H Metadata:true 'http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https://management.azure.com/' | python3 -c "import json,sys; print(json.load(sys.stdin)['access_token'])")"
    TOKEN_EXIT_CODE=$?
    set -e
    echo "${TOKEN_EXIT_CODE}" > /opt/lexonarchivebuilder/runner/last-token-exit-code
    DEALLOCATE_EXIT_CODE=0
    if [ "${TOKEN_EXIT_CODE}" -eq 0 ]; then
      set +e
      curl --fail -sS --connect-timeout 5 --max-time 30 -X POST -H "Authorization: Bearer ${ARM_TOKEN}" -H 'Content-Length: 0' "https://management.azure.com/subscriptions/__AZURE_SUBSCRIPTION_ID__/resourceGroups/__AZURE_RESOURCE_GROUP_NAME__/providers/Microsoft.Compute/virtualMachines/__VM_NAME__/deallocate?api-version=2023-09-01"
      DEALLOCATE_EXIT_CODE=$?
      set -e
    else
      DEALLOCATE_EXIT_CODE="${TOKEN_EXIT_CODE}"
    fi
    echo "${DEALLOCATE_EXIT_CODE}" > /opt/lexonarchivebuilder/runner/last-deallocate-exit-code
    SHUTDOWN_EXIT_CODE=0
    if [ "${DEALLOCATE_EXIT_CODE}" -ne 0 ]; then
      set +e
      systemctl poweroff --no-block
      SHUTDOWN_EXIT_CODE=$?
      set -e
      echo "${SHUTDOWN_EXIT_CODE}" > /opt/lexonarchivebuilder/runner/last-shutdown-exit-code
    fi
    if [ "${WORKLOAD_EXIT_CODE}" -ne 0 ]; then
      exit "${WORKLOAD_EXIT_CODE}"
    fi
    if [ "${DEALLOCATE_EXIT_CODE}" -ne 0 ]; then
      if [ "${SHUTDOWN_EXIT_CODE}" -ne 0 ]; then
        exit "${SHUTDOWN_EXIT_CODE}"
      fi
      exit "${DEALLOCATE_EXIT_CODE}"
    fi
    exit 0
    EOF
    chmod 0755 /usr/local/bin/lexonarchivebuilder-runner-wrapper.sh
    cat > /etc/systemd/system/lexonarchivebuilder-runner.service <<'EOF'
    [Unit]
    Description=LexonArchiveBuilder one-shot experiment run
    Wants=network-online.target docker.service
    After=network-online.target docker.service

    [Service]
    Type=oneshot
    ExecStart=/usr/local/bin/lexonarchivebuilder-runner-wrapper.sh

    [Install]
    WantedBy=multi-user.target
    EOF
    systemctl daemon-reload
    systemctl enable lexonarchivebuilder-runner.service
    systemctl start lexonarchivebuilder-runner.service
'''
var cloudInitWithWorkloadFiles = replace(
  replace(
    replace(
      replace(
        replace(
          cloudInitTemplate,
          '__WORKLOAD_ENVIRONMENT_FILE_BASE64__',
          workloadEnvironmentFileBase64
        ),
        '__CONTAINER_SAS_URL_BASE64__',
        containerSasUrlBase64
      ),
      '__STORAGE_ACCOUNT_NAME_BASE64__',
      storageAccountNameBase64
    ),
    '__CONTAINER_NAME_BASE64__',
    containerNameBase64
  ),
  '__RENDER_WORKLOAD_STORAGE_ENV_PY__',
  renderWorkloadStorageEnvScript
)
var cloudInitWithWorkloadFilesAndStorageRenderer = replace(
  cloudInitWithWorkloadFiles,
  '__WORKLOAD_SCRIPT_BASE64__',
  workloadScriptBase64
)
var cloudInit = replace(
  replace(
    replace(
      cloudInitWithWorkloadFilesAndStorageRenderer,
      '__AZURE_SUBSCRIPTION_ID__',
      azureSubscriptionId
    ),
    '__AZURE_RESOURCE_GROUP_NAME__',
    azureResourceGroupName
  ),
  '__VM_NAME__',
  vmName
)

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
