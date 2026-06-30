targetScope = 'resourceGroup'

@description('Location for the Azure experiment deployment.')
param location string = resourceGroup().location

@description('Tags applied to deployed experiment resources.')
param tags object = {}

@description('Name of the virtual network.')
param vnetName string = 'lab-experiment-vnet'

@description('Name of the storage account.')
param storageAccountName string

@description('Name of the blob container.')
param containerName string

@description('Expiry timestamp for the container SAS token.')
param sasExpiry string

@description('Service SAS permissions for the experiment container.')
param sasPermissions string = 'racwl'

@description('Name of the runner VM.')
param vmName string

@description('Size of the runner VM.')
param vmSize string = 'Standard_F1s'

@description('Admin username for the runner VM.')
param adminUsername string = 'azureuser'

@description('SSH public key for the runner VM.')
param sshPublicKey string

@description('Whether to assign a public IP address to the runner VM.')
param enablePublicIp bool = false

@description('Whether to allow inbound SSH on the runner VM subnet.')
param enableSshAccess bool = false

@description('Source prefixes allowed to use SSH when enabled.')
param sshSourcePrefixes array = []

@description('Environment file content written before the workload script runs.')
param workloadEnvironmentFile string

@description('Workload script content executed by the runner VM.')
param workloadScript string

var deploymentSuffix = uniqueString(vmName)

module network 'network.bicep' = {
  name: 'lab-experiment-network-${deploymentSuffix}'
  params: {
    location: location
    tags: tags
    vnetName: vnetName
    enableSshAccess: enableSshAccess
    sshSourcePrefixes: sshSourcePrefixes
  }
}

module storage 'storage.bicep' = {
  name: 'lab-experiment-storage-${deploymentSuffix}'
  params: {
    location: location
    tags: tags
    storageAccountName: storageAccountName
    containerName: containerName
    sasExpiry: sasExpiry
    sasPermissions: sasPermissions
  }
}

resource storageAccount 'Microsoft.Storage/storageAccounts@2023-05-01' existing = {
  name: storage.outputs.storageAccountName
}

var containerSasToken = storageAccount.listServiceSas('2023-05-01', {
  canonicalizedResource: '/blob/${storage.outputs.storageAccountName}/${storage.outputs.containerName}'
  signedResource: 'c'
  signedProtocol: 'https'
  signedPermission: sasPermissions
  signedExpiry: sasExpiry
  keyToSign: 'key1'
}).serviceSasToken
var containerSasUrl = '${storage.outputs.blobEndpoint}${storage.outputs.containerName}?${containerSasToken}'
var resolvedWorkloadEnvironmentFile = 'CONTAINER_SAS_URL=''${containerSasUrl}''
STORAGE_ACCOUNT_NAME=''${storage.outputs.storageAccountName}''
CONTAINER_NAME=''${storage.outputs.containerName}''
${workloadEnvironmentFile}'

module runner 'vm-runner.bicep' = {
  name: 'lab-experiment-runner-${deploymentSuffix}'
  params: {
    location: location
    tags: tags
    vmName: vmName
    vmSize: vmSize
    subnetId: network.outputs.vmSubnetId
    adminUsername: adminUsername
    sshPublicKey: sshPublicKey
    enablePublicIp: enablePublicIp
    azureSubscriptionId: subscription().subscriptionId
    azureResourceGroupName: resourceGroup().name
    workloadEnvironmentFile: resolvedWorkloadEnvironmentFile
    workloadScript: workloadScript
  }
}

output storageAccountName string = storage.outputs.storageAccountName
output containerName string = storage.outputs.containerName
output blobEndpoint string = storage.outputs.blobEndpoint
output vmName string = vmName
output vmPublicIpAddress string = runner.outputs.publicIpAddress
