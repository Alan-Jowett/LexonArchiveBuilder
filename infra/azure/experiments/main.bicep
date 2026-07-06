// SPDX-License-Identifier: MIT
// Copyright (c) 2026 LexonArchiveBuilder contributors

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

@description('Container SAS URL for the reusable experiment storage container.')
@secure()
param containerSasUrl string

@description('Name of the runner VM.')
param vmName string

@description('Size of the runner VM.')
param vmSize string = 'Standard_DS1_v2'

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
    containerSasUrl: containerSasUrl
    storageAccountName: storageAccountName
    containerName: containerName
    workloadEnvironmentFile: workloadEnvironmentFile
    workloadScript: workloadScript
  }
}

output vmName string = vmName
output vmPublicIpAddress string = runner.outputs.publicIpAddress
