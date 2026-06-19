@description('Location for CDN resources.')
param location string

@description('Tags applied to CDN resources.')
param tags object = {}

@description('Name of the CDN profile.')
param profileName string

@description('Name of the CDN endpoint.')
param endpointName string

@description('CDN SKU.')
@allowed([
  'Standard_Akamai'
])
param skuName string = 'Standard_Akamai'

@description('Storage account name used to generate the origin SAS token.')
param storageAccountName string

@description('Origin blob container name.')
param containerName string

@description('Blob hostname exposed as the custom origin.')
param storageBlobHostName string

@description('SAS expiry timestamp in UTC.')
param sasExpiry string

@description('Whether the generated SAS should include list permission.')
param includeSasListPermission bool = false

@description('Global CDN cache duration.')
param cacheDuration string = '7.00:00:00'

@description('Query string caching behavior for CDN.')
@allowed([
  'IgnoreQueryString'
  'BypassCaching'
  'UseQueryString'
])
param queryStringCachingBehavior string = 'IgnoreQueryString'

@description('Whether to emit a URL rewrite rule that prepends the container path.')
param enableUrlRewrite bool = true

@description('Optional custom domain hostname.')
param customDomainHostName string = ''

@description('Whether to emit the generated origin query string as a module output.')
param outputOriginQueryString bool = false

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

var originQueryString = '?${storage.listServiceSas(storage.apiVersion, sasConfig).serviceSasToken}'
var deliveryRules = concat(
  [
    {
      name: 'GlobalCachePolicy'
      order: 1
      actions: [
        {
          name: 'CacheExpiration'
          parameters: {
            typeName: 'DeliveryRuleCacheExpirationActionParameters'
            cacheBehavior: 'Override'
            cacheType: 'All'
            cacheDuration: cacheDuration
          }
        }
      ]
    }
  ],
  enableUrlRewrite ? [
    {
      name: 'ContainerPrefixRewrite'
      order: 2
      actions: [
        {
          name: 'UrlRewrite'
          parameters: {
            typeName: 'DeliveryRuleUrlRewriteActionParameters'
            sourcePattern: '/'
            destination: '/${containerName}/'
            preserveUnmatchedPath: true
          }
        }
      ]
    }
  ] : []
)

resource profile 'Microsoft.Cdn/profiles@2023-05-01' = {
  name: profileName
  location: 'Global'
  tags: tags
  sku: {
    name: skuName
  }
  properties: {
    originResponseTimeoutSeconds: 60
  }
}

resource endpoint 'Microsoft.Cdn/profiles/endpoints@2023-05-01' = {
  name: endpointName
  parent: profile
  location: 'Global'
  tags: tags
  properties: {
    isHttpAllowed: false
    isHttpsAllowed: true
    isCompressionEnabled: true
    queryStringCachingBehavior: queryStringCachingBehavior
    optimizationType: 'GeneralWebDelivery'
    originHostHeader: storageBlobHostName
    contentTypesToCompress: [
      'text/plain'
      'text/css'
      'text/javascript'
      'application/javascript'
      'application/json'
      'application/xml'
      'text/xml'
      'image/svg+xml'
    ]
    origins: [
      {
        name: 'blob-origin'
        properties: {
          hostName: storageBlobHostName
          httpPort: 80
          httpsPort: 443
          originHostHeader: storageBlobHostName
          priority: 1
          weight: 1000
          enabled: true
        }
      }
    ]
    deliveryPolicy: {
      rules: deliveryRules
    }
  }
}

resource customDomain 'Microsoft.Cdn/profiles/endpoints/customDomains@2021-06-01' = if (!empty(customDomainHostName)) {
  name: replace(customDomainHostName, '.', '-')
  parent: endpoint
  properties: {
    hostName: customDomainHostName
  }
}

output profileName string = profile.name
output endpointName string = endpoint.name
output hostName string = endpoint.properties.hostName
output originQueryString string = outputOriginQueryString ? originQueryString : ''
