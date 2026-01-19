// SASL Authentication
export const SASLMechanisms = {
  PLAIN: 'PLAIN',
  SCRAM_SHA_256: 'SCRAM-SHA-256',
  SCRAM_SHA_512: 'SCRAM-SHA-512',
  OAUTHBEARER: 'OAUTHBEARER',
  GSSAPI: 'GSSAPI'
} as const
export const allowedSASLMechanisms = Object.values(SASLMechanisms)
export type SASLMechanismLabel = keyof typeof SASLMechanisms
export type SASLMechanismValue = (typeof SASLMechanisms)[SASLMechanismLabel]

// Metadata API
// ./metadata/find-coordinator.ts
export const FindCoordinatorKeyTypes = { GROUP: 0, TRANSACTION: 1, SHARE: 2 } as const
export const allowedFindCoordinatorKeyTypes = Object.values(FindCoordinatorKeyTypes)
export type FindCoordinatorKeyTypeLabel = keyof typeof FindCoordinatorKeyTypes
export type FindCoordinatorKeyTypeValue = (typeof FindCoordinatorKeyTypes)[FindCoordinatorKeyTypeLabel]

// Producer API
export const ProduceAcks = {
  ALL: -1,
  NO_RESPONSE: 0,
  LEADER: 1
} as const
export const allowedProduceAcks = Object.values(ProduceAcks)
export type ProduceAckLabel = keyof typeof ProduceAcks
export type ProduceAcksValue = (typeof ProduceAcks)[ProduceAckLabel]

// Consumer API
export const GroupProtocols = { CLASSIC: 'classic', CONSUMER: 'consumer' } as const
export const allowedGroupProtocols = Object.values(GroupProtocols)
export type GroupProtocolLabel = keyof typeof GroupProtocols
export type GroupProtocolValue = (typeof GroupProtocols)[GroupProtocolLabel]

// ./consumer/fetch.ts
export const FetchIsolationLevels = { READ_UNCOMMITTED: 0, READ_COMMITTED: 1 } as const
export const allowedFetchIsolationLevels = Object.values(FetchIsolationLevels)
export type FetchIsolationLevelLabel = keyof typeof FetchIsolationLevels
export type FetchIsolationLevelValue = (typeof FetchIsolationLevels)[FetchIsolationLevelLabel]

export const ListOffsetTimestamps = { LATEST: -1n, EARLIEST: -2n } as const
export const allowedListOffsetTimestamps = Object.values(ListOffsetTimestamps)
export type ListOffsetTimestampLabel = keyof typeof ListOffsetTimestamps
export type ListOffsetTimestampValue = (typeof ListOffsetTimestamps)[ListOffsetTimestampLabel]

// Admin API
// ./admin/*-acls.ts - See: https://cwiki.apache.org/confluence/display/KAFKA/KIP-140%3A+Add+administrative+RPCs+for+adding%2C+deleting%2C+and+listing+ACLs
export const ResourceTypes = {
  UNKNOWN: 0,
  ANY: 1,
  TOPIC: 2,
  GROUP: 3,
  CLUSTER: 4,
  TRANSACTIONAL_ID: 5,
  DELEGATION_TOKEN: 6
} as const
export const allowedResourceTypes = Object.values(ResourceTypes)
export type ResourceTypeLabel = keyof typeof ResourceTypes
export type ResourceTypeValue = (typeof ResourceTypes)[ResourceTypeLabel]

export const ResourcePatternTypes = { UNKNOWN: 0, ANY: 1, MATCH: 2, LITERAL: 3, PREFIXED: 4 } as const
export const allowedResourcePatternTypes = Object.values(ResourcePatternTypes)
export type ResourcePatternTypeLabel = keyof typeof ResourcePatternTypes
export type ResourcePatternTypeValue = (typeof ResourcePatternTypes)[ResourcePatternTypeLabel]

export const AclOperations = {
  UNKNOWN: 0,
  ANY: 1,
  ALL: 2,
  READ: 3,
  WRITE: 4,
  CREATE: 5,
  DELETE: 6,
  ALTER: 7,
  DESCRIBE: 8,
  CLUSTER_ACTION: 9,
  DESCRIBE_CONFIGS: 10,
  ALTER_CONFIGS: 11,
  IDEMPOTENT_WRITE: 12
} as const
export const allowedAclOperations = Object.values(AclOperations)
export type AclOperationLabel = keyof typeof AclOperations
export type AclOperationValue = (typeof AclOperations)[AclOperationLabel]

export const AclPermissionTypes = { UNKNOWN: 0, ANY: 1, DENY: 2, ALLOW: 3 } as const
export const allowedAclPermissionTypes = Object.values(AclPermissionTypes)
export type AclPermissionTypeLabel = keyof typeof AclPermissionTypes
export type AclPermissionTypeValue = (typeof AclPermissionTypes)[AclPermissionTypeLabel]

// ./admin/*-configs.ts
export const ConfigSources = {
  UNKNOWN: 0,
  TOPIC_CONFIG: 1,
  DYNAMIC_BROKER_CONFIG: 2,
  DYNAMIC_DEFAULT_BROKER_CONFIG: 3,
  STATIC_BROKER_CONFIG: 4,
  DEFAULT_CONFIG: 5,
  DYNAMIC_BROKER_LOGGER_CONFIG: 6
} as const
export const allowedConfigSources = Object.values(ConfigSources)
export type ConfigSourceLabel = keyof typeof ConfigSources
export type ConfigSourceValue = (typeof ConfigSources)[ConfigSourceLabel]

export const ConfigTypes = {
  UNKNOWN: 0,
  TOPIC: 2,
  BROKER: 4,
  BROKER_LOGGER: 8
} as const
export const allowedConfigTypes = Object.values(ConfigTypes)
export type ConfigTypeLabel = keyof typeof ConfigTypes
export type ConfigTypeValue = (typeof ConfigTypes)[ConfigTypeLabel]

export const IncrementalAlterConfigTypes = { SET: 0, DELETE: 1, APPEND: 2, SUBTRACT: 3 } as const
export const allowedIncrementalAlterConfigTypes = Object.values(IncrementalAlterConfigTypes)
export type IncrementalAlterConfigTypeLabel = keyof typeof IncrementalAlterConfigTypes
export type IncrementalAlterConfigTypeValue = (typeof IncrementalAlterConfigTypes)[IncrementalAlterConfigTypeLabel]

// ./admin/*-client-quotas.ts
export const ClientQuotaMatchTypes = { EXACT: 0, DEFAULT: 1, ANY: 2 } as const
export const allowedClientQuotaMatchTypes = Object.values(ClientQuotaMatchTypes)
export type ClientQuotaMatchTypeLabel = keyof typeof ClientQuotaMatchTypes
export type ClientQuotaMatchTypeValue = (typeof ClientQuotaMatchTypes)[ClientQuotaMatchTypeLabel]

export const ClientQuotaEntityTypes = { CLIENT_ID: 'client-id', USER: 'user' } as const
export const allowedClientQuotaEntityTypes = Object.values(ClientQuotaEntityTypes)
export type ClientQuotaEntityTypeLabel = keyof typeof ClientQuotaEntityTypes
export type ClientQuotaEntityTypeValue = (typeof ClientQuotaEntityTypes)[ClientQuotaEntityTypeLabel]

export const ClientQuotaKeys = {
  PRODUCER_BYTE_RATE: 'producer_byte_rate',
  CONSUMER_BYTE_RATE: 'consumer_byte_rate',
  REQUEST_PERCENTAGE: 'request_percentage'
} as const
export const allowedClientQuotaKeys = Object.values(ClientQuotaKeys)
export type ClientQuotaKeyLabel = keyof typeof ClientQuotaKeys
export type ClientQuotaKeyValue = (typeof ClientQuotaKeys)[ClientQuotaKeyLabel]

// ./admin/*-scram-credentials.ts
export const ScramMechanisms = { UNKNOWN: 0, SCRAM_SHA_256: 1, SCRAM_SHA_512: 2 } as const
export const allowedScramMechanisms = Object.values(ScramMechanisms)
export type ScramMechanismLabel = keyof typeof ScramMechanisms
export type ScramMechanismValue = (typeof ScramMechanisms)[ScramMechanismLabel]

// ./admin/describe-cluster.ts
export const DescribeClusterEndpointTypes = { BROKERS: 1, CONTROLLERS: 2 } as const
export const allowedDescribeClusterEndpointTypes = Object.values(DescribeClusterEndpointTypes)
export type DescribeClusterEndpointTypeLabel = keyof typeof DescribeClusterEndpointTypes
export type DescribeClusterEndpointTypeValue = (typeof DescribeClusterEndpointTypes)[DescribeClusterEndpointTypeLabel]

// ./admin/list-groups.ts
export const ConsumerGroupStates = ['PREPARING_REBALANCE', 'COMPLETING_REBALANCE', 'STABLE', 'DEAD', 'EMPTY'] as const
export type ConsumerGroupStateValue = (typeof ConsumerGroupStates)[number]

// ./admin/list-transactions.ts
export const TransactionStates = [
  'EMPTY',
  'ONGOING',
  'PREPARE_ABORT',
  'COMMITTING',
  'ABORTING',
  'COMPLETE_COMMIT',
  'COMPLETE_ABORT'
] as const
export type TransactionState = (typeof TransactionStates)[number]

// ./admin/update-features.ts
export const FeatureUpgradeTypes = { UPGRADE: 1, SAFE_DOWNGRADE: 2, UNSAFE_DOWNGRADE: 3 } as const
export const allowedFeatureUpgradeTypes = Object.values(FeatureUpgradeTypes)
export type FeatureUpgradeTypeLabel = keyof typeof FeatureUpgradeTypes
export type FeatureUpgradeTypeValue = (typeof FeatureUpgradeTypes)[FeatureUpgradeTypeLabel]
