import type {
  AclEntry,
  AclFilter,
  IResourceConfig,
  ITopicConfig,
  ITopicPartitionConfig,
  PartitionReassignment,
  ResourceConfigQuery,
  SeekEntry,
  TopicPartitions
} from './types.ts'
import { KafkaJSNonRetriableError } from './errors.ts'

function invalid (message: string): never {
  throw new KafkaJSNonRetriableError(message)
}

export function validateTopics (topics: ITopicConfig[] | unknown): asserts topics is ITopicConfig[] {
  if (!Array.isArray(topics)) {
    invalid(`Invalid topics array ${String(topics)}`)
  }
  if (topics.some(topic => typeof topic?.topic !== 'string')) {
    invalid('Invalid topics array, the topic names have to be a valid string')
  }
  if (new Set(topics.map(topic => topic.topic)).size < topics.length) {
    invalid('Invalid topics array, it cannot have multiple entries for the same topic')
  }
  for (const { topic, configEntries } of topics) {
    if (configEntries == null) {
      continue
    }
    if (!Array.isArray(configEntries)) {
      invalid(`Invalid configEntries for topic "${topic}", must be an array`)
    }
    configEntries.forEach((entry, index) => {
      if (typeof entry !== 'object' || entry == null) {
        invalid(`Invalid configEntries for topic "${topic}". Entry ${index} must be an object`)
      }
      for (const property of ['name', 'value'] as const) {
        if (!Object.prototype.hasOwnProperty.call(entry, property) || typeof entry[property] !== 'string') {
          invalid(`Invalid configEntries for topic "${topic}". Entry ${index} must have a valid "${property}" property`)
        }
      }
    })
  }
}

export function validateTopicNames (topics: string[] | unknown): asserts topics is string[] {
  if (!Array.isArray(topics)) {
    invalid(`Invalid topics array ${String(topics)}`)
  }
  if (topics.some(topic => typeof topic !== 'string')) {
    invalid('Invalid topics array, the names must be a valid string')
  }
}

export function validateCreatePartitions (
  topicPartitions: ITopicPartitionConfig[] | unknown
): asserts topicPartitions is ITopicPartitionConfig[] {
  if (!Array.isArray(topicPartitions)) {
    invalid(`Invalid topic partitions array ${String(topicPartitions)}`)
  }
  if (topicPartitions.length === 0) {
    invalid('Empty topic partitions array')
  }
  if (topicPartitions.some(topic => typeof topic?.topic !== 'string')) {
    invalid('Invalid topic partitions array, the topic names have to be a valid string')
  }
  if (new Set(topicPartitions.map(topic => topic.topic)).size < topicPartitions.length) {
    invalid('Invalid topic partitions array, it cannot have multiple entries for the same topic')
  }
}

export function validateTopic (topic: unknown, quoted = false): asserts topic is string {
  if (!topic || typeof topic !== 'string') {
    invalid(quoted ? `Invalid topic "${String(topic)}"` : `Invalid topic ${String(topic)}`)
  }
}

export function validateGroupAndTopic (groupId: unknown, topic?: unknown): asserts groupId is string {
  if (!groupId) {
    invalid(`Invalid groupId ${String(groupId)}`)
  }
  if (arguments.length > 1 && !topic) {
    invalid(`Invalid topic ${String(topic)}`)
  }
}

export function validateSeekEntries (partitions: SeekEntry[] | unknown): asserts partitions is SeekEntry[] {
  if (!Array.isArray(partitions) || partitions.length === 0) {
    invalid('Invalid partitions')
  }
}

const configTypes = [0, 2, 4, 8]

export function validateConfigResources (
  resources: ResourceConfigQuery[] | IResourceConfig[] | unknown,
  alter: boolean
): asserts resources is ResourceConfigQuery[] | IResourceConfig[] {
  if (!Array.isArray(resources)) {
    invalid(`Invalid resources array ${String(resources)}`)
  }
  if (resources.length === 0) {
    invalid('Resources array cannot be empty')
  }
  const invalidType = resources.find(resource => !configTypes.includes(resource?.type))
  if (invalidType) {
    invalid(`Invalid resource type ${invalidType.type}: ${JSON.stringify(invalidType)}`)
  }
  const invalidName = resources.find(resource => !resource?.name || typeof resource.name !== 'string')
  if (invalidName) {
    invalid(`Invalid resource name ${invalidName.name}: ${JSON.stringify(invalidName)}`)
  }
  if (alter) {
    const invalidConfigs = (resources as IResourceConfig[]).find(resource => !Array.isArray(resource.configEntries))
    if (invalidConfigs) {
      invalid(
        `Invalid resource configEntries ${String(invalidConfigs.configEntries)}: ${JSON.stringify(invalidConfigs)}`
      )
    }
    const invalidValue = (resources as IResourceConfig[]).find(resource =>
      resource.configEntries.some(entry => typeof entry.name !== 'string' || typeof entry.value !== 'string'))
    if (invalidValue) {
      invalid(`Invalid resource config value: ${JSON.stringify(invalidValue)}`)
    }
  } else {
    const invalidConfigs = (resources as ResourceConfigQuery[]).find(
      resource => !Array.isArray(resource.configNames) && resource.configNames != null
    )
    if (invalidConfigs) {
      invalid(`Invalid resource configNames ${String(invalidConfigs.configNames)}: ${JSON.stringify(invalidConfigs)}`)
    }
  }
}

const aclOperations = Array.from({ length: 13 }, (_, index) => index)
const aclPatterns = Array.from({ length: 5 }, (_, index) => index)
const aclPermissions = Array.from({ length: 4 }, (_, index) => index)
const aclResources = Array.from({ length: 7 }, (_, index) => index)

export function validateAcls (entries: AclEntry[] | AclFilter[] | unknown, filters: boolean): void {
  const label = filters ? 'ACL Filter' : 'ACL'
  if (!Array.isArray(entries)) {
    invalid(`Invalid ${label} array ${String(entries)}`)
  }
  if (entries.length === 0) {
    invalid(`Empty ${label} array`)
  }
  for (const property of ['principal', 'host', 'resourceName'] as const) {
    const invalidEntry = entries.some(entry => {
      const value = entry?.[property]
      return filters ? typeof value !== 'string' && typeof value !== 'undefined' : typeof value !== 'string'
    })
    if (invalidEntry) {
      const names = property === 'resourceName' ? 'resourceNames' : `${property}s`
      invalid(`Invalid ${label} array, the ${names} have to be a valid string`)
    }
  }
  const enumerations: Array<[keyof AclEntry, number[], string]> = [
    ['operation', aclOperations, 'operation'],
    ['resourcePatternType', aclPatterns, 'resource pattern'],
    ['permissionType', aclPermissions, 'permission'],
    ['resourceType', aclResources, 'resource']
  ]
  for (const [property, values, name] of enumerations) {
    const invalidEntry = entries.find(entry => !values.includes(entry[property]))
    if (invalidEntry) {
      invalid(`Invalid ${name} type ${invalidEntry[property]}: ${JSON.stringify(invalidEntry)}`)
    }
  }
}

export function validateAclFilter (filter: AclFilter): void {
  for (const [property, name] of [
    ['principal', 'principal'],
    ['host', 'host'],
    ['resourceName', 'resourceName']
  ] as const) {
    if (typeof filter[property] !== 'string' && typeof filter[property] !== 'undefined') {
      invalid(`Invalid ${name}, the ${name} have to be a valid string`)
    }
  }
  const enumerations: Array<[keyof AclFilter, number[], string]> = [
    ['operation', aclOperations, 'operation'],
    ['resourcePatternType', aclPatterns, 'resource pattern filter'],
    ['permissionType', aclPermissions, 'permission'],
    ['resourceType', aclResources, 'resource']
  ]
  for (const [property, values, name] of enumerations) {
    if (!values.includes(filter[property] as number)) {
      invalid(`Invalid ${name} type ${filter[property]}`)
    }
  }
}

export function validateGroupIds (groupIds: string[] | unknown): asserts groupIds is string[] {
  if (!Array.isArray(groupIds)) {
    invalid(`Invalid groupIds array ${String(groupIds)}`)
  }
  const invalidGroupId = groupIds.some(groupId => typeof groupId !== 'string')
  if (invalidGroupId) {
    invalid(`Invalid groupId name: ${JSON.stringify(invalidGroupId)}`)
  }
}

export function validateReassignments (
  topics: PartitionReassignment[] | unknown
): asserts topics is PartitionReassignment[] {
  validateReassignmentTopics(topics)
  for (const { topic, partitionAssignment } of topics as PartitionReassignment[]) {
    if (!Array.isArray(partitionAssignment)) {
      invalid(`Invalid partitions array: ${String(partitionAssignment)} for topic: ${topic}`)
    }
    for (const { partition, replicas } of partitionAssignment) {
      if (typeof partition !== 'number' || partition < 0) {
        invalid(`Invalid partitions index: ${String(partition)} for topic: ${topic}`)
      }
      if (!Array.isArray(replicas)) {
        invalid(`Invalid replica assignment: ${String(replicas)} for topic: ${topic} on partition: ${partition}`)
      }
      if (replicas.some(replica => typeof replica !== 'number' || replica < 0)) {
        invalid(
          `Invalid replica assignment: ${replicas} for topic: ${topic} on partition: ${partition}. Replicas must be a non negative number`
        )
      }
    }
  }
}

export function validateListedReassignments (topics: TopicPartitions[] | null | undefined): void {
  if (topics == null) {
    return
  }
  validateReassignmentTopics(topics)
  for (const { topic, partitions } of topics) {
    if (!Array.isArray(partitions)) {
      invalid(`Invalid partition array: ${String(partitions)} for topic: ${topic}`)
    }
    if (partitions.some(partition => typeof partition !== 'number' || partition < 0)) {
      invalid(
        `Invalid partition array: ${partitions} for topic: ${topic}. The partition indices have to be a valid number greater than 0.`
      )
    }
  }
}

function validateReassignmentTopics (
  topics: Array<{ topic: string }> | unknown
): asserts topics is Array<{ topic: string }> {
  if (!Array.isArray(topics)) {
    invalid(`Invalid topics array ${String(topics)}`)
  }
  if (topics.some(topic => typeof topic?.topic !== 'string')) {
    invalid('Invalid topics array, the topic names have to be a valid string')
  }
  if (new Set(topics.map(topic => topic.topic)).size < topics.length) {
    invalid('Invalid topics array, it cannot have multiple entries for the same topic')
  }
}
