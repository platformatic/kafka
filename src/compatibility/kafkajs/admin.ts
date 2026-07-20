import type { CreateTopicsResponse } from '../../apis/admin/create-topics-v7.ts'
import type { DeleteAclsResponse } from '../../apis/admin/delete-acls-v3.ts'
import type { DeleteGroupsResponse } from '../../apis/admin/delete-groups-v2.ts'
import type { DeleteRecordsResponse } from '../../apis/admin/delete-records-v2.ts'
import type { DescribeConfigsResponse as NativeDescribeConfigsResponse } from '../../apis/admin/describe-configs-v4.ts'
import type { AdminOptions } from '../../clients/admin/types.ts'
import { ProtocolError } from '../../errors.ts'
import type {
  AclEntry,
  AclFilter,
  DeleteAclResponse,
  DeleteGroupsResult,
  DescribeAclResponse,
  DescribeConfigResponse,
  GroupDescriptions,
  IResourceConfig,
  ITopicConfig,
  ITopicMetadata,
  ITopicPartitionConfig,
  ListPartitionReassignmentsResponse,
  Logger,
  PartitionReassignment,
  ResourceConfigQuery,
  SeekEntry,
  TopicPartitions
} from './types.ts'
import { CompatibilityClient, call } from './common.ts'
import {
  KafkaJSAggregateError,
  KafkaJSAlterPartitionReassignmentsError,
  KafkaJSCreateTopicError,
  KafkaJSDeleteGroupsError,
  KafkaJSDeleteTopicRecordsError,
  KafkaJSNonRetriableError,
  KafkaJSTimeout,
  wrapError
} from './errors.ts'
import { KafkaJSAdminBridge } from './admin-native.ts'
import { connectionsApiChannel, connectionsQueueChannel } from '../../diagnostic.ts'
import {
  validateAclFilter,
  validateAcls,
  validateConfigResources,
  validateCreatePartitions,
  validateGroupAndTopic,
  validateGroupIds,
  validateListedReassignments,
  validateReassignments,
  validateSeekEntries,
  validateTopic,
  validateTopicNames,
  validateTopics
} from './admin-validation.ts'

const adminEvents = {
  CONNECT: 'admin.connect',
  DISCONNECT: 'admin.disconnect',
  REQUEST: 'admin.network.request',
  REQUEST_TIMEOUT: 'admin.network.request_timeout',
  REQUEST_QUEUE_SIZE: 'admin.network.request_queue_size'
} as const

const defaultTimeout = 5000

type ResponseCarrier = { response?: unknown }

function responseFromError<Response> (error: unknown): Response | undefined {
  if (typeof error === 'object' && error !== null && 'response' in error) {
    return (error as ResponseCarrier).response as Response | undefined
  }
  return undefined
}

function responsesFromError<Response> (error: unknown): Response[] {
  const responses: Response[] = []
  const pending: unknown[] = [error]
  const visited = new Set<unknown>()
  while (pending.length > 0) {
    const current = pending.shift()
    if (!current || typeof current !== 'object' || visited.has(current)) {
      continue
    }
    visited.add(current)
    if ('response' in current && current.response !== undefined) {
      responses.push(current.response as Response)
    }
    if ('cause' in current && current.cause) {
      pending.push(current.cause)
    }
    if ('errors' in current && Array.isArray(current.errors)) {
      pending.push(...current.errors)
    }
  }
  return responses
}

function protocolError (code: number): ReturnType<typeof wrapError> {
  return wrapError(new ProtocolError(code))
}

function groupState (state: string): GroupDescriptions['groups'][number]['state'] {
  const states: Record<string, GroupDescriptions['groups'][number]['state']> = {
    UNKNOWN: 'Unknown',
    PREPARING_REBALANCE: 'PreparingRebalance',
    COMPLETING_REBALANCE: 'CompletingRebalance',
    STABLE: 'Stable',
    DEAD: 'Dead',
    EMPTY: 'Empty'
  }
  return states[state.toUpperCase().replaceAll(' ', '_')] ?? 'Unknown'
}

export class Admin extends CompatibilityClient {
  readonly events = adminEvents
  private readonly options: AdminOptions
  private native: KafkaJSAdminBridge
  private readonly log: Logger
  private readonly diagnosticContext = {}
  private diagnosticsSubscribed = false

  constructor (options: AdminOptions, log: Logger) {
    super()
    this.options = options
    this.log = log.namespace('Admin')
    this.native = this.createNative()
  }

  private createNative (): KafkaJSAdminBridge {
    return new KafkaJSAdminBridge({ ...this.options, context: this.diagnosticContext })
  }

  async connect (): Promise<void> {
    if (this.connected) {
      return
    }
    if (this.native.closed) {
      this.native = this.createNative()
    }
    this.startRequestInstrumentation()
    try {
      await call(this.native.metadata({ topics: [] }))
    } catch (error) {
      this.stopRequestInstrumentation()
      throw error
    }
    this.connected = true
    this.emit(this.events.CONNECT)
  }

  async disconnect (): Promise<void> {
    if (!this.native.closed) {
      await call(this.native.close())
    }
    if (this.connected) {
      this.connected = false
      this.emit(this.events.DISCONNECT)
    }
    this.stopRequestInstrumentation()
  }

  async listTopics (): Promise<string[]> {
    this.operation('listTopics')
    return call(this.native.listTopics())
  }

  async createTopics (options: {
    validateOnly?: boolean
    waitForLeaders?: boolean
    timeout?: number
    topics: ITopicConfig[]
  }): Promise<boolean> {
    this.operation('createTopics')
    validateTopics(options?.topics)
    const timeout = options.timeout ?? defaultTimeout
    const topics = options.topics.map(topic => ({
      name: topic.topic,
      numPartitions: topic.replicaAssignment ? -1 : (topic.numPartitions ?? -1),
      replicationFactor: topic.replicaAssignment ? -1 : (topic.replicationFactor ?? -1),
      assignments:
        topic.replicaAssignment?.map(entry => ({
          partitionIndex: entry.partition,
          brokerIds: entry.replicas
        })) ?? [],
      configs: topic.configEntries ?? []
    }))

    try {
      await this.native.createTopicsRaw(topics, timeout, options.validateOnly ?? false)
    } catch (error) {
      const response = responseFromError<CreateTopicsResponse>(error)
      if (!response) {
        throw wrapError(error)
      }
      const failed = response.topics.filter(topic => topic.errorCode !== 0)
      if (failed.length === response.topics.length && failed.every(topic => topic.errorCode === 36)) {
        return false
      }
      throw new KafkaJSAggregateError(
        'Topic creation errors',
        failed.map(topic => new KafkaJSCreateTopicError(protocolError(topic.errorCode), topic.name))
      )
    }

    if (options.waitForLeaders !== false && options.topics.length > 0) {
      const deadline = Date.now() + timeout
      while (true) {
        try {
          const metadata = await this.fetchTopicMetadata({ topics: options.topics.map(topic => topic.topic) })
          if (
            metadata.topics.length === options.topics.length &&
            metadata.topics.every(topic => topic.partitions.every(partition => partition.leader >= 0))
          ) {
            break
          }
        } catch (error) {
          if ((error as { type?: string }).type !== 'LEADER_NOT_AVAILABLE') {
            throw error
          }
        }
        if (Date.now() >= deadline) {
          throw new KafkaJSTimeout('Timed out while waiting for topic leaders')
        }
        await new Promise(resolve => setTimeout(resolve, 100))
      }
    }
    return true
  }

  async deleteTopics (options: { topics: string[]; timeout?: number }): Promise<void> {
    this.operation('deleteTopics')
    validateTopicNames(options?.topics)
    await call(
      this.native.deleteTopicsRaw(
        options.topics.map(name => ({ name })),
        options.timeout ?? defaultTimeout
      )
    )
  }

  async createPartitions (options: {
    validateOnly?: boolean
    timeout?: number
    topicPartitions: ITopicPartitionConfig[]
  }): Promise<boolean> {
    this.operation('createPartitions')
    validateCreatePartitions(options?.topicPartitions)
    await call(
      this.native.createPartitionsRaw(
        options.topicPartitions.map(topic => ({
          name: topic.topic,
          count: topic.count,
          assignments: topic.assignments?.map(brokerIds => ({ brokerIds })) ?? null
        })),
        options.timeout ?? defaultTimeout,
        options.validateOnly ?? false
      )
    )
    return true
  }

  async fetchTopicMetadata (options?: { topics: string[] }): Promise<{ topics: ITopicMetadata[] }> {
    this.operation('fetchTopicMetadata')
    if (options?.topics) {
      for (const topic of options.topics) {
        validateTopic(topic)
      }
    }
    const topics = options?.topics?.length ? options.topics : await call(this.native.listTopics())
    const metadata = await call(this.native.metadata({ topics, forceUpdate: true }))
    return {
      topics: Array.from(metadata.topics, ([name, topic]) => ({
        name,
        partitions: topic.partitions.map((partition, partitionId) => ({
          partitionErrorCode: 0,
          partitionId,
          leader: partition.leader,
          replicas: partition.replicas,
          isr: partition.isr,
          offlineReplicas: partition.offlineReplicas
        }))
      }))
    }
  }

  async fetchOffsets (options: { groupId: string; topics?: string[]; resolveOffsets?: boolean }) {
    this.operation('fetchOffsets')
    validateGroupAndTopic(options?.groupId)
    const topics = options.topics ?? []
    if (!Array.isArray(topics)) {
      throw new KafkaJSNonRetriableError('Expected topics array to be set')
    }
    const groups = await call(
      this.native.listConsumerGroupOffsets({
        groups: [
          {
            groupId: options.groupId,
            topics:
              topics.length === 0
                ? undefined
                : await Promise.all(
                  topics.map(async name => ({
                    name,
                    partitionIndexes:
                        (await this.fetchTopicMetadata({ topics: [name] })).topics[0]?.partitions.map(
                          p => p.partitionId
                        ) ?? []
                  }))
                )
          }
        ]
      })
    )
    const result = groups[0]?.topics ?? []
    const resolved = new Map<string, Map<number, string>>()
    if (options.resolveOffsets) {
      for (const topic of result) {
        const offsets = new Map((await this.fetchTopicOffsets(topic.name)).map(offset => [offset.partition, offset]))
        const partitions = topic.partitions.map(partition => ({
          partition: partition.partitionIndex,
          offset:
            partition.committedOffset === -2n
              ? offsets.get(partition.partitionIndex)!.low
              : partition.committedOffset === -1n
                ? offsets.get(partition.partitionIndex)!.high
                : partition.committedOffset.toString()
        }))
        resolved.set(topic.name, new Map(partitions.map(partition => [partition.partition, partition.offset])))
        await this.setOffsets({ groupId: options.groupId, topic: topic.name, partitions })
      }
    }
    return result.map(topic => ({
      topic: topic.name,
      partitions: topic.partitions.map(partition => ({
        partition: partition.partitionIndex,
        offset: resolved.get(topic.name)?.get(partition.partitionIndex) ?? partition.committedOffset.toString(),
        metadata: partition.metadata || null
      }))
    }))
  }

  private async offsets (topic: string, timestamp: bigint) {
    const metadata = await this.fetchTopicMetadata({ topics: [topic] })
    const partitions = metadata.topics[0]?.partitions ?? []
    const result = await call(
      this.native.listOffsets({
        topics: [
          {
            name: topic,
            partitions: partitions.map(partition => ({ partitionIndex: partition.partitionId, timestamp }))
          }
        ]
      })
    )
    return result[0]?.partitions ?? []
  }

  async fetchTopicOffsets (topic: string): Promise<Array<SeekEntry & { high: string; low: string }>> {
    this.operation('fetchTopicOffsets')
    validateTopic(topic)
    const [latest, earliest] = await Promise.all([this.offsets(topic, -1n), this.offsets(topic, -2n)])
    const lows = new Map(earliest.map(partition => [partition.partitionIndex, partition.offset.toString()]))
    return latest.map(partition => ({
      partition: partition.partitionIndex,
      offset: partition.offset.toString(),
      high: partition.offset.toString(),
      low: lows.get(partition.partitionIndex) ?? '0'
    }))
  }

  async fetchTopicOffsetsByTimestamp (topic: string, timestamp?: number): Promise<SeekEntry[]> {
    this.operation('fetchTopicOffsetsByTimestamp')
    validateTopic(topic)
    const requestedTimestamp = timestamp === undefined ? -1n : BigInt(timestamp)
    const [requested, latest] = await Promise.all([this.offsets(topic, requestedTimestamp), this.offsets(topic, -1n)])
    const highs = new Map(latest.map(partition => [partition.partitionIndex, partition.offset.toString()]))
    return requested.map(partition => ({
      partition: partition.partitionIndex,
      offset: partition.offset >= 0n ? partition.offset.toString() : (highs.get(partition.partitionIndex) ?? '0')
    }))
  }

  async describeCluster () {
    this.operation('describeCluster')
    const metadata = await call(this.native.metadata({ topics: [], forceUpdate: true }))
    return {
      brokers: Array.from(metadata.brokers, ([nodeId, broker]) => ({ nodeId, host: broker.host, port: broker.port })),
      controller: metadata.controllerId < 0 ? null : metadata.controllerId,
      clusterId: metadata.id
    }
  }

  async setOffsets (options: { groupId: string; topic: string; partitions: SeekEntry[] }): Promise<void> {
    this.operation('setOffsets')
    validateGroupAndTopic(options?.groupId, options?.topic)
    validateSeekEntries(options.partitions)
    await this.assertInactiveGroup(options.groupId)
    await call(
      this.native.alterConsumerGroupOffsets({
        groupId: options.groupId,
        topics: [
          {
            name: options.topic,
            partitionOffsets: options.partitions.map(partition => ({
              partition: partition.partition,
              offset: BigInt(partition.offset)
            }))
          }
        ]
      })
    )
  }

  async resetOffsets (options: { groupId: string; topic: string; earliest?: boolean }): Promise<void> {
    this.operation('resetOffsets')
    validateGroupAndTopic(options?.groupId, options?.topic)
    await this.assertInactiveGroup(options.groupId)
    const offsets = await this.offsets(options.topic, options.earliest ? -2n : -1n)
    await call(
      this.native.alterConsumerGroupOffsets({
        groupId: options.groupId,
        topics: [
          {
            name: options.topic,
            partitionOffsets: offsets.map(partition => ({
              partition: partition.partitionIndex,
              offset: partition.offset
            }))
          }
        ]
      })
    )
  }

  async describeConfigs (options: {
    resources: ResourceConfigQuery[]
    includeSynonyms?: boolean
  }): Promise<DescribeConfigResponse> {
    this.operation('describeConfigs')
    validateConfigResources(options?.resources, false)
    const responses: NativeDescribeConfigsResponse[] = await call(
      this.native.describeConfigsRaw(
        options.resources.map(resource => ({
          resourceType: resource.type,
          resourceName: resource.name,
          configurationKeys: resource.configNames
        })),
        options.includeSynonyms ?? false
      )
    )
    return {
      throttleTime: Math.max(0, ...responses.map(response => response.throttleTimeMs)),
      resources: responses.flatMap(response =>
        response.results.map(resource => ({
          errorCode: resource.errorCode,
          errorMessage: resource.errorMessage ?? '',
          resourceName: resource.resourceName,
          resourceType: resource.resourceType as 0 | 2 | 4 | 8,
          configEntries: resource.configs.map(config => ({
            configName: config.name,
            configValue: config.value ?? '',
            isDefault: config.configSource === 5,
            configSource: config.configSource as 0 | 1 | 2 | 3 | 4 | 5 | 6,
            isSensitive: config.isSensitive,
            readOnly: config.readOnly,
            configSynonyms: config.synonyms.map(synonym => ({
              configName: synonym.name,
              configValue: synonym.value ?? '',
              configSource: synonym.source as 0 | 1 | 2 | 3 | 4 | 5 | 6
            }))
          }))
        })))
    }
  }

  async alterConfigs (options: { validateOnly?: boolean; resources: IResourceConfig[] }) {
    this.operation('alterConfigs')
    validateConfigResources(options?.resources, true)
    const responses = await call(
      this.native.alterConfigsRaw(
        options.resources.map(resource => ({
          resourceType: resource.type,
          resourceName: resource.name,
          configs: resource.configEntries
        })),
        options.validateOnly ?? false
      )
    )
    return {
      resources: responses.flatMap(response =>
        response.responses.map(resource => ({
          errorCode: resource.errorCode,
          errorMessage: resource.errorMessage,
          resourceType: resource.resourceType,
          resourceName: resource.resourceName
        })))
    }
  }

  async listGroups () {
    this.operation('listGroups')
    const groups = await call(this.native.listGroups())
    return { groups: Array.from(groups.values(), group => ({ groupId: group.id, protocolType: group.protocolType })) }
  }

  async deleteGroups (groupIds: string[]): Promise<DeleteGroupsResult[]> {
    this.operation('deleteGroups')
    validateGroupIds(groupIds)
    if (groupIds.length === 0) {
      return []
    }
    let responses: DeleteGroupsResponse[]
    try {
      responses = await this.native.deleteGroupsRaw(groupIds)
    } catch (error) {
      const response = responseFromError<DeleteGroupsResponse>(error)
      if (!response) {
        throw wrapError(error)
      }
      const groups = response.results
        .filter(group => group.errorCode !== 0)
        .map(group => ({
          groupId: group.groupId,
          errorCode: group.errorCode,
          error: protocolError(group.errorCode)
        }))
      throw new KafkaJSDeleteGroupsError('Error in DeleteGroups', groups)
    }
    return responses.flatMap(response => response.results.map(group => ({ groupId: group.groupId, errorCode: 0 })))
  }

  async describeGroups (groupIds: string[]): Promise<GroupDescriptions> {
    this.operation('describeGroups')
    const responses = await call(this.native.describeGroupsRaw(groupIds))
    return {
      groups: responses.flatMap(response =>
        response.groups.map(group => ({
          groupId: group.groupId,
          state: groupState(group.groupState),
          protocol: group.protocolData,
          protocolType: group.protocolType,
          members: group.members.map(member => ({
            clientHost: member.clientHost,
            clientId: member.clientId,
            memberId: member.memberId,
            memberAssignment: member.memberAssignment,
            memberMetadata: member.memberMetadata
          }))
        })))
    }
  }

  async describeAcls (filter: AclFilter): Promise<DescribeAclResponse> {
    this.operation('describeAcls')
    validateAclFilter(filter)
    const response = await call(
      this.native.describeAclsRaw([
        {
          ...filter,
          resourceName: filter.resourceName ?? null,
          principal: filter.principal ?? null,
          host: filter.host ?? null
        }
      ])
    )
    return {
      throttleTime: response.throttleTimeMs,
      errorCode: response.errorCode,
      errorMessage: response.errorMessage ?? undefined,
      resources: response.resources as DescribeAclResponse['resources']
    }
  }

  async deleteAcls (options: { filters: AclFilter[] }): Promise<DeleteAclResponse> {
    this.operation('deleteAcls')
    validateAcls(options?.filters, true)
    const response: DeleteAclsResponse = await call(
      this.native.deleteAclsRaw(
        options.filters.map(filter => ({
          ...filter,
          resourceName: filter.resourceName ?? null,
          principal: filter.principal ?? null,
          host: filter.host ?? null
        }))
      )
    )
    return {
      throttleTime: response.throttleTimeMs,
      filterResponses: response.filterResults.map(result => ({
        errorCode: result.errorCode,
        errorMessage: result.errorMessage ?? undefined,
        matchingAcls: result.matchingAcls.map(acl => ({
          ...acl,
          errorMessage: acl.errorMessage ?? undefined
        })) as DeleteAclResponse['filterResponses'][number]['matchingAcls']
      }))
    }
  }

  async createAcls (options: { acl: AclEntry[] }): Promise<boolean> {
    this.operation('createAcls')
    validateAcls(options?.acl, false)
    await call(this.native.createAcls({ creations: options.acl }))
    return true
  }

  async deleteTopicRecords (options: { topic: string; partitions: SeekEntry[] }): Promise<void> {
    this.operation('deleteTopicRecords')
    validateTopic(options?.topic, true)
    validateSeekEntries(options?.partitions)
    try {
      await this.native.deleteRecords({
        topics: [
          {
            name: options.topic,
            partitions: options.partitions.map(partition => ({
              partition: partition.partition,
              offset: BigInt(partition.offset)
            }))
          }
        ]
      })
    } catch (error) {
      const responses = responsesFromError<DeleteRecordsResponse>(error)
      if (responses.length === 0) {
        throw wrapError(error)
      }
      const failed = responses
        .flatMap(response => response.topics)
        .flatMap(topic => topic.partitions.filter(partition => partition.errorCode !== 0))
      throw new KafkaJSDeleteTopicRecordsError({
        partitions: failed.map(partition => ({
          topic: options.topic,
          partition: partition.partitionIndex,
          offset: options.partitions.find(entry => entry.partition === partition.partitionIndex)?.offset,
          error: protocolError(partition.errorCode)
        }))
      })
    }
  }

  async alterPartitionReassignments (request: { topics: PartitionReassignment[]; timeout?: number }): Promise<void> {
    this.operation('alterPartitionReassignments')
    validateReassignments(request?.topics)
    try {
      await this.native.alterPartitionReassignmentsRaw(
        request.timeout ?? defaultTimeout,
        request.topics.map(topic => ({
          name: topic.topic,
          partitions: topic.partitionAssignment.map(partition => ({
            partitionIndex: partition.partition,
            replicas: partition.replicas
          }))
        }))
      )
    } catch (error) {
      const source = responseFromError<{
        errorCode: number
        responses: Array<{ name: string; partitions: Array<{ partitionIndex: number; errorCode: number }> }>
      }>(error)
      if (!source) {
        throw wrapError(error)
      }
      const errors = source.responses.flatMap(topic =>
        topic.partitions
          .filter(partition => partition.errorCode !== 0)
          .map(
            partition =>
              new KafkaJSAlterPartitionReassignmentsError(
                protocolError(partition.errorCode),
                topic.name,
                partition.partitionIndex
              )
          ))
      if (source.errorCode !== 0) {
        errors.unshift(new KafkaJSAlterPartitionReassignmentsError(protocolError(source.errorCode)))
      }
      throw errors.length === 1 ? errors[0] : new KafkaJSAggregateError('Partition reassignment errors', errors)
    }
  }

  async listPartitionReassignments (
    request: {
      topics?: TopicPartitions[]
      timeout?: number
    } = {}
  ): Promise<ListPartitionReassignmentsResponse> {
    this.operation('listPartitionReassignments')
    validateListedReassignments(request.topics)
    const response = await call(
      this.native.listPartitionReassignmentsRaw(
        request.timeout ?? defaultTimeout,
        request.topics?.map(topic => ({ name: topic.topic, partitionIndexes: topic.partitions })) ?? null
      )
    )
    return {
      topics: response.topics.map(topic => ({
        topic: topic.name,
        partitions: topic.partitions.map(partition => ({
          partitionIndex: partition.partitionIndex,
          replicas: partition.replicas,
          addingReplicas: partition.addingReplicas,
          removingReplicas: partition.removingReplicas
        }))
      }))
    }
  }

  logger (): Logger {
    return this.log
  }

  private operation (_apiName: string): void {}

  private readonly onRequestDiagnostic = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as Record<string, unknown>
    const connection = diagnostic.connection as { context?: unknown } | undefined
    if (connection?.context === this.diagnosticContext) {
      this.emit(this.events.REQUEST, {
        apiKey: diagnostic.apiKey,
        apiName: diagnostic.apiName,
        apiVersion: diagnostic.apiVersion,
        broker: diagnostic.broker,
        clientId: diagnostic.clientId,
        correlationId: diagnostic.correlationId,
        createdAt: diagnostic.createdAt,
        sentAt: diagnostic.sentAt,
        pendingDuration: diagnostic.pendingDuration,
        duration: diagnostic.duration,
        size: diagnostic.size
      })
    }
  }

  private readonly onQueueDiagnostic = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as {
      connection?: { context?: unknown; host?: string; port?: number }
      queueSize?: number
    }
    if (diagnostic.connection?.context === this.diagnosticContext) {
      this.emit(this.events.REQUEST_QUEUE_SIZE, {
        broker: `${diagnostic.connection.host}:${diagnostic.connection.port}`,
        clientId: this.native.clientId,
        queueSize: diagnostic.queueSize
      })
    }
  }

  private readonly onRequestTimeoutDiagnostic = (rawDiagnostic: unknown): void => {
    const diagnostic = rawDiagnostic as Record<string, unknown>
    const connection = diagnostic.connection as { context?: unknown } | undefined
    const error = diagnostic.error as { code?: string } | undefined
    if (connection?.context === this.diagnosticContext && error?.code === 'PLT_KFK_TIMEOUT') {
      this.emit(this.events.REQUEST_TIMEOUT, {
        apiKey: diagnostic.apiKey,
        apiName: diagnostic.apiName,
        apiVersion: diagnostic.apiVersion,
        broker: diagnostic.broker,
        clientId: diagnostic.clientId,
        correlationId: diagnostic.correlationId,
        createdAt: diagnostic.createdAt,
        sentAt: diagnostic.sentAt,
        pendingDuration: diagnostic.pendingDuration
      })
    }
  }

  private startRequestInstrumentation (): void {
    if (!this.diagnosticsSubscribed) {
      connectionsApiChannel.asyncEnd.subscribe(this.onRequestDiagnostic)
      connectionsApiChannel.error.subscribe(this.onRequestTimeoutDiagnostic)
      connectionsQueueChannel.subscribe(this.onQueueDiagnostic)
      this.diagnosticsSubscribed = true
    }
  }

  private stopRequestInstrumentation (): void {
    if (this.diagnosticsSubscribed) {
      connectionsApiChannel.asyncEnd.unsubscribe(this.onRequestDiagnostic)
      connectionsApiChannel.error.unsubscribe(this.onRequestTimeoutDiagnostic)
      connectionsQueueChannel.unsubscribe(this.onQueueDiagnostic)
      this.diagnosticsSubscribed = false
    }
  }

  private async assertInactiveGroup (groupId: string): Promise<void> {
    const descriptions = await this.describeGroups([groupId])
    const state = descriptions.groups[0]?.state ?? 'Dead'
    if (state !== 'Empty' && state !== 'Dead') {
      throw new KafkaJSNonRetriableError(`The consumer group must have no running instances, current state: ${state}`)
    }
  }
}
