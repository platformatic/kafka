import type {
  AlterPartitionReassignmentsRequest,
  AlterPartitionReassignmentsResponse
} from '../../apis/admin/alter-partition-reassignments-v0.ts'
import type { CreatePartitionsRequest, CreatePartitionsResponse } from '../../apis/admin/create-partitions-v3.ts'
import type { CreateTopicsRequest, CreateTopicsResponse } from '../../apis/admin/create-topics-v7.ts'
import type { AlterConfigsRequest, AlterConfigsResponse } from '../../apis/admin/alter-configs-v2.ts'
import type { DeleteAclsRequest, DeleteAclsResponse } from '../../apis/admin/delete-acls-v3.ts'
import type { DeleteGroupsRequest, DeleteGroupsResponse } from '../../apis/admin/delete-groups-v2.ts'
import type { DeleteTopicsRequest, DeleteTopicsResponse } from '../../apis/admin/delete-topics-v6.ts'
import type { DescribeAclsRequest, DescribeAclsResponse } from '../../apis/admin/describe-acls-v3.ts'
import type { DescribeConfigsRequest, DescribeConfigsResponse } from '../../apis/admin/describe-configs-v4.ts'
import type { DescribeGroupsRequest, DescribeGroupsResponse } from '../../apis/admin/describe-groups-v5.ts'
import type {
  ListPartitionReassignmentsRequest,
  ListPartitionReassignmentsRequestTopic,
  ListPartitionReassignmentsResponse
} from '../../apis/admin/list-partition-reassignments-v0.ts'
import type { API, Callback } from '../../apis/definitions.ts'
import { FindCoordinatorKeyTypes } from '../../apis/enumerations.ts'
import type { AddOffsetsToTxnRequest, AddOffsetsToTxnResponse } from '../../apis/producer/add-offsets-to-txn-v4.ts'
import type {
  TxnOffsetCommitRequest,
  TxnOffsetCommitRequestTopic,
  TxnOffsetCommitResponse
} from '../../apis/producer/txn-offset-commit-v4.ts'
import type { AclFilter } from '../../apis/types.ts'
import type { Connection } from '../../network/connection.ts'
import { kGetApi, kGetConnection, kMetadata } from '../../clients/base/base.ts'
import { Admin } from '../../clients/admin/admin.ts'

type Request = unknown[]

function hasErrorCode (error: unknown, codes: Set<number>): boolean {
  const pending: unknown[] = [error]
  const visited = new Set<unknown>()
  while (pending.length > 0) {
    const current = pending.shift()
    if (!current || typeof current !== 'object' || visited.has(current)) {
      continue
    }
    visited.add(current)
    if ('apiCode' in current && typeof current.apiCode === 'number' && codes.has(current.apiCode)) {
      return true
    }
    if ('cause' in current && current.cause) {
      pending.push(current.cause)
    }
    if ('errors' in current && Array.isArray(current.errors)) {
      pending.push(...current.errors)
    }
  }
  return false
}

export class KafkaJSAdminBridge extends Admin {
  async sendOffsetsToTransactionRaw (
    transactionalId: string,
    producerId: bigint,
    producerEpoch: number,
    consumerGroupId: string,
    topics: TxnOffsetCommitRequestTopic[]
  ): Promise<void> {
    await this.routingRequest(
      async () => {
        const [coordinator] = await this.findCoordinator({
          keyType: FindCoordinatorKeyTypes.TRANSACTION,
          keys: [transactionalId]
        })
        const connection = await this.connection(coordinator)
        await this.request<AddOffsetsToTxnRequest, AddOffsetsToTxnResponse>(connection, 'AddOffsetsToTxn', [
          transactionalId,
          producerId,
          producerEpoch,
          consumerGroupId
        ])
      },
      new Set([14, 15, 16])
    )

    await this.routingRequest(
      async () => {
        const [coordinator] = await this.findCoordinator({
          keyType: FindCoordinatorKeyTypes.GROUP,
          keys: [consumerGroupId]
        })
        const connection = await this.connection(coordinator)
        await this.request<TxnOffsetCommitRequest, TxnOffsetCommitResponse>(connection, 'TxnOffsetCommit', [
          transactionalId,
          consumerGroupId,
          producerId,
          producerEpoch,
          -1,
          '',
          null,
          topics
        ])
      },
      new Set([14, 15, 16])
    )
  }

  async createTopicsRaw (
    topics: CreateTopicsRequest[0],
    timeout: number,
    validateOnly: boolean
  ): Promise<CreateTopicsResponse> {
    return this.controllerRequest<CreateTopicsRequest, CreateTopicsResponse>('CreateTopics', [
      topics,
      timeout,
      validateOnly
    ])
  }

  async createPartitionsRaw (
    topics: CreatePartitionsRequest[0],
    timeout: number,
    validateOnly: boolean
  ): Promise<CreatePartitionsResponse> {
    return this.controllerRequest<CreatePartitionsRequest, CreatePartitionsResponse>('CreatePartitions', [
      topics,
      timeout,
      validateOnly
    ])
  }

  async deleteTopicsRaw (topics: DeleteTopicsRequest[0], timeout: number): Promise<DeleteTopicsResponse> {
    return this.controllerRequest<DeleteTopicsRequest, DeleteTopicsResponse>('DeleteTopics', [topics, timeout])
  }

  async describeGroupsRaw (groups: string[]): Promise<DescribeGroupsResponse[]> {
    return this.routingRequest(
      async () => {
        const coordinators = await this.findCoordinator({ keyType: 0, keys: groups })
        const grouped = new Map<number, { connection: { host: string; port: number }; groups: string[] }>()
        for (const coordinator of coordinators) {
          const current = grouped.get(coordinator.nodeId) ?? { connection: coordinator, groups: [] }
          current.groups.push(coordinator.key)
          grouped.set(coordinator.nodeId, current)
        }
        return Promise.all(
          Array.from(grouped.values(), async group => {
            const connection = await this.connection(group.connection)
            return this.request<DescribeGroupsRequest, DescribeGroupsResponse>(connection, 'DescribeGroups', [
              group.groups,
              false
            ])
          })
        )
      },
      new Set([14, 15, 16])
    )
  }

  async deleteGroupsRaw (groups: string[]): Promise<DeleteGroupsResponse[]> {
    return Promise.all(
      groups.map(groupId =>
        this.routingRequest(
          async () => {
            const [coordinator] = await this.findCoordinator({ keyType: 0, keys: [groupId] })
            const connection = await this.connection(coordinator)
            return this.request<DeleteGroupsRequest, DeleteGroupsResponse>(connection, 'DeleteGroups', [[groupId]])
          },
          new Set([14, 15, 16])
        ))
    )
  }

  async describeConfigsRaw (
    resources: DescribeConfigsRequest[0],
    includeSynonyms: boolean
  ): Promise<DescribeConfigsResponse[]> {
    return this.routingRequest(
      async () => {
        const metadata = await this.clusterMetadata()
        const grouped = new Map<number, DescribeConfigsRequest[0]>()
        for (const resource of resources) {
          const nodeId =
            resource.resourceType === 4 || resource.resourceType === 8
              ? Number(resource.resourceName)
              : metadata.controllerId
          const current = grouped.get(nodeId) ?? []
          current.push(resource)
          grouped.set(nodeId, current)
        }
        return Promise.all(
          Array.from(grouped, async ([nodeId, brokerResources]) => {
            const broker = metadata.brokers.get(nodeId)
            if (!broker) {
              throw new Error(`Broker ${nodeId} not found`)
            }
            const connection = await this.connection(broker)
            return this.request<DescribeConfigsRequest, DescribeConfigsResponse>(connection, 'DescribeConfigs', [
              brokerResources,
              includeSynonyms,
              false
            ])
          })
        )
      },
      new Set([41])
    )
  }

  async alterConfigsRaw (resources: AlterConfigsRequest[0], validateOnly: boolean): Promise<AlterConfigsResponse[]> {
    return this.routingRequest(
      async () => {
        const metadata = await this.clusterMetadata()
        const grouped = new Map<number, AlterConfigsRequest[0]>()
        for (const resource of resources) {
          const nodeId =
            resource.resourceType === 4 || resource.resourceType === 8
              ? Number(resource.resourceName)
              : metadata.controllerId
          const current = grouped.get(nodeId) ?? []
          current.push(resource)
          grouped.set(nodeId, current)
        }
        return Promise.all(
          Array.from(grouped, async ([nodeId, brokerResources]) => {
            const broker = metadata.brokers.get(nodeId)
            if (!broker) {
              throw new Error(`Broker ${nodeId} not found`)
            }
            const connection = await this.connection(broker)
            return this.request<AlterConfigsRequest, AlterConfigsResponse>(connection, 'AlterConfigs', [
              brokerResources,
              validateOnly
            ])
          })
        )
      },
      new Set([41])
    )
  }

  async describeAclsRaw (filter: DescribeAclsRequest): Promise<DescribeAclsResponse> {
    return this.controllerRequest<DescribeAclsRequest, DescribeAclsResponse>('DescribeAcls', filter)
  }

  async deleteAclsRaw (filters: AclFilter[]): Promise<DeleteAclsResponse> {
    return this.controllerRequest<DeleteAclsRequest, DeleteAclsResponse>('DeleteAcls', [filters])
  }

  async alterPartitionReassignmentsRaw (
    timeout: number,
    topics: AlterPartitionReassignmentsRequest[2]
  ): Promise<AlterPartitionReassignmentsResponse> {
    return this.controllerRequest<AlterPartitionReassignmentsRequest, AlterPartitionReassignmentsResponse>(
      'AlterPartitionReassignments',
      [timeout, false, topics]
    )
  }

  async listPartitionReassignmentsRaw (
    timeout: number,
    topics: ListPartitionReassignmentsRequestTopic[] | null
  ): Promise<ListPartitionReassignmentsResponse> {
    return this.controllerRequest<ListPartitionReassignmentsRequest, ListPartitionReassignmentsResponse>(
      'ListPartitionReassignments',
      [timeout, topics] as unknown as ListPartitionReassignmentsRequest
    )
  }

  private async clusterMetadata () {
    return new Promise<Awaited<ReturnType<Admin['metadata']>>>((resolve, reject) => {
      this[kMetadata]({ topics: [], forceUpdate: true }, (error, metadata) => {
        if (error) {
          reject(error)
        } else {
          resolve(metadata!)
        }
      })
    })
  }

  private async controllerRequest<Arguments extends Request, Response> (
    name: string,
    args: Arguments
  ): Promise<Response> {
    return this.routingRequest(
      async () => {
        const metadata = await this.clusterMetadata()
        const controller = metadata.brokers.get(metadata.controllerId)
        if (!controller) {
          throw new Error('Kafka controller not found')
        }
        return this.request(await this.connection(controller), name, args)
      },
      new Set([41])
    )
  }

  private async routingRequest<Response> (operation: () => Promise<Response>, codes: Set<number>): Promise<Response> {
    let lastError: unknown
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        return await operation()
      } catch (error) {
        lastError = error
        if (!hasErrorCode(error, codes)) {
          throw error
        }
        this.clearMetadata()
      }
    }
    throw lastError
  }

  private async connection (broker: { host: string; port: number }): Promise<Connection> {
    return new Promise((resolve, reject) => {
      this[kGetConnection](broker, (error, connection) => {
        if (error) {
          reject(error)
        } else {
          resolve(connection!)
        }
      })
    })
  }

  private async request<Arguments extends Request, Response> (
    connection: Connection,
    name: string,
    args: Arguments
  ): Promise<Response> {
    return new Promise((resolve, reject) => {
      this[kGetApi]<Arguments, Response>(name, (apiError, api) => {
        if (apiError) {
          reject(apiError)
          return
        }
        const callback: Callback<Response> = (error, response) => {
          if (error) {
            reject(error)
          } else {
            resolve(response!)
          }
        }
        ;(api as API<Arguments, Response>)(connection, ...args, callback)
      })
    })
  }
}
