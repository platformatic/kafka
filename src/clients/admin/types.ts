import { type AlterClientQuotasRequestEntry } from '../../apis/admin/alter-client-quotas-v1.ts'
import { type CreateTopicsRequestTopicConfig } from '../../apis/admin/create-topics-v7.ts'
import { type DescribeClientQuotasRequestComponent } from '../../apis/admin/describe-client-quotas-v0.ts'
import {
  type DescribeLogDirsRequestTopic,
  type DescribeLogDirsResponse,
  type DescribeLogDirsResponseResult
} from '../../apis/admin/describe-log-dirs-v4.ts'
import { type ConsumerGroupState } from '../../apis/enumerations.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type BaseOptions } from '../base/types.ts'
import { type ExtendedGroupProtocolSubscription, type GroupAssignment } from '../consumer/types.ts'
import { type Acl, type AclFilter } from '../../apis/types.ts'

export interface BrokerAssignment {
  partition: number
  brokers: number[]
}

export interface CreatedTopic {
  id: string
  name: string
  partitions: number
  replicas: number
  configuration: Record<string, NullableString>
}

export interface GroupMember {
  id: string
  groupInstanceId: NullableString
  clientId: string
  clientHost: string
  metadata?: Omit<ExtendedGroupProtocolSubscription, 'memberId'>
  assignments?: Map<string, GroupAssignment>
}

export interface GroupBase {
  id: string
  state: ConsumerGroupState
  groupType: string
  protocolType: string
}

export interface Group extends Omit<GroupBase, 'groupType'> {
  protocol: string
  members: Map<string, GroupMember>
  authorizedOperations: number
}

// Currently empty but reserved for future use
export interface AdminOptions extends BaseOptions {}

export interface CreateTopicsOptions {
  topics: string[]
  partitions?: number
  replicas?: number
  assignments?: BrokerAssignment[]
  configs?: CreateTopicsRequestTopicConfig[]
}

export interface ListTopicsOptions {
  includeInternals?: boolean
}

export interface DeleteTopicsOptions {
  topics: string[]
}

export interface ListGroupsOptions {
  states?: ConsumerGroupState[]
  types?: string[]
}

export interface DescribeGroupsOptions {
  groups: string[]
  includeAuthorizedOperations?: boolean
}

export interface DeleteGroupsOptions {
  groups: string[]
}

export interface DescribeClientQuotasOptions {
  components: DescribeClientQuotasRequestComponent[]
  strict?: boolean
}

export interface AlterClientQuotasOptions {
  entries: AlterClientQuotasRequestEntry[]
  validateOnly?: boolean
}

export interface DescribeLogDirsOptions {
  topics: DescribeLogDirsRequestTopic[]
}

export interface BrokerLogDirDescription {
  broker: number
  throttleTimeMs: DescribeLogDirsResponse['throttleTimeMs']
  results: Omit<DescribeLogDirsResponseResult, 'errorCode'>[]
}

export interface CreateAclsOptions {
  creations: Acl[]
}

export interface DescribeAclsOptions {
  filter: AclFilter
}

export interface DeleteAclsOptions {
  filters: AclFilter[]
}
