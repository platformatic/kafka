import { channel, type TracingChannel, tracingChannel } from 'node:diagnostics_channel'
import { type Base } from './clients/base/base.ts'
import { type ConnectionPool } from './network/connection-pool.ts'
import { type Connection } from './network/connection.ts'

export type ClientType = 'base' | 'producer' | 'consumer' | 'admin'

export interface CreationEvent<InstanceType> {
  type: ClientType | 'connection' | 'connectionPool'
  instance: InstanceType
}

export type ConnectionDiagnosticEvent<Attributes = Record<string, unknown>> = { connection: Connection } & Attributes

export type ConnectionPoolDiagnosticEvent<Attributes = Record<string, unknown>> = {
  connectionPool: ConnectionPool
} & Attributes

export type ClientDiagnosticEvent<InstanceType extends Base = Base, Attributes = Record<string, unknown>> = {
  client: InstanceType
} & Attributes

export type TracingChannelWithName<EventType extends object> = TracingChannel<string, EventType> & { name: string }

export type DiagnosticContext<BaseContext = {}> = BaseContext & {
  operationId: bigint
  result?: unknown
  error?: unknown
}

export const channelsNamespace = 'plt:kafka' as const

let operationId = 0n

export function createDiagnosticContext<BaseContext = {}> (context: BaseContext): DiagnosticContext<BaseContext> {
  return { operationId: operationId++, ...context }
}

export function notifyCreation<InstanceType> (
  type: ClientType | 'connection' | 'connection-pool' | 'messages-stream',
  instance: InstanceType
): void {
  instancesChannel.publish({ type, instance })
}

export function createTracingChannel<DiagnosticEvent extends object> (
  name: string
): TracingChannelWithName<DiagnosticEvent> {
  name = `${channelsNamespace}:${name}`
  const channel = tracingChannel<string, DiagnosticEvent>(name) as TracingChannelWithName<DiagnosticEvent>
  channel.name = name
  return channel
}

// Generic channel for objects creation
export const instancesChannel = channel(`${channelsNamespace}:instances`)

// Connection related channels
export const connectionsConnectsChannel = createTracingChannel<ConnectionDiagnosticEvent>('connections:connects')
export const connectionsApiChannel = createTracingChannel<ConnectionDiagnosticEvent>('connections:api')
export const connectionsPoolGetsChannel = createTracingChannel<ConnectionPoolDiagnosticEvent>('connections:pool:get')

// Base channels
export const baseApisChannel = createTracingChannel<ClientDiagnosticEvent>('base:apis')
export const baseMetadataChannel = createTracingChannel<ClientDiagnosticEvent>('base:metadata')

// Admin channels
export const adminTopicsChannel = createTracingChannel<ClientDiagnosticEvent>('admin:topics')
export const adminGroupsChannel = createTracingChannel<ClientDiagnosticEvent>('admin:groups')
export const adminClientQuotasChannel = createTracingChannel<ClientDiagnosticEvent>('admin:clientQuotas')
export const adminLogDirsChannel = createTracingChannel<ClientDiagnosticEvent>('admin:logDirs')

// Producer channels
export const producerInitIdempotentChannel = createTracingChannel<ClientDiagnosticEvent>('producer:initIdempotent')
export const producerSendsChannel = createTracingChannel<ClientDiagnosticEvent>('producer:sends')

// Consumer channels
export const consumerGroupChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:group')
export const consumerHeartbeatChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:heartbeat')
export const consumerReceivesChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:receives')
export const consumerFetchesChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:fetches')
export const consumerConsumesChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:consumes')
export const consumerCommitsChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:commits')
export const consumerOffsetsChannel = createTracingChannel<ClientDiagnosticEvent>('consumer:offsets')
