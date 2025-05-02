import { channel, tracingChannel } from 'node:diagnostics_channel'
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

export const diagnosticChannelNamespace = 'plt.kafka' as const

export const instancesChannelName = `${diagnosticChannelNamespace}:instances` as const
export const instancesChannel = channel(instancesChannelName)

export const connectionsChannelName = `${diagnosticChannelNamespace}:connections` as const
export const connectionsChannel = tracingChannel<string, ConnectionDiagnosticEvent>(connectionsChannelName)

export const connectionPoolsChannelName = `${diagnosticChannelNamespace}:connectionPools` as const
export const connectionPoolsChannel = tracingChannel<string, ConnectionPoolDiagnosticEvent>(connectionPoolsChannelName)

export const clientsChannelName = `${diagnosticChannelNamespace}:clients` as const
export const clientsChannel = tracingChannel<string, ClientDiagnosticEvent>(clientsChannelName)

let operationId = 0n

export function createDiagnosticContext<T> (context: T): T & { operationId: bigint; result?: unknown; error?: unknown } {
  return { operationId: operationId++, ...context }
}

export function notifyCreation<InstanceType> (
  type: ClientType | 'connection' | 'connectionPool',
  instance: InstanceType
): void {
  instancesChannel.publish({ type, instance })
}
