import type BufferList from 'bl'
import { type Connection } from '../connection.ts'

export type SendCallback<T> = (error: Error | null | undefined, payload?: T) => void
export type ResponseParser<T> = (raw: BufferList) => T | Promise<T>

export function createAPI<RequestArguments extends Array<unknown>, ResponseType> (
  apiKey: number,
  apiVersion: number,
  createRequest: (...args: any[]) => BufferList | Promise<BufferList>,
  parseResponse: ResponseParser<ResponseType>,
  hasResponseHeaderTaggedFields: boolean = true
) {
  return async function api (connection: Connection, ...args: RequestArguments): Promise<ResponseType> {
    return connection.send(
      apiKey,
      apiVersion,
      await createRequest(...args),
      parseResponse,
      hasResponseHeaderTaggedFields
    )
  }
}
