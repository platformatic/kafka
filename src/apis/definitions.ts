import type BufferList from 'bl'
import { promisify } from 'node:util'
import { type Connection } from '../connection/connection.ts'
import { type Writer } from '../protocol/writer.ts'

export type Callback<T> = (error: Error | null, payload: T) => void

export type CallbackArguments<T> = [cb: Callback<T>]

export type ResponseParser<T> = (
  correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
) => T | Promise<T>

export type ResponseErrorWithLocation = [string, number]

export type APIWithCallback<RequestArguments extends Array<unknown>, ResponseType> = (
  connection: Connection,
  ...args: [...RequestArguments, ...Partial<CallbackArguments<ResponseType>>]
) => boolean

export type APIWithPromise<RequestArguments extends Array<unknown>, ResponseType> = (
  connection: Connection,
  ...args: RequestArguments
) => Promise<ResponseType>

export type API<Q extends Array<unknown>, S> = APIWithCallback<Q, S> & { async: APIWithPromise<Q, S> }

export function createAPI<RequestArguments extends Array<unknown>, ResponseType> (
  apiKey: number,
  apiVersion: number,
  createRequest: (...args: any[]) => Writer,
  parseResponse: ResponseParser<ResponseType>,
  hasRequestHeaderTaggedFields: boolean = true,
  hasResponseHeaderTaggedFields: boolean = true
): API<RequestArguments, ResponseType> {
  const api = function api (
    connection: Connection,
    ...args: [...RequestArguments, ...Partial<CallbackArguments<ResponseType>>]
  ): boolean {
    const cb = typeof args[args.length - 1] === 'function' ? (args.pop() as Callback<ResponseType>) : () => {}
    return connection.send(
      apiKey,
      apiVersion,
      () => createRequest(...args),
      parseResponse,
      hasRequestHeaderTaggedFields,
      hasResponseHeaderTaggedFields,
      cb
    )
  }

  api.async = promisify(api) as APIWithPromise<RequestArguments, ResponseType>
  return api
}
