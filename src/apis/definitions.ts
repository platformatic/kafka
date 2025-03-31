import type BufferList from 'bl'
import { promisify } from 'node:util'
import { type Connection } from '../connection/connection.ts'
import { type Writer } from '../protocol/writer.ts'

export type Callback<ReturnType> = (error: Error | null, payload: ReturnType) => void

export type CallbackArguments<ReturnType> = [cb: Callback<ReturnType>]

export type ResponseParser<ReturnType> = (
  correlationId: number,
  apiKey: number,
  apiVersion: number,
  raw: BufferList
) => ReturnType | Promise<ReturnType>

export type ResponseErrorWithLocation = [string, number]

export type APIWithCallback<RequestArguments extends Array<unknown>, ResponseType> = (
  connection: Connection,
  ...args: [...RequestArguments, ...Partial<CallbackArguments<ResponseType>>]
) => void

export type APIWithPromise<RequestArguments extends Array<unknown>, ResponseType> = (
  connection: Connection,
  ...args: RequestArguments
) => Promise<ResponseType>

export type API<RequestType extends Array<unknown>, ResponseType> = APIWithCallback<RequestType, ResponseType> & {
  async: APIWithPromise<RequestType, ResponseType>
}

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
  ): void {
    const cb = typeof args[args.length - 1] === 'function' ? (args.pop() as Callback<ResponseType>) : () => {}
    connection.send(
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
