import { promisify } from 'node:util'
import { type Connection } from '../network/connection.ts'
import { type Reader } from '../protocol/reader.ts'
import { type Writer } from '../protocol/writer.ts'
import { type NullableString } from '../protocol/definitions.ts'

export type Callback<ReturnType> = (error: Error | null, payload: ReturnType) => void

export type CallbackArguments<ReturnType> = [cb: Callback<ReturnType>]

export type RequestCreator = (...args: any[]) => Writer

export type ResponseParser<ReturnType> = (
  correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
) => ReturnType | Promise<ReturnType>

export type ResponseErrorWithLocation = [string, [number, NullableString]]

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
  key: number
  version: number
}

export function createAPI<RequestArguments extends Array<unknown>, ResponseType> (
  apiKey: number,
  apiVersion: number,
  createRequest: RequestCreator,
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
  api.key = apiKey
  api.version = apiVersion

  return api
}
