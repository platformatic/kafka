import { ResponseError } from '../../errors.ts'
import { type NullableString } from '../../protocol/definitions.ts'
import { type Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI } from '../definitions.ts'

export type DescribeClusterRequest = Parameters<typeof createRequest>

export interface DescribeClusterResponseBroker {
  brokerId: number
  host: string
  port: number
  rack: NullableString
}

export interface DescribeClusterResponse {
  throttleTimeMs: number
  errorCode: number
  errorMessage: NullableString
  endpointType: number
  clusterId: string
  controllerId: number
  brokers: DescribeClusterResponseBroker[]
  clusterAuthorizedOperations: number
}

/*
  DescribeCluster Request (Version: 0) => include_cluster_authorized_operations TAG_BUFFER
    include_cluster_authorized_operations => BOOLEAN
*/
export function createRequest (includeClusterAuthorizedOperations: boolean): Writer {
  return Writer.create().appendBoolean(includeClusterAuthorizedOperations).appendTaggedFields()
}

/*
  DescribeCluster Response (Version: 0) => throttle_time_ms error_code error_message cluster_id controller_id [brokers] cluster_authorized_operations TAG_BUFFER
    throttle_time_ms => INT32
    error_code => INT16
    error_message => COMPACT_NULLABLE_STRING
    cluster_id => COMPACT_STRING
    controller_id => INT32
    brokers => broker_id host port rack TAG_BUFFER
      broker_id => INT32
      host => COMPACT_STRING
      port => INT32
      rack => COMPACT_NULLABLE_STRING
    cluster_authorized_operations => INT32
*/
export function parseResponse (
  _correlationId: number,
  apiKey: number,
  apiVersion: number,
  reader: Reader
): DescribeClusterResponse {
  const response: DescribeClusterResponse = {
    throttleTimeMs: reader.readInt32(),
    errorCode: reader.readInt16(),
    errorMessage: reader.readNullableString(),
    endpointType: 0,
    clusterId: reader.readString(),
    controllerId: reader.readInt32(),
    brokers: reader.readArray(r => {
      return {
        brokerId: r.readInt32(),
        host: r.readString(),
        port: r.readInt32(),
        rack: r.readNullableString()
      }
    }),
    clusterAuthorizedOperations: reader.readInt32()
  }

  if (response.errorCode !== 0) {
    throw new ResponseError(apiKey, apiVersion, { '/': [response.errorCode, response.errorMessage] }, response)
  }

  return response
}

export const api = createAPI<DescribeClusterRequest, DescribeClusterResponse>(60, 0, createRequest, parseResponse)
