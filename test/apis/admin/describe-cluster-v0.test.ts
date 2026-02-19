import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, describeClusterV0 } from '../../../src/index.ts'

const { createRequest, parseResponse } = describeClusterV0

test('createRequest serializes parameters correctly', () => {
  const includeClusterAuthorizedOperations = true

  const writer = createRequest(includeClusterAuthorizedOperations)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read includeClusterAuthorizedOperations boolean
  const includeClusterAuthorizedOpsValue = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(
    includeClusterAuthorizedOpsValue,
    true,
    'includeClusterAuthorizedOperations should be serialized correctly'
  )
})

test('createRequest serializes false include_cluster_authorized_operations correctly', () => {
  const includeClusterAuthorizedOperations = false

  const writer = createRequest(includeClusterAuthorizedOperations)
  const reader = Reader.from(writer)

  // Read includeClusterAuthorizedOperations boolean
  const includeClusterAuthorizedOpsValue = reader.readBoolean()

  // Verify serialized data
  ok(includeClusterAuthorizedOpsValue === false, 'includeClusterAuthorizedOperations should be set to false')
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response with the Writer (no endpointType in v0)
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-cluster') // clusterId
    .appendInt32(1) // controllerId
    .appendArray(
      [
        {
          brokerId: 1,
          host: 'kafka-1',
          port: 9092,
          rack: 'rack-1'
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.brokerId).appendString(broker.host).appendInt32(broker.port).appendString(broker.rack)
      }
    )
    .appendInt32(0) // clusterAuthorizedOperations
    .appendTaggedFields()

  const response = parseResponse(1, 60, 0, Reader.from(writer))

  // Verify response structure (no endpointType in v0)
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      endpointType: 0,
      clusterId: 'test-cluster',
      controllerId: 1,
      brokers: [
        {
          brokerId: 1,
          host: 'kafka-1',
          port: 9092,
          rack: 'rack-1'
        }
      ],
      clusterAuthorizedOperations: 0
    },
    'Response should match expected structure'
  )
})

test('parseResponse correctly processes a response with multiple brokers', () => {
  // Create a response with multiple brokers
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-cluster') // clusterId
    .appendInt32(2) // controllerId
    .appendArray(
      [
        {
          brokerId: 1,
          host: 'kafka-1',
          port: 9092,
          rack: 'rack-1'
        },
        {
          brokerId: 2,
          host: 'kafka-2',
          port: 9092,
          rack: 'rack-2'
        },
        {
          brokerId: 3,
          host: 'kafka-3',
          port: 9092,
          rack: null
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.brokerId).appendString(broker.host).appendInt32(broker.port).appendString(broker.rack)
      }
    )
    .appendInt32(3) // clusterAuthorizedOperations
    .appendTaggedFields()

  const response = parseResponse(1, 60, 0, Reader.from(writer))

  // Verify brokers count
  deepStrictEqual(response.brokers.length, 3, 'Should parse multiple brokers correctly')

  // Verify controller ID
  deepStrictEqual(response.controllerId, 2, 'Should parse controller ID correctly')

  // Verify a broker with rack = null
  deepStrictEqual(
    response.brokers[2],
    {
      brokerId: 3,
      host: 'kafka-3',
      port: 9092,
      rack: null
    },
    'Should parse broker with null rack correctly'
  )

  // Verify clusterAuthorizedOperations
  deepStrictEqual(response.clusterAuthorizedOperations, 3, 'Should parse clusterAuthorizedOperations correctly')
})

test('parseResponse throws ResponseError on error response', () => {
  // Create a response with an error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(45) // errorCode INVALID_REQUEST
    .appendString('Invalid request') // errorMessage
    .appendString('test-cluster') // clusterId
    .appendInt32(-1) // controllerId
    .appendArray([], () => {}) // Empty brokers array
    .appendInt32(0) // clusterAuthorizedOperations
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 60, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error response
      deepStrictEqual(err.response.errorCode, 45, 'Error code should be preserved in the response')
      deepStrictEqual(err.response.errorMessage, 'Invalid request', 'Error message should be preserved in the response')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendString('test-cluster') // clusterId
    .appendInt32(1) // controllerId
    .appendArray(
      [
        {
          brokerId: 1,
          host: 'kafka-1',
          port: 9092,
          rack: null
        }
      ],
      (w, broker) => {
        w.appendInt32(broker.brokerId).appendString(broker.host).appendInt32(broker.port).appendString(broker.rack)
      }
    )
    .appendInt32(0) // clusterAuthorizedOperations
    .appendTaggedFields()

  const response = parseResponse(1, 60, 0, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
