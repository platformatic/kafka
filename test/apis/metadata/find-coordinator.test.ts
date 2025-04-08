import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { findCoordinatorV6, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = findCoordinatorV6

test('createRequest serializes request parameters correctly', () => {
  // Values for the request
  const keyType = 0 // 0 for GROUP, 1 for TRANSACTION
  const coordinatorKeys = ['group-1', 'group-2']

  const writer = createRequest(keyType, coordinatorKeys)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read and verify all data in a single assertion
  deepStrictEqual(
    {
      keyType: reader.readInt8(),
      coordinatorKeys: reader.readArray(r => r.readString(), true, false)
    },
    {
      keyType,
      coordinatorKeys
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest handles transaction coordinator key type', () => {
  // Values for the request
  const keyType = 1 // 1 for TRANSACTION
  const coordinatorKeys = ['transaction-1']

  const writer = createRequest(keyType, coordinatorKeys)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read and verify all data in a single assertion
  deepStrictEqual(
    {
      keyType: reader.readInt8(),
      coordinatorKeys: reader.readArray(r => r.readString(), true, false)
    },
    {
      keyType,
      coordinatorKeys
    },
    'Serialized transaction request should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Coordinators array (compact format)
    .appendArray(
      [
        {
          key: 'group-1',
          nodeId: 1,
          host: 'broker1.example.com',
          port: 9092,
          errorCode: 0,
          errorMessage: null
        },
        {
          key: 'group-2',
          nodeId: 2,
          host: 'broker2.example.com',
          port: 9092,
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, coordinator) => {
        w.appendString(coordinator.key)
          .appendInt32(coordinator.nodeId)
          .appendString(coordinator.host)
          .appendInt32(coordinator.port)
          .appendInt16(coordinator.errorCode)
          .appendString(coordinator.errorMessage)
      }
    )
    .appendUnsignedVarInt(0) // root tagged fields

  const response = parseResponse(1, 10, 6, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    coordinators: [
      {
        key: 'group-1',
        nodeId: 1,
        host: 'broker1.example.com',
        port: 9092,
        errorCode: 0,
        errorMessage: null
      },
      {
        key: 'group-2',
        nodeId: 2,
        host: 'broker2.example.com',
        port: 9092,
        errorCode: 0,
        errorMessage: null
      }
    ]
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    // Coordinators array (compact format)
    .appendArray(
      [
        {
          key: 'group-1',
          nodeId: 1,
          host: 'broker1.example.com',
          port: 9092,
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, coordinator) => {
        w.appendString(coordinator.key)
          .appendInt32(coordinator.nodeId)
          .appendString(coordinator.host)
          .appendInt32(coordinator.port)
          .appendInt16(coordinator.errorCode)
          .appendString(coordinator.errorMessage)
      }
    )
    .appendUnsignedVarInt(0) // root tagged fields

  const response = parseResponse(1, 10, 6, writer.bufferList)

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    coordinators: [
      {
        key: 'group-1',
        nodeId: 1,
        host: 'broker1.example.com',
        port: 9092,
        errorCode: 0,
        errorMessage: null
      }
    ]
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    // Coordinators array (compact format)
    .appendArray(
      [
        {
          key: 'group-1',
          nodeId: -1,
          host: 'broker1.example.com',
          port: 9092,
          errorCode: 15,
          errorMessage: 'Coordinator not available'
        }
      ],
      (w, coordinator) => {
        w.appendString(coordinator.key)
          .appendInt32(coordinator.nodeId)
          .appendString(coordinator.host)
          .appendInt32(coordinator.port)
          .appendInt16(coordinator.errorCode)
          .appendString(coordinator.errorMessage)
      }
    )
    .appendUnsignedVarInt(0) // root tagged fields

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 10, 6, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error location and code
      ok(typeof err.errors === 'object')

      // Verify the response is preserved
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        coordinators: [
          {
            key: 'group-1',
            nodeId: -1,
            host: 'broker1.example.com',
            port: 9092,
            errorCode: 15,
            errorMessage: 'Coordinator not available'
          }
        ]
      })

      return true
    }
  )
})
