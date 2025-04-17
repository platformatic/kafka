import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { getTelemetrySubscriptionsV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = getTelemetrySubscriptionsV0

test('createRequest with client instance ID', () => {
  const clientInstanceId = '12345678-1234-1234-1234-123456789abc'
  const writer = createRequest(clientInstanceId)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and verify values
  deepStrictEqual(
    {
      clientInstanceId: reader.readUUID()
    },
    {
      clientInstanceId
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest without client instance ID uses default UUID', () => {
  const writer = createRequest()

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read and verify values
  deepStrictEqual(
    {
      clientInstanceId: reader.readUUID()
    },
    {
      clientInstanceId: '00000000-0000-0000-0000-000000000000' // When clientInstanceId is undefined, it serializes as all zeros UUID
    },
    'Serialized request with default UUID should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendUUID('12345678-1234-1234-1234-123456789abc') // clientInstanceId
    .appendInt32(123) // subscriptionId
    // acceptedCompressionTypes array - compact format
    .appendArray(
      [0, 1], // NONE, GZIP
      (w, compressionType) => {
        w.appendInt8(compressionType)
      },
      true,
      false // No tagged fields for simple values
    )
    .appendInt32(30000) // pushIntervalMs
    .appendInt32(1048576) // telemetryMaxBytes
    .appendBoolean(true) // deltaTemporality
    // requestedMetrics array - compact format
    .appendArray(
      ['cpu.usage', 'memory.usage'],
      (w, metric) => {
        w.appendString(metric, true) // compact string
      },
      true,
      false // No tagged fields for simple values
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 71, 0, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    clientInstanceId: '12345678-1234-1234-1234-123456789abc',
    subscriptionId: 123,
    acceptedCompressionTypes: [0, 1],
    pushIntervalMs: 30000,
    telemetryMaxBytes: 1048576,
    deltaTemporality: true,
    requestedMetrics: ['cpu.usage', 'memory.usage']
  })
})

test('parseResponse handles response with empty arrays', () => {
  // Create a response with empty arrays
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendUUID('12345678-1234-1234-1234-123456789abc') // clientInstanceId
    .appendInt32(123) // subscriptionId
    // Empty acceptedCompressionTypes array - compact format
    .appendArray(
      [], // Empty array
      () => {}, // No elements to process
      true,
      false // No tagged fields for simple values
    )
    .appendInt32(30000) // pushIntervalMs
    .appendInt32(1048576) // telemetryMaxBytes
    .appendBoolean(false) // deltaTemporality
    // Empty requestedMetrics array - compact format
    .appendArray(
      [], // Empty array
      () => {}, // No elements to process
      true,
      false // No tagged fields for simple values
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 71, 0, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    clientInstanceId: '12345678-1234-1234-1234-123456789abc',
    subscriptionId: 123,
    acceptedCompressionTypes: [],
    pushIntervalMs: 30000,
    telemetryMaxBytes: 1048576,
    deltaTemporality: false,
    requestedMetrics: []
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendUUID('12345678-1234-1234-1234-123456789abc') // clientInstanceId
    .appendInt32(123) // subscriptionId
    // acceptedCompressionTypes array - compact format
    .appendArray(
      [0], // NONE
      (w, compressionType) => {
        w.appendInt8(compressionType)
      },
      true,
      false // No tagged fields for simple values
    )
    .appendInt32(30000) // pushIntervalMs
    .appendInt32(1048576) // telemetryMaxBytes
    .appendBoolean(true) // deltaTemporality
    // requestedMetrics array - compact format
    .appendArray(
      ['cpu.usage'],
      (w, metric) => {
        w.appendString(metric, true) // compact string
      },
      true,
      false // No tagged fields for simple values
    )
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 71, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    clientInstanceId: '12345678-1234-1234-1234-123456789abc',
    subscriptionId: 123,
    acceptedCompressionTypes: [0],
    pushIntervalMs: 30000,
    telemetryMaxBytes: 1048576,
    deltaTemporality: true,
    requestedMetrics: ['cpu.usage']
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(42) // errorCode (non-zero)
    .appendUUID('12345678-1234-1234-1234-123456789abc') // clientInstanceId
    .appendInt32(0) // subscriptionId
    // Empty acceptedCompressionTypes array - compact format
    .appendArray(
      [], // Empty array
      () => {}, // No elements to process
      true,
      false // No tagged fields for simple values
    )
    .appendInt32(0) // pushIntervalMs
    .appendInt32(0) // telemetryMaxBytes
    .appendBoolean(false) // deltaTemporality
    // Empty requestedMetrics array - compact format
    .appendArray(
      [], // Empty array
      () => {}, // No elements to process
      true,
      false // No tagged fields for simple values
    )
    .appendInt8(0) // Root tagged fields

  throws(
    () => {
      parseResponse(1, 71, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error response details
      deepStrictEqual(err.response, {
        throttleTimeMs: 0,
        errorCode: 42,
        clientInstanceId: '12345678-1234-1234-1234-123456789abc',
        subscriptionId: 0,
        acceptedCompressionTypes: [],
        pushIntervalMs: 0,
        telemetryMaxBytes: 0,
        deltaTemporality: false,
        requestedMetrics: []
      })

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      return true
    }
  )
})
