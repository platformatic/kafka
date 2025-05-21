import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { listClientMetricsResourcesV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = listClientMetricsResourcesV0

test('createRequest returns a correctly structured empty request', () => {
  const writer = createRequest()

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // This API has no request parameters, just a tagged fields marker
  deepStrictEqual(reader.readUnsignedInt8(), 0)
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no resources
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    // Empty clientMetricsResources array in compact format
    .appendArray(
      [], // Empty array
      () => {} // No elements to process
    )
    .appendInt8(0) // Root tagged fields (none)

  const response = parseResponse(1, 74, 0, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    clientMetricsResources: []
  })
})

test('parseResponse correctly processes a response with multiple resources', () => {
  // Create a successful response with multiple resources
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    // clientMetricsResources array in compact format
    .appendArray([{ name: 'cpu' }, { name: 'memory' }], (w, resource) => {
      w.appendString(resource.name, true) // compact string
    })
    .appendInt8(0) // Root tagged fields (none)

  const response = parseResponse(1, 74, 0, Reader.from(writer))

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0,
    clientMetricsResources: [{ name: 'cpu' }, { name: 'memory' }]
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    // clientMetricsResources array (empty)
    .appendArray(
      [], // Empty array
      () => {} // No elements to process
    )
    .appendInt8(0) // Root tagged fields (none)

  const response = parseResponse(1, 74, 0, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0,
    clientMetricsResources: []
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(42) // errorCode (non-zero)
    // clientMetricsResources array (empty)
    .appendArray(
      [], // Empty array
      () => {} // No elements to process
    )
    .appendInt8(0) // Root tagged fields (none)

  throws(
    () => {
      parseResponse(1, 74, 0, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify the error response details
      deepStrictEqual(err.response, {
        errorCode: 42,
        throttleTimeMs: 0,
        clientMetricsResources: []
      })

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      return true
    }
  )
})
