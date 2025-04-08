import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { pushTelemetryV0, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = pushTelemetryV0

test('createRequest serializes parameters correctly', () => {
  const clientInstanceId = '12345678-1234-1234-1234-123456789abc'
  const subscriptionId = 123
  const terminating = false
  const compressionType = 0 // NONE
  const metrics = Buffer.from('metric data')

  const writer = createRequest(clientInstanceId, subscriptionId, terminating, compressionType, metrics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read all the values
  const readValues = {
    clientInstanceId: reader.readUUID(),
    subscriptionId: reader.readInt32(),
    terminating: reader.readBoolean(),
    compressionType: reader.readInt8(),
    metrics: reader.readBytes()
  }

  // Verify all values match expected
  deepStrictEqual(
    readValues,
    {
      clientInstanceId,
      subscriptionId,
      terminating,
      compressionType,
      metrics: Buffer.from('metric data')
    },
    'Serialized request should match expected structure'
  )
})

test('createRequest with terminating flag true', () => {
  const clientInstanceId = '12345678-1234-1234-1234-123456789abc'
  const subscriptionId = 123
  const terminating = true // Set terminating to true
  const compressionType = 1 // GZIP
  const metrics = Buffer.from('compressed metric data')

  const writer = createRequest(clientInstanceId, subscriptionId, terminating, compressionType, metrics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read all the values
  const readValues = {
    clientInstanceId: reader.readUUID(),
    subscriptionId: reader.readInt32(),
    terminating: reader.readBoolean(),
    compressionType: reader.readInt8(),
    metrics: reader.readBytes()
  }

  // Verify all values match expected
  deepStrictEqual(
    readValues,
    {
      clientInstanceId,
      subscriptionId,
      terminating,
      compressionType,
      metrics: Buffer.from('compressed metric data')
    },
    'Serialized request with terminating flag should match expected structure'
  )
})

test('createRequest with empty metrics', () => {
  const clientInstanceId = '12345678-1234-1234-1234-123456789abc'
  const subscriptionId = 123
  const terminating = false
  const compressionType = 0 // NONE
  const metrics = Buffer.alloc(0) // Empty metrics

  const writer = createRequest(clientInstanceId, subscriptionId, terminating, compressionType, metrics)

  // Verify it returns a Writer
  ok(writer instanceof Writer)

  // Read the serialized data to verify correctness
  const reader = new Reader(writer.bufferList)

  // Read all the values
  const readValues = {
    clientInstanceId: reader.readUUID(),
    subscriptionId: reader.readInt32(),
    terminating: reader.readBoolean(),
    compressionType: reader.readInt8(),
    metrics: reader.readBytes()
  }

  // Verify all values match expected
  deepStrictEqual(
    readValues,
    {
      clientInstanceId,
      subscriptionId,
      terminating,
      compressionType,
      metrics // Empty buffer
    },
    'Serialized request with empty metrics should match expected structure'
  )
})

test('parseResponse correctly processes a successful response', () => {
  // Create a successful response
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 72, 0, writer.bufferList)

  // Verify structure
  deepStrictEqual(response, {
    throttleTimeMs: 0,
    errorCode: 0
  })
})

test('parseResponse handles response with throttling', () => {
  // Create a response with throttling
  const writer = Writer.create()
    .appendInt32(100) // throttleTimeMs (non-zero value for throttling)
    .appendInt16(0) // errorCode (success)
    .appendInt8(0) // Root tagged fields

  const response = parseResponse(1, 72, 0, writer.bufferList)

  // Verify response structure
  deepStrictEqual(response, {
    throttleTimeMs: 100,
    errorCode: 0
  })
})

test('parseResponse throws error on non-zero error code', () => {
  // Create a response with error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(42) // errorCode (non-zero)
    .appendInt8(0) // Root tagged fields

  throws(
    () => {
      parseResponse(1, 72, 0, writer.bufferList)
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      ok(err.message.includes('Received response with error while executing API'))

      // Verify error details
      deepStrictEqual(err.response, {
        errorCode: 42,
        throttleTimeMs: 0
      })

      // Check that errors object exists
      ok(err.errors && typeof err.errors === 'object')

      return true
    }
  )
})
