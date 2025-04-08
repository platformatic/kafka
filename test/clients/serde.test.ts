import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import {
  deserializersFrom,
  jsonDeserializer,
  jsonSerializer,
  serializersFrom,
  stringDeserializer,
  stringDeserializers,
  stringSerializer,
  stringSerializers
} from '../../src/index.ts'

test('stringSerializer converts string to Buffer', () => {
  const testString = 'hello world'
  const result = stringSerializer(testString)

  strictEqual(Buffer.isBuffer(result), true)
  strictEqual(result?.toString('utf-8'), testString)
})

test('stringSerializer returns undefined for non-string input', () => {
  // @ts-expect-error Testing with invalid input
  const result = stringSerializer(123)
  strictEqual(result, undefined)

  // @ts-expect-error Testing with invalid input
  const objResult = stringSerializer({ key: 'value' })
  strictEqual(objResult, undefined)

  // @ts-expect-error Testing with invalid input
  const nullResult = stringSerializer(null)
  strictEqual(nullResult, undefined)
})

test('stringDeserializer converts Buffer to string', () => {
  const testBuffer = Buffer.from('hello world', 'utf-8')
  const result = stringDeserializer(testBuffer)

  strictEqual(typeof result, 'string')
  strictEqual(result, 'hello world')
})

test('stringDeserializer returns undefined for non-Buffer input', () => {
  const result = stringDeserializer('not a buffer')
  strictEqual(result, undefined)

  // @ts-expect-error Testing with invalid input
  const objResult = stringDeserializer({})
  strictEqual(objResult, undefined)

  // @ts-expect-error Testing with invalid input
  const nullResult = stringDeserializer(null)
  strictEqual(nullResult, undefined)
})

test('jsonSerializer converts object to Buffer', () => {
  const testObj = { key: 'value', num: 123, nested: { prop: true } }
  const result = jsonSerializer(testObj)

  strictEqual(Buffer.isBuffer(result), true)

  // Parsing back to verify content
  const parsed = JSON.parse(result!.toString('utf-8'))
  deepStrictEqual(parsed, testObj)
})

test('jsonSerializer handles primitives', () => {
  const numberResult = jsonSerializer(123)
  strictEqual(numberResult?.toString(), '123')

  const stringResult = jsonSerializer('test')
  strictEqual(stringResult?.toString(), '"test"')

  const boolResult = jsonSerializer(true)
  strictEqual(boolResult?.toString(), 'true')

  const nullResult = jsonSerializer(null)
  strictEqual(nullResult?.toString(), 'null')
})

test('jsonDeserializer converts Buffer to object', () => {
  const testObj = { key: 'value', num: 123, nested: { prop: true } }
  const buffer = Buffer.from(JSON.stringify(testObj), 'utf-8')
  const result = jsonDeserializer(buffer)

  strictEqual(typeof result, 'object')
  deepStrictEqual(result, testObj)
})

test('jsonDeserializer returns undefined for non-Buffer input', () => {
  const result = jsonDeserializer('not a buffer')
  strictEqual(result, undefined)

  // @ts-expect-error Testing with invalid input
  const objResult = jsonDeserializer({})
  strictEqual(objResult, undefined)

  // @ts-expect-error Testing with invalid input
  const nullResult = jsonDeserializer(null)
  strictEqual(nullResult, undefined)
})

test('jsonDeserializer with type parameter', () => {
  interface TestType {
    id: number
    name: string
  }

  const testData: TestType = { id: 1, name: 'test' }
  const buffer = Buffer.from(JSON.stringify(testData), 'utf-8')

  const result = jsonDeserializer<TestType>(buffer)
  strictEqual(result?.id, 1)
  strictEqual(result?.name, 'test')
})

test('serializersFrom creates serializers object from a single serializer', () => {
  const testSerializer = (data?: string): Buffer | undefined => {
    return data ? Buffer.from(`serialized-${data}`) : undefined
  }

  const serializers = serializersFrom(testSerializer)

  strictEqual(typeof serializers, 'object')
  strictEqual(typeof serializers.key, 'function')
  strictEqual(typeof serializers.value, 'function')
  strictEqual(typeof serializers.headerKey, 'function')
  strictEqual(typeof serializers.headerValue, 'function')

  strictEqual(serializers.key, testSerializer)
  strictEqual(serializers.value, testSerializer)
  strictEqual(serializers.headerKey, testSerializer)
  strictEqual(serializers.headerValue, testSerializer)

  // Verify functionality
  const result = serializers.key('test')
  strictEqual(result?.toString(), 'serialized-test')
})

test('deserializersFrom creates deserializers object from a single deserializer', () => {
  const testDeserializer = (data?: Buffer): string | undefined => {
    return data ? `deserialized-${data.toString()}` : undefined
  }

  const deserializers = deserializersFrom(testDeserializer)

  strictEqual(typeof deserializers, 'object')
  strictEqual(typeof deserializers.key, 'function')
  strictEqual(typeof deserializers.value, 'function')
  strictEqual(typeof deserializers.headerKey, 'function')
  strictEqual(typeof deserializers.headerValue, 'function')

  strictEqual(deserializers.key, testDeserializer)
  strictEqual(deserializers.value, testDeserializer)
  strictEqual(deserializers.headerKey, testDeserializer)
  strictEqual(deserializers.headerValue, testDeserializer)

  // Verify functionality
  const result = deserializers.key(Buffer.from('test'))
  strictEqual(result, 'deserialized-test')
})

test('stringSerializers is exported and contains string serializer functions', () => {
  strictEqual(typeof stringSerializers, 'object')
  strictEqual(stringSerializers.key, stringSerializer)
  strictEqual(stringSerializers.value, stringSerializer)
  strictEqual(stringSerializers.headerKey, stringSerializer)
  strictEqual(stringSerializers.headerValue, stringSerializer)

  // Verify functionality
  const result = stringSerializers.key('test')
  strictEqual(result?.toString(), 'test')
})

test('stringDeserializers is exported and contains string deserializer functions', () => {
  strictEqual(typeof stringDeserializers, 'object')
  strictEqual(stringDeserializers.key, stringDeserializer)
  strictEqual(stringDeserializers.value, stringDeserializer)
  strictEqual(stringDeserializers.headerKey, stringDeserializer)
  strictEqual(stringDeserializers.headerValue, stringDeserializer)

  // Verify functionality
  const result = stringDeserializers.key(Buffer.from('test'))
  strictEqual(result, 'test')
})
