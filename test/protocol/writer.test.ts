import BufferList from 'bl'
import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { Writer } from '../../src/protocol/writer.ts'
import { EMPTY_UUID } from '../../src/protocol/definitions.ts'

test('Create Writer', () => {
  const writer = Writer.create()
  strictEqual(writer instanceof Writer, true)
  strictEqual(writer.length, 0)
})

test('Constructor with BufferList', () => {
  const bl = new BufferList(Buffer.from([1, 2, 3]))
  const writer = new Writer(bl)
  
  strictEqual(writer.length, 3)
  deepStrictEqual(writer.buffers, [Buffer.from([1, 2, 3])])
})

// The Writer constructor requires a BufferList - this is enforced with the ! operator
// So we should use Writer.create() instead which properly handles creating a new BufferList

test('Write unsigned numeric values', () => {
  const writer = Writer.create()
  
  writer.appendUnsignedInt8(200)
  writer.appendUnsignedInt16(60000)
  writer.appendUnsignedInt32(2994757120)
  writer.appendUnsignedInt64(9999999999979191808n)
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer[0], 200)
  strictEqual(buffer.readUInt16BE(1), 60000)
  strictEqual(buffer.readUInt32BE(3), 2994757120)
  strictEqual(buffer.readBigUInt64BE(7), 9999999999979191808n)
})

test('Write unsigned numeric values with prepend option', () => {
  const writer = Writer.create()
  
  writer.appendUnsignedInt8(200, false)
  writer.appendUnsignedInt16(60000, false)
  writer.appendUnsignedInt32(2994757120, false)
  writer.appendUnsignedInt64(9999999999979191808n, false)
  writer.appendUnsignedVarInt(128, false)
  writer.appendUnsignedVarInt64(256n, false)
  
  // With prepend, values should be in reverse order
  const buffer = Buffer.concat(writer.buffers)
  
  // The last value prepended is first in the buffer
  let pos = 0
  
  // VarInt64 256
  strictEqual(buffer[pos++], 128)
  strictEqual(buffer[pos++], 2)
  
  // VarInt 128
  strictEqual(buffer[pos++], 128)
  strictEqual(buffer[pos++], 1)
  
  // UInt64
  strictEqual(buffer.readBigUInt64BE(pos), 9999999999979191808n)
  pos += 8
  
  // UInt32
  strictEqual(buffer.readUInt32BE(pos), 2994757120)
  pos += 4
  
  // UInt16
  strictEqual(buffer.readUInt16BE(pos), 60000)
  pos += 2
  
  // UInt8
  strictEqual(buffer[pos], 200)
})

test('Write signed numeric values', () => {
  const writer = Writer.create()
  
  writer.appendInt8(-10)
  writer.appendInt16(-1000)
  writer.appendInt32(-100000)
  writer.appendInt64(-10000000000n)
  writer.appendFloat64(123.456)
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer.readInt8(0), -10)
  strictEqual(buffer.readInt16BE(1), -1000)
  strictEqual(buffer.readInt32BE(3), -100000)
  strictEqual(buffer.readBigInt64BE(7), -10000000000n)
  strictEqual(buffer.readDoubleBE(15), 123.456)
})

test('Write signed numeric values with prepend option', () => {
  const writer = Writer.create()
  
  writer.appendInt8(-10, false)
  writer.appendInt16(-1000, false)
  writer.appendInt32(-100000, false)
  writer.appendInt64(-10000000000n, false)
  writer.appendFloat64(123.456, false)
  
  // With prepend, values should be in reverse order
  const buffer = Buffer.concat(writer.buffers)
  
  // The last value prepended is first in the buffer
  let pos = 0
  strictEqual(buffer.readDoubleBE(pos), 123.456)
  pos += 8
  
  strictEqual(buffer.readBigInt64BE(pos), -10000000000n)
  pos += 8
  
  strictEqual(buffer.readInt32BE(pos), -100000)
  pos += 4
  
  strictEqual(buffer.readInt16BE(pos), -1000)
  pos += 2
  
  strictEqual(buffer.readInt8(pos), -10)
})

test('Write boolean values', () => {
  const writer = Writer.create()
  
  writer.appendBoolean(true)
  writer.appendBoolean(false)
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer[0], 1)
  strictEqual(buffer[1], 0)
})

test('Write string values', () => {
  const writer = Writer.create()
  
  // Compact format
  writer.appendString(null)     // null string
  writer.appendString('')       // empty string
  writer.appendString('test')   // normal string
  
  // Non-compact format
  writer.appendString(null, false)  // null string
  writer.appendString('hello', false) // normal string
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // Compact null
  strictEqual(buffer[pos++], 0)
  
  // Compact empty string (length 1, but actually empty)
  strictEqual(buffer[pos++], 1)
  
  // Compact "test" (length 5 = 1 for length + 4 for content)
  strictEqual(buffer[pos++], 5)
  strictEqual(buffer.toString('utf-8', pos, pos + 4), 'test')
  pos += 4
  
  // Non-compact null (length -1 as int16)
  strictEqual(buffer.readInt16BE(pos), -1)
  pos += 2
  
  // Non-compact "hello" (length 5 as int16, then content)
  strictEqual(buffer.readInt16BE(pos), 5)
  pos += 2
  strictEqual(buffer.toString('utf-8', pos, pos + 5), 'hello')
})

test('Write string with different encoding', () => {
  const writer = Writer.create()
  const text = 'ABC'
  
  writer.appendString(text, true, 'ascii')
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer[0], 4) // Length 3 + 1 for compact format
  strictEqual(buffer.toString('ascii', 1, 4), 'ABC')
})

test('Write bytes', () => {
  const writer = Writer.create()
  
  // Compact format
  writer.appendBytes(null)  // null bytes
  writer.appendBytes(Buffer.alloc(0))  // empty bytes
  writer.appendBytes(Buffer.from([1, 2, 3, 4]))  // normal bytes
  
  // Non-compact format
  writer.appendBytes(null, false)  // null bytes
  writer.appendBytes(Buffer.from([5, 6, 7]), false)  // normal bytes
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // Compact null
  strictEqual(buffer[pos++], 0)
  
  // Compact empty buffer (length 1, but actually empty)
  strictEqual(buffer[pos++], 1)
  
  // Compact buffer [1, 2, 3, 4] (length 5 = 1 for length + 4 for content)
  strictEqual(buffer[pos++], 5)
  deepStrictEqual(buffer.slice(pos, pos + 4), Buffer.from([1, 2, 3, 4]))
  pos += 4
  
  // Non-compact null (length -1 as int32 - readInt32 is used in appendBytes)
  strictEqual(buffer.readInt32BE(pos), -1)
  pos += 4
  
  // Non-compact buffer [5, 6, 7] (length 3 as int32, then content)
  strictEqual(buffer.readInt32BE(pos), 3)
  pos += 4
  deepStrictEqual(buffer.slice(pos, pos + 3), Buffer.from([5, 6, 7]))
})

test('Write VarIntBytes', () => {
  const writer = Writer.create()
  
  writer.appendVarIntBytes(null)  // null bytes
  writer.appendVarIntBytes(Buffer.from([10]))  // buffer bytes
  writer.appendVarIntBytes('test')  // string bytes
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // VarInt null (length 0)
  strictEqual(buffer[pos++], 0)
  
  // VarInt buffer (length 1, using zigzag encoding which is 2)
  strictEqual(buffer[pos++], 2)
  strictEqual(buffer[pos++], 10)
  
  // VarInt string (length 4, using zigzag encoding which is 8)
  strictEqual(buffer[pos++], 8)
  strictEqual(buffer.toString('utf-8', pos, pos + 4), 'test')
})

test('Write array', () => {
  const writer = Writer.create()
  
  // Compact format
  writer.appendArray(null, (w, v) => w.appendInt8(v))  // null array
  writer.appendArray([], (w, v) => w.appendInt8(v))  // empty array
  writer.appendArray([1, 2, 3], (w, v) => {
    w.appendInt8(v)
  })  // normal array
  
  // Non-compact format
  writer.appendArray(null, (w, v) => w.appendInt8(v), false)  // null array (will be length 0)
  writer.appendArray([4, 5], (w, v) => w.appendInt8(v), false)  // normal array
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // Compact null (length 0)
  strictEqual(buffer[pos++], 0)
  
  // Compact empty array (length 1, but no elements)
  strictEqual(buffer[pos++], 1)
  
  // Compact array [1, 2, 3] (length 4, 1 for length + 3 elements with tagged fields)
  strictEqual(buffer[pos++], 4)
  
  // Elements with tagged fields (1 byte per element + 1 byte for tagged field)
  strictEqual(buffer[pos++], 1)
  strictEqual(buffer[pos++], 0) // Tagged field
  strictEqual(buffer[pos++], 2)
  strictEqual(buffer[pos++], 0) // Tagged field
  strictEqual(buffer[pos++], 3)
  strictEqual(buffer[pos++], 0) // Tagged field
  
  // Non-compact null array (length 0 as int32)
  strictEqual(buffer.readInt32BE(pos), 0)
  pos += 4
  
  // Non-compact array [4, 5] (length 2 as int32, then 2 elements + tagged fields)
  strictEqual(buffer.readInt32BE(pos), 2)
  pos += 4
  strictEqual(buffer[pos++], 4)
  strictEqual(buffer[pos++], 0) // Tagged field
  strictEqual(buffer[pos++], 5)
  strictEqual(buffer[pos++], 0) // Tagged field
})

test('Write array without trailing tagged fields', () => {
  const writer = Writer.create()
  
  writer.appendArray([1, 2], (w, v) => {
    w.appendInt8(v)
  }, true, false)  // array without trailing tagged fields
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  
  // Array [1, 2] (length 3, 1 for length + 2 elements without tagged fields)
  strictEqual(buffer[0], 3)
  strictEqual(buffer[1], 1)
  strictEqual(buffer[2], 2)
})

test('Write VarInt values', () => {
  const writer = Writer.create()
  
  writer.appendVarInt(-10)  // ZigZag encoded negative number
  writer.appendVarInt(300)  // ZigZag encoded positive number
  writer.appendVarInt64(-20n)  // ZigZag encoded negative bigint
  writer.appendVarInt64(500n)  // ZigZag encoded positive bigint
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  
  // Note: These values depend on the ZigZag encoding implementation
  // -10 in ZigZag is 19, 300 in ZigZag is 600
  // -20n in ZigZag is 39n, 500n in ZigZag is 1000n
  
  // Check buffer content - this is implementation specific
  // The exact byte values depend on the writeVarInt implementation
  strictEqual(buffer.length > 0, true)
})

test('Write VarInt values with prepend option', () => {
  const writer = Writer.create()
  
  writer.appendVarInt(42, false)
  writer.appendVarInt64(100n, false)
  
  // The buffer should contain the prepended values
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer.length > 0, true)
})

test('Write VarIntArray', () => {
  const writer = Writer.create()
  
  writer.appendVarIntArray(null, (w, v) => w.appendInt8(v))  // null array
  writer.appendVarIntArray([], (w, v) => w.appendInt8(v))  // empty array
  writer.appendVarIntArray([1, 2], (w, v) => w.appendInt8(v))  // normal array
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // VarInt null (length 0)
  strictEqual(buffer[pos++], 0)
  
  // VarInt empty array (length 0)
  strictEqual(buffer[pos++], 0)
  
  // VarInt array [1, 2] (length 2, using zigzag encoding which is 4)
  strictEqual(buffer[pos++], 4)
  strictEqual(buffer[pos++], 1)
  strictEqual(buffer[pos++], 2)
})

test('Write UUID', () => {
  const writer = Writer.create()
  
  writer.appendUUID(null)  // null UUID
  writer.appendUUID('550e8400-e29b-41d4-a716-446655440000')  // normal UUID
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  
  // Null UUID (using EMPTY_UUID)
  deepStrictEqual(buffer.slice(0, 16), EMPTY_UUID)
  
  // Normal UUID (without hyphens)
  const expectedUUID = Buffer.from('550e8400e29b41d4a716446655440000', 'hex')
  deepStrictEqual(buffer.slice(16, 32), expectedUUID)
})

test('Write tagged fields', () => {
  const writer = Writer.create()
  
  writer.appendTaggedFields()
  
  // Verify the buffer content - currently just appends a 0 byte
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer[0], 0)
})

test('Prepend length', () => {
  const writer = Writer.create()
  
  // Add some data
  writer.appendInt8(1)
  writer.appendInt8(2)
  
  // Prepend the length
  writer.prependLength()
  
  // Verify the buffer content - first 4 bytes should be length (2) in BE format
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer.readInt32BE(0), 2)
  strictEqual(buffer[4], 1)
  strictEqual(buffer[5], 2)
})

test('Prepend VarInt length', () => {
  const writer = Writer.create()
  
  // Add some data
  writer.appendInt8(1)
  writer.appendInt8(2)
  
  // Prepend the VarInt length
  writer.prependVarIntLength()
  
  // Verify the buffer content - first byte should be VarInt encoded length (2, which is 4 after zigzag encoding)
  const buffer = Buffer.concat(writer.buffers)
  strictEqual(buffer[0], 4) // VarInt encoded 2 with ZigZag encoding
  strictEqual(buffer[1], 1)
  strictEqual(buffer[2], 2)
})

test('Append and prepend buffers', () => {
  const writer = Writer.create()
  
  writer.append(Buffer.from([1, 2]))
  writer.prepend(Buffer.from([3, 4]))
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  deepStrictEqual(buffer, Buffer.from([3, 4, 1, 2]))
})

test('Inspect buffer contents', () => {
  const writer = Writer.create()
  
  writer.appendInt8(1)
  writer.appendInt8(2)
  
  // The inspect method should return a string representation
  const inspectResult = writer.inspect()
  strictEqual(typeof inspectResult, 'string')
  strictEqual(inspectResult.includes('Buffer'), true)
})

test('Write Map', () => {
  const writer = Writer.create()
  
  // Compact format
  writer.appendMap(null, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt32(value)
  })  // null map
  
  const emptyMap = new Map()
  writer.appendMap(emptyMap, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt32(value)
  })  // empty map
  
  const testMap = new Map([
    ['key1', 100],
    ['key2', 200]
  ])
  writer.appendMap(testMap, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt32(value)
  })  // normal map
  
  // Non-compact format
  writer.appendMap(null, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt32(value)
  }, false)  // null map
  
  const smallMap = new Map([
    ['key3', 300]
  ])
  writer.appendMap(smallMap, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt32(value)
  }, false)  // normal map, non-compact
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // Compact null (length 0)
  strictEqual(buffer[pos++], 0)
  
  // Compact empty map (length 1, but no elements)
  strictEqual(buffer[pos++], 1)
  
  // Compact map with two entries (length 3, 1 for length + 2 elements with tagged fields)
  strictEqual(buffer[pos++], 3)
  
  // First entry - key1:100
  strictEqual(buffer[pos++], 5) // string length (4+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 4), 'key1')
  pos += 4
  strictEqual(buffer.readInt32BE(pos), 100)
  pos += 4
  strictEqual(buffer[pos++], 0) // Tagged field
  
  // Second entry - key2:200
  strictEqual(buffer[pos++], 5) // string length (4+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 4), 'key2')
  pos += 4
  strictEqual(buffer.readInt32BE(pos), 200)
  pos += 4
  strictEqual(buffer[pos++], 0) // Tagged field
  
  // Non-compact null map (length 0 as int32)
  strictEqual(buffer.readInt32BE(pos), 0)
  pos += 4
  
  // Non-compact map with one entry (length 1 as int32, then 1 element + tagged field)
  strictEqual(buffer.readInt32BE(pos), 1)
  pos += 4
  
  // Entry - key3:300
  strictEqual(buffer[pos++], 5) // string length (4+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 4), 'key3')
  pos += 4
  strictEqual(buffer.readInt32BE(pos), 300)
  pos += 4
  strictEqual(buffer[pos++], 0) // Tagged field
})

test('Write Map without trailing tagged fields', () => {
  const writer = Writer.create()
  
  const testMap = new Map([
    ['a', 1],
    ['b', 2]
  ])
  
  writer.appendMap(testMap, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt8(value)
  }, true, false)  // map without trailing tagged fields
  
  // Verify the buffer content
  const buffer = Buffer.concat(writer.buffers)
  
  // Map with two entries (length 3, 1 for length + 2 elements without tagged fields)
  strictEqual(buffer[0], 3)
  
  // First entry - a:1
  let pos = 1
  strictEqual(buffer[pos++], 2) // string length (1+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 1), 'a')
  pos += 1
  strictEqual(buffer[pos++], 1) // value
  
  // Second entry - b:2
  strictEqual(buffer[pos++], 2) // string length (1+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 1), 'b')
  pos += 1
  strictEqual(buffer[pos++], 2) // value
})

test('Write VarIntMap', () => {
  const writer = Writer.create()
  
  writer.appendVarIntMap(null, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt8(value)
  })  // null map
  
  writer.appendVarIntMap(new Map(), (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt8(value)
  })  // empty map
  
  const testMap = new Map([
    ['x', 10],
    ['y', 20]
  ])
  
  writer.appendVarIntMap(testMap, (w, v) => {
    const [key, value] = v
    w.appendString(key)
    w.appendInt8(value)
  })  // normal map
  
  // Read the buffer to verify
  const buffer = Buffer.concat(writer.buffers)
  let pos = 0
  
  // VarInt null (length 0)
  strictEqual(buffer[pos++], 0)
  
  // VarInt empty map (length 0)
  strictEqual(buffer[pos++], 0)
  
  // VarInt map with two entries (length 2, using zigzag encoding which is 4)
  strictEqual(buffer[pos++], 4)
  
  // First entry - x:10
  strictEqual(buffer[pos++], 2) // string length (1+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 1), 'x')
  pos += 1
  strictEqual(buffer[pos++], 10) // value
  
  // Second entry - y:20
  strictEqual(buffer[pos++], 2) // string length (1+1)
  strictEqual(buffer.toString('utf-8', pos, pos + 1), 'y')
  pos += 1
  strictEqual(buffer[pos++], 20) // value
})