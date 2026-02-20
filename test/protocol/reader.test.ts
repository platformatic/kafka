import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import test from 'node:test'
import { DynamicBuffer, Reader } from '../../src/index.ts'

test('static isReader', () => {
  ok(Reader.isReader(new Reader(new DynamicBuffer())))
  ok(!Reader.isReader('STRING'))
})

test('static from and constructor', () => {
  // Test instantiation with DynamicBuffer
  const bl = new DynamicBuffer(Buffer.from([1, 2, 3]))
  const reader1 = new Reader(bl)
  strictEqual(reader1.position, 0)
  strictEqual(reader1.readInt8(), 1)

  // Test instantiation with Reader.from using a Buffer
  const buffer = Buffer.from([4, 5, 6])
  const reader2 = Reader.from(buffer)
  strictEqual(reader2.position, 0)
  strictEqual(reader2.readInt8(), 4)

  // Test instantiation with Reader.from using a DynamicBuffer
  const reader3 = Reader.from(new DynamicBuffer(Buffer.from([7, 8, 9])))
  strictEqual(reader3.position, 0)
  strictEqual(reader3.readInt8(), 7)

  const buffer2 = new DynamicBuffer(Buffer.from([1, 2, 3]))
  const reader4 = Reader.from(buffer2)

  strictEqual(reader4 instanceof Reader, true)
  strictEqual(reader4.position, 0)
  strictEqual(reader4.readInt8(), 1)
})

test('reset', () => {
  // Test reset position only
  const buffer = Buffer.from([1, 2, 3])
  const reader = Reader.from(buffer)

  reader.readInt8() // position = 1
  reader.readInt8() // position = 2
  strictEqual(reader.position, 2)

  reader.reset()
  strictEqual(reader.position, 0)
  strictEqual(reader.readInt8(), 1) // Should read from the beginning again

  // Test reset with new Buffer
  const newBuffer = Buffer.from([10, 20, 30])
  reader.reset(newBuffer)
  strictEqual(reader.position, 0)
  strictEqual(reader.readInt8(), 10) // Should read from the new buffer

  // Test reset with new DynamicBuffer
  const newDynamicBuffer = new DynamicBuffer(Buffer.from([100, 200]))
  reader.reset(newDynamicBuffer)
  strictEqual(reader.position, 0)
  strictEqual(reader.readInt8(), 100) // Should read from the new DynamicBuffer
})

test('inspect', () => {
  const buffer = Buffer.from([0xde, 0xad, 0xbe, 0xef])
  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.inspect(), 'dead beef')

  reader.skip(2)
  strictEqual(reader.inspect(), 'beef')
})

test('skip', () => {
  const buffer = Buffer.from([1, 2, 3, 4, 5])
  const reader = new Reader(new DynamicBuffer(buffer))

  reader.skip(2)
  strictEqual(reader.readInt8(), 3)
  reader.skip(1)
  strictEqual(reader.readInt8(), 5)
})

test('peekInt8', () => {
  const buffer = Buffer.from([10, 20, 30])
  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekInt8(), 10)
  strictEqual(reader.position, 0) // Position should not change

  strictEqual(reader.readInt8(), 10)
  strictEqual(reader.position, 1) // Position should change now

  strictEqual(reader.peekInt8(), 20)
  strictEqual(reader.readInt8(), 20)
  strictEqual(reader.position, 2)
})

test('peekUnsignedInt*', () => {
  const buffer = Buffer.from([
    // uint8: 200
    200,
    // uint16: 60000
    234, 96,
    // uint32: 2994757120
    178, 128, 94, 0,
    // uint64: 9999999999979191808n
    138, 199, 35, 4, 136, 170, 126, 0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekUnsignedInt8(), 200)
  strictEqual(reader.readUnsignedInt8(), 200)

  strictEqual(reader.peekUnsignedInt16(), 60000)
  strictEqual(reader.readUnsignedInt16(), 60000)

  strictEqual(reader.peekUnsignedInt32(), 2994757120)
  strictEqual(reader.readUnsignedInt32(), 2994757120)

  strictEqual(reader.peekUnsignedInt64(), 9999999999979191808n)
  strictEqual(reader.readUnsignedInt64(), 9999999999979191808n)
})

test('peekUnsignedVarInt and readUnsignedVarInt', () => {
  // These values represent simple unsigned varints
  const buffer = Buffer.from([
    // UnsignedVarInt: 10 (just one byte)
    10,
    // UnsignedVarInt: 128 (requires multiple bytes)
    128, 1
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekUnsignedVarInt(), 10)
  strictEqual(reader.readUnsignedVarInt(), 10)

  strictEqual(reader.peekUnsignedVarInt(), 128)
  strictEqual(reader.readUnsignedVarInt(), 128)
})

test('peekInt*', () => {
  const buffer = Buffer.from([
    // int8: -10
    246,
    // int16: -1000
    252, 24,
    // int32: -100000
    255, 254, 121, 96,
    // int64: -10000000000n
    255, 255, 255, 253, 171, 244, 28, 0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekInt8(), -10)
  strictEqual(reader.readInt8(), -10)

  strictEqual(reader.peekInt16(), -1000)
  strictEqual(reader.readInt16(), -1000)

  strictEqual(reader.peekInt32(), -100000)
  strictEqual(reader.readInt32(), -100000)

  strictEqual(reader.peekInt64(), -10000000000n)
  strictEqual(reader.readInt64(), -10000000000n)
})

test('peekFloat64', () => {
  const buffer = Buffer.from([
    // float64: 123.456
    64, 94, 221, 47, 26, 159, 190, 119
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekFloat64(), 123.456)
  strictEqual(reader.readFloat64(), 123.456)
})

test('peekBoolean', () => {
  const buffer = Buffer.from([1, 0])
  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekBoolean(), true)
  strictEqual(reader.readBoolean(), true)

  strictEqual(reader.peekBoolean(), false)
  strictEqual(reader.readBoolean(), false)
})

test('peekUUID', () => {
  const uuidHex = '550e8400e29b41d4a716446655440000'
  const buffer = Buffer.from(uuidHex, 'hex')

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.peekUUID(), uuidHex)
  strictEqual(reader.readUUID(), '550e8400-e29b-41d4-a716-446655440000')
})

test('peekVarInt and readVarInt', () => {
  // These values represent ZigZag encoded varints
  const buffer = Buffer.from([
    // ZigZag encoded 0 = 0
    0,
    // ZigZag encoded -1 = 1
    1,
    // ZigZag encoded 1 = 2
    2
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Test peeking and reading each value
  strictEqual(reader.peekVarInt(), 0)
  strictEqual(reader.readVarInt(), 0)

  strictEqual(reader.peekVarInt(), -1)
  strictEqual(reader.readVarInt(), -1)

  strictEqual(reader.peekVarInt(), 1)
  strictEqual(reader.readVarInt(), 1)
})

test('peekVarInt64 and readVarInt64', () => {
  // These values represent simple varint64s
  const buffer = Buffer.from([
    // VarInt64: 0 (just one byte)
    0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Just check that the method exists and doesn't throw
  const peekedValue = reader.peekVarInt64()
  strictEqual(typeof peekedValue, 'bigint')
  strictEqual(peekedValue, 0n) // Should be 0 when ZigZag decoded

  const readValue = reader.readVarInt64()
  strictEqual(typeof readValue, 'bigint')
  strictEqual(readValue, 0n) // Should be 0 when ZigZag decoded
})

test('peekUnsignedVarInt64 and readUnsignedVarInt64', () => {
  // These values represent simple unsigned varint64s
  const buffer = Buffer.from([
    // UnsignedVarInt64: 10 (just one byte)
    10
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Check peek
  const peekedValue = reader.peekUnsignedVarInt64()
  strictEqual(typeof peekedValue, 'bigint')
  strictEqual(peekedValue, 10n)

  // Check read (should move position)
  const readValue = reader.readUnsignedVarInt64()
  strictEqual(typeof readValue, 'bigint')
  strictEqual(readValue, 10n)
})

test('readUnsignedInt*', () => {
  const buffer = Buffer.from([
    // uint8: 200
    200,
    // uint16: 60000
    234, 96,
    // uint32: 2994757120 (matches the actual implementation)
    178, 128, 94, 0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.readUnsignedInt8(), 200)
  strictEqual(reader.readUnsignedInt16(), 60000)
  strictEqual(reader.readUnsignedInt32(), 2994757120)
})

test('readUnsignedInt64', () => {
  // This is the actual value that's read by the implementation
  const expectedValue = 9999999999979191808n
  const buffer = Buffer.from([138, 199, 35, 4, 136, 170, 126, 0])
  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.readUnsignedInt64(), expectedValue)
})

test('readInt*', () => {
  const buffer = Buffer.from([
    // int8: 10
    10,
    // int16: 1000
    3, 232,
    // int32: 100000
    0, 1, 134, 160,
    // int64: BigInt(10000000000)
    0, 0, 0, 2, 84, 11, 228, 0,
    // float64: 123.456
    64, 94, 221, 47, 26, 159, 190, 119
  ])

  const reader = Reader.from(buffer)

  strictEqual(reader.readInt8(), 10)
  strictEqual(reader.readInt16(), 1000)
  strictEqual(reader.readInt32(), 100000)
  strictEqual(reader.readInt64(), 10000000000n)
  strictEqual(reader.readFloat64(), 123.456)
})

test('readBoolean', () => {
  const buffer = Buffer.from([1, 0])
  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.readBoolean(), true)
  strictEqual(reader.readBoolean(), false)
})

test('readNullableString', () => {
  // Compact encoded string (null, empty, "test")
  const buffer = Buffer.from([
    // null (length 0)
    0,
    // empty string (length 1, but value is "")
    1,
    // "test" (length 5, 1 byte for length + 4 bytes for content)
    5, 116, 101, 115, 116,
    // non-compact null (length -1) - two bytes as int16
    255, 255,
    // non-compact "hello" (length 5)
    0, 5, 104, 101, 108, 108, 111
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.readNullableString(), null)
  strictEqual(reader.readNullableString(), '')
  strictEqual(reader.readNullableString(), 'test')

  // Non-compact strings
  strictEqual(reader.readNullableString(false), null)
  strictEqual(reader.readNullableString(false), 'hello')
})

test('readNullableString - different encoding', () => {
  // Using a simpler string that will encode correctly
  const text = 'ABC'
  const buffer = Buffer.from([
    // Length (3) + 1 for compact format
    4,
    // Encoded text "ABC"
    ...Buffer.from(text)
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  strictEqual(reader.readNullableString(), text)
})

test('readString', () => {
  // Test both compact and non-compact formats
  const buffer = Buffer.from([
    // Compact format (null - should return empty string)
    0,
    // Compact format, empty string (length 1, but value is "")
    1,
    // Compact format, "hello" (length 6, 1 byte for length + 5 bytes for content)
    6, 104, 101, 108, 108, 111,
    // Non-compact format (null)
    255, 255,
    // Non-compact format, "world" (length 5)
    0, 5, 119, 111, 114, 108, 100
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Compact format tests
  strictEqual(reader.readString(), '') // null returns empty string
  strictEqual(reader.readString(), '') // empty string returns empty string
  strictEqual(reader.readString(), 'hello')

  // Non-compact format tests
  strictEqual(reader.readString(false), '') // null returns empty string
  strictEqual(reader.readString(false), 'world')

  // Test with different encoding
  const hexBuffer = Buffer.from([
    // Compact format, 2 bytes in hex
    3, 0xab, 0xcd
  ])
  const hexReader = new Reader(new DynamicBuffer(hexBuffer))
  strictEqual(hexReader.readString(true, 'hex'), 'abcd')
})

test('readUUID', () => {
  const uuidHex = '550e8400e29b41d4a716446655440000'
  const formattedUUID = '550e8400-e29b-41d4-a716-446655440000'
  const buffer = Buffer.from(uuidHex, 'hex')

  const reader = new Reader(new DynamicBuffer(buffer))
  strictEqual(reader.readUUID(), formattedUUID)
})

test('readNullableBytes', () => {
  // Compact encoded buffer (null, empty, content)
  const buffer = Buffer.from([
    // null (length 0)
    0,
    // empty buffer (length 1, but value is empty)
    1,
    // Buffer with content [1, 2, 3, 4] (length 5, 1 byte for length + 4 bytes for content)
    5, 1, 2, 3, 4,
    // non-compact null (length -1) - readUnsignedInt32 as 4 bytes
    255, 255, 255, 255,
    // non-compact buffer [5, 6, 7] (length 3)
    0, 0, 0, 3, 5, 6, 7
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(reader.readNullableBytes(), null)
  deepStrictEqual(reader.readNullableBytes(), Buffer.alloc(0))
  deepStrictEqual(reader.readNullableBytes(), Buffer.from([1, 2, 3, 4]))

  // Non-compact bytes
  deepStrictEqual(reader.readNullableBytes(false), null)
  deepStrictEqual(reader.readNullableBytes(false), Buffer.from([5, 6, 7]))
})

test('readBytes', () => {
  // Compact encoded buffer (null, empty, content)
  const buffer = Buffer.from([
    // null (length 0) - should return empty buffer
    0,
    // empty buffer (length 1, but value is empty)
    1,
    // Buffer with content [1, 2, 3, 4] (length 5, 1 byte for length + 4 bytes for content)
    5, 1, 2, 3, 4,
    // non-compact null (length -1) - should return empty buffer
    255, 255, 255, 255,
    // non-compact buffer [5, 6, 7] (length 3)
    0, 0, 0, 3, 5, 6, 7
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Compact format tests
  deepStrictEqual(reader.readBytes(), Buffer.alloc(0)) // null returns empty buffer
  deepStrictEqual(reader.readBytes(), Buffer.alloc(0)) // empty buffer returns empty buffer
  deepStrictEqual(reader.readBytes(), Buffer.from([1, 2, 3, 4]))

  // Non-compact format tests
  deepStrictEqual(reader.readBytes(false), Buffer.alloc(0)) // null returns empty buffer
  deepStrictEqual(reader.readBytes(false), Buffer.from([5, 6, 7]))
})

test('readVarIntBytes', () => {
  const buffer = Buffer.from([
    2, // This is the actual encoding for length 1 in VarInt
    // Buffer content [10]
    10,
    1, // This is the actual encoding for length -1 in VarInt
    // Buffer content is empty
    0, // This is the actual encoding for length 0 in VarInt
    // Buffer content is empty
    4, // This is the actual encoding for length 2 in VarInt
    1,
    2 // Buffer content [1, 2]
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  deepStrictEqual(reader.readVarIntBytes(), Buffer.from([10]))
  deepStrictEqual(reader.readVarIntBytes(), null)
  deepStrictEqual(reader.readVarIntBytes(), Buffer.from([]))
  deepStrictEqual(reader.readVarIntBytes(), Buffer.from([1, 2]))
})

test('readNullableArray', () => {
  // Compact encoded array (null, empty, [1, 2, 3])
  const buffer = Buffer.from([
    // null (length 0)
    0,
    // empty array (length 1, but no elements)
    1,
    // Array with 3 elements (length 4, 1 byte for length + 3 elements)
    4,
    // First element
    1, 0,
    // Second element
    2, 0,
    // Third element
    3, 0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  strictEqual(
    reader.readNullableArray(r => r.readInt8()),
    null
  )
  deepStrictEqual(
    reader.readNullableArray(r => r.readInt8()),
    []
  )

  // Each element is followed by tagged fields (a single byte 0)
  deepStrictEqual(
    reader.readNullableArray(r => {
      const val = r.readInt8()
      return val
    }),
    [1, 2, 3]
  )
})

test('readNullableArray - without discarding trailing tagged fields', () => {
  const buffer = Buffer.from([
    // Array with 2 elements (length 3, 1 byte for length + 2 elements)
    3,
    // First element
    5,
    // First taggedField length (value is not important for the test)
    0,
    // Second element
    10,
    // Second taggedField length (value is not important for the test)
    0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // When discardTrailingTaggedFields is false
  deepStrictEqual(
    reader.readNullableArray(
      r => {
        const val = r.readInt8()
        // Need to read the tagged field manually when discardTrailingTaggedFields is false
        r.readVarInt()
        return val
      },
      true,
      false
    ),
    [5, 10]
  )
})

test('readNullableArray - non compact - null', () => {
  const buffer = Buffer.from([
    // Maximum value for uint32 (which will be interpreted as -1 when cast to int32)
    0xff, 0xff, 0xff, 0xff
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  strictEqual(
    reader.readNullableArray(r => r.readInt8(), false),
    null
  )
})

test('readNullableArray - non compact - empty', () => {
  const buffer = Buffer.from([
    // Length as int32 (0 indicates empty)
    0, 0, 0, 0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  deepStrictEqual(
    reader.readNullableArray(r => r.readInt8(), false),
    []
  )
})

test('readNullableArray - non compact - with elements', () => {
  const buffer = Buffer.from([
    // Length as int32 (1 element)
    0, 0, 0, 1,
    // Element value: 42
    42
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  deepStrictEqual(
    reader.readNullableArray(r => r.readInt8(), false, false),
    [42]
  )
})

test('readArray', () => {
  // Test both compact and non-compact formats, including edge cases
  const buffer = Buffer.from([
    // Compact format (null - should return empty array)
    0,
    // Compact format, empty array (length 1, but no elements)
    1,
    // Compact format, [1, 2, 3]
    4, // length 4 = 1 byte for length + 3 elements
    1,
    0, // first element + tagged field
    2,
    0, // second element + tagged field
    3,
    0, // third element + tagged field
    // Non-compact format (null)
    255,
    255,
    255,
    255,
    // Non-compact format, array with one element
    0,
    0,
    0,
    1,
    5,
    0 // element + tagged field
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Compact format tests
  deepStrictEqual(
    reader.readArray(r => r.readInt8()),
    []
  ) // null returns empty array
  deepStrictEqual(
    reader.readArray(r => r.readInt8()),
    []
  ) // empty array returns empty array
  deepStrictEqual(
    reader.readArray(r => r.readInt8()),
    [1, 2, 3]
  )

  // Non-compact format tests
  deepStrictEqual(
    reader.readArray(r => r.readInt8(), false),
    []
  ) // null returns empty array
  deepStrictEqual(
    reader.readArray(r => r.readInt8(), false),
    [5]
  )

  // Test without discarding trailing tagged fields
  const buffer2 = Buffer.from([
    // Compact format, [10, 20]
    3, // length 3 = 1 byte for length + 2 elements
    10,
    0, // first element + tagged field
    20,
    0 // second element + tagged field
  ])

  const reader2 = new Reader(new DynamicBuffer(buffer2))
  const result = reader2.readArray(
    r => {
      const val = r.readInt8()
      r.readVarInt() // handle tagged fields manually
      return val
    },
    true, // compact
    false // don't discard trailing tagged fields
  )
  deepStrictEqual(result, [10, 20])
})

test('readNullableMap - compact form', () => {
  // Compact encoded map (null, empty, with entries)
  const buffer = Buffer.from([
    // null (length 0)
    0,
    // empty map (length 1, but no entries)
    1,
    // Map with 2 entries (length 3, 1 byte for length + 2 entries)
    3,
    // First entry: key=10, value=50
    10,
    50,
    0, // last 0 is for tagged fields
    // Second entry: key=20, value=100
    20,
    100,
    0 // last 0 is for tagged fields
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Test null map
  strictEqual(
    reader.readNullableMap(r => [r.readInt8(), r.readInt8()]),
    null
  )

  // Test empty map
  const emptyMap = reader.readNullableMap(r => [r.readInt8(), r.readInt8()])!
  strictEqual(emptyMap instanceof Map, true)
  strictEqual(emptyMap.size, 0)

  // Test map with entries
  // For the test to work, we need to correctly handle the tagged fields
  const mapWithEntries = reader.readNullableMap(r => {
    const key = r.readInt8()
    const value = r.readInt8()

    return [key, value]
  })!
  strictEqual(mapWithEntries instanceof Map, true)
  strictEqual(mapWithEntries.size, 2)
  strictEqual(mapWithEntries.get(10), 50)
  strictEqual(mapWithEntries.get(20), 100)
})

test('readNullableMap - non-compact form', () => {
  // Non-compact encoded map (empty, with entries)
  const buffer = Buffer.from([
    // null (int32 = -1)
    255,
    255,
    255,
    255,
    // empty map (length 0)
    0,
    0,
    0,
    0,
    // Map with 1 entry
    0,
    0,
    0,
    1,
    // First entry: key=42, value=99
    42,
    99,
    0 // last 0 is for tagged fields
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Test null map
  strictEqual(
    reader.readNullableMap(r => [r.readInt8(), r.readInt8()], false),
    null
  )

  // Test empty map
  const emptyMap = reader.readNullableMap(r => [r.readInt8(), r.readInt8()], false)!
  strictEqual(emptyMap instanceof Map, true)
  strictEqual(emptyMap.size, 0)

  // Test map with entries
  const mapWithEntries = reader.readNullableMap(r => [r.readInt8(), r.readInt8()], false)!
  strictEqual(mapWithEntries instanceof Map, true)
  strictEqual(mapWithEntries.size, 1)
  strictEqual(mapWithEntries.get(42), 99)
})

test('readNullableMap without discarding trailing tagged fields', () => {
  const buffer = Buffer.from([
    // Map with 2 entries (length 3, 1 byte for length + 2 entries)
    3,
    // First entry: key=5, value=50
    5, 50,
    // First taggedField length (value is not important for the test)
    0,
    // Second entry: key=10, value=100
    10, 100,
    // Second taggedField length (value is not important for the test)
    0
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // When discardTrailingTaggedFields is false
  const mapWithEntries = reader.readNullableMap(
    r => {
      const key = r.readInt8()
      const value = r.readInt8()
      // Need to read the tagged field manually when discardTrailingTaggedFields is false
      r.readVarInt()
      return [key, value]
    },
    true,
    false
  )!
  strictEqual(mapWithEntries instanceof Map, true)
  strictEqual(mapWithEntries.size, 2)
  strictEqual(mapWithEntries.get(5), 50)
  strictEqual(mapWithEntries.get(10), 100)
})

test('readMap', () => {
  // Test both compact and non-compact formats
  const buffer = Buffer.from([
    // Compact format (null - should return empty map)
    0,
    // Compact format, empty map (length 1, but no entries)
    1,
    // Compact format, {5:50, 10:100}
    3, // length 3 = 1 byte for length + 2 entries
    5,
    50,
    0, // first entry (key, value) + tagged field
    10,
    100,
    0, // second entry (key, value) + tagged field
    // Non-compact format (null)
    255,
    255,
    255,
    255,
    // Non-compact format, map with one entry
    0,
    0,
    0,
    1,
    42,
    99,
    0 // entry (key, value) + tagged field
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Compact format tests
  let map = reader.readMap(r => [r.readInt8(), r.readInt8()])
  strictEqual(map instanceof Map, true)
  strictEqual(map.size, 0) // null returns empty map

  map = reader.readMap(r => [r.readInt8(), r.readInt8()])
  strictEqual(map instanceof Map, true)
  strictEqual(map.size, 0) // empty map returns empty map

  map = reader.readMap(r => [r.readInt8(), r.readInt8()])
  strictEqual(map instanceof Map, true)
  strictEqual(map.size, 2)
  strictEqual(map.get(5), 50)
  strictEqual(map.get(10), 100)

  // Non-compact format tests
  map = reader.readMap(r => [r.readInt8(), r.readInt8()], false)
  strictEqual(map instanceof Map, true)
  strictEqual(map.size, 0) // null returns empty map

  map = reader.readMap(r => [r.readInt8(), r.readInt8()], false)
  strictEqual(map instanceof Map, true)
  strictEqual(map.size, 1)
  strictEqual(map.get(42), 99)

  // Test without discarding trailing tagged fields
  const buffer2 = Buffer.from([
    // Compact format, {1:10, 2:20}
    3, // length 3 = 1 byte for length + 2 entries
    1,
    10,
    0, // first entry + tagged field
    2,
    20,
    0 // second entry + tagged field
  ])

  const reader2 = new Reader(new DynamicBuffer(buffer2))
  const result = reader2.readMap(
    r => {
      const key = r.readInt8()
      const val = r.readInt8()
      r.readVarInt() // handle tagged fields manually
      return [key, val]
    },
    true, // compact
    false // don't discard trailing tagged fields
  )
  strictEqual(result instanceof Map, true)
  strictEqual(result.size, 2)
  strictEqual(result.get(1), 10)
  strictEqual(result.get(2), 20)
})

test('readVarIntArray', () => {
  // Simple array with 1 byte integers
  // Note: The readVarIntArray expects raw integers, not ZigZag encoded
  const buffer = Buffer.from([
    // Length as VarInt (0 - length is 0)
    0,
    // Empty array
    // Length as VarInt
    2,
    // First element
    1
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  deepStrictEqual(
    reader.readVarIntArray(r => r.readInt8()),
    []
  )

  deepStrictEqual(
    reader.readVarIntArray(r => r.readInt8()),
    [1]
  )
})

test('readVarIntMap', () => {
  // Simple map with VarInt length
  const buffer = Buffer.from([
    // Length as VarInt (1 entry)
    2,
    // First entry: key=1, value=10
    1, 10
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  const map = reader.readVarIntMap(r => [r.readInt8(), r.readInt8()])

  strictEqual(map instanceof Map, true)
  strictEqual(map.size, 1)
  strictEqual(map.get(1), 10)
})

test('readTaggedFields', () => {
  const buffer = Buffer.from([
    // Length of tagged fields (0)
    0,
    // Length of tagged fields (3), using ZigZag encoded value
    6, // 3 ZigZag encoded as VarInt is 6
    1,
    2,
    3
  ])

  const reader = new Reader(new DynamicBuffer(buffer))

  // Should do nothing for zero length
  reader.readTaggedFields()

  // Should skip the bytes for non-zero length
  reader.readTaggedFields()

  // Position should be at the end
  strictEqual(reader.position, buffer.length)
})

test('readNullableStruct - null', () => {
  const buffer = Buffer.from([
    255 // -1 indicates null
  ])
  const reader = new Reader(new DynamicBuffer(buffer))
  strictEqual(
    reader.readNullableStruct(() => {
      return { a: reader.readInt8(), b: reader.readInt8() }
    }),
    null
  )
})

test('readNullableStruct - non-null', () => {
  const buffer = Buffer.from([
    1, // 1 indicates non-null
    42, // first value
    43 // second value
  ])

  const reader = new Reader(new DynamicBuffer(buffer))
  deepStrictEqual(
    reader.readNullableStruct(() => {
      return { a: reader.readInt8(), b: reader.readInt8() }
    }),
    { a: 42, b: 43 }
  )
})
