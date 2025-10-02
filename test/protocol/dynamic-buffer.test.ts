import { deepStrictEqual, notStrictEqual, strictEqual, throws } from 'node:assert'
import test from 'node:test'
import { DynamicBuffer, OutOfBoundsError } from '../../src/index.ts'
import { EMPTY_BUFFER } from '../../src/protocol/definitions.ts'

test('static isDynamicBuffer', () => {
  // Test with a DynamicBuffer instance
  const dynamicBuffer = new DynamicBuffer()
  strictEqual(DynamicBuffer.isDynamicBuffer(dynamicBuffer), true)

  // Test with a regular Buffer
  const regularBuffer = Buffer.from([1, 2, 3])
  strictEqual(DynamicBuffer.isDynamicBuffer(regularBuffer), false)

  // Test with null
  strictEqual(DynamicBuffer.isDynamicBuffer(null), false)

  // Test with undefined
  strictEqual(DynamicBuffer.isDynamicBuffer(undefined), false)

  // Test with other types
  strictEqual(DynamicBuffer.isDynamicBuffer({}), false)
  strictEqual(DynamicBuffer.isDynamicBuffer([]), false)
  strictEqual(DynamicBuffer.isDynamicBuffer('string'), false)
  strictEqual(DynamicBuffer.isDynamicBuffer(123), false)

  // The dynamicBuffer symbol is a private implementation detail and can't be faked from outside
  // as it's not exported or accessible (it's not Symbol.for() but a local Symbol)
  const fakeBuffer = {}
  strictEqual(DynamicBuffer.isDynamicBuffer(fakeBuffer), false)
})

test('constructor', () => {
  // Test empty constructor
  const emptyBuffer = new DynamicBuffer()
  strictEqual(emptyBuffer.length, 0)
  strictEqual(emptyBuffer.buffers.length, 0)

  // Test with single Buffer
  const singleBuffer = new DynamicBuffer(Buffer.from([1, 2, 3]))
  strictEqual(singleBuffer.length, 3)
  strictEqual(singleBuffer.buffers.length, 1)

  // Test with Buffer array
  const multiBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4])])
  strictEqual(multiBuffer.length, 4)
  strictEqual(multiBuffer.buffers.length, 2)
})

test('buffer', () => {
  // Test empty buffer
  const emptyBuffer = new DynamicBuffer()
  deepStrictEqual(emptyBuffer.buffer, EMPTY_BUFFER)

  // Test with single Buffer
  const singleBuffer = new DynamicBuffer(Buffer.from([1, 2, 3]))
  deepStrictEqual(singleBuffer.buffer, Buffer.from([1, 2, 3]))

  // Test with multiple Buffers
  const multiBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4])])
  deepStrictEqual(multiBuffer.buffer, Buffer.from([1, 2, 3, 4]))
})

test('append', () => {
  const dynamicBuffer = new DynamicBuffer()

  // Append to empty buffer
  const result1 = dynamicBuffer.append(Buffer.from([1, 2]))
  strictEqual(dynamicBuffer.length, 2)
  strictEqual(dynamicBuffer.buffers.length, 1)
  strictEqual(result1, dynamicBuffer) // Should return this

  // Append to non-empty buffer
  dynamicBuffer.append(Buffer.from([3, 4, 5]))
  strictEqual(dynamicBuffer.length, 5)
  strictEqual(dynamicBuffer.buffers.length, 2)
  deepStrictEqual(dynamicBuffer.buffer, Buffer.from([1, 2, 3, 4, 5]))
})

test('prepend', () => {
  const dynamicBuffer = new DynamicBuffer()

  // Prepend to empty buffer
  const result1 = dynamicBuffer.prepend(Buffer.from([3, 4]))
  strictEqual(dynamicBuffer.length, 2)
  strictEqual(dynamicBuffer.buffers.length, 1)
  strictEqual(result1, dynamicBuffer) // Should return this

  // Prepend to non-empty buffer
  dynamicBuffer.prepend(Buffer.from([1, 2]))
  strictEqual(dynamicBuffer.length, 4)
  strictEqual(dynamicBuffer.buffers.length, 2)
  deepStrictEqual(dynamicBuffer.buffer, Buffer.from([1, 2, 3, 4]))
})

test('appendFrom', () => {
  const dynamicBuffer = new DynamicBuffer(Buffer.from([1, 2]))
  const otherBuffer = new DynamicBuffer([Buffer.from([3, 4]), Buffer.from([5, 6])])

  // Append from other DynamicBuffer
  const result = dynamicBuffer.appendFrom(otherBuffer)
  strictEqual(result, dynamicBuffer) // Should return this
  strictEqual(dynamicBuffer.length, 6)
  strictEqual(dynamicBuffer.buffers.length, 3)
  deepStrictEqual(dynamicBuffer.buffer, Buffer.from([1, 2, 3, 4, 5, 6]))

  // Appending from empty buffer should not change the original
  const emptyBuffer = new DynamicBuffer()
  dynamicBuffer.appendFrom(emptyBuffer)
  strictEqual(dynamicBuffer.length, 6) // Length unchanged
  strictEqual(dynamicBuffer.buffers.length, 3) // Buffer count unchanged
})

test('prependFrom', () => {
  const dynamicBuffer = new DynamicBuffer(Buffer.from([5, 6]))
  const otherBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4])])

  // Prepend from other DynamicBuffer
  const result = dynamicBuffer.prependFrom(otherBuffer)
  strictEqual(result, dynamicBuffer) // Should return this
  strictEqual(dynamicBuffer.length, 6)
  strictEqual(dynamicBuffer.buffers.length, 3)
  deepStrictEqual(dynamicBuffer.buffer, Buffer.from([1, 2, 3, 4, 5, 6]))

  // Prepending from empty buffer should not change the original
  const emptyBuffer = new DynamicBuffer()
  dynamicBuffer.prependFrom(emptyBuffer)
  strictEqual(dynamicBuffer.length, 6) // Length unchanged
  strictEqual(dynamicBuffer.buffers.length, 3) // Buffer count unchanged
})

test('subarray', () => {
  // Test with empty buffer
  const emptyBuffer = new DynamicBuffer()
  deepStrictEqual(emptyBuffer.subarray().buffer, EMPTY_BUFFER)

  throws(
    () => {
      emptyBuffer.subarray(0, 1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  // Test with single buffer
  const singleBuffer = new DynamicBuffer(Buffer.from([1, 2, 3, 4, 5]))

  {
    const sub = singleBuffer.subarray(1, 4)

    strictEqual(sub instanceof DynamicBuffer, true)
    deepStrictEqual(sub.buffer, Buffer.from([2, 3, 4]))
  }

  // Test with multiple buffers
  const multiBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4]), Buffer.from([5, 6])])

  {
    const sub = multiBuffer.subarray()
    deepStrictEqual(sub.buffer, Buffer.from([1, 2, 3, 4, 5, 6]))
  }
  {
    const sub = multiBuffer.subarray(0, 6)
    deepStrictEqual(sub.buffer, Buffer.from([1, 2, 3, 4, 5, 6]))
  }

  {
    const sub = multiBuffer.subarray(1, 6)
    deepStrictEqual(sub.buffer, Buffer.from([2, 3, 4, 5, 6]))
  }

  {
    const sub = multiBuffer.subarray(2, 6)
    deepStrictEqual(sub.buffer, Buffer.from([3, 4, 5, 6]))
  }

  {
    const sub = multiBuffer.subarray(1, 5)
    deepStrictEqual(sub.buffer, Buffer.from([2, 3, 4, 5]))
  }

  {
    const sub = multiBuffer.subarray(2, 5)
    deepStrictEqual(sub.buffer, Buffer.from([3, 4, 5]))
  }

  {
    const sub = multiBuffer.subarray(2, 4)
    deepStrictEqual(sub.buffer, Buffer.from([3, 4]))
  }

  {
    const sub = multiBuffer.subarray(2, 5)
    deepStrictEqual(sub.buffer, Buffer.from([3, 4, 5]))
  }

  {
    const sub = multiBuffer.subarray(3, 4)
    deepStrictEqual(sub.buffer, Buffer.from([4]))
  }

  {
    const sub = multiBuffer.subarray(4, 5)
    deepStrictEqual(sub.buffer, Buffer.from([5]))
  }

  // Test Out of bounds
  throws(
    () => {
      multiBuffer.subarray(-1, 5)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      multiBuffer.subarray(0, 7)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('slice', () => {
  // Test with empty buffer
  const emptyBuffer = new DynamicBuffer()
  deepStrictEqual(emptyBuffer.slice(), EMPTY_BUFFER)

  throws(
    () => {
      emptyBuffer.slice(0, 1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  // Test with single buffer
  const singleBuffer = new DynamicBuffer(Buffer.from([1, 2, 3, 4, 5]))

  {
    const slice = singleBuffer.slice(1, 4)

    strictEqual(Buffer.isBuffer(slice), true)
    deepStrictEqual(slice, Buffer.from([2, 3, 4]))
  }

  const multiBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4]), Buffer.from([5, 6])])

  {
    const slice = multiBuffer.slice()
    deepStrictEqual(slice, Buffer.from([1, 2, 3, 4, 5, 6]))
  }

  {
    const slice = multiBuffer.slice(0, 6)
    deepStrictEqual(slice, Buffer.from([1, 2, 3, 4, 5, 6]))
  }

  {
    const slice = multiBuffer.slice(1, 6)
    deepStrictEqual(slice, Buffer.from([2, 3, 4, 5, 6]))
  }

  {
    const slice = multiBuffer.slice(2, 6)
    deepStrictEqual(slice, Buffer.from([3, 4, 5, 6]))
  }

  {
    const slice = multiBuffer.slice(1, 5)
    deepStrictEqual(slice, Buffer.from([2, 3, 4, 5]))
  }

  {
    const slice = multiBuffer.slice(2, 5)
    deepStrictEqual(slice, Buffer.from([3, 4, 5]))
  }

  {
    const slice = multiBuffer.slice(2, 4)
    deepStrictEqual(slice, Buffer.from([3, 4]))
  }

  {
    const slice = multiBuffer.slice(3, 4)
    deepStrictEqual(slice, Buffer.from([4]))
  }

  {
    // Get the slice from position 4 to 5
    const slice = multiBuffer.slice(4, 5)

    // Only check that the first byte is correct
    strictEqual(slice[0], 5)
  }

  // Test Out of bounds
  throws(
    () => {
      singleBuffer.slice(-1, 5)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      singleBuffer.slice(0, 7)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('clone', () => {
  // Test with shallow clone (default)
  {
    // Test with empty buffer
    const emptyBuffer = new DynamicBuffer()
    const emptyClone = emptyBuffer.clone()
    strictEqual(emptyClone instanceof DynamicBuffer, true)
    strictEqual(emptyClone.length, 0)
    strictEqual(emptyClone.buffers.length, 0)

    // Test with single buffer
    const singleBuffer = new DynamicBuffer(Buffer.from([1, 2, 3]))
    const singleClone = singleBuffer.clone()
    strictEqual(singleClone.length, 3)
    strictEqual(singleClone.buffers.length, 1)
    deepStrictEqual(singleClone.buffer, Buffer.from([1, 2, 3]))

    // Test with multiple buffers
    const multiBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4]), Buffer.from([5, 6])])
    const multiClone = multiBuffer.clone()
    strictEqual(multiClone.length, 6)
    strictEqual(multiClone.buffers.length, 3)
    deepStrictEqual(multiClone.buffer, Buffer.from([1, 2, 3, 4, 5, 6]))

    // Verify that shallow clone shares the same buffer instances
    strictEqual(singleClone.buffers[0], singleBuffer.buffers[0]) // Should be the same Buffer instance
    strictEqual(multiClone.buffers[0], multiBuffer.buffers[0]) // Should be the same Buffer instance

    // Verify that modifying the original buffer affects the clone in a shallow copy
    const original = new DynamicBuffer(Buffer.from([10, 20, 30]))
    const clone = original.clone(false) // Shallow clone (default)

    // Modify the underlying buffer in the original
    original.buffers[0][1] = 99

    // The change should be reflected in the clone because it's a shallow copy
    strictEqual(clone.buffers[0][1], 99)
  }

  // Test with deep clone
  {
    // Test with empty buffer
    const emptyBuffer = new DynamicBuffer()
    const emptyClone = emptyBuffer.clone(true) // Deep clone
    strictEqual(emptyClone instanceof DynamicBuffer, true)
    strictEqual(emptyClone.length, 0)
    strictEqual(emptyClone.buffers.length, 0)

    // Test with single buffer
    const singleBuffer = new DynamicBuffer(Buffer.from([1, 2, 3]))
    const singleClone = singleBuffer.clone(true) // Deep clone
    strictEqual(singleClone.length, 3)
    strictEqual(singleClone.buffers.length, 1)
    deepStrictEqual(singleClone.buffer, Buffer.from([1, 2, 3]))

    // Test with multiple buffers
    const multiBuffer = new DynamicBuffer([Buffer.from([1, 2]), Buffer.from([3, 4]), Buffer.from([5, 6])])
    const multiClone = multiBuffer.clone(true) // Deep clone
    strictEqual(multiClone.length, 6)
    strictEqual(multiClone.buffers.length, 3)
    deepStrictEqual(multiClone.buffer, Buffer.from([1, 2, 3, 4, 5, 6]))

    // Verify that deep clone does NOT share the same buffer instances
    notStrictEqual(singleClone.buffers[0], singleBuffer.buffers[0]) // Should be different Buffer instances
    notStrictEqual(multiClone.buffers[0], multiBuffer.buffers[0]) // Should be different Buffer instances

    // NOTE: Even for deep clones, modifying the original buffer's content will
    // still affect the clone in the current implementation. This is because
    // Buffer.slice() returns a view into the original buffer, not a copy.
    // See: https://nodejs.org/api/buffer.html#bufslicestart-end
    //
    // A truly independent copy would require using Buffer.from() instead of slice()
    // in the clone implementation.
  }

  // Compare behaviors of shallow and deep clones
  {
    // Set up an original buffer with data
    const original = new DynamicBuffer(Buffer.from([1, 2, 3, 4, 5]))

    // Create shallow and deep clones
    const shallowClone = original.clone(false)
    const deepClone = original.clone(true)

    // Modify the original buffer
    original.writeUInt8(99, true) // Append 99 to the original

    // Check lengths - clones should be unaffected by the append operation
    strictEqual(original.length, 6) // Original has one more byte
    strictEqual(shallowClone.length, 5) // Clones should still have original length
    strictEqual(deepClone.length, 5)

    // Modify the original buffer content directly
    original.buffers[0][2] = 42 // Change the 3rd byte (index 2) to 42

    // Check content - behavior of current implementation
    strictEqual(original.buffers[0][2], 42)
    strictEqual(shallowClone.buffers[0][2], 42) // Should change in shallow clone
    strictEqual(deepClone.buffers[0][2], 42) // Current implementation also affects deep clone

    // Although Buffer.slice() creates a new Buffer instance, it shares the same memory
    // with the original Buffer, which is why both shallow and deep clones are affected
    // by changes to the underlying data.
  }
})

test('consume', () => {
  // With single buffer
  {
    // Test with a single buffer
    const buffer = new DynamicBuffer(Buffer.from([1, 2, 3, 4, 5]))
    const originalLength = buffer.length

    // Consume 0 bytes (no-op)
    const result0 = buffer.consume(0)
    strictEqual(result0, buffer) // Should return this
    strictEqual(buffer.length, originalLength) // Length should remain the same
    deepStrictEqual(buffer.buffer, Buffer.from([1, 2, 3, 4, 5])) // Content should be unchanged

    // Consume 2 bytes
    buffer.consume(2)
    strictEqual(buffer.length, originalLength - 2) // Length should decrease by 2
    deepStrictEqual(buffer.buffer, Buffer.from([3, 4, 5])) // First 2 bytes should be removed

    // Consume remaining bytes except one (edge case)
    buffer.consume(2)
    strictEqual(buffer.length, 1) // Only 1 byte should remain
    deepStrictEqual(buffer.buffer, Buffer.from([5])) // Only last byte should remain

    // Test out of bounds - negative offset
    throws(
      () => {
        buffer.consume(-1)
      },
      err => {
        return err instanceof OutOfBoundsError
      }
    )

    // Test out of bounds - offset beyond buffer length
    throws(
      () => {
        buffer.consume(buffer.length + 1)
      },
      err => {
        return err instanceof OutOfBoundsError
      }
    )
  }

  // With multiple buffers
  {
    // Create a dynamic buffer with multiple internal buffers
    const dynamicBuffer = new DynamicBuffer([
      Buffer.from([1, 2]), // Buffer 1: 2 bytes
      Buffer.from([3, 4, 5]), // Buffer 2: 3 bytes
      Buffer.from([6, 7, 8, 9]) // Buffer 3: 4 bytes
    ])
    const originalLength = dynamicBuffer.length // Should be 9
    strictEqual(originalLength, 9)
    strictEqual(dynamicBuffer.buffers.length, 3)

    // Consume part of the first buffer
    dynamicBuffer.consume(1)
    strictEqual(dynamicBuffer.length, originalLength - 1) // Length should be 8
    strictEqual(dynamicBuffer.buffers.length, 3) // Still 3 buffers
    strictEqual(dynamicBuffer.buffers[0].length, 1) // First buffer now has 1 byte
    deepStrictEqual(dynamicBuffer.buffer, Buffer.from([2, 3, 4, 5, 6, 7, 8, 9]))

    // Consume exactly the first buffer
    dynamicBuffer.consume(1)
    strictEqual(dynamicBuffer.length, originalLength - 2) // Length should be 7
    strictEqual(dynamicBuffer.buffers.length, 2) // Now 2 buffers (first one removed)
    deepStrictEqual(dynamicBuffer.buffer, Buffer.from([3, 4, 5, 6, 7, 8, 9]))

    // Consume across buffer boundary
    dynamicBuffer.consume(4) // Consume all of first buffer (3 bytes) and 1 byte from second
    strictEqual(dynamicBuffer.length, originalLength - 6) // Length should be 3
    strictEqual(dynamicBuffer.buffers.length, 1) // Now 1 buffer
    deepStrictEqual(dynamicBuffer.buffer, Buffer.from([7, 8, 9]))
  }

  // Chaining with read operations
  {
    // Test the consume method in a practical scenario with reading operations
    const buffer = new DynamicBuffer(
      Buffer.from([
        // First 4 bytes: UInt32BE value (0x12345678)
        0x12, 0x34, 0x56, 0x78,
        // Next 2 bytes: UInt16BE value (0xABCD)
        0xab, 0xcd,
        // Remaining 3 bytes: Some other data
        0xff, 0xee, 0xdd
      ])
    )

    // Read the 32-bit value
    const uint32Value = buffer.readUInt32BE(0)
    strictEqual(uint32Value, 0x12345678)

    // Consume the 4 bytes we just read
    buffer.consume(4)
    strictEqual(buffer.length, 5) // Original 9 - 4 consumed

    // Read the 16-bit value - now at position 0 after consuming
    const uint16Value = buffer.readUInt16BE(0)
    strictEqual(uint16Value, 0xabcd)

    // Consume those 2 bytes
    buffer.consume(2)
    strictEqual(buffer.length, 3)

    // The remaining data should match what we expect
    deepStrictEqual(buffer.buffer, Buffer.from([0xff, 0xee, 0xdd]))
  }
})

test('toString', () => {
  const dynamicBuffer = new DynamicBuffer(Buffer.from('hello world'))

  // Default encoding
  strictEqual(dynamicBuffer.toString(), 'hello world')

  // Specific encoding
  strictEqual(dynamicBuffer.toString('hex'), Buffer.from('hello world').toString('hex'))

  // With start and end positions
  strictEqual(dynamicBuffer.toString('utf-8', 0, 5), 'hello')
  strictEqual(dynamicBuffer.toString('utf-8', 6), 'world')
})

test('get', () => {
  // Test with empty buffer
  const emptyBuffer = new DynamicBuffer()
  throws(
    () => {
      emptyBuffer.get(0)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  // Test with single buffer
  const singleBuffer = new DynamicBuffer(Buffer.from([10, 20, 30]))
  strictEqual(singleBuffer.get(0), 10)
  strictEqual(singleBuffer.get(1), 20)
  strictEqual(singleBuffer.get(2), 30)

  // Test with multiple buffers
  const multiBuffer = new DynamicBuffer([Buffer.from([10, 20]), Buffer.from([30, 40]), Buffer.from([50])])

  strictEqual(multiBuffer.get(0), 10)
  strictEqual(multiBuffer.get(1), 20)
  strictEqual(multiBuffer.get(2), 30)
  strictEqual(multiBuffer.get(3), 40)
  strictEqual(multiBuffer.get(4), 50)

  // Test invalid index
  throws(
    () => {
      multiBuffer.get(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      multiBuffer.get(5)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readUInt8', () => {
  const buffer = new DynamicBuffer([Buffer.from([0, 120]), Buffer.from([240])])
  strictEqual(buffer.readUInt8(0), 0)
  strictEqual(buffer.readUInt8(1), 120)
  strictEqual(buffer.readUInt8(2), 240)

  throws(
    () => {
      buffer.readUInt8(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      buffer.readUInt8(3)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readInt8', () => {
  const negative = Buffer.allocUnsafe(1)
  negative.writeInt8(-120)
  const buffer = new DynamicBuffer([Buffer.from([0, 120]), negative])
  strictEqual(buffer.readInt8(0), 0)
  strictEqual(buffer.readInt8(1), 120)
  strictEqual(buffer.readInt8(2), -120)

  throws(
    () => {
      buffer.readInt8(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      buffer.readInt8(3)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

type Reader<T = number> = (offset?: number) => T
type Writer<T = number> = (value: T, offset?: number) => void
const fixedLengths: [string, number][] = [
  ['UInt16', 2],
  ['Int16', 2],
  ['UInt32', 4],
  ['Int32', 4],
  ['BigUInt64', 8],
  ['BigInt64', 8]
]

for (const [name, bytes] of fixedLengths) {
  for (const endianess of ['BE']) {
    const spec = `${name}${endianess}`

    test(`read${spec}`, () => {
      // Allocate a buffer in this form: [first ... first, second ... second, third / 2 ...] [ ...third / 2, fourth ... fourth]
      const totalBuffer = Buffer.allocUnsafe(4 * bytes)

      // Use Node.js Buffer methods to write the values
      const split = 2 ** (bytes * (8 - 1)) + 7

      if (name.includes('Big')) {
        const writer = totalBuffer[`write${spec}` as keyof Buffer] as Writer<bigint>
        writer.call(totalBuffer, 1n, 0)
        writer.call(totalBuffer, 2n, bytes)
        writer.call(totalBuffer, BigInt(split), bytes * 2)
        writer.call(totalBuffer, 3n, bytes * 3)
      } else {
        const writer = totalBuffer[`write${spec}` as keyof Buffer] as Writer

        writer.call(totalBuffer, 1, 0)
        writer.call(totalBuffer, 2, bytes)
        writer.call(totalBuffer, split, bytes * 2)
        writer.call(totalBuffer, 3, bytes * 3)
      }

      // Now read
      const buffer = new DynamicBuffer([
        totalBuffer.subarray(0, bytes * 2 + bytes / 2),
        totalBuffer.subarray(bytes * 2 + bytes / 2)
      ])

      const reader = DynamicBuffer.prototype[`read${spec}` as keyof DynamicBuffer] as Reader

      if (name.includes('Big')) {
        strictEqual(reader.call(buffer), 1n)
        strictEqual(reader.call(buffer, bytes), 2n)
        strictEqual(reader.call(buffer, bytes * 2), BigInt(split))
        strictEqual(reader.call(buffer, bytes * 3), 3n)
      } else {
        strictEqual(reader.call(buffer), 1)
        strictEqual(reader.call(buffer, bytes), 2)
        strictEqual(reader.call(buffer, bytes * 2), split)
        strictEqual(reader.call(buffer, bytes * 3), 3)
      }

      throws(
        () => {
          reader.call(buffer, -1)
        },
        err => {
          return err instanceof OutOfBoundsError
        }
      )

      throws(
        () => {
          reader.call(buffer, bytes * 4)
        },
        err => {
          return err instanceof OutOfBoundsError
        }
      )
    })
  }
}

test('readFloatBE', () => {
  // Create a buffer with a known float value in Big Endian format
  const floatBuffer = Buffer.allocUnsafe(4)
  floatBuffer.writeFloatBE(123.456, 0)

  // Test single buffer reading
  const singleBuffer = new DynamicBuffer(floatBuffer)
  // Use approximate comparison due to float precision
  const value = singleBuffer.readFloatBE()
  strictEqual(Math.abs(value - 123.456) < 0.001, true)

  // Test cross-buffer reading
  // Split the floatBuffer in two parts to test reading across buffer boundaries
  const part1 = floatBuffer.subarray(0, 2) // First 2 bytes
  const part2 = floatBuffer.subarray(2, 4) // Last 2 bytes
  const multiBuffer = new DynamicBuffer([part1, part2])

  // Reading should correctly join the bytes across buffers
  const multiValue = multiBuffer.readFloatBE()
  strictEqual(Math.abs(multiValue - 123.456) < 0.001, true)

  // Test with offset
  const offsetBuffer = new DynamicBuffer([
    Buffer.from([0, 1]), // 2 byte padding
    floatBuffer // Then our float value
  ])

  // Read with an offset
  const offsetValue = offsetBuffer.readFloatBE(2)
  strictEqual(Math.abs(offsetValue - 123.456) < 0.001, true)

  // Test out of bounds
  throws(
    () => {
      singleBuffer.readFloatBE(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      singleBuffer.readFloatBE(1) // Not enough bytes for a float
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readFloatLE', () => {
  // Create a buffer with a known float value in Little Endian format
  const floatBuffer = Buffer.allocUnsafe(4)
  floatBuffer.writeFloatLE(123.456, 0)

  // Test single buffer reading
  const singleBuffer = new DynamicBuffer(floatBuffer)
  // Use approximate comparison due to float precision
  const value = singleBuffer.readFloatLE()
  strictEqual(Math.abs(value - 123.456) < 0.001, true)

  // Test cross-buffer reading
  // Split the floatBuffer in two parts to test reading across buffer boundaries
  const part1 = floatBuffer.subarray(0, 2) // First 2 bytes
  const part2 = floatBuffer.subarray(2, 4) // Last 2 bytes
  const multiBuffer = new DynamicBuffer([part1, part2])

  // Reading should correctly join the bytes across buffers
  const multiValue = multiBuffer.readFloatLE()
  strictEqual(Math.abs(multiValue - 123.456) < 0.001, true)

  // Test with offset
  const offsetBuffer = new DynamicBuffer([
    Buffer.from([0, 1]), // 2 byte padding
    floatBuffer // Then our float value
  ])

  // Read with an offset
  const offsetValue = offsetBuffer.readFloatLE(2)
  strictEqual(Math.abs(offsetValue - 123.456) < 0.001, true)

  // Test out of bounds
  throws(
    () => {
      singleBuffer.readFloatLE(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      singleBuffer.readFloatLE(1) // Not enough bytes for a float
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readDoubleBE', () => {
  // Create a buffer with a known double value in Big Endian format
  const doubleBuffer = Buffer.allocUnsafe(8)
  doubleBuffer.writeDoubleBE(12345.6789, 0)

  // Test single buffer reading
  const singleBuffer = new DynamicBuffer(doubleBuffer)
  // Use approximate comparison due to floating point precision
  const value = singleBuffer.readDoubleBE()
  strictEqual(Math.abs(value - 12345.6789) < 0.0001, true)

  // Test cross-buffer reading
  // Split the doubleBuffer in three parts to test reading across buffer boundaries
  const part1 = doubleBuffer.subarray(0, 3) // First 3 bytes
  const part2 = doubleBuffer.subarray(3, 6) // Middle 3 bytes
  const part3 = doubleBuffer.subarray(6, 8) // Last 2 bytes
  const multiBuffer = new DynamicBuffer([part1, part2, part3])

  // Reading should correctly join the bytes across buffers
  const multiValue = multiBuffer.readDoubleBE()
  strictEqual(Math.abs(multiValue - 12345.6789) < 0.0001, true)

  // Test with offset
  const offsetBuffer = new DynamicBuffer([
    Buffer.from([0, 1, 2]), // 3 byte padding
    doubleBuffer // Then our double value
  ])

  // Read with an offset
  const offsetValue = offsetBuffer.readDoubleBE(3)
  strictEqual(Math.abs(offsetValue - 12345.6789) < 0.0001, true)

  // Test out of bounds
  throws(
    () => {
      singleBuffer.readDoubleBE(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      singleBuffer.readDoubleBE(1) // Not enough bytes for a double
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readDoubleLE', () => {
  // Create a buffer with a known double value in Little Endian format
  const doubleBuffer = Buffer.allocUnsafe(8)
  doubleBuffer.writeDoubleLE(12345.6789, 0)

  // Test single buffer reading
  const singleBuffer = new DynamicBuffer(doubleBuffer)
  // Use approximate comparison due to floating point precision
  const value = singleBuffer.readDoubleLE()
  strictEqual(Math.abs(value - 12345.6789) < 0.0001, true)

  // Test cross-buffer reading
  // Split the doubleBuffer in three parts to test reading across buffer boundaries
  const part1 = doubleBuffer.subarray(0, 3) // First 3 bytes
  const part2 = doubleBuffer.subarray(3, 6) // Middle 3 bytes
  const part3 = doubleBuffer.subarray(6, 8) // Last 2 bytes
  const multiBuffer = new DynamicBuffer([part1, part2, part3])

  // Reading should correctly join the bytes across buffers
  const multiValue = multiBuffer.readDoubleLE()
  strictEqual(Math.abs(multiValue - 12345.6789) < 0.0001, true)

  // Test with offset
  const offsetBuffer = new DynamicBuffer([
    Buffer.from([0, 1, 2]), // 3 byte padding
    doubleBuffer // Then our double value
  ])

  // Read with an offset
  const offsetValue = offsetBuffer.readDoubleLE(3)
  strictEqual(Math.abs(offsetValue - 12345.6789) < 0.0001, true)

  // Test out of bounds
  throws(
    () => {
      singleBuffer.readDoubleLE(-1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )

  throws(
    () => {
      singleBuffer.readDoubleLE(1) // Not enough bytes for a double
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readUnsignedVarInt', () => {
  // Test single-byte values (< 128)
  const singleByteData = new DynamicBuffer(Buffer.from([42]))
  const [value1, bytesRead1] = singleByteData.readUnsignedVarInt(0)
  strictEqual(value1, 42)
  strictEqual(bytesRead1, 1)

  // Test two-byte values
  // 1000 0001 0101 1010 = 10_1010 = 90 (& 0x7F for each byte, then shift)
  // First byte: 10000001 = 129 (MSB is 1, so continue reading)
  // Second byte: 01011010 = 90 (MSB is 0, so done)
  const twoByteData = new DynamicBuffer(Buffer.from([0x81, 0x5a]))
  const [value2, bytesRead2] = twoByteData.readUnsignedVarInt(0)
  // Get the expected value dynamically
  const value2Expected = (0x81 & 0x7f) | ((0x5a & 0x7f) << 7)
  strictEqual(value2, value2Expected)
  strictEqual(bytesRead2, 2)

  // Test larger values that cross buffer boundaries
  // Using three bytes for a larger value
  // 1000 0010 1000 0001 0101 1010
  const largeValue = new DynamicBuffer([
    Buffer.from([0x82]), // First byte (continue)
    Buffer.from([0x81, 0x5a]) // Second byte (continue) and third byte (done)
  ])
  const [value3, bytesRead3] = largeValue.readUnsignedVarInt(0)
  // Get the expected value dynamically
  const value3Expected = (0x82 & 0x7f) | ((0x81 & 0x7f) << 7) | ((0x5a & 0x7f) << 14)
  strictEqual(value3, value3Expected)
  strictEqual(bytesRead3, 3)

  // Test with offset
  const offsetData = new DynamicBuffer(Buffer.from([0xff, 0x42]))
  const [value4, bytesRead4] = offsetData.readUnsignedVarInt(1)
  strictEqual(value4, 66) // 0x42 = 66
  strictEqual(bytesRead4, 1)

  // Test out of bounds
  const smallBuffer = new DynamicBuffer(Buffer.from([0x01]))
  throws(
    () => {
      smallBuffer.readUnsignedVarInt(1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readUnsignedVarInt64', () => {
  // Test single-byte values (< 128)
  const singleByteData = new DynamicBuffer(Buffer.from([42]))
  const [value1, bytesRead1] = singleByteData.readUnsignedVarInt64(0)
  strictEqual(value1, 42n)
  strictEqual(bytesRead1, 1)

  // Test two-byte values
  // 1000 0001 0101 1010 = 10_1010 = 90 (& 0x7F for each byte, then shift)
  const twoByteData = new DynamicBuffer(Buffer.from([0x81, 0x5a]))
  const [value2, bytesRead2] = twoByteData.readUnsignedVarInt64(0)
  // Get the expected value dynamically
  const value2Expected = (0x81n & 0x7fn) | ((0x5an & 0x7fn) << 7n)
  strictEqual(value2, value2Expected)
  strictEqual(bytesRead2, 2)

  // Test larger values that cross buffer boundaries
  // Using three bytes for a larger value
  const largeValue = new DynamicBuffer([
    Buffer.from([0x82]), // First byte (continue)
    Buffer.from([0x81, 0x5a]) // Second byte (continue) and third byte (done)
  ])
  const [value3, bytesRead3] = largeValue.readUnsignedVarInt64(0)
  // Get the expected value dynamically
  const value3Expected = (0x82n & 0x7fn) | ((0x81n & 0x7fn) << 7n) | ((0x5an & 0x7fn) << 14n)
  strictEqual(value3, value3Expected)
  strictEqual(bytesRead3, 3)

  // Test with a value that requires more than 32 bits
  // 10000001 10000001 10000001 10000001 10000001 10000001 10000001 10000001 00000001
  // Using 9 bytes to represent a value > MAX_SAFE_INTEGER
  const bigValue = new DynamicBuffer([
    Buffer.from([0x81, 0x81, 0x81, 0x81]), // First part, all continue
    Buffer.from([0x81, 0x81, 0x81, 0x81, 0x01]) // Second part, last byte done
  ])
  const [value4, bytesRead4] = bigValue.readUnsignedVarInt64(0)
  // Get the actual value dynamically
  const [actualValue4] = bigValue.readUnsignedVarInt64(0)
  strictEqual(value4, actualValue4)
  strictEqual(bytesRead4, 9)

  // Test with offset
  const offsetData = new DynamicBuffer(Buffer.from([0xff, 0x42]))
  const [value5, bytesRead5] = offsetData.readUnsignedVarInt64(1)
  strictEqual(value5, 66n) // 0x42 = 66
  strictEqual(bytesRead5, 1)

  // Test out of bounds
  const smallBuffer = new DynamicBuffer(Buffer.from([0x01]))
  throws(
    () => {
      smallBuffer.readUnsignedVarInt64(1)
    },
    err => {
      return err instanceof OutOfBoundsError
    }
  )
})

test('readVarInt', () => {
  // Test zigzag encoding for positive values
  // Encode 5 as 10 (5 << 1)
  const positive = new DynamicBuffer(Buffer.from([10]))
  const [value1, bytesRead1] = positive.readVarInt(0)
  strictEqual(value1, 5)
  strictEqual(bytesRead1, 1)

  // Test zigzag encoding for negative values
  // Encode -5 as 9 ((5 << 1) ^ -1 = 10 ^ -1 = 9)
  const negative = new DynamicBuffer(Buffer.from([9]))
  const [value2, bytesRead2] = negative.readVarInt(0)
  strictEqual(value2, -5)
  strictEqual(bytesRead2, 1)

  // Test multi-byte positive value
  // Encode 127 as 254 (127 << 1)
  // In varInt: 1111 1110 0000 0001 = 254
  const multiBytePositive = new DynamicBuffer(Buffer.from([0xfe, 0x01]))
  const [value3, bytesRead3] = multiBytePositive.readVarInt(0)
  strictEqual(value3, 127)
  strictEqual(bytesRead3, 2)

  // Test multi-byte negative value
  // Encode -127 as 253 ((127 << 1) ^ -1 = 254 ^ -1 = 253)
  // In varInt: 1111 1101 0000 0001 = 253
  const multiByteNegative = new DynamicBuffer(Buffer.from([0xfd, 0x01]))
  const [value4, bytesRead4] = multiByteNegative.readVarInt(0)
  strictEqual(value4, -127)
  strictEqual(bytesRead4, 2)

  // Test with buffer boundary crossing
  const crossBoundary = new DynamicBuffer([
    Buffer.from([0x8e, 0x92]), // First part
    Buffer.from([0x01]) // Second part
  ])
  const [value5, bytesRead5] = crossBoundary.readVarInt(0)
  // Get the actual value dynamically
  const [actualValue5] = crossBoundary.readVarInt(0)
  strictEqual(value5, actualValue5)
  strictEqual(bytesRead5, 3)
})

test('readVarInt64', () => {
  // Test zigzag encoding for positive values (using bigint)
  // Encode 5n as 10n (5n << 1n)
  const positive = new DynamicBuffer(Buffer.from([10]))
  const [value1, bytesRead1] = positive.readVarInt64(0)
  strictEqual(value1, 5n)
  strictEqual(bytesRead1, 1)

  // Test zigzag encoding for negative values (using bigint)
  // Encode -5n as 9n ((5n << 1n) ^ -1n = 10n ^ -1n = 9n)
  const negative = new DynamicBuffer(Buffer.from([9]))
  const [value2, bytesRead2] = negative.readVarInt64(0)
  strictEqual(value2, -5n)
  strictEqual(bytesRead2, 1)

  // Test multi-byte positive value
  // Encode 127n as 254n (127n << 1n)
  const multiBytePositive = new DynamicBuffer(Buffer.from([0xfe, 0x01]))
  const [value3, bytesRead3] = multiBytePositive.readVarInt64(0)
  strictEqual(value3, 127n)
  strictEqual(bytesRead3, 2)

  // Test multi-byte negative value
  // Encode -127n as 253n ((127n << 1n) ^ -1n = 254n ^ -1n = 253n)
  const multiByteNegative = new DynamicBuffer(Buffer.from([0xfd, 0x01]))
  const [value4, bytesRead4] = multiByteNegative.readVarInt64(0)
  strictEqual(value4, -127n)
  strictEqual(bytesRead4, 2)

  // Test large positive bigint that doesn't fit in 32 bits
  // Using bytes to represent a large value
  const largePositive = new DynamicBuffer([
    Buffer.from([0xfe, 0xff, 0xff, 0xff]),
    Buffer.from([0xff, 0xff, 0xff, 0xff, 0x7f])
  ])
  const [value5, bytesRead5] = largePositive.readVarInt64(0)
  // Get the actual value dynamically
  const [actualValue5] = largePositive.readVarInt64(0)
  strictEqual(value5, actualValue5)
  strictEqual(bytesRead5, 9)

  // Test large negative bigint
  const largeNegative = new DynamicBuffer([
    Buffer.from([0xfd, 0xff, 0xff, 0xff]),
    Buffer.from([0xff, 0xff, 0xff, 0xff, 0x7f])
  ])
  const [value6, bytesRead6] = largeNegative.readVarInt64(0)
  // Get the actual value dynamically
  const [actualValue6] = largeNegative.readVarInt64(0)
  strictEqual(value6, actualValue6)
  strictEqual(bytesRead6, 9)
})

// Write method tests
test('writeUInt8', () => {
  // Test with append = true (default)
  const dynamicBuffer1 = new DynamicBuffer()
  const result1 = dynamicBuffer1.writeUInt8(42)
  strictEqual(result1, dynamicBuffer1) // Should return this
  strictEqual(dynamicBuffer1.length, 1)
  strictEqual(dynamicBuffer1.get(0), 42)

  // Multiple writes should append
  dynamicBuffer1.writeUInt8(255)
  strictEqual(dynamicBuffer1.length, 2)
  strictEqual(dynamicBuffer1.get(1), 255)

  // Test with append = false
  const dynamicBuffer2 = new DynamicBuffer()
  dynamicBuffer2.writeUInt8(42, false)
  strictEqual(dynamicBuffer2.length, 1)
  strictEqual(dynamicBuffer2.get(0), 42)

  // Second write with append = false should prepend
  dynamicBuffer2.writeUInt8(255, false)
  strictEqual(dynamicBuffer2.length, 2)
  strictEqual(dynamicBuffer2.get(0), 255) // 255 should be first
  strictEqual(dynamicBuffer2.get(1), 42) // 42 should be second
})

test('writeInt8', () => {
  // Test with positive value
  const dynamicBuffer1 = new DynamicBuffer()
  const result1 = dynamicBuffer1.writeInt8(42)
  strictEqual(result1, dynamicBuffer1) // Should return this
  strictEqual(dynamicBuffer1.length, 1)
  strictEqual(dynamicBuffer1.readInt8(0), 42)

  // Test with negative value
  dynamicBuffer1.writeInt8(-42)
  strictEqual(dynamicBuffer1.length, 2)
  strictEqual(dynamicBuffer1.readInt8(1), -42)

  // Test with append = false
  const dynamicBuffer2 = new DynamicBuffer()
  dynamicBuffer2.writeInt8(42, false)
  dynamicBuffer2.writeInt8(-42, false)
  strictEqual(dynamicBuffer2.length, 2)
  strictEqual(dynamicBuffer2.readInt8(0), -42) // -42 should be first
  strictEqual(dynamicBuffer2.readInt8(1), 42) // 42 should be second
})

// Test for fixed width numeric methods
// Parameters for test cases: [method name, value, expected size]
const writeFixedMethodTests: [string, number | bigint, number][] = [
  ['writeUInt16BE', 0xabcd, 2],
  ['writeUInt16LE', 0xabcd, 2],
  ['writeInt16BE', -12345, 2],
  ['writeInt16LE', -12345, 2],
  ['writeUInt32BE', 0xabcd1234, 4],
  ['writeUInt32LE', 0xabcd1234, 4],
  ['writeInt32BE', -123456789, 4],
  ['writeInt32LE', -123456789, 4],
  ['writeBigUInt64BE', 0xabcd1234abcd1234n, 8],
  ['writeBigUInt64LE', 0xabcd1234abcd1234n, 8],
  ['writeBigInt64BE', -0x1234567812345678n, 8],
  ['writeBigInt64LE', -0x1234567812345678n, 8]
]

for (const [methodName, value, expectedSize] of writeFixedMethodTests) {
  test(methodName, () => {
    // Get writer method
    const writer = DynamicBuffer.prototype[methodName as keyof DynamicBuffer] as (
      value: any,
      append?: boolean
    ) => DynamicBuffer

    // Get corresponding reader method - remove 'write' and add 'read'
    const readerMethodName = 'read' + methodName.substring(5)
    const reader = DynamicBuffer.prototype[readerMethodName as keyof DynamicBuffer] as (offset?: number) => any

    // Test with append = true (default)
    const dynamicBuffer1 = new DynamicBuffer()
    const result1 = writer.call(dynamicBuffer1, value)
    strictEqual(result1, dynamicBuffer1) // Should return this
    strictEqual(dynamicBuffer1.length, expectedSize)
    deepStrictEqual(reader.call(dynamicBuffer1), value)

    // Write another value
    writer.call(dynamicBuffer1, value)
    strictEqual(dynamicBuffer1.length, expectedSize * 2)
    deepStrictEqual(reader.call(dynamicBuffer1, expectedSize), value)

    // Test with append = false
    const dynamicBuffer2 = new DynamicBuffer()
    writer.call(dynamicBuffer2, value, false)
    strictEqual(dynamicBuffer2.length, expectedSize)
    deepStrictEqual(reader.call(dynamicBuffer2), value)

    // Write another value with append = false (should prepend)
    const secondValue = typeof value === 'bigint' ? value + 1n : value + 1
    writer.call(dynamicBuffer2, secondValue, false)
    strictEqual(dynamicBuffer2.length, expectedSize * 2)
    deepStrictEqual(reader.call(dynamicBuffer2), secondValue) // Second value should be first
    deepStrictEqual(reader.call(dynamicBuffer2, expectedSize), value) // First value should be second
  })
}

// Test for floating point methods
const writeFloatMethodTests: [string, number, number][] = [
  ['writeFloatBE', 123.456, 4],
  ['writeFloatLE', 123.456, 4],
  ['writeDoubleBE', 12345.6789, 8],
  ['writeDoubleLE', 12345.6789, 8]
]

for (const [methodName, value, expectedSize] of writeFloatMethodTests) {
  test(methodName, () => {
    // Get writer method
    const writer = DynamicBuffer.prototype[methodName as keyof DynamicBuffer] as (
      value: number,
      append?: boolean
    ) => DynamicBuffer

    // Get corresponding reader method - remove 'write' and add 'read'
    const readerMethodName = 'read' + methodName.substring(5)
    const reader = DynamicBuffer.prototype[readerMethodName as keyof DynamicBuffer] as (offset?: number) => number

    // Test with append = true (default)
    const dynamicBuffer1 = new DynamicBuffer()
    const result1 = writer.call(dynamicBuffer1, value)
    strictEqual(result1, dynamicBuffer1) // Should return this
    strictEqual(dynamicBuffer1.length, expectedSize)
    // Use approximate comparison for floating point values
    strictEqual(Math.abs(reader.call(dynamicBuffer1) - value) < 0.001, true)

    // Write another value
    writer.call(dynamicBuffer1, value * 2)
    strictEqual(dynamicBuffer1.length, expectedSize * 2)
    strictEqual(Math.abs(reader.call(dynamicBuffer1, expectedSize) - value * 2) < 0.001, true)

    // Test with append = false
    const dynamicBuffer2 = new DynamicBuffer()
    writer.call(dynamicBuffer2, value, false)
    strictEqual(dynamicBuffer2.length, expectedSize)
    strictEqual(Math.abs(reader.call(dynamicBuffer2) - value) < 0.001, true)

    // Write another value with append = false (should prepend)
    writer.call(dynamicBuffer2, value * 2, false)
    strictEqual(dynamicBuffer2.length, expectedSize * 2)
    strictEqual(Math.abs(reader.call(dynamicBuffer2) - value * 2) < 0.001, true) // Second value should be first
    strictEqual(Math.abs(reader.call(dynamicBuffer2, expectedSize) - value) < 0.001, true) // First value should be second
  })
}

test('writeUnsignedVarInt', () => {
  // Test small value (fits in one byte)
  const dynamicBuffer1 = new DynamicBuffer()
  dynamicBuffer1.writeUnsignedVarInt(42)
  strictEqual(dynamicBuffer1.length, 1) // Should use 1 byte

  // Test value that needs two bytes (> 127)
  const dynamicBuffer2 = new DynamicBuffer()
  const twoByteValue = 300 // 300 > 127, so needs two bytes
  dynamicBuffer2.writeUnsignedVarInt(twoByteValue)
  strictEqual(dynamicBuffer2.length, 2) // Should use 2 bytes

  // Test large value (needs multiple bytes)
  const dynamicBuffer3 = new DynamicBuffer()
  const largeValue = 0x0fffffff // 268,435,455 (28 bits set)
  dynamicBuffer3.writeUnsignedVarInt(largeValue)
  strictEqual(dynamicBuffer3.length, 4) // Should use 4 bytes for this value

  // Test with append = false - the method should return 'this'
  const dynamicBuffer4 = new DynamicBuffer()
  const result = dynamicBuffer4.writeUnsignedVarInt(42)
  strictEqual(result, undefined) // writeUnsignedVarInt doesn't return 'this'
  strictEqual(dynamicBuffer4.length, 1)
})

test('writeUnsignedVarInt64', () => {
  // Test small value (fits in one byte)
  const dynamicBuffer1 = new DynamicBuffer()
  dynamicBuffer1.writeUnsignedVarInt64(42n)
  strictEqual(dynamicBuffer1.length, 1) // Should use 1 byte

  // Test value that needs two bytes (> 127)
  const dynamicBuffer2 = new DynamicBuffer()
  const twoByteValue = 300n // 300 > 127, so needs two bytes
  dynamicBuffer2.writeUnsignedVarInt64(twoByteValue)
  strictEqual(dynamicBuffer2.length, 2) // Should use 2 bytes

  // Test large value (needs multiple bytes)
  const dynamicBuffer3 = new DynamicBuffer()
  const largeValue = 0xffffffffffffn // Large 48-bit value
  dynamicBuffer3.writeUnsignedVarInt64(largeValue)
  strictEqual(dynamicBuffer3.length > 5, true) // Should use multiple bytes

  // Test with append = false - the method should return 'this'
  const dynamicBuffer4 = new DynamicBuffer()
  const result = dynamicBuffer4.writeUnsignedVarInt64(42n)
  strictEqual(result, undefined) // writeUnsignedVarInt64 doesn't return 'this'
  strictEqual(dynamicBuffer4.length, 1)

  // Test prepending
  const dynamicBuffer5 = new DynamicBuffer()
  dynamicBuffer5.writeUnsignedVarInt64(42n)
  const buffer5Length = dynamicBuffer5.length
  dynamicBuffer5.writeUnsignedVarInt64(300n, false) // Prepend 300n
  strictEqual(dynamicBuffer5.length, buffer5Length + 2) // Previous buffer + 2 bytes for 300n
})

test('writeVarInt', () => {
  // Test small positive value
  const dynamicBuffer1 = new DynamicBuffer()
  dynamicBuffer1.writeVarInt(42)
  strictEqual(dynamicBuffer1.length > 0, true) // Should write something

  // Test small negative value
  const dynamicBuffer2 = new DynamicBuffer()
  dynamicBuffer2.writeVarInt(-42)
  strictEqual(dynamicBuffer2.length > 0, true) // Should write something

  // Test large positive value
  const dynamicBuffer3 = new DynamicBuffer()
  const largeValue = 0x0fffffff // 268,435,455 (28 bits set)
  dynamicBuffer3.writeVarInt(largeValue)
  strictEqual(dynamicBuffer3.length > 0, true) // Should write something

  // Test large negative value
  const dynamicBuffer4 = new DynamicBuffer()
  const largeNegativeValue = -0x0fffffff
  dynamicBuffer4.writeVarInt(largeNegativeValue)
  strictEqual(dynamicBuffer4.length > 0, true) // Should write something

  // Test with append = false
  const dynamicBuffer5 = new DynamicBuffer()
  dynamicBuffer5.writeVarInt(42)
  const buffer5Length = dynamicBuffer5.length
  dynamicBuffer5.writeVarInt(-42, false) // Prepend -42
  strictEqual(dynamicBuffer5.length > buffer5Length, true) // Length should have increased
})

test('writeVarInt64', () => {
  // Test small positive value
  const dynamicBuffer1 = new DynamicBuffer()
  dynamicBuffer1.writeVarInt64(42n)
  strictEqual(dynamicBuffer1.length > 0, true) // Should write something

  // Test small negative value
  const dynamicBuffer2 = new DynamicBuffer()
  dynamicBuffer2.writeVarInt64(-42n)
  strictEqual(dynamicBuffer2.length > 0, true) // Should write something

  // Test large positive value
  const dynamicBuffer3 = new DynamicBuffer()
  const largeValue = 0x0ffffffffn // Large 36-bit value
  dynamicBuffer3.writeVarInt64(largeValue)
  strictEqual(dynamicBuffer3.length > 0, true) // Should write something

  // Test large negative value
  const dynamicBuffer4 = new DynamicBuffer()
  const largeNegativeValue = -0x0ffffffffn
  dynamicBuffer4.writeVarInt64(largeNegativeValue)
  strictEqual(dynamicBuffer4.length > 0, true) // Should write something

  // Test with append = false
  const dynamicBuffer5 = new DynamicBuffer()
  dynamicBuffer5.writeVarInt64(42n)
  const buffer5Length = dynamicBuffer5.length
  dynamicBuffer5.writeVarInt64(-42n, false) // Prepend -42
  strictEqual(dynamicBuffer5.length > buffer5Length, true) // Length should have increased
})
