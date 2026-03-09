import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { alterPartitionReassignmentsV1, Reader, ResponseError, Writer } from '../../../src/index.ts'

const { createRequest, parseResponse } = alterPartitionReassignmentsV1

test('createRequest serializes allowReplicationFactorChange correctly', () => {
  const writer = createRequest(30000, false, [
    {
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, replicas: [1, 2, 3] }]
    }
  ])

  ok(writer instanceof Writer)

  const reader = Reader.from(writer)
  deepStrictEqual(reader.readInt32(), 30000)
  deepStrictEqual(reader.readBoolean(), false)
  deepStrictEqual(
    reader.readArray(() => {
      const name = reader.readString()
      const partitions = reader.readArray(() => {
        return {
          partitionIndex: reader.readInt32(),
          replicas: reader.readArray(() => reader.readInt32(), true, false)
        }
      })

      return { name, partitions }
    }),
    [
      {
        name: 'test-topic',
        partitions: [{ partitionIndex: 0, replicas: [1, 2, 3] }]
      }
    ]
  )
})

test('parseResponse correctly processes a successful response', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendString(null)
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [{ partitionIndex: 0, errorCode: 0, errorMessage: null }]
        }
      ],
      (w, response) => {
        w.appendString(response.name).appendArray(response.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendString(partition.errorMessage)
        })
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 45, 1, Reader.from(writer))

  deepStrictEqual(response.responses, [
    {
      name: 'test-topic',
      partitions: [{ partitionIndex: 0, errorCode: 0, errorMessage: null }]
    }
  ])
})

test('parseResponse throws ResponseError on partition error', () => {
  const writer = Writer.create()
    .appendInt32(0)
    .appendInt16(0)
    .appendString(null)
    .appendArray(
      [
        {
          name: 'test-topic',
          partitions: [{ partitionIndex: 0, errorCode: 39, errorMessage: 'invalid replica assignment' }]
        }
      ],
      (w, response) => {
        w.appendString(response.name).appendArray(response.partitions, (w, partition) => {
          w.appendInt32(partition.partitionIndex).appendInt16(partition.errorCode).appendString(partition.errorMessage)
        })
      }
    )
    .appendTaggedFields()

  throws(
    () => {
      parseResponse(1, 45, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError)
      deepStrictEqual(err.response.responses[0].partitions[0].errorCode, 39)
      return true
    }
  )
})
