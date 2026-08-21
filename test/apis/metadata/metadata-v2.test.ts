import { strictEqual } from 'node:assert'
import test from 'node:test'
import { metadataV2, Reader, Writer } from '../../../src/index.ts'

test('Metadata v2 reads the cluster id and normalizes unavailable fields', () => {
  strictEqual(metadataV2.createRequest(null).length, 4)
  const response = metadataV2.parseResponse(
    1,
    3,
    2,
    Reader.from(
      Writer.create()
        .appendArray([], () => {}, false, false)
        .appendString('cluster', false)
        .appendInt32(1)
        .appendArray(
          ['topic'],
          (w, topic) => w.appendInt16(0).appendString(topic, false).appendBoolean(true).appendArray([], () => {}, false, false),
          false,
          false
        )
    )
  )
  strictEqual(response.topics[0].isInternal, true)
})
