import { strictEqual } from 'node:assert'
import test from 'node:test'
import { metadataV3, Reader, Writer } from '../../../src/index.ts'

test('Metadata v3 reads throttle time without the auto-create request field', () => {
  strictEqual(metadataV3.createRequest(null).length, 4)
  const response = metadataV3.parseResponse(
    1,
    3,
    3,
    Reader.from(
      Writer.create()
        .appendInt32(100)
        .appendArray([], () => {}, false, false)
        .appendString(null, false)
        .appendInt32(-1)
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
