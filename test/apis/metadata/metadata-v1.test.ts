import { deepStrictEqual, strictEqual } from 'node:assert'
import test from 'node:test'
import { metadataV1, Reader, Writer } from '../../../src/index.ts'

test('Metadata v1 reads the controller and broker rack', () => {
  strictEqual(metadataV1.createRequest(null).length, 4)
  const response = metadataV1.parseResponse(
    1,
    3,
    1,
    Reader.from(
      Writer.create()
        .appendArray(
          [{ id: 1 }],
          (w, broker) =>
            w.appendInt32(broker.id).appendString('broker', false).appendInt32(9092).appendString('rack', false),
          false,
          false
        )
        .appendInt32(1)
        .appendArray(
          ['topic'],
          (w, topic) => w.appendInt16(0).appendString(topic, false).appendBoolean(true).appendArray([], () => {}, false, false),
          false,
          false
        )
    )
  )
  deepStrictEqual(response.brokers[0], { nodeId: 1, host: 'broker', port: 9092, rack: 'rack' })
  strictEqual(response.controllerId, 1)
  strictEqual(response.throttleTimeMs, 0)
  strictEqual(response.clusterId, null)
  strictEqual(response.topics[0].isInternal, true)
})
