import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import test, { type TestContext } from 'node:test'
import { ProduceAcks } from '../../src/index.ts'
import {
  createConsumer,
  createProducer,
  createTopic,
  forEachVersion,
  pinApiVersions,
  stringDeserializers,
  stringSerializers
} from './helpers.ts'

// Produce v3-v11 against a real broker. The protocol tests only prove the codec agrees with itself:
// they parse bytes the same test authored with Writer, so a misread schema passes both ways.

/**
 * Consumes until every wanted key has been seen, then stops. Returns what was actually seen so the
 * caller can diff it: stopping on a count instead would race the partition interleaving.
 */
async function drain (
  consumer: any,
  topic: string,
  wanted: Set<string>,
  key: (message: any) => string = m => `${m.partition}:${m.key}:${m.value}`
): Promise<Set<string>> {
  const stream = await consumer.consume({ topics: [topic], mode: 'earliest', maxWaitTime: 500 })
  const seen = new Set<string>()

  try {
    for await (const message of stream) {
      seen.add(key(message))

      if (wanted.size > 0 && Array.from(wanted).every(entry => seen.has(entry))) {
        break
      }
    }
  } finally {
    await stream.close()
  }

  return seen
}

async function produceAt (t: TestContext, version: number, topic: string, partitions: number) {
  const producer = await pinApiVersions(
    createProducer(t, { serializers: stringSerializers }),
    { Produce: version }
  )

  const messages = []
  for (let partition = 0; partition < partitions; partition++) {
    messages.push({ topic, partition, key: `k${partition}`, value: `v${version}-${partition}` })
  }

  return producer.send({ messages, acks: ProduceAcks.ALL })
}

test('Produce round-trips messages at every broker-supported version', async t => {
  const topic = await createTopic(t, 3)
  const probe = createProducer(t)

  // The consumer reads everything back at the broker's newest version. Whatever a legacy Produce
  // wrote has to be indistinguishable from what the current one writes.
  const consumer = createConsumer(t, { deserializers: stringDeserializers })

  const expected: string[] = []
  const produced: { version: number; offsets: string[] }[] = []

  await forEachVersion(t, probe, 'Produce', async version => {
    const result = await produceAt(t, version, topic, 3)

    ok(result.offsets, `Produce v${version} returned no offsets`)
    strictEqual(result.offsets!.length, 3, `Produce v${version} acknowledged the wrong partition count`)

    for (const offset of result.offsets!) {
      strictEqual(offset.topic, topic)
      ok(offset.offset >= 0n, `Produce v${version} returned a negative offset for partition ${offset.partition}`)
    }

    for (let partition = 0; partition < 3; partition++) {
      expected.push(`${partition}:k${partition}:v${version}-${partition}`)
    }

    produced.push({
      version,
      offsets: result
        .offsets!.slice()
        .sort((a, b) => a.partition - b.partition)
        .map(o => `${o.partition}@${o.offset}`)
    })
  })

  ok(produced.length > 1, 'expected more than one usable Produce version')

  // Every version has to report the same monotonic offsets for the same write pattern.
  for (let i = 1; i < produced.length; i++) {
    const previous = produced[i - 1]
    const current = produced[i]

    for (let partition = 0; partition < 3; partition++) {
      const previousOffset = BigInt(previous.offsets[partition].split('@')[1])
      const currentOffset = BigInt(current.offsets[partition].split('@')[1])

      strictEqual(
        currentOffset,
        previousOffset + 1n,
        `Produce v${current.version} partition ${partition} did not continue from v${previous.version}`
      )
    }
  }

  // Read until every expected message has been seen. A non idempotent producer is at least once,
  // so a retried send legitimately duplicates a record: compare the sets, not the multisets.
  const received = await drain(consumer, topic, new Set(expected))

  deepStrictEqual(
    Array.from(received).sort(),
    Array.from(new Set(expected)).sort(),
    'messages written by legacy Produce versions differ'
  )
})

test('Produce preserves headers, keys and null values at every version', async t => {
  const topic = await createTopic(t, 1)
  const probe = createProducer(t)

  const consumer = createConsumer(t, { deserializers: stringDeserializers })

  const versions: number[] = []

  await forEachVersion(t, probe, 'Produce', async version => {
    const producer = await pinApiVersions(
      createProducer(t, { serializers: stringSerializers }),
      { Produce: version }
    )

    // An empty (non null) value is the case which trips Writer.appendVarIntBytes.
    await producer.send({
      messages: [
        { topic, partition: 0, key: `hdr-${version}`, value: '', headers: new Map([['trace', `t-${version}`]]) },
        { topic, partition: 0, key: `null-${version}`, value: undefined }
      ],
      acks: ProduceAcks.ALL
    })

    versions.push(version)
  })

  const wanted = new Set(versions.flatMap(version => [`hdr-${version}`, `null-${version}`]))
  const received = new Map<string, { value: unknown; headers: Map<string, string> }>()

  await drain(consumer, topic, wanted, message => {
    received.set(message.key, { value: message.value, headers: message.headers })
    return message.key
  })

  for (const version of versions) {
    const withHeaders = received.get(`hdr-${version}`)
    ok(withHeaders, `Produce v${version} lost the message with headers`)
    strictEqual(withHeaders!.value, '', `Produce v${version} did not preserve an empty value`)
    strictEqual(withHeaders!.headers.get('trace'), `t-${version}`, `Produce v${version} did not preserve headers`)

    const nullValue = received.get(`null-${version}`)
    ok(nullValue, `Produce v${version} lost the message with a null value`)
    strictEqual(nullValue!.value, undefined, `Produce v${version} did not preserve a null value`)
  }
})

test('Produce surfaces broker errors at every version', async t => {
  const probe = createProducer(t)

  await forEachVersion(t, probe, 'Produce', async version => {
    const producer = await pinApiVersions(createProducer(t, { serializers: stringSerializers, retries: 0 }), {
      Produce: version
    })

    // The topic does not exist and autocreation is off, so the client must surface the failure
    // rather than reporting a successful write. Only the fact of the error is comparable across
    // versions: errorMessage does not exist on the wire below Produce v8.
    await t.assert.rejects(
      () =>
        producer.send({
          messages: [{ topic: `missing-topic-${version}-${Date.now()}`, partition: 0, key: 'k', value: 'v' }],
          acks: ProduceAcks.ALL
        }),
      (error: Error) => {
        ok(error.message.length > 0, `Produce v${version} produced an empty error`)
        return true
      }
    )
  })
})
