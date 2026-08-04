import { Writer } from '../../../src/protocol/writer.ts'
import { fetchResponseTraits } from './codecs.ts'

export interface FetchResponseShape {
  /** Topic name, used verbatim below v13 and hashed into a UUID from v13 up. */
  topic: string
  topicId: string
  /** One entry per partition, each carrying an already encoded records batch. */
  partitions: { index: number, records: Writer }[]
}

/**
 * Builds a Fetch response for any implemented version from the five traits in fetchResponseTraits.
 *
 * The alternative is fourteen near identical builders copied out of the protocol tests. This is
 * self checking rather than merely shorter: the benchmark parses whatever it writes and asserts the
 * record count survives the round trip, so a mistake here fails loudly instead of quietly measuring
 * a malformed buffer.
 */
export function createFetchResponse (version: number, shape: FetchResponseShape): Writer {
  const traits = fetchResponseTraits(version)
  const { flexible } = traits
  const writer = Writer.create().appendInt32(0)

  if (traits.hasSessionHeader) {
    writer.appendInt16(0).appendInt32(0)
  }

  writer.appendArray(
    [shape],
    (w, topic) => {
      if (traits.topicAsUuid) {
        w.appendUUID(topic.topicId)
      } else {
        w.appendString(topic.topic, flexible)
      }

      w.appendArray(
        topic.partitions,
        (w, partition) => {
          w.appendInt32(partition.index).appendInt16(0).appendInt64(100n).appendInt64(100n)

          if (traits.hasLogStartOffset) {
            w.appendInt64(0n)
          }

          // Empty, but present at every version.
          w.appendArray([], () => {}, flexible, flexible)

          if (traits.hasPreferredReadReplica) {
            w.appendInt32(-1)
          }

          if (flexible) {
            // COMPACT_RECORDS: a varint of length + 1, then the raw batch.
            w.appendUnsignedVarInt(partition.records.length + 1).appendFrom(partition.records)
          } else {
            w.appendInt32(partition.records.length).appendFrom(partition.records)
          }

          if (flexible) {
            // The partition level tagged fields (diverging_epoch, current_leader, snapshot_id) are
            // read by the codec itself, so appendArray must not also emit them.
            w.appendTaggedFields()
          }
        },
        flexible,
        false
      )
    },
    flexible,
    flexible
  )

  if (flexible) {
    // Response level tagged fields; node_endpoints (tag 0, v16+) is simply absent.
    writer.appendTaggedFields()
  }

  return writer
}

/** A stable UUID for a topic name, so the v13+ wire size is realistic rather than all zeroes. */
export function topicIdFor (topic: string): string {
  const hex = Buffer.from(topic.padEnd(16, '.')).subarray(0, 16).toString('hex')

  return [hex.slice(0, 8), hex.slice(8, 12), hex.slice(12, 16), hex.slice(16, 20), hex.slice(20, 32)].join('-')
}
