import { Reader } from '../protocol/reader.ts'

export type TaggedFieldHandler = (reader: Reader) => void
export type TaggedFieldHandlers = Readonly<Record<number, TaggedFieldHandler>>

/**
 * Decodes known flexible-version tagged fields and skips fields the caller does not handle.
 */
export function readKnownTaggedFields (reader: Reader, handlers: TaggedFieldHandlers): void {
  const count = reader.readUnsignedVarInt()

  for (let index = 0; index < count; index++) {
    const tag = reader.readUnsignedVarInt()
    const size = reader.readUnsignedVarInt()

    if (size > reader.remaining) {
      throw new RangeError(`Tagged field ${tag} declares ${size} bytes but only ${reader.remaining} remain`)
    }

    const handler = handlers[tag]
    if (handler) {
      const payload = Reader.from(reader.buffer.slice(reader.position, reader.position + size))

      try {
        handler(payload)
      } finally {
        reader.skip(size)
      }
    } else {
      reader.skip(size)
    }
  }
}
