import * as apis from '../../../src/apis/index.ts'
import { type RequestCreator, type ResponseParser } from '../../../src/apis/definitions.ts'
import { codecName, implementedVersions } from '../../../test/helpers/api-versions.ts'

// The codec modules all export the same three symbols but with per-version request signatures, so
// they are reached through the package's own RequestCreator/ResponseParser types rather than a
// hand-written union of every version's arguments.
export interface CodecModule {
  createRequest: RequestCreator
  parseResponse: ResponseParser<unknown>
  api: { key: number, version: number }
}

export interface Codec extends CodecModule {
  name: string
  version: number
}

export function codec (name: string, version: number): Codec {
  const module = apis[codecName(name, version) as keyof typeof apis] as unknown as CodecModule | undefined

  if (!module) {
    throw new Error(`${name} v${version} is not implemented by this package.`)
  }

  return { name, version, ...module }
}

export function codecs (name: string): Codec[] {
  return implementedVersions(name).map(version => codec(name, version))
}

/**
 * Which framing a Produce version uses.
 *
 * Flexible versions (KIP-482) arrived in Produce v9: compact strings, varint lengths and a tagged
 * field section on every struct. Everything below writes fixed width INT16/INT32 length prefixes.
 */
export function produceIsFlexible (version: number): boolean {
  return version >= 9
}

/**
 * The shape of a Fetch response at a given version, as five independent traits.
 *
 * Reading them off the schema once, here, is what lets a single synthesizer stand in for fourteen
 * hand written response builders.
 */
export interface FetchResponseTraits {
  /** error_code and session_id in the response header. Added in v7. */
  hasSessionHeader: boolean
  /** log_start_offset per partition. Added in v5. */
  hasLogStartOffset: boolean
  /** preferred_read_replica per partition. Added in v11. */
  hasPreferredReadReplica: boolean
  /** Compact collections and tagged field sections. Added in v12. */
  flexible: boolean
  /** Topics identified by a 16 byte UUID rather than by name. Added in v13 (KIP-516). */
  topicAsUuid: boolean
}

export function fetchResponseTraits (version: number): FetchResponseTraits {
  return {
    hasSessionHeader: version >= 7,
    hasLogStartOffset: version >= 5,
    hasPreferredReadReplica: version >= 11,
    flexible: version >= 12,
    topicAsUuid: version >= 13
  }
}
