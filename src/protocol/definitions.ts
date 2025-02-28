import type BufferList from 'bl'

// Note that in this file "== null" is purposely used instead of "===" to check for both null and undefined

export const INT8_SIZE = 1
export const INT16_SIZE = 2
export const INT32_SIZE = 4
export const INT64_SIZE = 8
export const UUID_SIZE = 16

export const EMPTY_BUFFER = Buffer.alloc(0)
export const EMPTY_UUID = Buffer.alloc(UUID_SIZE)

// Since it is serialized at either 0 (for nullable) or 1 (since length is stored as length + 1), it always uses a single byte
export const EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE = INT8_SIZE

// TODO(ShogunPanda): Tagged fields are not supported yet
export const EMPTY_TAGGED_FIELDS_BUFFER = Buffer.from([0])

export type Collection = string | Buffer | BufferList | Array<unknown> | Map<unknown, unknown> | Set<unknown>

export type NullableString = string | undefined | null
