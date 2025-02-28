import type BufferList from 'bl'
import { sizeOfUnsignedVarInt } from './varint32.ts'

// Note that in this file "== null" is purposely used instead of "===" to check for both null and undefined

export const INT8_SIZE = 1
export const INT16_SIZE = 2
export const INT32_SIZE = 4
export const INT64_SIZE = 8
export const UUID_SIZE = 16

// Since it is serialized at either 0 (for nullable) or 1 (since length is stored as length + 1), it always uses a single byte
export const EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE = INT8_SIZE

// TODO(ShogunPanda): Tagged fields are not supported yet
export const EMPTY_TAGGED_FIELDS_BUFFER = Buffer.from([0])

export type Collection = string | Buffer | BufferList | Array<unknown> | Map<unknown, unknown> | Set<unknown>

// String sizes are stored as INT16, while bytes and arrays sizes are stored as INT32
export function sizeOfCollection (
  value: Collection | null | undefined,
  includeLength: boolean = true,
  nullType: 'string' | 'other' = 'other'
): number {
  if (value == null) {
    return nullType === 'string' ? INT16_SIZE : INT32_SIZE
  }

  const length = (value as string).length ?? (value as Set<unknown>).size
  return (typeof value === 'string' ? INT16_SIZE : INT32_SIZE) + (includeLength ? length : 0)
}

export function sizeOfCompactable (value: Collection | null | undefined, includeLength: boolean = true): number {
  if (value == null) {
    return EMPTY_OR_SINGLE_COMPACT_LENGTH_SIZE
  }

  const length = (value as string).length ?? (value as Set<unknown>).size
  return sizeOfUnsignedVarInt(length + 1) + (includeLength ? length : 0)
}

export function sizeOfCompactableLength (value: Collection | null | undefined): number {
  return sizeOfCompactable(value, false)
}
