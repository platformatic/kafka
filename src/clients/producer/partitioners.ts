import { murmur2 } from '../../protocol/murmur2.ts'
import { type MessageToProduce } from '../../protocol/records.ts'

const compatibilityMurmur2Mask = 0x7fffffff

export function defaultPartitioner<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  _: MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
  key?: Buffer | undefined
): number {
  /* c8 ignore next - Hard to test */
  return Buffer.isBuffer(key) ? murmur2(key) >>> 0 : 0
}

export function compatibilityPartitioner<Key = Buffer, Value = Buffer, HeaderKey = Buffer, HeaderValue = Buffer> (
  _: MessageToProduce<Key, Value, HeaderKey, HeaderValue>,
  key?: Buffer | undefined
): number {
  /* c8 ignore next - Hard to test */
  return Buffer.isBuffer(key) ? murmur2(key) & compatibilityMurmur2Mask : 0
}
