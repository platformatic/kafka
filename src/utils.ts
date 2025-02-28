import BufferList from 'bl'
import { inspect } from 'node:util'

export function inspectBuffer (buffer: Buffer | BufferList): string {
  return buffer
    .toString('hex')
    .replaceAll(/(.{4})/g, '$1 ')
    .trim()
}

export function inspectResponse (label: string, response: unknown): string {
  return inspect({ label, response }, false, 10)
}

export function groupByProperty<K, T> (entries: T[], property: keyof T): Map<K, T[]> {
  const grouped: Map<K, T[]> = new Map()

  for (const entry of entries) {
    const value = entry[property] as K

    if (!grouped.has(value)) {
      grouped.set(value, [])
    }

    grouped.get(value)!.push(entry)
  }

  return grouped
}

// TODO(ShogunPanda): Send a PR to bl
export function bufferListPrepend (bl: BufferList, buffer: Buffer): void {
  // @ts-expect-error _bufs is not exposed
  bl._bufs.unshift(buffer)
  bl.length += buffer.length
}
