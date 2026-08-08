import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { api } from '../../../src/apis/admin/delete-acls-v1.ts'

test('DeleteAcls v1 uses classic request and response headers', () => {
  const sent: unknown[][] = []
  api({ send: (...args: unknown[]) => sent.push(args) } as never, [])
  deepStrictEqual(sent[0].slice(0, 2).concat(sent[0].slice(4, 6)), [31, 1, false, false])
})
