import { strictEqual } from 'node:assert'
import test from 'node:test'
import { runAsyncSeries } from '../../src/registries/abstract.ts'

test('runAsyncSeries should complete with empty collections', async () => {
  let operationCalls = 0

  await new Promise<void>((resolve, reject) => {
    runAsyncSeries(
      (_item: string, callback) => {
        operationCalls++
        callback(null)
      },
      [],
      0,
      error => {
        if (error) {
          reject(error)
          return
        }

        resolve()
      }
    )
  })

  strictEqual(operationCalls, 0)
})
