// This file uses internal APIs to check the general low-level API developer experience.

import { deepStrictEqual } from 'assert'
import test from 'node:test'
// Technically V4 is the latest version, but we use V3 in the tests so that it is also compatible with older brokers (2.4.0+)
import { api } from '../../src/apis/metadata/api-versions-v3.ts'
import { Connection } from '../../src/index.ts'
import { kafkaBootstrapServers } from '../helpers.ts'

test('any API should work in promise mode or callback mode', async t => {
  const connection = new Connection('clientId')
  t.after(() => connection.close())

  const [host, port] = kafkaBootstrapServers[0].split(':')
  await connection.connect(host, Number(port))

  const promiseResponse = await api.async(connection, 'test-client', '1.0.0')

  const callbackResponse = await new Promise((resolve, reject) => {
    api(connection, 'test-client', '1.0.0', (...args) => {
      const [error, response] = args
      if (error) {
        reject(error)
      } else {
        resolve(response)
      }
    })
  })

  // This call has no callback but it will not fail
  api(connection, 'test-client', '1.0.0')

  deepStrictEqual(promiseResponse, callbackResponse)
  deepStrictEqual(promiseResponse.apiKeys[0].name, 'Produce')
})
