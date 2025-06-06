import { api as apiVersionsV3 } from '../../../src/apis/metadata/api-versions-v3.ts'
import { Connection } from '../../../src/network/connection.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const connection = new Connection('123', {
  // tls: {
  //   rejectUnauthorized: false,
  //   cert: await readFile(resolve(import.meta.dirname, '../../ssl/client.pem')),
  //   key: await readFile(resolve(import.meta.dirname, '../../ssl/client.key'))
  // }
})
await connection.connect('localhost', 9092)

await performAPICallWithRetry('ApiVersions', () => apiVersionsV3.async(connection, 'kafka', '1.0.0'))

await connection.close()
