import { alterUserScramCredentialsV0 } from '../../src/apis/admin/alter-user-scram-credentials.ts'
import { describeUserScramCredentialsV0 } from '../../src/apis/admin/describe-user-scram-credentials.ts'
import { ScramMechanisms } from '../../src/apis/enumerations.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('AlterUserScramCredentials', () =>
  alterUserScramCredentialsV0.async(
    connection,
    [],
    [
      {
        name: 'user',
        mechanism: ScramMechanisms.SCRAM_SHA_256,
        iterations: 16384,
        salt: Buffer.alloc(10),
        saltedPassword: Buffer.alloc(20)
      }
    ]
  )
)

await performAPICallWithRetry('DescribeUserScramCredentials', () =>
  describeUserScramCredentialsV0.async(connection, [{ name: 'user' }])
)

await connection.close()
