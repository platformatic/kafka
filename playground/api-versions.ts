import { apiVersionsV4 } from '../src/apis/api-versions.ts'
import { Connection } from '../src/connection.ts'
import { inspectResponse } from '../src/utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

console.log(inspectResponse('ApiVersions', await apiVersionsV4(connection, 'kafka', '1.0.0')))

await connection.close()
