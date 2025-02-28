import { metadataV12 } from '../../src/apis/metadata/metadata.ts'
import { Connection } from '../../src/connection.ts'
import { performAPICallWithRetry } from '../utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

await performAPICallWithRetry('Metadata', () => metadataV12.async(connection, ['temp'], false, true))

await connection.close()
