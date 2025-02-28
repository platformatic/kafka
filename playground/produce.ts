import { produceV11 } from '../src/apis/produce.ts'
import { Connection } from '../src/connection.ts'
import { inspectResponse } from '../src/utils.ts'

const connection = new Connection('123')
await connection.start('localhost', 9092)

console.log(
  inspectResponse(
    'Produce',
    await produceV11(
      connection,
      1,
      0,
      [
        { topic: 'temp', partition: 0, key: '111', value: '222', headers: { a: '123', b: Buffer.from([97, 98, 99]) } },
        { topic: 'temp', partition: 0, key: '333', value: '444', timestamp: 12345678n }
      ],
      'zstd'
    )
  )
)

await connection.close()
