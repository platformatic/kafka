import JSON5 from 'json5'
import { glob, readFile, writeFile } from 'node:fs/promises'
import { basename, resolve } from 'node:path'
if (process.argv.length < 3) {
  console.error('Please provide the path to the Kafka source code')
  process.exit(1)
}

const kafkaSource = process.argv[2]

const KafkaAPIByName: Record<string, number> = {}
const KafkaAPIById: Record<number, string> = {}

for await (const file of glob(resolve(kafkaSource, 'clients/src/main/resources/common/message/*Request.json'))) {
  const spec = JSON5.parse(await readFile(file, 'utf-8'))
  const key = spec.apiKey as number
  const name = basename(file, 'Request.json')

  KafkaAPIByName[name] = key
  KafkaAPIById[key] = name
}

await writeFile(
  new URL('../src/apis/definitions.ts', import.meta.url),
  `
  export const KafkaAPIByName: Record<string, number> = Object.freeze(${JSON.stringify(KafkaAPIByName, null, 2)})
      
  export const KafkaAPIById: Record<number, string> = Object.freeze(${JSON.stringify(KafkaAPIById, null, 2)})
  `
)
