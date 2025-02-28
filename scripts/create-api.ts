import { writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'
import { parseArgs } from 'node:util'
import { camelCase, kebabCase, pascalCase } from 'scule'
import { formatOutput } from './utils.ts'

const template = `
import BufferList from 'bl'
import { Reader } from '../../protocol/reader.ts'
import { Writer } from '../../protocol/writer.ts'
import { createAPI, type ResponseErrorWithLocation } from '../index.ts'
import { ResponseError } from '../../errors.ts'

export type #{type}Request = Parameters<typeof createRequest>

export interface #{type}Response {}

/*

*/
function createRequest (): Writer {
  return Writer.create().appendTaggedFields()
}

/*

*/
function parseResponse (_correlationId: number, apiKey: number, apiVersion: number, raw: BufferList): #{type}Response {
  const reader = Reader.from(raw)
  const errors: ResponseErrorWithLocation[] = []

  const response: #{type}Response = {}

  if (errors.length) {
    throw new ResponseError(apiKey, apiVersion, { errors: Object.fromEntries(errors), response })
  }

  return response
}

export const #{api} = createAPI<#{type}Request, #{type}Response>(#{code}, #{version}, createRequest, parseResponse)
`

async function main () {
  let values: Record<string, string> = {}

  try {
    values = parseArgs({
      args: process.argv.slice(2),
      options: {
        name: { type: 'string', required: true },
        section: { type: 'string', required: true },
        code: { type: 'string', required: true },
        version: { type: 'string', required: true }
      }
    }).values

    if (!values.section || !values.name || !values.code || !values.version) {
      throw new Error('Missing argument.')
    }
  } catch (e) {
    console.error('Please provide the --section, --name, --code and --version options.')
    process.exit(1)
  }

  const { section, name, code, version } = values

  const typeName = pascalCase(name!)
  const fileName = kebabCase(typeName)

  const context: Record<string, string> = {
    type: typeName,
    api: camelCase(typeName + 'V' + version),
    code,
    version
  }

  const output = template.replace(/#\{([a-z]+)\}/g, (_, key) => context[key]!)
  const destination = resolve(import.meta.dirname, `../src/apis/${section}/${fileName}.ts`)

  await writeFile(destination, await formatOutput(output, destination), 'utf-8')
}

await main()
