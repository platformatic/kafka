import { deepStrictEqual } from 'node:assert'
import { execFileSync } from 'node:child_process'
import { test } from 'node:test'

const script = `
  import { createRequire } from 'node:module'
  await import('./src/index.ts')
  console.log(JSON.stringify(Object.keys(createRequire(import.meta.url).cache)))
`

test('importing the package should not load the libraries which are only needed by optional features', () => {
  const output = execFileSync(process.execPath, ['--input-type=module', '-e', script], {
    cwd: new URL('..', import.meta.url),
    encoding: 'utf-8'
  })

  const loaded = (JSON.parse(output) as string[]).map(file => file.replaceAll('\\', '/'))
  const unexpected = /node_modules\/(ajv|avsc|protobufjs|ajv-draft-04)\/|node_modules\/mnemonist\/index/

  deepStrictEqual(
    loaded.filter(file => unexpected.test(file)),
    []
  )
})
