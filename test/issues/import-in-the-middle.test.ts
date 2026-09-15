// Related issue: https://github.com/nodejs/import-in-the-middle/issues/290

import { strictEqual } from 'node:assert'
import { execFileSync } from 'node:child_process'
import { resolve } from 'node:path'
import { pathToFileURL } from 'node:url'
import { test } from 'node:test'

test('imports the package when import-in-the-middle is active', () => {
  const registerPath = resolve(import.meta.dirname, '../fixtures/import-in-the-middle-register.mjs')
  const packagePath = pathToFileURL(resolve(import.meta.dirname, '../../dist/index.js')).href
  const output = execFileSync(
    process.execPath,
    [
      '--import',
      registerPath,
      '--input-type=module',
      '--eval',
      `import { Producer } from '${packagePath}'; console.log(Producer.name)`
    ],
    { encoding: 'utf8', timeout: 5_000 }
  )

  strictEqual(output.trim(), 'Producer')
})
