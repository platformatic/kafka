// Related issue: https://github.com/platformatic/kafka/issues/181

import { deepStrictEqual } from 'node:assert'
import { execSync } from 'node:child_process'
import { mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { test } from 'node:test'

test('should import library without hanging when OTEL hook is active', async () => {
  // Create a minimal test script that imports the library
  const indexFile = resolve(import.meta.dirname, '../../dist/index.js')
  const testScript = `
    import '${indexFile}'
    console.log('SUCCESS')
    process.exit(0)
  `

  // Write test script to a temporary file
  const tmpDir = await mkdtemp(join(tmpdir(), 'kafka-otel-test-'))

  try {
    const scriptPath = join(tmpDir, 'test-import.mjs')
    await writeFile(scriptPath, testScript)

    // Try to import the library with OTEL hooks enabled
    const output = await execSync(
      `${process.argv[0]} --experimental-loader=@opentelemetry/instrumentation/hook.mjs --no-warnings ${scriptPath}`,
      { timeout: 5000 } // If it doesn't complete in 5 seconds, we consider it a infinite hang
    )

    deepStrictEqual(output.toString().trim(), 'SUCCESS')
  } finally {
    // Cleanup
    try {
      await rm(tmpDir, { recursive: true, force: true })
    } catch (err) {
      // Ignore cleanup errors
    }
  }
})
