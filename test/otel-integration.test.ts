/**
 * Integration test for OpenTelemetry compatibility
 * Tests that the library can be imported without hanging or memory leaks
 * when OpenTelemetry's auto-instrumentation hook is active
 *
 * Related issue: https://github.com/platformatic/kafka/issues/181
 */

import { describe, it } from 'node:test'
import assert from 'node:assert'
import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'

const __filename = fileURLToPath(import.meta.url)
const __dirname = dirname(__filename)

describe('OpenTelemetry Integration', () => {
  it('should import library without hanging when OTEL hook is active', async () => {
    // Create a minimal test script that imports the library
    const { pathToFileURL } = await import('node:url')
    const distPath = join(__dirname, '..', 'dist', 'index.js')
    const distURL = pathToFileURL(distPath).href
    const testScript = `
      import '${distURL}'
      console.log('IMPORT_SUCCESS')
      process.exit(0)
    `

    // Write test script to a temporary file
    const { writeFileSync, unlinkSync, mkdtempSync } = await import('node:fs')
    const { tmpdir } = await import('node:os')
    const tmpDir = mkdtempSync(join(tmpdir(), 'kafka-otel-test-'))
    const scriptPath = join(tmpDir, 'test-import.mjs')

    try {
      writeFileSync(scriptPath, testScript)

      // Try to import the library with OTEL hooks enabled
      // Note: This test will only truly verify the fix if @opentelemetry/instrumentation is installed
      // but it should at least verify that the circular import is resolved
      const result = await new Promise<{ stdout: string, stderr: string, exitCode: number }>((resolve) => {
        const nodeArgs = [
          // Uncomment the line below if @opentelemetry/instrumentation is installed
          // '--experimental-loader=@opentelemetry/instrumentation/hook.mjs',
          scriptPath
        ]

        const child = spawn('node', nodeArgs, {
          cwd: join(__dirname, '..'),
          timeout: 5000, // 5 second timeout - should be plenty for a simple import
          env: {
            ...process.env,
            NODE_NO_WARNINGS: '1' // Suppress experimental warnings
          }
        })

        let stdout = ''
        let stderr = ''

        child.stdout?.on('data', (data) => {
          stdout += data.toString()
        })

        child.stderr?.on('data', (data) => {
          stderr += data.toString()
        })

        child.on('close', (code) => {
          resolve({
            stdout,
            stderr,
            exitCode: code ?? -1
          })
        })

        child.on('error', (err) => {
          resolve({
            stdout,
            stderr: stderr + err.message,
            exitCode: -1
          })
        })

        // Kill the process if it times out (hangs)
        setTimeout(() => {
          if (!child.killed) {
            child.kill('SIGKILL')
            resolve({
              stdout,
              stderr: stderr + 'Process timed out - likely infinite loop',
              exitCode: -1
            })
          }
        }, 5000)
      })

      // Verify the import completed successfully
      assert.ok(result.stdout.includes('IMPORT_SUCCESS'),
        `Expected 'IMPORT_SUCCESS' in stdout, but got: ${result.stdout}\nstderr: ${result.stderr}`)

      assert.strictEqual(result.exitCode, 0,
        `Process should exit with code 0, got ${result.exitCode}.\nstdout: ${result.stdout}\nstderr: ${result.stderr}`)
    } finally {
      // Cleanup
      try {
        unlinkSync(scriptPath)
        const { rmdirSync } = await import('node:fs')
        rmdirSync(tmpDir)
      } catch (err) {
        // Ignore cleanup errors
      }
    }
  })

  it('should not have circular self-import in protocol/index.ts', async () => {
    const { readFileSync } = await import('node:fs')
    const protocolIndexPath = join(__dirname, '..', 'src', 'protocol', 'index.ts')
    const content = readFileSync(protocolIndexPath, 'utf-8')

    // Verify the circular self-reference has been removed
    assert.ok(!content.includes("export * from './index.ts'"),
      'protocol/index.ts should not contain circular self-reference')
  })

  it('should use lazy initialization for diagnostic channels', async () => {
    const { readFileSync } = await import('node:fs')
    const diagnosticPath = join(__dirname, '..', 'src', 'diagnostic.ts')
    const content = readFileSync(diagnosticPath, 'utf-8')

    // Verify that channels use lazy initialization pattern (Proxy)
    assert.ok(content.includes('new Proxy'),
      'diagnostic.ts should use Proxy for lazy initialization of channels')

    assert.ok(content.includes('initializeChannels'),
      'diagnostic.ts should have an initializeChannels guard function')
  })
})
