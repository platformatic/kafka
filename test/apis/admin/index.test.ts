import { ok } from 'node:assert'
import test from 'node:test'
import * as adminApis from '../../../src/apis/admin/index.ts'

test('admin/index.ts exports the correct APIs', () => {
  // Get all exports
  const exportedKeys = Object.keys(adminApis)
  
  // Verify we have a reasonable number of exports
  // The admin directory has many API files, so we should have many exports
  ok(exportedKeys.length > 30, `Should have many exports, got ${exportedKeys.length}`)
  
  // Check for specific API exports (sample check)
  const expectedExports = [
    'createTopicsV7',
    'deleteTopicsV6',
    'describeConfigsV4',
    'alterConfigsV2',
    'listGroupsV5'
  ]
  
  for (const expectedExport of expectedExports) {
    ok(
      exportedKeys.includes(expectedExport),
      `Expected export "${expectedExport}" to be included in the barrel export`
    )
    
    // Verify each export is a function
    ok(
      typeof adminApis[expectedExport] === 'function',
      `Export "${expectedExport}" should be a function`
    )
  }
})

// A more thorough test would directly import specific APIs and test their functionality,
// but that's already covered by the individual API test files.