import { ok, strictEqual } from 'node:assert'
import test from 'node:test'
import * as securityExports from '../../../src/apis/security/index.ts'

test('security/index.ts exports all security APIs', () => {
  // Check that the expected APIs are exported
  ok('saslHandshakeV1' in securityExports, 'saslHandshakeV1 should be exported')
  ok('saslAuthenticateV2' in securityExports, 'saslAuthenticateV2 should be exported')

  // Verify the exported APIs are functions
  strictEqual(typeof securityExports.saslHandshakeV1, 'function')
  strictEqual(typeof securityExports.saslAuthenticateV2, 'function')
})
