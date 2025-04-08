import { metadataV12 } from '../../../src/apis/metadata/metadata.ts'
import { saslHandshakeV1 } from '../../../src/apis/security/sasl-handshake.ts'
import { Connection } from '../../../src/network/connection.ts'
import { authenticate } from '../../../src/protocol/sasl/scram-sha.ts'
import { performAPICallWithRetry } from '../../utils.ts'

const sasl = false
const connection = new Connection('123')
await connection.connect('localhost', 9092)

if (sasl) {
  // await performAPICallWithRetry('SaslHandshake', () => saslHandshakeV1.async(connection, 'PLAIN'))

  // await performAPICallWithRetry('SaslAuthenticate', () =>
  //   saslAuthenticateV2.async(connection, createAuthenticationRequest('client', 'client'))
  // )

  await performAPICallWithRetry('SaslHandshake', () => saslHandshakeV1.async(connection, 'SCRAM-SHA-256'))

  await performAPICallWithRetry('SaslAuthenticate', () => authenticate(connection, 'SHA-256', 'client', 'client'))
}

await performAPICallWithRetry('Metadata', () => metadataV12.async(connection, null, false, true))

await connection.close()
