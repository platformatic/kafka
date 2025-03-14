import { SaslAuthenticateResponse, saslAuthenticateV2 } from '../../apis/security/sasl-authenticate.ts'
import { type Connection } from '../../connection.ts'

export function authenticate (
  connection: Connection,
  username: string,
  password: string
): Promise<SaslAuthenticateResponse> {
  return saslAuthenticateV2.async(connection, Buffer.from(['', username, password].join('\0')))
}
