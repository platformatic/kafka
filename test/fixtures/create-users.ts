import { pbkdf2Sync, randomBytes } from 'crypto'
import { alterUserScramCredentialsV0, Connection } from '../../src/index.ts'

// Create passwords as Confluent Kafka images don't support it
export async function createScramUsers (broker: { host: string; port: number }) {
  const connection = new Connection('123')
  await connection.connect(broker.host, broker.port + 10000)

  const password = 'admin'
  const salt = randomBytes(16) // 16 byte di salt casuale
  const iterations = 4096
  const mechanisms = [
    { algorithm: 'sha256', mechanism: 1, length: 32 }, // SCRAM-SHA-256
    { algorithm: 'sha512', mechanism: 2, length: 64 } // SCRAM-SHA-512
  ]

  for (const { algorithm, mechanism, length } of mechanisms) {
    // SaltedPassword
    const saltedPassword = pbkdf2Sync(password, salt, iterations, length, algorithm)

    await alterUserScramCredentialsV0.api.async(
      connection,
      [],
      [{ name: 'admin', mechanism, iterations, salt, saltedPassword }]
    )
  }

  await connection.close()
}
