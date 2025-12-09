import { deepStrictEqual } from 'node:assert'
import { test } from 'node:test'
import { AuthenticationError } from '../../../src/errors.ts'
import { getCredential } from '../../../src/protocol/sasl/credential-provider.ts'

test('getCredential with string credential', (_, done) => {
  const credential = 'test-credential'

  getCredential('username', credential, (error: Error | null, result?: string) => {
    deepStrictEqual(error, null)
    deepStrictEqual(result, credential)
    done()
  })
})

test('getCredential with invalid credential type', (_, done) => {
  const credential = undefined as any

  getCredential('username', credential, error => {
    const authenticationError = error as AuthenticationError
    deepStrictEqual(authenticationError instanceof AuthenticationError, true)
    deepStrictEqual(authenticationError.message, 'The username should be a value or a function.')
    deepStrictEqual(authenticationError.code, 'PLT_KFK_AUTHENTICATION')
    done()
  })
})

test('getCredential with function provider returning string', (_, done) => {
  const credential = 'test-credential'
  const provider = () => credential

  getCredential('password', provider, (error: Error | null, result?: string) => {
    deepStrictEqual(error, null)
    deepStrictEqual(result, credential)
    done()
  })
})

test('getCredential with function provider returning promise', (_, done) => {
  const credential = 'test-credential'
  const provider = () => Promise.resolve(credential)

  getCredential('token', provider, (error: Error | null, result?: string) => {
    deepStrictEqual(error, null)
    deepStrictEqual(result, credential)
    done()
  })
})

test('getCredential with function provider returning non-value', (_, done) => {
  const provider = () => undefined

  getCredential('username', provider, (error: Error | null) => {
    const authenticationError = error as AuthenticationError

    deepStrictEqual(authenticationError instanceof AuthenticationError, true)
    deepStrictEqual(
      authenticationError.message,
      'The username provider should return a string or a promise that resolves to a value.'
    )
    deepStrictEqual(authenticationError.code, 'PLT_KFK_AUTHENTICATION')
    done()
  })
})

test('getCredential with promise provider resolving to non-value', (_, done) => {
  const provider = () => Promise.resolve(null as any)

  getCredential('password', provider, (error: Error | null) => {
    const authenticationError = error as AuthenticationError

    deepStrictEqual(authenticationError instanceof AuthenticationError, true)
    deepStrictEqual(authenticationError.message, 'The password provider should resolve to a value.')
    deepStrictEqual(authenticationError.code, 'PLT_KFK_AUTHENTICATION')
    done()
  })
})

test('getCredential with promise provider rejecting', (_, done) => {
  const originalError = new Error('Provider failed')
  const provider = () => Promise.reject(originalError)

  getCredential('token', provider, (error: Error | null) => {
    const authenticationError = error as AuthenticationError

    deepStrictEqual(error instanceof AuthenticationError, true)
    deepStrictEqual(authenticationError.message, 'The token provider threw an error.')
    deepStrictEqual(authenticationError.code, 'PLT_KFK_AUTHENTICATION')
    deepStrictEqual(authenticationError.cause, originalError)
    done()
  })
})

test('getCredential with function provider throwing synchronously', (_, done) => {
  const originalError = new Error('Provider failed')
  const provider = () => {
    throw originalError
  }

  getCredential('username', provider, (error: Error | null) => {
    const authenticationError = error as AuthenticationError

    deepStrictEqual(authenticationError instanceof AuthenticationError, true)
    deepStrictEqual(authenticationError.message, 'The username provider threw an error.')
    deepStrictEqual(authenticationError.code, 'PLT_KFK_AUTHENTICATION')
    deepStrictEqual(authenticationError.cause, originalError)
    done()
  })
})
