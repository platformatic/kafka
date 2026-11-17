import type * as AjvModule from 'ajv'
import type { AnySchema, KeywordCxt, ValidateFunction } from 'ajv'
import type * as Ajv2020Module from 'ajv/dist/2020.js'
import type { Ajv2020 } from 'ajv/dist/2020.js'
import type { KeywordErrorCxt } from 'ajv/dist/types/index.js'
import { createRequire } from 'node:module'
import type { EnumerationDefinition } from './utils.ts'

/*
  The validators of the options of the clients are generated ahead of time (see scripts/generate-validators.ts),
  so the package does not need ajv to validate them. Nothing in this file loads ajv until it is really needed,
  which is why it is only imported for its types and loaded with require().
*/
type AjvOptions = ConstructorParameters<typeof Ajv2020>[0]

export interface OptionsValidator<T = unknown> extends ValidateFunction<T> {
  /** @deprecated It will be removed in the next major version. */
  schemaEnv: ValidateFunction<T>['schemaEnv']
  /** @deprecated It will be removed in the next major version. */
  source?: ValidateFunction<T>['source']
  /** @deprecated It will be removed in the next major version. */
  evaluated?: ValidateFunction<T>['evaluated']
}

let require: NodeJS.Require | undefined
let instance: Ajv2020 | undefined
const boundMethods = new WeakMap<Function, Function>()

// The keywords are defined via code, rather than via a validation function, as that is the only form which
// can be written in the generated validators.
export function createAjv (options: AjvOptions = {}): Ajv2020 {
  require ??= createRequire(import.meta.url)

  const { _, str, stringify } = require('ajv') as typeof AjvModule
  const { Ajv2020 } = require('ajv/dist/2020.js') as typeof Ajv2020Module
  const ajv = new Ajv2020({ allErrors: true, coerceTypes: false, strict: true, ...options })

  ajv.addKeyword({
    keyword: 'bigint',
    schemaType: 'boolean',
    code (cxt: KeywordCxt) {
      cxt.fail(_`typeof ${cxt.data} !== "bigint"`)
    },
    error: { message: 'must be bigint' }
  })

  ajv.addKeyword({
    keyword: 'map',
    schemaType: 'boolean',
    code (cxt: KeywordCxt) {
      cxt.fail(_`!(${cxt.data} instanceof Map)`)
    },
    error: { message: 'must be Map' }
  })

  ajv.addKeyword({
    keyword: 'function',
    schemaType: 'boolean',
    code (cxt: KeywordCxt) {
      cxt.fail(_`typeof ${cxt.data} !== "function"`)
    },
    error: { message: 'must be function' }
  })

  ajv.addKeyword({
    keyword: 'buffer',
    schemaType: 'boolean',
    code (cxt: KeywordCxt) {
      cxt.fail(_`!Buffer.isBuffer(${cxt.data})`)
    },
    error: { message: 'must be Buffer' }
  })

  ajv.addKeyword({
    keyword: 'gteProperty',
    schemaType: 'string',
    code (cxt: KeywordCxt) {
      cxt.fail(_`!(${cxt.data} >= ${cxt.it.parentData}[${cxt.schema as string}])`)
    },
    error: {
      message: (cxt: KeywordErrorCxt) => str`must be greater than or equal to $dataVar$/${cxt.schema as string}`
    }
  })

  ajv.addKeyword({
    keyword: 'lteProperty',
    schemaType: 'string',
    code (cxt: KeywordCxt) {
      cxt.fail(_`!(${cxt.data} < ${cxt.it.parentData}[${cxt.schema as string}])`)
    },
    error: {
      message: (cxt: KeywordErrorCxt) => str`must be less than or equal to $dataVar$/${cxt.schema as string}`
    }
  })

  ajv.addKeyword({
    keyword: 'enumeration', // This mimics the enum keyword but defines a custom error message
    schemaType: 'object',
    code (cxt: KeywordCxt) {
      const { allowed } = cxt.schema as EnumerationDefinition<string | number>
      cxt.fail(_`!${stringify(allowed)}.includes(${cxt.data})`)
    },
    error: {
      message: (cxt: KeywordErrorCxt) => str`${(cxt.schema as EnumerationDefinition<string | number>).errorMessage!}`
    }
  })

  return ajv
}

function getAjv (): Ajv2020 {
  instance ??= createAjv()
  return instance
}

/**
 * The ajv instance with the custom keywords used by the schemas of the options. It is created when first used.
 *
 * @deprecated It will be removed in the next major version.
 */
export const ajv: Ajv2020 = new Proxy({} as Ajv2020, {
  get (_, property) {
    const value = Reflect.get(getAjv(), property)

    if (typeof value !== 'function' || property === 'constructor') {
      return value
    }

    // Methods must run on the real instance, not on this proxy
    let bound = boundMethods.get(value)

    if (!bound) {
      bound = value.bind(getAjv()) as Function
      boundMethods.set(value, bound)
    }

    return bound
  },
  set (_, property, value) {
    return Reflect.set(getAjv(), property, value)
  },
  has (_, property) {
    return Reflect.has(getAjv(), property)
  },
  ownKeys () {
    return Reflect.ownKeys(getAjv())
  },
  getOwnPropertyDescriptor (_, property) {
    const descriptor = Reflect.getOwnPropertyDescriptor(getAjv(), property)

    // The target of the proxy does not have the property, so it cannot be reported as not configurable
    return descriptor && { ...descriptor, configurable: true }
  },
  defineProperty (_, property, descriptor) {
    return Reflect.defineProperty(getAjv(), property, descriptor)
  },
  deleteProperty (_, property) {
    return Reflect.deleteProperty(getAjv(), property)
  },
  getPrototypeOf () {
    return Reflect.getPrototypeOf(getAjv())
  }
})

/*
  Adds to a generated validator the properties which ajv sets on the ones it compiles.

  The schema is the one the validator was generated from. What describes the compiled code is only available
  by compiling the schema, which is done if and when those properties are read.
*/
export function defineValidator<T = unknown> (validate: Function, getSchema: () => AnySchema): OptionsValidator<T> {
  let compiled: ValidateFunction<T> | undefined

  function getCompiled (): ValidateFunction<T> {
    compiled ??= getAjv().compile<T>(getSchema())
    return compiled
  }

  return Object.defineProperties(validate, {
    schema: { get: getSchema, enumerable: true, configurable: true },
    schemaEnv: { get: () => getCompiled().schemaEnv, enumerable: true, configurable: true },
    source: { get: () => getCompiled().source, enumerable: true, configurable: true }
  }) as OptionsValidator<T>
}
