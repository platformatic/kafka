import { deepStrictEqual, ok, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { Ajv2020 } from 'ajv/dist/2020.js'
import { execFileSync } from 'node:child_process'
import { checkValidators, sources } from '../../scripts/generate-validators.ts'
import * as api from '../../src/index.ts'
import { ajv, type OptionsValidator } from '../../src/index.ts'

test('the generated validators should match the schemas', async () => {
  const outdated = await checkValidators()

  deepStrictEqual(
    outdated,
    [],
    `The generated validators of these clients are out of date: ${outdated.join(', ')}. Run: npm run generate:validators`
  )
})

test('ajv setup', () => {
  {
    const schema = {
      type: 'object',
      properties: {
        value: { bigint: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ value: 42n }), true)
    strictEqual(validate({ value: 42 }), false)
  }

  {
    const schema = {
      type: 'object',
      properties: {
        value: { map: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ value: new Map() }), true)
    strictEqual(validate({ value: 42 }), false)
  }

  {
    const schema = {
      type: 'object',
      properties: {
        callback: { function: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ callback: () => {} }), true)
    strictEqual(validate({ callback: 42 }), false)
  }

  {
    const schema = {
      type: 'object',
      properties: {
        value: { buffer: true }
      }
    }

    const validate = ajv.compile(schema)
    strictEqual(validate({ value: Buffer.alloc(0) }), true)
    strictEqual(validate({ value: 42 }), false)
  }
})

test('ajv setup - properties comparison', () => {
  const validate = ajv.compile({
    type: 'object',
    properties: {
      min: { type: 'number' },
      max: { type: 'number', gteProperty: 'min' },
      low: { type: 'number', lteProperty: 'max' }
    }
  })

  strictEqual(validate({ min: 1, max: 2 }), true)
  strictEqual(validate({ min: 2, max: 2 }), true)
  strictEqual(validate({ min: 3, max: 2 }), false)
  strictEqual(validate.errors![0].message, 'must be greater than or equal to $dataVar$/min')
  strictEqual(validate.errors![0].instancePath, '/max')

  strictEqual(validate({ min: 0, low: 1, max: 2 }), true)
  strictEqual(validate({ min: 0, low: 2, max: 2 }), false)
  strictEqual(validate.errors![0].message, 'must be less than or equal to $dataVar$/max')
})

test('ajv setup - enumeration', () => {
  const validate = ajv.compile({
    type: 'object',
    properties: {
      value: { enumeration: { allowed: ['a', 1], errorMessage: 'should be one of a or 1' } }
    }
  })

  strictEqual(validate({ value: 'a' }), true)
  strictEqual(validate({ value: 1 }), true)
  strictEqual(validate({ value: 'b' }), false)
  strictEqual(validate.errors![0].message, 'should be one of a or 1')
})

test('the generated validators should expose what the ones compiled by ajv expose', () => {
  const exported = api as unknown as Record<string, unknown>
  let count = 0

  for (const validators of Object.values(sources)) {
    for (const [name, schema] of Object.entries(validators)) {
      const validator = exported[name] as OptionsValidator
      const compiled = ajv.compile(exported[schema] as object)

      strictEqual(typeof validator, 'function', name)
      strictEqual(validator.schema, exported[schema], name)
      // The generated code has no properties whose value is undefined
      deepStrictEqual(validator.evaluated, JSON.parse(JSON.stringify(compiled.evaluated)), name)
      strictEqual(validator.schemaEnv.schema, exported[schema], name)
      strictEqual(validator.schemaEnv, validator.schemaEnv, name)
      strictEqual(validator.source, undefined, name)
      count++
    }
  }

  strictEqual(count, 40)
})

test('ajv should behave as an instance of Ajv2020', () => {
  ok(ajv instanceof Ajv2020)
  strictEqual(ajv.constructor, Ajv2020)
  strictEqual(ajv.compile, ajv.compile)
  ok('opts' in ajv)
  ok(Object.keys(ajv).includes('opts'))
  strictEqual(ajv.opts.allErrors, true)
  strictEqual(Object.getOwnPropertyDescriptor(ajv, 'opts')!.value, ajv.opts)

  // Methods are bound to the real instance
  const { compile, errorsText } = ajv
  const validate = compile({ type: 'object', properties: { value: { bigint: true } } })
  strictEqual(validate({ value: 1n }), true)
  strictEqual(validate({ value: 1 }), false)
  strictEqual(errorsText(validate.errors), 'data/value must be bigint')

  // It can be modified as the real instance
  ajv.addKeyword({ keyword: 'testOnly', schemaType: 'boolean' })
  ok(ajv.getKeyword('testOnly'))
  ajv.removeKeyword('testOnly')
  strictEqual(ajv.getKeyword('testOnly'), false)

  const extended = ajv as unknown as Record<string, unknown>
  extended.testOnly = 1
  strictEqual(extended.testOnly, 1)
  Object.defineProperty(extended, 'testOnly', { value: 2, configurable: true })
  strictEqual(extended.testOnly, 2)
  delete extended.testOnly
  strictEqual('testOnly' in extended, false)
})

test('ajv should only be loaded when it is used', () => {
  const script = `
    import { createRequire } from 'node:module'
    const { ajv, baseOptionsValidator } = await import('./src/index.ts')

    function loaded () {
      return Object.keys(createRequire(import.meta.url).cache).some(file => /node_modules.ajv./.test(file))
    }

    baseOptionsValidator({ clientId: 'id' })
    const schema = baseOptionsValidator.schema.type
    const before = loaded()
    const environment = typeof baseOptionsValidator.schemaEnv
    const after = loaded()
    console.log(JSON.stringify({ schema, before, environment, after, valid: ajv.validate({ type: 'string' }, 'a') }))
  `

  const output = execFileSync(process.execPath, ['--input-type=module', '-e', script], {
    cwd: new URL('../..', import.meta.url),
    encoding: 'utf-8'
  })

  deepStrictEqual(JSON.parse(output), { schema: 'object', before: false, environment: 'object', after: true, valid: true })
})
