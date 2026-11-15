import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { ajv, checkValidators } from '../../scripts/generate-validators.ts'

test('the generated validators should match the schemas', async () => {
  // If this fails, run: npm run generate:validators
  deepStrictEqual(await checkValidators(), [])
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
