import { deepStrictEqual, ok, throws } from 'node:assert'
import test from 'node:test'
import { Reader, ResponseError, Writer, updateFeaturesV1 } from '../../../src/index.ts'

const { createRequest, parseResponse } = updateFeaturesV1

test('createRequest serializes basic parameters correctly', () => {
  const timeoutMs = 30000
  const featureUpdates = [
    {
      feature: 'test-feature',
      maxVersionLevel: 2,
      upgradeType: 1
    }
  ]
  const validateOnly = false

  const writer = createRequest(timeoutMs, featureUpdates, validateOnly)

  // Verify it returns a Writer
  ok(writer instanceof Writer, 'Should return a Writer instance')

  // Read the serialized data to verify correctness
  const reader = Reader.from(writer)

  // Read timeoutMs
  const serializedTimeoutMs = reader.readInt32()

  // Read featureUpdates array
  const serializedFeatureUpdates = reader.readArray(() => {
    const feature = reader.readString()
    const maxVersionLevel = reader.readInt16()
    const upgradeType = reader.readInt8()
    return { feature, maxVersionLevel, upgradeType }
  })

  // Read validateOnly flag
  const serializedValidateOnly = reader.readBoolean()

  // Verify serialized data
  deepStrictEqual(serializedTimeoutMs, 30000, 'Timeout should be serialized correctly')

  deepStrictEqual(
    serializedFeatureUpdates,
    [
      {
        feature: 'test-feature',
        maxVersionLevel: 2,
        upgradeType: 1
      }
    ],
    'Feature updates should be serialized correctly'
  )

  deepStrictEqual(serializedValidateOnly, false, 'ValidateOnly flag should be serialized correctly')
})

test('createRequest serializes empty feature updates array correctly', () => {
  const timeoutMs = 30000
  const featureUpdates: [] = []
  const validateOnly = false

  const writer = createRequest(timeoutMs, featureUpdates, validateOnly)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read featureUpdates array
  const serializedFeatureUpdates = reader.readArray(() => {
    const feature = reader.readString()
    const maxVersionLevel = reader.readInt16()
    const upgradeType = reader.readInt8()
    return { feature, maxVersionLevel, upgradeType }
  })

  // Verify empty array
  deepStrictEqual(serializedFeatureUpdates, [], 'Empty feature updates array should be serialized correctly')
})

test('createRequest serializes multiple feature updates correctly', () => {
  const timeoutMs = 30000
  const featureUpdates = [
    {
      feature: 'feature-1',
      maxVersionLevel: 1,
      upgradeType: 0
    },
    {
      feature: 'feature-2',
      maxVersionLevel: 2,
      upgradeType: 1
    },
    {
      feature: 'feature-3',
      maxVersionLevel: 3,
      upgradeType: 2
    }
  ]
  const validateOnly = false

  const writer = createRequest(timeoutMs, featureUpdates, validateOnly)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Read featureUpdates array
  const serializedFeatureUpdates = reader.readArray(() => {
    const feature = reader.readString()
    const maxVersionLevel = reader.readInt16()
    const upgradeType = reader.readInt8()
    return { feature, maxVersionLevel, upgradeType }
  })

  // Verify multiple features
  deepStrictEqual(
    serializedFeatureUpdates,
    [
      {
        feature: 'feature-1',
        maxVersionLevel: 1,
        upgradeType: 0
      },
      {
        feature: 'feature-2',
        maxVersionLevel: 2,
        upgradeType: 1
      },
      {
        feature: 'feature-3',
        maxVersionLevel: 3,
        upgradeType: 2
      }
    ],
    'Multiple feature updates should be serialized correctly'
  )
})

test('createRequest serializes validateOnly flag correctly', () => {
  const timeoutMs = 30000
  const featureUpdates = [
    {
      feature: 'test-feature',
      maxVersionLevel: 2,
      upgradeType: 1
    }
  ]
  const validateOnly = true

  const writer = createRequest(timeoutMs, featureUpdates, validateOnly)
  const reader = Reader.from(writer)

  // Skip timeoutMs
  reader.readInt32()

  // Skip featureUpdates array
  reader.readArray(() => {
    reader.readString()
    reader.readInt16()
    reader.readInt8()
    return {}
  })

  // Read validateOnly flag
  const serializedValidateOnly = reader.readBoolean()

  // Verify validateOnly flag
  deepStrictEqual(serializedValidateOnly, true, 'ValidateOnly flag true should be serialized correctly')
})

test('parseResponse correctly processes a successful empty response', () => {
  // Create a successful response with no results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  const response = parseResponse(1, 57, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(
    response,
    {
      throttleTimeMs: 0,
      errorCode: 0,
      errorMessage: null,
      results: []
    },
    'Empty response should match expected structure'
  )
})

test('parseResponse correctly processes a successful response with results', () => {
  // Create a successful response with results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          feature: 'test-feature',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.feature).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 57, 1, Reader.from(writer))

  // Verify response structure
  deepStrictEqual(response.results.length, 1, 'Should have one result')

  const result = response.results[0]
  deepStrictEqual(result.feature, 'test-feature', 'Feature name should be parsed correctly')
  deepStrictEqual(result.errorCode, 0, 'Result error code should be parsed correctly')
  deepStrictEqual(result.errorMessage, null, 'Result error message should be parsed correctly')
})

test('parseResponse correctly processes multiple results', () => {
  // Create a response with multiple results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray(
      [
        {
          feature: 'feature-1',
          errorCode: 0,
          errorMessage: null
        },
        {
          feature: 'feature-2',
          errorCode: 0,
          errorMessage: null
        }
      ],
      (w, result) => {
        w.appendString(result.feature).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  const response = parseResponse(1, 57, 1, Reader.from(writer))

  // Verify multiple results
  deepStrictEqual(response.results.length, 2, 'Should have two results')

  // Verify feature names
  deepStrictEqual(
    response.results.map(r => r.feature),
    ['feature-1', 'feature-2'],
    'Feature names should be parsed correctly'
  )
})

test('parseResponse throws ResponseError on top-level error response', () => {
  // Create a response with a top-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(42) // errorCode FEATURE_UPDATE_FAILED
    .appendString('Feature update failed') // errorMessage
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 57, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify error details
      deepStrictEqual(err.response.errorCode, 42, 'Error code should be preserved in the response')

      deepStrictEqual(
        err.response.errorMessage,
        'Feature update failed',
        'Error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse throws ResponseError on result-level error response', () => {
  // Create a response with a result-level error
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // No top-level error
    .appendString(null) // No top-level error message
    .appendArray(
      [
        {
          feature: 'test-feature',
          errorCode: 42, // FEATURE_UPDATE_FAILED
          errorMessage: 'Feature update failed'
        }
      ],
      (w, result) => {
        w.appendString(result.feature).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 57, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify result-level error
      deepStrictEqual(err.response.results[0].errorCode, 42, 'Result error code should be preserved in the response')

      deepStrictEqual(
        err.response.results[0].errorMessage,
        'Feature update failed',
        'Result error message should be preserved in the response'
      )

      return true
    }
  )
})

test('parseResponse throws ResponseError with mixed success and error results', () => {
  // Create a response with mixed success and error results
  const writer = Writer.create()
    .appendInt32(0) // throttleTimeMs
    .appendInt16(0) // No top-level error
    .appendString(null) // No top-level error message
    .appendArray(
      [
        {
          feature: 'feature-1',
          errorCode: 0, // Success
          errorMessage: null
        },
        {
          feature: 'feature-2',
          errorCode: 42, // FEATURE_UPDATE_FAILED
          errorMessage: 'Feature update failed'
        }
      ],
      (w, result) => {
        w.appendString(result.feature).appendInt16(result.errorCode).appendString(result.errorMessage)
      }
    )
    .appendTaggedFields()

  // Verify that parsing throws ResponseError
  throws(
    () => {
      parseResponse(1, 57, 1, Reader.from(writer))
    },
    (err: any) => {
      ok(err instanceof ResponseError, 'Should be a ResponseError')

      // Verify successful result is preserved
      deepStrictEqual(err.response.results[0].errorCode, 0, 'Success error code should be preserved')

      // Verify error result is preserved
      deepStrictEqual(err.response.results[1].errorCode, 42, 'Error code should be preserved')

      deepStrictEqual(err.response.results[1].feature, 'feature-2', 'Feature name should be preserved for error result')

      return true
    }
  )
})

test('parseResponse handles throttle time correctly', () => {
  // Create a response with non-zero throttle time
  const throttleTimeMs = 5000
  const writer = Writer.create()
    .appendInt32(throttleTimeMs) // throttleTimeMs
    .appendInt16(0) // errorCode
    .appendString(null) // errorMessage
    .appendArray([], () => {}) // Empty results array
    .appendTaggedFields()

  const response = parseResponse(1, 57, 1, Reader.from(writer))

  // Verify throttle time
  deepStrictEqual(response.throttleTimeMs, throttleTimeMs, 'Throttle time should be parsed correctly')
})
