import { deepStrictEqual, ok } from 'node:assert'
import test from 'node:test'
import * as telemetryAPIs from '../../../src/apis/telemetry/index.ts'

test('telemetry API exports are correctly defined', () => {
  // Check that all telemetry APIs are exported
  ok('getTelemetrySubscriptionsV0' in telemetryAPIs, 'getTelemetrySubscriptionsV0 should be exported')
  ok('listClientMetricsResourcesV0' in telemetryAPIs, 'listClientMetricsResourcesV0 should be exported')
  ok('pushTelemetryV0' in telemetryAPIs, 'pushTelemetryV0 should be exported')
  
  // Verify that each API is a function
  deepStrictEqual(typeof telemetryAPIs.getTelemetrySubscriptionsV0, 'function')
  deepStrictEqual(typeof telemetryAPIs.listClientMetricsResourcesV0, 'function')
  deepStrictEqual(typeof telemetryAPIs.pushTelemetryV0, 'function')
  
  // Verify each API has async capability
  ok('async' in telemetryAPIs.getTelemetrySubscriptionsV0, 'getTelemetrySubscriptionsV0 should have async property')
  ok('async' in telemetryAPIs.listClientMetricsResourcesV0, 'listClientMetricsResourcesV0 should have async property')
  ok('async' in telemetryAPIs.pushTelemetryV0, 'pushTelemetryV0 should have async property')
})