import { deepStrictEqual } from 'node:assert'
import test from 'node:test'
import { murmur2 } from '../../src/protocol/murmur2.ts'

// Sample copied from https://github.com/tulios/kafkajs/blob/55b0b416308b9e597a5a6b97b0a6fd6b846255dc/src/producer/partitioners/default/murmur2.spec.js
const samples = {
  0: 971027396,
  1: -1993445489,
  128: -326012175,
  2187: -1508407203,
  16384: -325739742,
  78125: -1654490814,
  279936: 1462227128,
  823543: -2014198330,
  2097152: 607668903,
  4782969: -1182699775,
  10000000: -1830336757,
  19487171: -1603849305,
  35831808: -857013643,
  62748517: -1167431028,
  105413504: -381294639,
  170859375: -1658323481,
  '100:48069': 1009543857
}

for (const [input, output] of Object.entries(samples)) {
  test(`perform murmur2 computations - ${input}`, () => {
    deepStrictEqual(murmur2(input), output, `${input} - ${output}`)
  })
}
