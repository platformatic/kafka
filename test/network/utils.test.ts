import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { parseBroker } from '../../src/index.ts'

test('parseBroker should parse host:port strings', () => {
  deepStrictEqual(parseBroker('localhost:9092'), { host: 'localhost', port: 9092 })
  deepStrictEqual(parseBroker('127.0.0.1:19092'), { host: '127.0.0.1', port: 19092 })
})

test('parseBroker should use the default port when none is provided', () => {
  deepStrictEqual(parseBroker('localhost'), { host: 'localhost', port: 9092 })
  deepStrictEqual(parseBroker('127.0.0.1', 19092), { host: '127.0.0.1', port: 19092 })
})

test('parseBroker should parse bracketed IPv6 addresses', () => {
  deepStrictEqual(parseBroker('[::1]:9092'), { host: '::1', port: 9092 })
  deepStrictEqual(parseBroker('[2001:db8::1]:19092'), { host: '2001:db8::1', port: 19092 })
  deepStrictEqual(parseBroker('[::ffff:192.0.2.1]:9092'), { host: '::ffff:192.0.2.1', port: 9092 })
  deepStrictEqual(parseBroker('[fe80::1%lo0]:9092'), { host: 'fe80::1%lo0', port: 9092 })
})

test('parseBroker should use the default port for bracketed IPv6 hosts without a port', () => {
  deepStrictEqual(parseBroker('[::1]'), { host: '::1', port: 9092 })
  deepStrictEqual(parseBroker('[2001:db8::1]', 19092), { host: '2001:db8::1', port: 19092 })
})

test('parseBroker should treat unbracketed IPv6 addresses as host-only', () => {
  deepStrictEqual(parseBroker('::1'), { host: '::1', port: 9092 })
  deepStrictEqual(parseBroker('2001:db8::1'), { host: '2001:db8::1', port: 9092 })
  deepStrictEqual(parseBroker('2001:db8::1:9092', 19092), { host: '2001:db8::1:9092', port: 19092 })
})

test('parseBroker should return broker objects unchanged when the host has no brackets', () => {
  const broker = { host: 'localhost', port: 9092 }

  strictEqual(parseBroker(broker), broker)
})

test('parseBroker should strip brackets from broker object hosts', () => {
  deepStrictEqual(parseBroker({ host: '[::1]', port: 9092 }), { host: '::1', port: 9092 })
  deepStrictEqual(parseBroker({ host: '[2001:db8::1]', port: 19092 }), { host: '2001:db8::1', port: 19092 })
})
