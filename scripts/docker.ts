#!/usr/bin/env -S node

import { execSync } from 'node:child_process'
import { resolve } from 'node:path'
import { parseArgs } from 'node:util'

const args = process.argv.slice(2)

const lastArg = args.indexOf('--')

let scriptArgs: string[] = []
let dockerComposeArgs: string[] = []

if (lastArg !== -1) {
  scriptArgs = args.slice(0, lastArg)
  dockerComposeArgs = args.slice(lastArg + 1)
} else {
  dockerComposeArgs = args
}

const {
  values: { version }
} = parseArgs({
  args: scriptArgs,
  options: {
    // Rule of thumb: Confluent Kafka Version = Apache Kafka Version + 4.0.0
    version: { type: 'string', short: 'v', default: '7.9.0' }
  }
})

dockerComposeArgs.unshift('-f', 'compose.yml')
const bin = execSync('which docker-compose', { encoding: 'utf8' }).trim()
console.log(`Executing \x1b[1mKAFKA_VERSION=${version} ${[bin, ...dockerComposeArgs].join(' ')}\x1b[22m ...\x1b[0m`)

process.chdir(resolve(import.meta.dirname, '../docker'))
// @ts-expect-error Types not update to date
process.execve(bin, [bin, ...dockerComposeArgs], { ...process.env, KAFKA_VERSION: version })
