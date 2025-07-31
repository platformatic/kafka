#!/usr/bin/env -S node --experimental-strip-types --disable-warning=ExperimentalWarning

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
  values: { version, configuration }
} = parseArgs({
  args: scriptArgs,
  options: {
    version: { type: 'string', short: 'v', default: '3.9.0' },
    configuration: { type: 'string', short: 'c', default: 'local' }
  }
})

dockerComposeArgs.unshift('-f', `compose-${configuration}.yml`)
const bin = execSync('which docker-compose', { encoding: 'utf8' }).trim()
console.log(`Executing \x1b[1mKAFKA_VERSION=${version} ${[bin, ...dockerComposeArgs].join(' ')}\x1b[22m ...\x1b[0m`)

process.chdir(resolve(import.meta.dirname, '../docker'))
// @ts-expect-error Types not update to date
process.execve(bin, [bin, ...dockerComposeArgs], { ...process.env, KAFKA_VERSION: version })
