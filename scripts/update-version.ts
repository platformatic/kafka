#!/usr/bin/env -S node --experimental-strip-types --disable-warning=ExperimentalWarning

import { execSync } from 'node:child_process'
import { readFile, writeFile } from 'node:fs/promises'

const defaultUser = 'mcollina'

const users: Record<string, [string, string]> = {
  mcollina: ['Matteo Collina', 'hello@matteocollina.com'],
  ShogunPanda: ['Paolo Insogna', 'paolo@cowtech.it']
}

const version = process.argv[2]!.replace(/^v/, '')
let username = process.argv[3]
let userInfo = users[username]

if (!userInfo) {
  username = defaultUser
  userInfo = users[defaultUser]
}

// Update package.json
const packageJson = JSON.parse(await readFile('package.json', 'utf8'))
packageJson.version = process.argv[2].replace(/^v/, '')
await writeFile('package.json', JSON.stringify(packageJson, null, 2))

// Commit changes
execSync(`git commit -a -m "chore: Bumped v${version}." -m "Signed-off-by: ${userInfo[0]} <${userInfo[1]}>"`)
