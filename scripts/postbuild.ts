#!/usr/bin/env -S node

import { readFile, writeFile } from 'node:fs/promises'
import { resolve } from 'node:path'

const { name, version } = JSON.parse(await readFile(resolve(process.cwd(), 'package.json'), 'utf-8'))

const file = import.meta.url.includes('dist')
  ? new URL('../src/version.js', import.meta.url)
  : new URL('../dist/version.js', import.meta.url)

// To address https://github.com/platformatic/kafka/issues/91
await writeFile(file, `export const name = "${name}";\nexport const version = "${version}";\n`, 'utf-8')
