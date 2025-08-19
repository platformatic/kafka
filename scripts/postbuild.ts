#!/usr/bin/env -S node

import { writeFile } from 'node:fs/promises'
import { name, version } from '../src/version.ts'

const file = import.meta.url.endsWith('/dist/scripts/postbuild.js')
  ? new URL('version.js', import.meta.url)
  : new URL('../dist/version.js', import.meta.url)

// To address https://github.com/platformatic/kafka/issues/91
await writeFile(
  file,
  `export const name = "${name}";\nexport const version = "${version}";\n`,
  'utf-8'
)
