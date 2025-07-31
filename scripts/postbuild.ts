#!/usr/bin/env -S node --experimental-strip-types --disable-warning=ExperimentalWarning

import { writeFile } from 'node:fs/promises'
import { name, version } from '../src/version.ts'

// To address https://github.com/platformatic/kafka/issues/91
await writeFile(
  new URL('../dist/version.js', import.meta.url),
  `export const name = "${name}";\nexport const version = "${version}";\n`,
  'utf-8'
)
