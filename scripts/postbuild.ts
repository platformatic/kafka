#!/usr/bin/env -S node

import generate from '@babel/generator'
import { parse } from '@babel/parser'
import traverse from '@babel/traverse'
import { type ExportAllDeclaration, type Identifier } from '@babel/types'
import { glob, mkdir, readFile, writeFile } from 'node:fs/promises'
import { dirname, resolve } from 'node:path'

async function generateVersion () {
  const { name, version } = JSON.parse(await readFile(resolve(process.cwd(), 'package.json'), 'utf-8'))

  const file = import.meta.url.includes('dist')
    ? new URL('../src/version.js', import.meta.url)
    : new URL('../dist/version.js', import.meta.url)

  // To address https://github.com/platformatic/kafka/issues/91
  await writeFile(file, `export const name = "${name}";\nexport const version = "${version}";\n`, 'utf-8')
}

async function dropTsExtensionsInDeclarations () {
  const distDir = resolve(process.cwd(), 'dist')

  // First of all, gather all .d.ts files
  const dtsFiles = await glob('**/*.d.ts', { cwd: distDir })

  for await (const file of dtsFiles) {
    const source = await readFile(resolve(distDir, file), 'utf-8')
    const destination = resolve(distDir, 'typescript-4/dist', file)
    const ast = parse(source, { sourceType: 'module', plugins: ['typescript'] })

    traverse.default(ast, {
      // Do not use .ts extensions in import/export statements
      ImportDeclaration (path) {
        if (path.node.source.value.endsWith('.ts')) {
          path.node.source.value = path.node.source.value.slice(0, -3)
        }
      },
      ExportDeclaration (path) {
        const node = path.node as ExportAllDeclaration
        if (node.source?.value.endsWith('.ts')) {
          node.source.value = node.source.value.slice(0, -3)
        }
      },
      TSImportType (path) {
        if (path.node.argument?.value.endsWith('.ts')) {
          path.node.argument.value = path.node.argument.value.slice(0, -3)
        }
      },
      // Buffer<T> to Buffer
      TSTypeReference (path) {
        const node = path.node

        if ((node.typeName as Identifier).name === 'Buffer') {
          node.typeParameters = null
        }
      }
    })

    await mkdir(dirname(destination), { recursive: true })
    await writeFile(destination, generate.default(ast).code, 'utf-8')
  }
}

await generateVersion()
await dropTsExtensionsInDeclarations()
