import { ArtifactNotFoundError, DefaultArtifactClient } from '@actions/artifact'
import { readdir } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { setTimeout } from 'node:timers/promises'

async function collectFiles (directory) {
  const files = []

  for (const entry of await readdir(directory, { withFileTypes: true })) {
    if (entry.name.startsWith('.')) {
      continue
    }

    const path = join(directory, entry.name)

    if (entry.isDirectory()) {
      files.push(...(await collectFiles(path)))
    } else if (entry.isFile()) {
      files.push(path)
    }
  }

  return files
}

export async function uploadArtifacts (name, directory, client = new DefaultArtifactClient()) {
  if (!name || !directory) {
    throw new Error('Artifact name and directory are required')
  }

  const root = resolve(directory)
  let files

  try {
    files = await collectFiles(root)
  } catch (error) {
    if (error.code !== 'ENOENT') {
      throw error
    }

    files = []
  }

  if (!files.length) {
    console.log(`No files to upload in ${root}`)
    return
  }

  for (let attempt = 1; attempt <= 5; attempt++) {
    console.log(`Uploading ${name} (attempt ${attempt}/5)`)

    try {
      // Remove any artifact left by an earlier, partially successful attempt.
      if (attempt > 1) {
        try {
          await client.deleteArtifact(name)
        } catch (error) {
          if (!(error instanceof ArtifactNotFoundError)) {
            throw error
          }
        }
      }

      return await client.uploadArtifact(name, files, root)
    } catch (error) {
      console.warn(`Artifact upload attempt ${attempt}/5 failed: ${error.message}`)

      if (attempt === 5) {
        throw error
      }

      await setTimeout(5000)
    }
  }
}
