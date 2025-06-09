import { styleText } from 'node:util'
import { table } from 'table'
import * as supportedApis from '../src/apis/index.ts'
import { Base } from '../src/index.ts'

function formatRange (min: number, max: number): string {
  return min !== max
    ? `${styleText('bold', min.toString())} to ${styleText('bold', max!.toString())}`
    : styleText('bold', min.toString())
}

async function main (): Promise<void> {
  const client = new Base({ clientId: 'id', bootstrapBrokers: [process.argv[2]], retries: 0, strict: true })
  const actualApis = await client.listApis()

  const rows: string[][] = [
    [
      styleText('bold', 'Name'),
      styleText('bold', 'Supported'),
      styleText('bold', 'Selected version'),
      styleText('bold', 'To implement'),
      styleText('bold', 'Supported from us'),
      styleText('bold', 'Supported from the broker')
    ]
  ]

  for (const { name, minVersion, maxVersion } of actualApis) {
    // First of all, all matching APIs
    const apiPrefix = ((name.slice(0, 1).toLowerCase() + name.slice(1)) as keyof typeof supportedApis) + 'V'

    // We do reverse sort as we want to find the highest version first
    const supported = Object.keys(supportedApis)
      .filter(key => key.startsWith(apiPrefix))
      .map(k => parseInt(k.match(/\d+$/)?.[0]!))
      .sort((a, b) => b - a)

    const minSupportedVersion = supported.at(-1)
    const maxSupportedVersion = supported.at(0)
    const selectedVersion = supported.find(v => v >= minVersion && v <= maxVersion)

    if (typeof selectedVersion === 'number') {
      rows.push([
        styleText('bold', name),
        styleText(['bold', 'green'], 'Yes'),
        styleText('bold', selectedVersion!.toString()),
        '',
        '',
        ''
      ])
    } else {
      rows.push([
        styleText('bold', name),
        styleText(['bold', 'red'], 'No'),
        '',
        typeof minSupportedVersion === 'number'
          ? formatRange(maxVersion, minSupportedVersion! - 1)
          : formatRange(maxVersion!, maxVersion!),
        typeof minSupportedVersion === 'number'
          ? formatRange(minSupportedVersion!, maxSupportedVersion!)
          : styleText(['bold', 'red'], 'Unsupported'),
        formatRange(minVersion!, maxVersion!)
      ])
    }

    if (typeof selectedVersion === 'number') {
      process.exitCode = 1
    }
  }

  console.log(table(rows, { drawHorizontalLine: (index: number, size: number) => index < 2 || index >= size }))
  await client.close()
}

await main()
