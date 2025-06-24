import { ESLint } from 'eslint'
import { format } from 'prettier'

export async function formatOutput (output: string, filePath: string): Promise<string> {
  // Format with prettier
  output = await format(output, {
    parser: 'typescript',
    printWidth: 120,
    semi: false,
    singleQuote: true,
    bracketSpacing: true,
    trailingComma: 'none',
    arrowParens: 'avoid'
  })

  // Lint with eslint
  const eslint = new ESLint({ fix: true })
  const [result] = await eslint.lintText(output, { filePath })

  return result.output ?? result.source! ?? output
}
