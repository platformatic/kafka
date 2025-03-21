import neostandard from 'neostandard'

const eslint = [
  ...neostandard({ ts: true }),
  {
    files: ['**/*.ts'],
    rules: {
      '@typescript-eslint/consistent-type-imports': ['error', { fixStyle: 'inline-type-imports' }]
    }
  }
]

export default eslint
