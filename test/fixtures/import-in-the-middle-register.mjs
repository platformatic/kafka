import { register } from 'node:module'
import { addHook } from 'import-in-the-middle'

register('import-in-the-middle/hook.mjs', import.meta.url)

addHook((_name, exports) => exports)
