import { createRequire } from 'node:module'
import type { API } from './definitions.ts'
import { apiLoaders, apiPaths } from './loaders.ts'

/*
  The codecs are loaded on demand, once a client has negotiated which version of an API to use.

  They are loaded synchronously with require(), so that whoever asks for a codec gets it in the same tick.
  Under webpack, use import() even if the original files are still present: require() could load
  a second copy of the module graph outside the bundle.
*/
type Codec = { api: API<any, any> }
// eslint-disable-next-line camelcase -- Webpack injects this identifier.
declare const __webpack_require__: unknown

// The sources are TypeScript files, which is how they are run by the tests
const extension = import.meta.url.endsWith('.ts') ? '.ts' : '.js'
const loadedApis = new Map<string, API<any, any>>()
let require: NodeJS.Require | undefined
let deprecationWarned = false

function requireCodec<T extends object = Codec> (path: string): T {
  require ??= createRequire(import.meta.url)
  return require(path + extension)
}

// Returns undefined when the codec does not exist or when it can only be loaded asynchronously
export function getApi (name: string): API<any, any> | undefined {
  let api = loadedApis.get(name)

  // eslint-disable-next-line camelcase -- Webpack injects this identifier.
  const isWebpack = typeof __webpack_require__ !== 'undefined'
  if (!api && !isWebpack && typeof apiPaths[name] === 'string') {
    try {
      api = requireCodec(apiPaths[name]).api
      loadedApis.set(name, api)
      /* c8 ignore next 3 - Only reachable when the package is bundled */
    } catch {
      // Fall back to import()
    }
  }

  return api
}

/* c8 ignore next 6 - Only reachable when the package is bundled */
export async function loadApi (name: string): Promise<API<any, any>> {
  const { api } = await apiLoaders[name]()
  loadedApis.set(name, api)

  return api
}

/*
  Creates an object which behaves like the module at the given path, but only loads it the first time
  any of its properties is accessed. It is used to keep the codecs available from the main entry point.
*/
export function lazy<T extends object> (path: string): T {
  const target = Object.create(null) as T
  let loaded = false

  function load (): T {
    if (!loaded) {
      if (!deprecationWarned) {
        deprecationWarned = true

        process.emitWarning(
          'Importing the API codecs from @platformatic/kafka is deprecated and will not be possible in the next major version. Import them from @platformatic/kafka/apis instead.',
          { type: 'DeprecationWarning', code: 'PLT_KFK_DEP_APIS' }
        )
      }

      // The properties of a module are not configurable, which the target of a proxy must reflect
      Object.defineProperties(target, Object.getOwnPropertyDescriptors(requireCodec<T>(path)))
      loaded = true
    }

    return target
  }

  return new Proxy(target, {
    get (_, property) {
      return Reflect.get(load(), property)
    },
    has (_, property) {
      return Reflect.has(load(), property)
    },
    ownKeys () {
      return Reflect.ownKeys(load())
    },
    getOwnPropertyDescriptor (_, property) {
      return Reflect.getOwnPropertyDescriptor(load(), property)
    }
  })
}
