import { type Callback } from '../apis/definitions.ts'
import {
  type BeforeDeserializationHook,
  type BeforeHookPayloadType,
  type BeforeSerializationHook,
  type Deserializers,
  type Serializers
} from '../clients/serde.ts'
import { type MessageToProduce } from '../protocol/records.ts'

export interface SchemaRegistry<
  Id = unknown,
  Schema = unknown,
  Key = Buffer,
  Value = Buffer,
  HeaderKey = Buffer,
  HeaderValue = Buffer
> {
  get (id: Id): Schema | undefined
  fetch (id: Id, callback?: (error?: Error) => void): void | Promise<void>

  getSchemaId (payload: Buffer | MessageToProduce<Key, Value, HeaderKey, HeaderValue>, type?: BeforeHookPayloadType): Id

  getSerializers (): Serializers<Key, Value, HeaderKey, HeaderValue>
  getDeserializers (): Deserializers<Key, Value, HeaderKey, HeaderValue>

  getBeforeSerializationHook (): BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue>
  getBeforeDeserializationHook (): BeforeDeserializationHook
}

export function runAsyncSeries<V> (
  operation: (item: V, cb: Callback<void>) => void | Promise<void>,
  collection: V[],
  index: number,
  callback: Callback<void>
): void {
  operation(collection[index], error => {
    if (error) {
      callback(error)
      return
    } else if (index === collection.length - 1) {
      callback(null)
      return
    }

    runAsyncSeries(operation, collection, index + 1, callback)
  })
}

/* c8 ignore start */
export class AbstractSchemaRegistry<
  Id = unknown,
  Schema = unknown,
  Key = Buffer,
  Value = Buffer,
  HeaderKey = Buffer,
  HeaderValue = Buffer
> implements SchemaRegistry<Id, Schema, Key, Value, HeaderKey, HeaderValue> {
  get (_: Id): Schema | undefined {
    throw new Error('AbstractSchemaRegistry.get() should be implemented in subclasses.')
  }

  fetch (_i: unknown, _c?: (error?: Error) => void): void | Promise<void> {
    throw new Error('AbstractSchemaRegistry.fetch() should be implemented in subclasses.')
  }

  getSchemaId (_p: Buffer | MessageToProduce<Key, Value, HeaderKey, HeaderValue>, _t?: BeforeHookPayloadType): Id {
    throw new Error('AbstractSchemaRegistry.getSchemaId() should be implemented in subclasses.')
  }

  getSerializers (): Serializers<Key, Value, HeaderKey, HeaderValue> {
    throw new Error('AbstractSchemaRegistry.getSerializers() should be implemented in subclasses.')
  }

  getDeserializers (): Deserializers<Key, Value, HeaderKey, HeaderValue> {
    throw new Error('AbstractSchemaRegistry.getDeserializers() should be implemented in subclasses.')
  }

  getBeforeSerializationHook (): BeforeSerializationHook<Key, Value, HeaderKey, HeaderValue> {
    throw new Error('AbstractSchemaRegistry.getBeforeSerializationHook() should be implemented in subclasses.')
  }

  getBeforeDeserializationHook (): BeforeDeserializationHook {
    throw new Error('AbstractSchemaRegistry.getBeforeDeserializationHook() should be implemented in subclasses.')
  }
}
/* c8 ignore end */
