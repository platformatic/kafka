# Playgrounds files documentations

Each of the file in the `playground` is meant to be invoked via shell unless documented differently.

```
node playground/clients/admin-groups.ts
```

## Admin API

### Topic management - Single broker

The `playground/clients/admin-topics-single.ts` can be used to ensure the presence of `temp1` and `temp2` topics, used in other scripts.

### Topic management - Multiple brokers

The `playground/clients/admin-topics-multiple.ts` can be used to ensure the presence of `temp1` and `temp2` topics, used in other scripts.

It will try to specifically assigning partitions to different brokers (as specified in `docker-compose-multiple.yml`).

### Consumer group and rebalancing management

The `playground/clients/admin-groups.ts` will verify consumer group behavior by performing the following flow:

1. Create a consumer (will create group).
2. Create a second consumer (will trigger rebalance).
3. Close second consumer (will trigger rebalance).
4. List and describe groups.
5. Delete group (will create new group).
6. Close first consumer (will trigger rebalance).

## Producer API

All scripts in this API require the `playground/clients/admin-topics-single.ts` or `playground/clients/admin-topics-multiple.ts` to have been run first.

### Basic producer

The `playground/clients/producer.ts` will perform a series of 3 produce with 6 messages each.

### Idempotent producer

The `playground/clients/producer-idempotent.ts` will perform 4 produces of one message each. The produces are started 2 by 2 in parallel, but since Kafka limits the max inflights for idempotent producers to 1, you should see 4 different offsets.

### Permanent producer

The `playground/clients/producer-forever.ts` will perform continous producing of a message every 100ms. Each message has a increasing value for keys, values, header keys and header values. It can be stopped only via `Ctrl+C`.

## Consumer API

All scripts in this API require the `playground/clients/admin-topics-single.ts` or `playground/clients/admin-topics-multiple.ts` to have been run first.

### Basic consumer

The `playground/clients/consumer.ts` will consumer from a topic. Both `on('data')` and `for-await-of` are used.

### Consumer rebalance

The `playground/clients/consumer-rebalance.ts` will very assignments of topic and partitions by performing the following flow:

1. Create a consumer and subscribe to a topic.
2. Subscribe to a second topic (will trigger rebalance).
3. Create a second consumer (will trigger rebalance)
4. Close first consumer (will trigger rebalance).
5. Close second consumer.

## Low level APIs

The files in the `playground/apis` test low-level API directly.
