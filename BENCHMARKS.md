# Kafka Client Performance Benchmarks

This document contains performance benchmarks comparing `@platformatic/kafka` against other popular Kafka client libraries in the Node.js ecosystem.

## Test Environment

The benchmarks were conducted to evaluate the performance characteristics of different Kafka client libraries under various messaging patterns. All tests were performed under controlled conditions to ensure fair comparison.

### Libraries Tested

The following Kafka client libraries were included in the benchmark comparison:

- **@platformatic/kafka** - The highest performing library across all test scenarios
- **KafkaJS** - Popular Node.js Kafka client
- **node-rdkafka** - Node.js bindings for librdkafka
- **@confluentinc/kafka-javascript** - Confluent's official JavaScript client (with both KafkaJS and node-rdkafka backends)

These results demonstrate `@platformatic/kafka`'s superior performance characteristics, making it an excellent choice for high-throughput Kafka applications in Node.js environments.

## Producer Performance Benchmarks

### Single Message Per Iteration

This test measures producer throughput when sending one message per operation, with a total of 100,000 messages produced. This pattern simulates applications that send individual messages as events occur, representing a common real-world usage pattern.

**Test Configuration:**

- Messages per iteration: 1
- Total messages: 100,000
- Metric: Operations per second (op/sec)

```
╔═══════════════════════════════════════════════╤═════════╤═════════════════╤═══════════╤══════════════════════════╗
║ Slower tests                                  │ Samples │          Result │ Tolerance │ Difference with previous ║
╟───────────────────────────────────────────────┼─────────┼─────────────────┼───────────┼──────────────────────────╢
║ node-rdkafka                                  │  100000 │ 17452.32 op/sec │ ± 18.42 % │                          ║
║ @confluentinc/kafka-javascript (KafkaJS)      │  100000 │ 20215.32 op/sec │ ± 13.10 % │ + 15.83 %                ║
║ @confluentinc/kafka-javascript (node-rdkafka) │  100000 │ 20771.13 op/sec │ ± 12.69 % │ + 2.75 %                 ║
║ KafkaJS                                       │  100000 │ 61877.92 op/sec │ ±  0.67 % │ + 197.90 %               ║
╟───────────────────────────────────────────────┼─────────┼─────────────────┼───────────┼──────────────────────────╢
║ Fastest test                                  │ Samples │          Result │ Tolerance │ Difference with previous ║
╟───────────────────────────────────────────────┼─────────┼─────────────────┼───────────┼──────────────────────────╢
║ @platformatic/kafka                           │  100000 │ 95039.18 op/sec │ ±  0.71 % │ + 53.59 %                ║
╚═══════════════════════════════════════════════╧═════════╧═════════════════╧═══════════╧══════════════════════════╝
```

### Batch Message Production

This test measures producer throughput when sending 100 messages per operation, with a total of 100,000 messages produced. This pattern simulates high-throughput applications that batch messages for improved efficiency, which is common in data pipeline and ETL scenarios.

**Test Configuration:**

- Messages per iteration: 100
- Total messages: 100,000
- Metric: Operations per second (op/sec)

```
╔═══════════════════════════════════════════════╤═════════╤════════════════╤═══════════╤══════════════════════════╗
║ Slower tests                                  │ Samples │         Result │ Tolerance │ Difference with previous ║
╟───────────────────────────────────────────────┼─────────┼────────────────┼───────────┼──────────────────────────╢
║ node-rdkafka                                  │    1000 │  706.69 op/sec │ ± 69.72 % │                          ║
║ @confluentinc/kafka-javascript (KafkaJS)      │    1000 │ 2511.97 op/sec │ ±  1.00 % │ + 255.45 %               ║
║ @confluentinc/kafka-javascript (node-rdkafka) │    1000 │ 2568.78 op/sec │ ±  0.85 % │ + 2.26 %                 ║
║ KafkaJS                                       │    1000 │ 3144.13 op/sec │ ±  2.62 % │ + 22.40 %                ║
╟───────────────────────────────────────────────┼─────────┼────────────────┼───────────┼──────────────────────────╢
║ Fastest test                                  │ Samples │         Result │ Tolerance │ Difference with previous ║
╟───────────────────────────────────────────────┼─────────┼────────────────┼───────────┼──────────────────────────╢
║ @platformatic/kafka                           │    1000 │ 4488.69 op/sec │ ±  3.76 % │ + 42.76 %                ║
╚═══════════════════════════════════════════════╧═════════╧════════════════╧═══════════╧══════════════════════════╝
```

## Consumer Performance Benchmarks

### Message Consumption Throughput

This test measures consumer throughput when processing 100,000 messages. The benchmark compares different consumption patterns including stream-based and event-driven approaches across various client libraries.

**Test Configuration:**

- Total messages consumed: 100,000
- Consumption patterns: Stream-based and event-driven
- Metric: Operations per second (op/sec)

```
╔════════════════════════════════════════════════════════╤═════════╤══════════════════╤═══════════╤══════════════════════════╗
║ Slower tests                                           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────────────────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ node-rdkafka (stream)                                  │  100000 │  49423.52 op/sec │ ±  6.63 % │                          ║
║ @confluentinc/kafka-javascript (node-rdkafka, stream)  │  100000 │  55990.54 op/sec │ ± 10.58 % │ + 13.29 %                ║
║ @confluentinc/kafka-javascript (KafkaJS)               │  100000 │ 123289.64 op/sec │ ± 16.86 % │ + 120.20 %               ║
║ @confluentinc/kafka-javascript (node-rdkafka, evented) │  100000 │ 125986.77 op/sec │ ± 23.14 % │ + 2.19 %                 ║
║ KafkaJS                                                │  100000 │ 126963.99 op/sec │ ±  4.15 % │ + 0.78 %                 ║
║ node-rdkafka (evented)                                 │  100000 │ 135950.87 op/sec │ ± 19.03 % │ + 7.08 %                 ║
╟────────────────────────────────────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ Fastest test                                           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────────────────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ @platformatic/kafka                                    │  100015 │ 152567.14 op/sec │ ±  1.48 % │ + 12.22 %                ║
╚════════════════════════════════════════════════════════╧═════════╧══════════════════╧═══════════╧══════════════════════════╝
```

## Performance Summary

The benchmarks demonstrate that `@platformatic/kafka` consistently outperforms other Kafka client libraries across different usage patterns:

### Key Results

- **Producer (Single Message)**: `@platformatic/kafka` achieves 95,039 op/sec, which is 53.59% faster than KafkaJS, the second-best performer
- **Producer (Batch Messages)**: `@platformatic/kafka` reaches 4,488 op/sec, outperforming competitors by 42.76%
- **Consumer Performance**: `@platformatic/kafka` delivers 152,567 op/sec, providing a 12.22% improvement over the next fastest option
