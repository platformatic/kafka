export const topic = 'benchmarks'
export const brokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

// This is needed by KafkaJS
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'
