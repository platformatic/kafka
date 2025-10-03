export const topic = 'benchmarks'
export const brokers = ['localhost:9011', 'localhost:9012', 'localhost:9013']

// This is needed by KafkaJS
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'
