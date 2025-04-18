export const topic = 'benchmarks'
export const brokers = ['localhost:29092', 'localhost:39092', 'localhost:49092']

// This is needed by KafkaJS
process.env.KAFKAJS_NO_PARTITIONER_WARNING = '1'
