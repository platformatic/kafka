// IMPORTANT: Never export this file in index.ts - The symbols are meant to be private

export const kInstance = Symbol('plt.kafka.base.instance')
export const kRefreshOffsetsAndFetch = Symbol('plt.kafka.messagesStream.refreshOffsetsAndFetch')
export const kAutocommit = Symbol('plt.kafka.messagesStream.autocommit')

export const kTransaction = Symbol('plt.kafka.producer.transactions')
export const kTransactionPrepare = Symbol('plt.kafka.producer.transactions.prepare')
export const kTransactionAddPartitions = Symbol('plt.kafka.producer.transactions.addPartitions')
export const kTransactionAddOffsets = Symbol('plt.kafka.producer.transactions.addOffsets')
export const kTransactionCommitOffset = Symbol('plt.kafka.producer.transactions.commitOffsets')
export const kTransactionEnd = Symbol('plt.kafka.producer.transactions.end')
export const kTransactionCancel = Symbol('plt.kafka.producer.transactions.cancel')
export const kTransactionFindCoordinator = Symbol('plt.kafka.producer.transactions.findCoordinator')
