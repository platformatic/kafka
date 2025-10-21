// IMPORTANT: Never export this file in index.ts - The symbols are meant to be private

export const kInstance = Symbol('plt.kafka.base.instance')
export const kRefreshOffsetsAndFetch = Symbol('plt.kafka.messagesStream.refreshOffsetsAndFetch')
export const kAutocommit = Symbol('plt.kafka.messagesStream.autocommit')
