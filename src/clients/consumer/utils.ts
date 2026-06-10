export function partitionKey (topic: string, partition: number): string {
  return `${topic}:${partition}`
}

export function nextFetchEpoch (epoch: number): number {
  if (epoch < 0) {
    return 0
  }

  return epoch >= 0x7fffffff ? 1 : epoch + 1
}
