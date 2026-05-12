export function partitionKey (topic: string, partition: number): string {
  return `${topic}:${partition}`
}
