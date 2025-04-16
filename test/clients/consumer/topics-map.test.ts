import { deepStrictEqual, strictEqual } from 'node:assert'
import { test } from 'node:test'
import { TopicsMap } from '../../../src/clients/consumer/topics-map.ts'

test('TopicsMap - should create an empty map initially', () => {
  const topicsMap = new TopicsMap()

  strictEqual(topicsMap.size, 0, 'Map should be empty initially')
  deepStrictEqual(topicsMap.current, [], 'Current topics list should be empty initially')
})

test('TopicsMap - should track a topic and return true for first addition', () => {
  const topicsMap = new TopicsMap()

  const result = topicsMap.track('test-topic')

  strictEqual(result, true, 'Should return true when tracking a new topic')
  strictEqual(topicsMap.size, 1, 'Map should have one entry')
  strictEqual(topicsMap.get('test-topic'), 1, 'Counter should be 1 for the topic')
  deepStrictEqual(topicsMap.current, ['test-topic'], 'Current topics list should be updated')
})

test('TopicsMap - should increment counter for already tracked topic and return false', () => {
  const topicsMap = new TopicsMap()

  topicsMap.track('test-topic')
  const result = topicsMap.track('test-topic')

  strictEqual(result, false, 'Should return false when tracking an existing topic')
  strictEqual(topicsMap.size, 1, 'Map should still have one entry')
  strictEqual(topicsMap.get('test-topic'), 2, 'Counter should be 2 for the topic')
  deepStrictEqual(topicsMap.current, ['test-topic'], 'Current topics list should remain the same')
})

test('TopicsMap - should track multiple topics at once', () => {
  const topicsMap = new TopicsMap()

  const results = topicsMap.trackAll('topic1', 'topic2', 'topic3')

  deepStrictEqual(results, [true, true, true], 'Should return true for all new topics')
  strictEqual(topicsMap.size, 3, 'Map should have three entries')
  strictEqual(topicsMap.get('topic1'), 1, 'Counter should be 1 for topic1')
  strictEqual(topicsMap.get('topic2'), 1, 'Counter should be 1 for topic2')
  strictEqual(topicsMap.get('topic3'), 1, 'Counter should be 1 for topic3')
  deepStrictEqual(
    topicsMap.current.sort(),
    ['topic1', 'topic2', 'topic3'].sort(),
    'Current topics list should contain all topics'
  )
})

test('TopicsMap - should support passing flattened arrays to trackAll', () => {
  const topicsMap = new TopicsMap()

  const results = topicsMap.trackAll('topic1', 'topic2', 'topic3')

  deepStrictEqual(results, [true, true, true], 'Should return true for all new topics')
  strictEqual(topicsMap.size, 3, 'Map should have three entries')
  deepStrictEqual(
    topicsMap.current.sort(),
    ['topic1', 'topic2', 'topic3'].sort(),
    'Current topics list should contain all topics'
  )
})

test('TopicsMap - should untrack a topic and return true when removed completely', () => {
  const topicsMap = new TopicsMap()

  topicsMap.track('test-topic')
  const result = topicsMap.untrack('test-topic')

  strictEqual(result, true, 'Should return true when removing the last reference to a topic')
  strictEqual(topicsMap.size, 0, 'Map should be empty after untracking')
  strictEqual(topicsMap.get('test-topic'), undefined, 'Topic should be removed from the map')
  deepStrictEqual(topicsMap.current, [], 'Current topics list should be empty')
})

test('TopicsMap - should decrement counter and return false when topic still tracked', () => {
  const topicsMap = new TopicsMap()

  topicsMap.track('test-topic')
  topicsMap.track('test-topic') // increment to 2
  const result = topicsMap.untrack('test-topic')

  strictEqual(result, false, 'Should return false when topic still has references')
  strictEqual(topicsMap.size, 1, 'Map should still have the entry')
  strictEqual(topicsMap.get('test-topic'), 1, 'Counter should be decremented to 1')
  deepStrictEqual(topicsMap.current, ['test-topic'], 'Current topics list should remain the same')
})

test('TopicsMap - should return false when untracking a non-existent topic', () => {
  const topicsMap = new TopicsMap()

  const result = topicsMap.untrack('non-existent-topic')

  strictEqual(result, false, 'Should return false when untracking a non-existent topic')
  strictEqual(topicsMap.size, 0, 'Map should remain empty')
  deepStrictEqual(topicsMap.current, [], 'Current topics list should remain empty')
})

test('TopicsMap - should untrack multiple topics at once', () => {
  const topicsMap = new TopicsMap()

  topicsMap.trackAll('topic1', 'topic2', 'topic3')
  const results = topicsMap.untrackAll('topic1', 'topic2')

  deepStrictEqual(results, [true, true], 'Should return true for completely removed topics')
  strictEqual(topicsMap.size, 1, 'Map should have one remaining entry')
  strictEqual(topicsMap.get('topic1'), undefined, 'topic1 should be removed')
  strictEqual(topicsMap.get('topic2'), undefined, 'topic2 should be removed')
  strictEqual(topicsMap.get('topic3'), 1, 'topic3 should still be tracked')
  deepStrictEqual(topicsMap.current, ['topic3'], 'Current topics list should only contain topic3')
})

test('TopicsMap - should support passing flattened arrays to untrackAll', () => {
  const topicsMap = new TopicsMap()

  topicsMap.trackAll('topic1', 'topic2', 'topic3')
  const results = topicsMap.untrackAll('topic1', 'topic2')

  deepStrictEqual(results, [true, true], 'Should return true for completely removed topics')
  strictEqual(topicsMap.size, 1, 'Map should have one remaining entry')
  deepStrictEqual(topicsMap.current, ['topic3'], 'Current topics list should only contain topic3')
})

test('TopicsMap - should handle complex tracking/untracking scenarios correctly', () => {
  const topicsMap = new TopicsMap()

  // Track some topics multiple times
  topicsMap.track('topic1')
  topicsMap.track('topic1')
  topicsMap.track('topic2')
  topicsMap.track('topic3')
  topicsMap.track('topic3')

  deepStrictEqual(
    topicsMap.current.sort(),
    ['topic1', 'topic2', 'topic3'].sort(),
    'Current topics list should contain all topics'
  )

  // Untrack some topics
  topicsMap.untrack('topic1') // decrements to 1
  topicsMap.untrack('topic2') // removes completely

  strictEqual(topicsMap.size, 2, 'Map should have two entries remaining')
  strictEqual(topicsMap.get('topic1'), 1, 'topic1 counter should be 1')
  strictEqual(topicsMap.get('topic2'), undefined, 'topic2 should be removed')
  strictEqual(topicsMap.get('topic3'), 2, 'topic3 counter should be 2')

  deepStrictEqual(
    topicsMap.current.sort(),
    ['topic1', 'topic3'].sort(),
    'Current topics list should be updated correctly'
  )

  // Untrack remaining topics
  topicsMap.untrack('topic1') // removes completely
  topicsMap.untrack('topic3') // decrements to 1
  topicsMap.untrack('topic3') // removes completely

  strictEqual(topicsMap.size, 0, 'Map should be empty')
  deepStrictEqual(topicsMap.current, [], 'Current topics list should be empty')
})
