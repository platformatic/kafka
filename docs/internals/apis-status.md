# Supported APIs

The version ranges below are implemented message codec versions, not supported broker-version ranges. Broker compatibility is limited to [Apache Kafka 3.5.0 through 4.2.0](../../README.md#supported-kafka-version).

## Producer API

| Name               | ID  | Version |
| ------------------ | --- | ------- |
| Metadata           | 3   | 0-12    |
| ApiVersions        | 18  | 0-4     |
| Produce            | 0   | 3-11    |
| FindCoordinator    | 10  | 0-6     |
| InitProducerId     | 22  | 0-5     |
| AddPartitionsToTxn | 24  | 0-5     |
| AddOffsetsToTxn    | 25  | 0-4     |
| EndTxn             | 26  | 0-4     |
| TxnOffsetCommit    | 28  | 0-4     |

> Produce v0-v2 use the legacy MessageSet format. This client supports Record Batches, which were introduced by Produce v3, so v3 is the minimum supported Produce version.

## Consumer API

| Name                   | ID  | Version |
| ---------------------- | --- | ------- |
| Fetch                  | 1   | 4-17    |
| ListOffsets            | 2   | 0-9     |
| OffsetCommit           | 8   | 0-9     |
| OffsetFetch            | 9   | 0-9     |
| OffsetForLeaderEpoch   | 23  | 0-4     |
| JoinGroup              | 11  | 0-9     |
| Heartbeat              | 12  | 0-4     |
| LeaveGroup             | 13  | 0-5     |
| SyncGroup              | 14  | 0-5     |
| ConsumerGroupHeartbeat | 68  | 0-1     |

> Fetch v0-v3 use the legacy MessageSet format. This client supports Record Batches, which were introduced by Fetch v4, so v4 is the minimum supported Fetch version.

## Admin API

| Name                         | ID  | Version |
| ---------------------------- | --- | ------- |
| DescribeGroups               | 15  | 0-5     |
| ListGroups                   | 16  | 0-5     |
| CreateTopics                 | 19  | 0-7     |
| DeleteTopics                 | 20  | 0-6     |
| DeleteRecords                | 21  | 0-2     |
| DescribeAcls                 | 29  | 0-3     |
| CreateAcls                   | 30  | 0-3     |
| DeleteAcls                   | 31  | 0-3     |
| DescribeConfigs              | 32  | 0-4     |
| AlterConfigs                 | 33  | 0-2     |
| AlterReplicaLogDirs          | 34  | 0-2     |
| DescribeLogDirs              | 35  | 0-4     |
| CreatePartitions             | 37  | 0-3     |
| CreateDelegationToken        | 38  | 0-3     |
| RenewDelegationToken         | 39  | 0-2     |
| ExpireDelegationToken        | 40  | 0-2     |
| DescribeDelegationToken      | 41  | 0-3     |
| DeleteGroups                 | 42  | 0-2     |
| IncrementalAlterConfigs      | 44  | 0-1     |
| AlterPartitionReassignments  | 45  | 0-1     |
| ListPartitionReassignments   | 46  | 0       |
| OffsetDelete                 | 47  | 0       |
| DescribeClientQuotas         | 48  | 0-1     |
| AlterClientQuotas            | 49  | 0-1     |
| DescribeUserScramCredentials | 50  | 0       |
| AlterUserScramCredentials    | 51  | 0       |
| DescribeQuorum               | 55  | 0-2     |
| AlterPartition               | 56  | 0-3     |
| UpdateFeatures               | 57  | 0-1     |
| Envelope                     | 58  | 0       |
| DescribeCluster              | 60  | 0-1     |
| DescribeProducers            | 61  | 0       |
| UnregisterBroker             | 64  | 0       |
| DescribeTransactions         | 65  | 0       |
| ListTransactions             | 66  | 0-2     |
| ConsumerGroupDescribe        | 69  | 0       |
| DescribeTopicPartitions      | 75  | 0       |

## Miscellaneous

| Section   | Name                       | ID  | Version |
| --------- | -------------------------- | --- | ------- |
| Telemetry | GetTelemetrySubscriptions  | 71  | 0       |
| Telemetry | PushTelemetry              | 72  | 0       |
| Telemetry | ListClientMetricsResources | 74  | 0       |
| SASL      | SaslHandshake              | 17  | 0-1     |
| SASL      | SaslAuthenticate           | 36  | 0-2     |

> The standalone SaslHandshake codec supports v0-v1, but connections use v1 only. SASL authentication requires Kafka 1.0.0 or later because the pre-1.0 raw-SASL flow is intentionally unsupported.

# Unsupported APIs

This is a non-exhaustive list of broker-only APIs outside this client's scope. Other Apache Kafka APIs may be unsupported or not exposed by the public client API.

## Broker API (Out of Scope)

| Name                 | ID  |
| -------------------- | --- |
| LeaderAndIsr         | 4   |
| StopReplica          | 5   |
| UpdateMetadata       | 6   |
| ControlledShutdown   | 7   |
| WriteTxnMarkers      | 27  |
| ElectLeaders         | 43  |
| AllocateProducerIds  | 67  |
| AddRaftVoter         | 80  |
| RemoveRaftVoter      | 81  |
