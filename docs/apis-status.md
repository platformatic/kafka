# Supported APIs

## Producer API

| Name               | ID  | Version |
| ------------------ | --- | ------- |
| Metadata           | 3   | 12      |
| ApiVersions        | 18  | 4       |
| Produce            | 0   | 11      |
| FindCoordinator    | 10  | 6       |
| InitProducerId     | 22  | 5       |
| AddPartitionsToTxn | 24  | 5       |
| AddOffsetsToTxn    | 25  | 4       |
| EndTxn             | 26  | 4       |
| TxnOffsetCommit    | 28  | 4       |

## Consumer API

| Name                   | ID  | Version |
| ---------------------- | --- | ------- |
| Fetch                  | 1   | 17      |
| ListOffsets            | 2   | 9       |
| OffsetCommit           | 8   | 9       |
| OffsetFetch            | 9   | 9       |
| JoinGroup              | 11  | 9       |
| Heartbeat              | 12  | 4       |
| LeaveGroup             | 13  | 5       |
| SyncGroup              | 14  | 5       |
| ConsumerGroupHeartbeat | 68  | 0       |

## Admin API

| Name                         | ID  | Version |
| ---------------------------- | --- | ------- |
| DescribeGroups               | 15  | 5       |
| ListGroups                   | 16  | 5       |
| CreateTopics                 | 19  | 7       |
| DeleteTopics                 | 20  | 6       |
| DeleteRecords                | 21  | 2       |
| DescribeAcls                 | 29  | 3       |
| CreateAcls                   | 30  | 3       |
| DeleteAcls                   | 31  | 3       |
| DescribeConfigs              | 32  | 4       |
| AlterConfigs                 | 33  | 2       |
| AlterReplicaLogDirs          | 34  | 2       |
| DescribeLogDirs              | 35  | 4       |
| CreatePartitions             | 37  | 3       |
| CreateDelegationToken        | 38  | 3       |
| RenewDelegationToken         | 39  | 2       |
| ExpireDelegationToken        | 40  | 2       |
| DescribeDelegationToken      | 41  | 3       |
| DeleteGroups                 | 42  | 2       |
| IncrementalAlterConfigs      | 44  | 1       |
| AlterPartitionReassignments  | 45  | 0       |
| ListPartitionReassignments   | 46  | 0       |
| OffsetDelete                 | 47  | 0       |
| DescribeClientQuotas         | 48  | 1       |
| AlterClientQuotas            | 49  | 1       |
| DescribeUserScramCredentials | 50  | 0       |
| AlterUserScramCredentials    | 51  | 0       |
| DescribeQuorum               | 55  | 2       |
| AlterPartition               | 56  | 3       |
| UpdateFeatures               | 57  | 1       |
| Envelope                     | 58  | 0       |
| DescribeCluster              | 60  | 1       |
| DescribeProducers            | 61  | 0       |
| UnregisterBroker             | 64  | 0       |
| DescribeTransactions         | 65  | 0       |
| ListTransactions             | 66  | 1       |
| ConsumerGroupDescribe        | 69  | 0       |
| DescribeTopicPartitions      | 75  | 0       |

## Miscellaneous

| Section   | Name                       | ID  | Version |
| --------- | -------------------------- | --- | ------- |
| Telemetry | GetTelemetrySubscriptions  | 71  | 0       |
| Telemetry | PushTelemetry              | 72  | 0       |
| Telemetry | ListClientMetricsResources | 74  | 0       |
| SASL      | SaslHandshake              | 17  | 1       |
| SASL      | SaslAuthenticate           | 36  | 2       |

# Unsupported APIs

## Broker API (Out of Scope)

| Name                 | ID  |
| -------------------- | --- |
| LeaderAndIsr         | 4   |
| StopReplica          | 5   |
| UpdateMetadata       | 6   |
| ControlledShutdown   | 7   |
| OffsetForLeaderEpoch | 23  |
| WriteTxnMarkers      | 27  |
| ElectLeaders         | 43  |
| AllocateProducerIds  | 67  |
| AddRaftVoter         | 80  |
| RemoveRaftVoter      | 81  |
