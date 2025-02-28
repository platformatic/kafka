# API Support

## Producer API

| Name               | ID  | Status |
| ------------------ | --- | ------ |
| ListOffsets        | 2   | Done   |
| Metadata           | 3   | Done   |
| ApiVersions        | 18  | Done   |
| Produce            | 0   | Done   |
| FindCoordinator    | 10  | Done   |
| InitProducerId     | 22  | Done   |
| AddPartitionsToTxn | 24  | Done   |
| AddOffsetsToTxn    | 25  | Done   |
| EndTxn             | 26  | Done   |
| TxnOffsetCommit    | 28  | Done   |

## Consumer API

| Name                   | ID  | Status |
| ---------------------- | --- | ------ |
| Fetch                  | 1   | Done   |
| OffsetCommit           | 8   | Done   |
| OffsetFetch            | 9   | Done   |
| JoinGroup              | 11  | Done   |
| Heartbeat              | 12  | Done   |
| LeaveGroup             | 13  | Done   |
| SyncGroup              | 14  | Done   |
| ConsumerGroupHeartbeat | 68  | Done   |

## Admin API

| Name                         | ID  | Status |
| ---------------------------- | --- | ------ |
| DescribeGroups               | 15  | Done   |
| ListGroups                   | 16  | Done   |
| CreateTopics                 | 19  | Done   |
| DeleteTopics                 | 20  | Done   |
| DeleteRecords                | 21  | Done   |
| DescribeAcls                 | 29  | Done   |
| CreateAcls                   | 30  | Done   |
| DeleteAcls                   | 31  | Done   |
| DescribeConfigs              | 32  | Done   |
| AlterConfigs                 | 33  | Done   |
| AlterReplicaLogDirs          | 34  | Done   |
| DescribeLogDirs              | 35  | Done   |
| CreatePartitions             | 37  | Done   |
| CreateDelegationToken        | 38  | Done   |
| RenewDelegationToken         | 39  | Done   |
| ExpireDelegationToken        | 40  | Done   |
| DescribeDelegationToken      | 41  | Done   |
| DeleteGroups                 | 42  | Done   |
| IncrementalAlterConfigs      | 44  | Done   |
| AlterPartitionReassignments  | 45  | Done   |
| ListPartitionReassignments   | 46  | Done   |
| OffsetDelete                 | 47  | Done   |
| DescribeClientQuotas         | 48  | Done   |
| AlterClientQuotas            | 49  | Done   |
| DescribeUserScramCredentials | 50  | Done   |
| AlterUserScramCredentials    | 51  | Done   |
| DescribeQuorum               | 55  | Done   |
| AlterPartition               | 56  | Done   |
| UpdateFeatures               | 57  | Done   |
| Envelope                     | 58  | Done   |
| DescribeCluster              | 60  | Done   |
| DescribeProducers            | 61  | Done   |
| UnregisterBroker             | 64  | Done   |
| DescribeTransactions         | 65  | Done   |
| ListTransactions             | 66  | Done   |
| ConsumerGroupDescribe        | 69  | Done   |
| DescribeTopicPartitions      | 75  | Done   |

## Miscellaneous

| Section   | Name                       | ID   | Status |
| --------- | -------------------------- | ---- | ------ |
| Telemetry | GetTelemetrySubscriptions  | 71   | Done   |
| Telemetry | PushTelemetry              | 72   | Done   |
| Telemetry | ListClientMetricsResources | 74   | Done   |
| SSL       | SSL Authentication         | NONE |        |
| SASL      | SaslHandshake              | 17   |        |
| SASL      | SaslAuthenticate           | 36   |        |

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
