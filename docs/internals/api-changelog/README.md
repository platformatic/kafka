# Kafka API Codec Changelogs

These changelogs summarize wire-schema changes for the Kafka API codec versions implemented by this package. They are derived from the [Apache Kafka message definitions](https://github.com/apache/kafka/tree/trunk/clients/src/main/resources/common/message). Behavioral changes that do not alter the schema are identified explicitly.

The implemented API versions are listed in the [API status](../apis-status.md).

## Producer

- [AddOffsetsToTxn](add-offsets-to-txn.md)
- [AddPartitionsToTxn](add-partitions-to-txn.md)
- [EndTxn](end-txn.md)
- [InitProducerId](init-producer-id.md)
- [Produce](produce.md)
- [TxnOffsetCommit](txn-offset-commit.md)

## Consumer

- [ConsumerGroupHeartbeat](consumer-group-heartbeat.md)
- [Fetch](fetch.md)
- [Heartbeat](heartbeat.md)
- [JoinGroup](join-group.md)
- [LeaveGroup](leave-group.md)
- [ListOffsets](list-offsets.md)
- [OffsetCommit](offset-commit.md)
- [OffsetFetch](offset-fetch.md)
- [OffsetForLeaderEpoch](offset-for-leader-epoch.md)
- [SyncGroup](sync-group.md)

## Metadata And Security

- [ApiVersions](api-versions.md)
- [FindCoordinator](find-coordinator.md)
- [Metadata](metadata.md)
- [SaslAuthenticate](sasl-authenticate.md)
- [SaslHandshake](sasl-handshake.md)

## Admin

- [AlterClientQuotas](alter-client-quotas.md)
- [AlterConfigs](alter-configs.md)
- [AlterPartition](alter-partition.md)
- [AlterPartitionReassignments](alter-partition-reassignments.md)
- [AlterReplicaLogDirs](alter-replica-log-dirs.md)
- [AlterUserScramCredentials](alter-user-scram-credentials.md)
- [CreateAcls](create-acls.md)
- [CreateDelegationToken](create-delegation-token.md)
- [CreatePartitions](create-partitions.md)
- [CreateTopics](create-topics.md)
- [DeleteAcls](delete-acls.md)
- [DeleteGroups](delete-groups.md)
- [DeleteRecords](delete-records.md)
- [DeleteTopics](delete-topics.md)
- [DescribeAcls](describe-acls.md)
- [DescribeClientQuotas](describe-client-quotas.md)
- [DescribeCluster](describe-cluster.md)
- [DescribeConfigs](describe-configs.md)
- [DescribeDelegationToken](describe-delegation-token.md)
- [DescribeGroups](describe-groups.md)
- [DescribeLogDirs](describe-log-dirs.md)
- [DescribeProducers](describe-producers.md)
- [DescribeQuorum](describe-quorum.md)
- [DescribeTopicPartitions](describe-topic-partitions.md)
- [DescribeTransactions](describe-transactions.md)
- [DescribeUserScramCredentials](describe-user-scram-credentials.md)
- [Envelope](envelope.md)
- [ExpireDelegationToken](expire-delegation-token.md)
- [IncrementalAlterConfigs](incremental-alter-configs.md)
- [ListClientMetricsResources](list-client-metrics-resources.md)
- [ListGroups](list-groups.md)
- [ListPartitionReassignments](list-partition-reassignments.md)
- [ListTransactions](list-transactions.md)
- [OffsetDelete](offset-delete.md)
- [RenewDelegationToken](renew-delegation-token.md)
- [UnregisterBroker](unregister-broker.md)
- [UpdateFeatures](update-features.md)

## Telemetry

- [GetTelemetrySubscriptions](get-telemetry-subscriptions.md)
- [PushTelemetry](push-telemetry.md)
