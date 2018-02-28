### 0.1.14-alpha03 - 25.2.2018
* Improved broker TCP connection lifecycle management.
* Fixed offset commit bug wherein it would commit offsets for partitions no longer assigned

### 0.1.14-alpha02 - 8.2.2018
* Adjusting FSharp.Core reference

### 0.1.13 - 20.12.2017
* Roll-up of 0.1.13-alpha builds after testing

### 0.1.13-alpha02 - 19.12.2017 
* WARN: Default to use v0.10.1 of broker protocol.
* Print client and protocol version.

### 0.1.13-alpha01 - 18.12.2017 
* Consumers will continue to run if not assigned partitions.
* Fixed 'Consumer.fetchOffsets' returning no offsets when Array.empty is passed in as partitions per topic
* Fixed 'Consumer.stream' function that sometimes causes starvation when AsyncSeq.mergeAll would prioritize first few tasks rather than later
* Added auto versioning support for Kafka APIs for Produce, Fetch, Offset, Metadata, OffsetCommit, OffsetFetch, GroupCoordinator, JoinGroup, Heartbeat, LeaveGroup, 
SyncGroup, DescribeGroups, ListGroups, ApiVersions.
* Added an INFO message printed by connection to show protocol and client versions.
* Adjusted ConsumerConfig.[fetchMaxWaitMs, fetchMinBytes] to match Kafka native client defaults.
* BufferingProducer.produceBatched

### 0.1.12 - 13.12.2017
* Buffering producer experimental module.

### 0.1.11 - 12.12.2017
* Added support for all versions of the OffsetFetch API in the Kafka protocol.
* BREAKING: Offset commit queue is moved to module `PeriodicCommitQueue`. See: https://jet.github.io/kafunk/consuming.html#Periodic-Offset-Commit.
* Producer performance improvements.
* Fixed bug with producing using version >= 0.10.0 wherein empty key/value would be written

### 0.1.10 - 15.11.2017
* Fix another scenario referenced in 0.1.9 producer batching fix.

### 0.1.9 - 13.11.2017
* Fix Producer batching wherein if Producer.produceBatched was called with a batch size larger than the max, it would still send it.

### 0.1.8 - 30.10.2017
* Fix producer recovery wherein a produce batch timeout may overlap with broker connection failures, resulting in retries without rediscovery.

### 0.1.7 - 24.10.2017
* Fix Join/Sync group response for v10 and greater

### 0.1.6 - 23.10.2017
* Cumulitive release.

### 0.1.6-rc3 - 18.10.2017
* NEW: ConsumerInfo.consumerGroups returns information about all consumer groups in the cluster, including group members and the topics-partitions they are assgined.

### 0.1.6-rc2 - 17.10.2017
* Producer retry without recovery when recovery not needed.
* Ensure that Resource isn't corrupted after a failed resource recovery attempt and can be retried again.
* Resource performance improvement.

### 0.1.6-rc1 - 13.10.2017
* Fixed 'ConsumerProgress.progress' returning -1 for offsets when Array.empty is passed in

### 0.1.5 - 11.10.2017
* Cumulitive release with fixes since 0.1.4

### 0.1.5-rc6 - 10.10.2017
* Producer performance improvement

### 0.1.5-rc5 - 07.10.2017
* BUG: Producer.produceBatched doesn't recover after transient failures

### 0.1.5-rc4 - 06.10.2017
* BUG: race condition in consumer group join wherein multiple groups created on the same connection might prevent a group coordinator from being retrieved

### 0.1.5-rc3 - 02.10.2017
* Adjust retry policy defaults: reduce retries to fail fast
* Exposed internal ConsumerGroup.decodeMemberAssignment
* Improve consumer group stability, ensuring escalation in all consumer failure modes

### 0.1.5-rc2 - 27.09.2017
* AsyncSeq v2.0.14
* Change offset range argument type Consumer.streamRange

### 0.1.5-rc1 - 26.09.2017

* Added `Consumer.streamRange` to read a specified range of offsets.
* Added `Consumer.periodicOffsetCommitter` which can help ensuring periodic offset commits when using `Consumer.stream`.
* Logging improvements: changed some ERROR messages to WARN, added group coordinator to group related logs, caught missing partition info error
* AsyncSeq v2.0.13

### 0.1.4 - 03.08.2017

* Fixed `FetchResponse` `MessageTooBigException` when a message set has been compressed. (#160)

### 0.1.4-beta - 25.07.2017

* Fixed v0.10.1 protocol bug for `Offset` API.
* Fixed v0.10.1 protocol bug for `JoinGroup` API. 
* Added `ApiVersionsRequest`.
* Added support for automatically detecting API versions via `ApiVersionRequest` using `KafkaConfig.autoApiVersions`.

### 0.1.3-alpha - 14.07.2017

* Added ProducerResult.count indicating the number of messages produced in a batch for a partition.

### 0.1.2-alpha - 03.07.2017

* Fixed consumer offset commit [bug](https://github.com/jet/kafunk/issues/152) wherein after a rebalance a consumer gets assigned a new partition
  which doesn't receive any messages for longer than the offset retention period, the offsets would be lost.
  This would only happen after a rebalance not after initial join.

### 0.1.1-alpha - 25.05.2017

* Snappy compression.
* Fixed lag calculation bug in ConsumerInfo module.

### 0.1.0 - 01.05.2017

* Ensure recovery during broker restart (virtual broker routing).
* Add Producer.produceBatched with improved API and performance:
	- Takes a batch of messages rather than a function returning a batch.
	- Parallelizes requests across brokers.

### 0.0.43-alpha001 - 24.04.2017

* Fix message set decompression (take 3)

### 0.0.41-alpha001 - 24.04.2017

* Fix message set decompression (take 2)

### 0.0.40-alpha001 - 21.04.2017

* Fix message set decompression

### 0.0.39-alpha001 - 11.04.2017

* Unroll loop in producer to make more efficient
* Request buffer pool
* Added ZK offset migration script in tests project
* Support v10.0.1 for Offset API
* BREAKING:
	- MessageSet tuples replaced with structs
	- ProduceRequest tuples replaced with structs
	- ProduceResponse tuples replaced with structs
	- OffsetCommitRequest added support for v1 (in addition to v0 and v2)

### 0.0.38-alpha001 - 29.03.2017

* Fix use of IVar where concurrent puts are possible
* Fix #124
* Fix #126

### 0.0.37-alpha001 - 27.03.2017

* Fix producer bug where during recovery, messages in flight would be lost and never timeout.

### 0.0.36-alpha001 - 23.03.2017

* Improve produce and fetch codec performance.
* Fix possible deadlock with Async.AwaitTask usage.

### 0.0.35-alpha001 - 08.04.2017

* Fix CRC32 check.

### 0.0.34-alpha001 - 08.03.2017

* Make fetch response decoding more efficient (eliminate intermediate tuple allocations).
* Make CRC check on fetch response configurable (defaults to true).

### 0.0.33-alpha001 - 01.03.2017

* Ensure fetch errors are escalated to the top-level consumer.

### 0.0.32-alpha001 - 28.02.2017

* Fix bug in ConsumerInfo module where offset information for all partitions wouldn't be retrieved and would throw during the merge.
* Fix Async.parallel used internall for testing.

### 0.0.31-alpha001 - 24.02.2017

* Allow disabling of Console logger
* Ensure periodic offset committer commits offsets on start

### 0.0.30-alpha001 - 24.02.2017

* Improve producer batching performance
* Fix all offset commit

### 0.0.29-alpha001 - 23.02.2017

* Improve producer performance
* Improve producer error messaging

### 0.0.28-alpha001 - 22.02.2017

* Commit offsets even when unchanged to prevent loss due to retention.
* BREAKING:
	- Removed ConsumerConfig.initialFetchTime, consolidated into ConsumerConfig.autoOffsetReset
	- Replaced offsetOutOfRangeAction with autoOffsetReset and new union type AutoOffsetReset

### 0.0.27-alpha001 - 16.02.2017

* Hide internal members, including Async
* Refine fault tolerance defaults.

### 0.0.26-alpha001 - 14.02.2017

* Special Valentine's day edition.
* Refined flow for handling escalated failures.
* Fix error code MessageSizeTooLarge.
* Default client.id = "" rather than Guid; new Guid for connection id.
* Expose connection, producer, consumer config.

### 0.0.25-alpha001 - 10.02.2017

* Default producer to RequiredAcks.AllInSync
* Fix range consumer partition assignment strategy

### 0.0.24-alpha001 - 08.02.2017

* Adjust consumer group heartbeat defaults
* Added AsyncSeq tests

### 0.0.23-alpha001 - 08.02.2017

* Improve producer performance (batch size measurement)

### 0.0.22-alpha001 - 08.02.2017

* Producer in-memory buffer in bytes
* ConsumerInfo module for consumer progress tracking

### 0.0.21-alpha001 - 07.02.2017

* Updated exampled/readme
* Breaking changes:
	- Consumer.stream drops bufferSize parameter
	- ProducerResult contains a single partition-offset pair rather than array

### 0.0.20-alpha001 - 06.02.2017

* Producer buffering by size in bytes
* Fixex bug where producer errors weren't being surfaced
* Breaking changes:
    - ProducerConfig buffer size settings are new
    - Producer.produce takes a single message rather than array
    - Consumer.consume arguments reordered
* Logging improvements (more compact, more info)
* Improved producer-consumer test

### 0.0.19-alpha001 - 25.01.2017

* Producer buffering
* Ensure cancellation propagated to consumer on group close
* Logging improvements

### 0.0.18-alpha001 - 20.01.2017

* Fix ConsumerMessageSet.lag measure
* Propagate cancellation to consumer fetch process

### 0.0.17-alpha001 - 17.01.2017

* Discovery brokers by DNS when appropriate
* Explicit channel failure and recovery contract


### 0.0.16-alpha001 - 09.01.2017

* Fix bug in assignment strategy where all available metadate were used rather than that of subscribed topic

### 0.0.15-alpha001 - 09.01.2017

* Pass consumer state to consume callback (BREAKING)

### 0.0.14-alpha001 - 09.01.2017

* Consumer group assignment strategies configurable

### 0.0.13-alpha001 - 06.01.2017

* Refine Fetch consumer offsets API

### 0.0.12-alpha001 - 06.01.2017

* Current consumer state API
* Fetch consumer offsets API

### 0.0.11-alpha001 - 05.01.2017

* Hide internal members
* Documentation

### 0.0.10-alpha001 - 04.01.2017

* Log leader-size member assignments
* Fix consumer NotCoordinatorForGroup error recovery

### 0.0.9-alpha001 - 03.01.2017

* Refine TCP error recovery

### 0.0.8-alpha001 - 03.01.2017

* v0.10.1 support

### 0.0.7-alpha001 - 02.01.2017

* Fix overflow bug in Partitioner.roundRobin
* Error flow monitoring

### 0.0.6 - 30.12.2016

* Fix bug in Async.choose
* Consumer.commitOffsetsToTime
* Consumer doesn't fetch offsets until its starts consuming

### 0.0.5 - 30.12.2016

* Auto recover producer from error code 2

### 0.0.4 - 29.12.2016

* Consumer fetch request batching by broker
* Fix protocol codec issue for batched fetch responses
* Periodic offset commit

### 0.0.3 - 28.12.2016

* Fix a few reconnection/fault tolerance bugs
* Adjust consumer api to allow offset strategies to be parameterized

### 0.0.2 - 29.11.2016

* A more complete consumer API is now available
* We now use FSharp.Control.AsyncSeq instead of an internal version
* Adjustments have been made to connection recovery and general fault tolerance (still early)
* Improvements to message grouping should improve producer throughput

### 0.0.1 - 20.07.2016
* initial
