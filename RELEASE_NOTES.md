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
