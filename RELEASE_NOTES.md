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
