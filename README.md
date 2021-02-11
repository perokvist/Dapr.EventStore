[![NuGet Version and Downloads count](https://buildstats.info/nuget/StateStore.Dapr.EventStore?includePreReleases=true)](https://www.nuget.org/packages/StateStore.Dapr.EventStore/)
![Build](https://github.com/perokvist/Dapr.EventStore/workflows/.NET%20Core/badge.svg)

# Dapr.EventStore
Demo implementation of an naive EventStore with DaprClient (as feedback)

## DaprClient

Dapr state is key/value, a [key scheme](https://github.com/dapr/docs/blob/master/reference/api/state_api.md#key-scheme) app-id || key for the non actor scenario. 

State store could have transactional support, and be optimized for bulk operations. Due to these options the EventStore has a few different modes, called *SliceMode*. Default if *Off*.
Slices are used when bulk write/read is not optimal, or to minimize transactions, when all opererations is in there own.

### Append (slice mode - Off)
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each set of events as one entry per event with "streamName-Version" as key. To come close to append, etags are used to check that the events not exist. 
The stream head is updated when a new slice is written, the head and events is in the same transaction.

### Append (slice mode - TwoPhased)
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each set/slice of events as one entry with "streamName-Version" as key. To come close to append, etags are used to check that the slice not exist. The stream head is updated when a new slice is written, but the head and slice is not in the same transaction.

### Append (slice mode - Transactional)
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each set/slice of events as one entry with "streamName-Version" as key.To come close to append, etags are used to check that the slice not exist. The stream head is updated when a new slice is written, the head and slice is in the same transaction.

### Load/Read (off)
To be able to read all event in a stream, first the current version is read for the stream head, then all events is read using bulk read until given version is reached. So there is multiple reads, one for head and one for event (but optimized based on the underlaying store's bulk read implementation).

### Load/Read (slice)
To be able to read all event in a stream, first the current version is read for the stream head, then starting with the latest slice, each slice is read until given version is reached. So there is multiple reads, one for head and one for each slice.

### Partitioning
Partitioning follows a fixed [key scheme](https://v1-rc3.docs.dapr.io/reference/api/state_api/#key-scheme), this could effect the underlaying stores partioning. Some stores lets you control partition through metadata. The event store lets you pass custom meta data based on stream.

## Actors

TBD.

Actor follow a different [key scheme](https://github.com/dapr/docs/blob/master/reference/api/state_api.md#key-scheme), that might allow a compontent to support a multi-operation, and eiser implementation of query by state key *.

