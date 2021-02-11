[![NuGet Version and Downloads count](https://buildstats.info/nuget/Dapr.EventStore?includePreReleases=true)](https://www.nuget.org/packages/Dapr.EventStore/)
![Build](https://github.com/perokvist/Dapr.EventStore/workflows/.NET%20Core/badge.svg)

# Dapr.EventStore
Demo implementation of an naive EventStore with DaprClient (as feedback)

## DaprClient

Dapr state is key/value, a [key scheme](https://github.com/dapr/docs/blob/master/reference/api/state_api.md#key-scheme) app-id || key for the non actor scenario. 

### Append
To build an "append only" stream on top of that, this ES save a entry for stream head, with streamName as key, and each set/slice of events as one entry with "streamName-Version" as key. To come close to append, etags are used to check that the slice not exist. The stream head is updated when a new slice is written, but the head and slice is not in the same transaction.

### Load/Read
To be able to read all event in a stream, first the current version is read for the stream head, the starting with the latest slice, each slice is read until given version is reached. So there is multiple reads, one for head and one for each slice.

### Wishlist
If able to control partitionKey and entryKey, then each stream could be app-id || streamname and each entry an event with the key being the EventId. This would give possible transaction guarantees within the partition and reading the on partitionKey, *.

How such support would look like in the client ? Or would this kind of storage has its own apis?

Or should the Actor approach be used, and use another key scheme, and then the same multi-operation support? Then querying on state keys needs support.

## Actors

TBD.

Actor follow a different [key scheme](https://github.com/dapr/docs/blob/master/reference/api/state_api.md#key-scheme), that might allow a compontent to support a multi-operation, and eiser implementation of query by state key *.

