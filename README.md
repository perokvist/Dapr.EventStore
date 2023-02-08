[![NuGet Version and Downloads count](https://buildstats.info/nuget/StateStore.Dapr.EventStore?includePreReleases=true)](https://www.nuget.org/packages/StateStore.Dapr.EventStore/)
![Build](https://github.com/perokvist/Dapr.EventStore/workflows/.NET%20Core/badge.svg)

# Dapr.EventStore
Demo implementation of an basic EventStore with DaprClient.

## DaprClient

Dapr state is key/value, a [key scheme](https://github.com/dapr/docs/blob/master/reference/api/state_api.md#key-scheme) app-id || key for the non actor scenario. 

State store could have transactional support, and be optimized for bulk operations. Due to these options the EventStore has a few different modes, called *SliceMode*. Default if *Off*.
Slices are used when bulk write/read is not optimal, or to minimize transactions, when all opererations is in there own.

### Append (slice mode - Off)
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each set of events as one entry per event with "streamName-Version" as key. To come close to append, etags are used to check that the events not exist. 
The stream head is updated when a new slice is written, the head and events is in the same transaction.

#### Stream head

``` json
{
    "id": "es-test||teststream-f83d3|head",
    "value": {
        "Version": 2
    },
    "partitionKey": "teststream-f83d3",
    ...
}
```

#### Event
``` json
{
    "id": "es-test||teststream-f83d3|2",
    "value": {
        "Data": "hello 2",
        "EventId": "2bde5f48-a5a7-4b68-b8bc-5d769ef95917",
        "EventName": "test",
        "StreamName": "teststream-f83d3",
        "Version": 2
    },
    "partitionKey": "teststream-f83d3",
    ...
}
```

### Append (slice mode - TwoPhased)
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each set/slice of events as one entry with "streamName-Version" as key. To come close to append, etags are used to check that the slice not exist. The stream head is updated when a new slice is written, but the head and slice is not in the same transaction.

#### Stream head

``` json
{
    "id": "es-test||teststream-36087|head",
    "value": {
        "Version": 2
    },
    "partitionKey": "teststream-36087",
    ...
}
```

#### Event

> Not that value is an array of events.

``` json
{
    "id": "es-test||teststream-36087|2",
    "value": [
        {
            "Data": "hello 2",
            "EventId": "e17976c9-e151-4b28-aa53-f0640677ff6e",
            "EventName": "test",
            "StreamName": "teststream-36087",
            "Version": 2
        }
    ],
    "partitionKey": "teststream-36087",
    ...
}
```

### Append (slice mode - Transactional)
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each set/slice of events as one entry with "streamName-Version" as key.To come close to append, etags are used to check that the slice not exist. The stream head is updated when a new slice is written, the head and slice is in the same transaction.

#### Stream head

``` json
{
    "id": "es-test||teststream-77b42|head",
    "value": {
        "Version": 2
    },
    "partitionKey": "teststream-77b42",
    ...
}
```

#### Event

> Note event are store binary serialized (even if meta data sets json , [issue](https://github.com/perokvist/Dapr.EventStore/issues/24))

``` json
{
    "id": "es-test||teststream-77b42|2",
    "value": "W3siRXZlbnRJZCI6ImIyMzU2YWY1LTcwYjYtNDE3Yi04Nzc5LWRhYmIyMzc5YTRkYiIsIkV2ZW50TmFtZSI6InkiLCJTdHJlYW1OYW1lIjoidGVzdHN0cmVhbS03N2I0MiIsIkRhdGEiOiJoZWxsbyAyIiwiVmVyc2lvbiI6Mn1d",
    "partitionKey": "teststream-77b42",
    ...
}
```

### Append (slice mode - OffAndSharedAll)

Store all events in a collection with the same partitionKey.
To build an "append only" stream on top of that, this ES saves a entry for stream head, with streamName as key, and each event as one entry with stream and eventId as key.


#### Stream head

``` json
{
    "id": "es-test||teststream-22f9f|head",
    "value": {
        "Version": 2
    },
    "partitionKey": "all",
    ...
}
```

#### Event

> Note that the id is compoiste with eventId and partition is set to "all".

``` json
{
    "id": "es-test||teststream-22f9f|2d49634a-5ed6-4217-83fc-526d05d51dfd",
    "value": {
        "Data": "hello 2",
        "EventId": "2d49634a-5ed6-4217-83fc-526d05d51dfd",
        "EventName": "y",
        "StreamName": "teststream-22f9f",
        "Version": 2
    },
    "partitionKey": "all",
    ...
}
```

### Load/Read (off)
To be able to read all event in a stream, first the current version is read for the stream head, then all events is read using bulk read until given version is reached. So there is multiple reads, one for head and one for event (but optimized based on the underlaying store's bulk read implementation).

### Load/Read (slice)
To be able to read all event in a stream, first the current version is read for the stream head, then starting with the latest slice, each slice is read until given version is reached. So there is multiple reads, one for head and one for each slice.

### Load/Read (slice - OffAndSharedAll)
This mode uses the Query api to get the stream events. Sorting is in memory - [Issue](https://github.com/perokvist/Dapr.EventStore/issues/23)

### Partitioning
Partitioning follows a fixed [key scheme](https://v1-rc3.docs.dapr.io/reference/api/state_api/#key-scheme), this could effect the underlaying stores partioning. Some stores lets you control partition through metadata. The event store lets you pass custom meta data based on stream.

## Actors

TBD.

Actor follow a different [key scheme](https://github.com/dapr/docs/blob/master/reference/api/state_api.md#key-scheme), that might allow a compontent to support a multi-operation, and eiser implementation of query by state key *.

## Test

The test suite runs agaist a local dapr instance.

``` bash
dapr run --app-id es-test --dapr-grpc-port 50001`
```

Using the cosmos emulator a state compontent could be configured. 

> When dapr initializes the databae and collection need to exist. The test suite will tear down and recreate the collection.

``` json
apiVersion: dapr.io/v1alpha1
kind: Component
metadata:
  name: statestore
spec:
  type: state.azure.cosmosdb
  version: v1
  metadata:
  - name: url
    value: https://localhost:8081
  - name: masterKey
    value: C2y6yDjf5/R+ob0N8A7Cgv30VRDJIWEHLM+4QDU5DE2nQ9nDuVTqobD4b8mGGyPMbIZnqyMsEcaGQy67XIw/Jw==
  - name: database
    value: statestore
  - name: collection
    value: statestore
```

The eventstore test suite need this environment variable to be set (in setup of the test suite), to run against the local dapr instance. Without this set if will run against a local "fake" dapr client.

``` csharp
Environment.SetEnvironmentVariable("DAPR_GRPC_PORT", "50001");
```