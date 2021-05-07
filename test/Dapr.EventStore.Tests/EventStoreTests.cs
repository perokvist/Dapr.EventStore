using Dapr.Client;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Xunit;

namespace Dapr.EventStore.Tests
{
    public class EventStoreTests
    {
        private readonly DaprClient client;
        private readonly DaprEventStore store;
        private readonly string streamName;

        public EventStoreTests()
        {
            Environment.SetEnvironmentVariable("DAPR_GRPC_PORT", "50001");
            var inDapr = Environment.GetEnvironmentVariable("DAPR_GRPC_PORT") != null;

            if (inDapr)
            {
                client = new DaprClientBuilder()
                    .UseJsonSerializationOptions(new JsonSerializerOptions { PropertyNameCaseInsensitive = true })
                    .Build();

                store = new DaprEventStore(client, NullLogger<DaprEventStore>.Instance)
                {
                    StoreName = "statestore",
                    MetaProvider = stream => new Dictionary<string, string>
                    {
                        { "partitionKey", streamName }
                    }
                };
            }
            else
            {
                client = new StateTestClient();
                store = new DaprEventStore(new StateTestClient(), NullLogger<DaprEventStore>.Instance);
            }

            streamName = $"teststream-{Guid.NewGuid().ToString().Substring(0, 5)}";
        }

        [Fact]
        public async Task ProtoEtagCheckAsync()
        {
            var store = "localcosmos";
            var key = Guid.NewGuid().ToString().Substring(0, 5);
            await client.SaveStateAsync(store, key, EventData.Create("test", new TestEvent("id", "hey") , 1));
            var (value, etag) = await client.GetStateAndETagAsync<EventData>(store, key);
            await client.TrySaveStateAsync(store, key, value with { Version = 2 }, etag); 
        }

        [Theory]
        [InlineData("statestore")]
        [InlineData("localcosmos")]
        [Trait("Category", "Integration")]
        public async Task ByteVsJsonCheckAsync(string store)
        {
            var key = Guid.NewGuid().ToString().Substring(0, 5);
            var @event = EventData.Create("test", new TestEvent("id", "hey"));
            var req = new StateTransactionRequest(key, JsonSerializer.SerializeToUtf8Bytes(@event), StateOperationType.Upsert);
            await client.ExecuteStateTransactionAsync(store, new[] { req });
            var (value, etag) = await client.GetStateAndETagAsync<EventData>(store, key);
        }

        [Theory]
        [InlineData("statestore")]
        [InlineData("localcosmos")]
        [Trait("Category", "Integration")]
        public async Task ByteVsJsonGetBulkCheckAsync(string store)
        {
            var key = Guid.NewGuid().ToString().Substring(0, 5);
            var @event = EventData.Create("test", new TestEvent("id", "hey"));
            var req = new StateTransactionRequest(key, JsonSerializer.SerializeToUtf8Bytes(@event), StateOperationType.Upsert);
            await client.ExecuteStateTransactionAsync(store, new[] { req });
            var items = await client.GetBulkStateAsync(store, new[] { key }, null);
            var events = items.Select(x => JsonSerializer.Deserialize<EventData>(x.Value)).ToList();
        }

        public record TestEvent(string Id, string Title);

        public class Envelope<T>
        {
            public string Messsage { get; set; }
            public T Event { get; set; }
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadReturnsDeserialized(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;
            var @event = EventData.Create("test", new TestEvent("id", "hey"));
            _ = await store.AppendToStreamAsync(streamName, 0, new EventData[] { @event });
            var stream = store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal("hey", (await stream.FirstAsync()).EventAs<TestEvent>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true }).Title);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadReturnsDeserializedEnvelope(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;
            var @event = EventData.Create("test", new Envelope<TestEvent> { Event = new TestEvent("id", "hey"), Messsage = "test" });
            _ = await store.AppendToStreamAsync(streamName, 0, new EventData[] { @event });
            var stream = store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal("hey", (await stream.LastAsync()).EventAs<Envelope<TestEvent>>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true }).Event.Title);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;
            _ = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            var stream = store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(1, (await stream.LastAsync()).Version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadMutipleReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            await store.AppendToStreamAsync(streamName, 1, new EventData[]{ EventData.Create("test", "hello 2") });
            await store.AppendToStreamAsync(streamName, 2, new EventData[]{ EventData.Create("test", "hello 3") });

            var stream = store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(3, (await stream.LastAsync()).Version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadArrayReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]
            {
                EventData.Create("test", "hello 1"),
                EventData.Create("test", "hello 2"),
                EventData.Create("test", "hello 3"),
                EventData.Create("test", "hello 4")
            });

            var stream = store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(4, (await stream.LastAsync()).Version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadMultipleArraysReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]
            {
                EventData.Create("test", "hello 1"),
                EventData.Create("test", "hello 2"),
            });

            await store.AppendToStreamAsync(streamName, 2, new EventData[]
            {
                EventData.Create("test", "hello 3"),
                EventData.Create("test", "hello 4")
            });

            var stream = store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(4, (await stream.LastAsync()).Version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadMultipleArraysReturnsVersionInSlice(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]
            {
                EventData.Create("test", "hello 1"),
                EventData.Create("test", "hello 2"),
            });

            await store.AppendToStreamAsync(streamName, 2, new EventData[]
            {
                EventData.Create("test","hello 3"),
                EventData.Create("test","hello 4")
            });

            var events = store.LoadEventStreamAsync(streamName, 2);

            Assert.Equal(4, (await events.LastAsync()).Version);
            Assert.Equal(3, await events.CountAsync());
            Assert.Equal(4, (await events.LastAsync()).Version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task AppendReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            var version = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });

            Assert.Equal(1, version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task AppendMultipleReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            await store.AppendToStreamAsync(streamName, 1, new EventData[]{ EventData.Create("test", "hello 2") });
            var version = await store.AppendToStreamAsync(streamName, 2, new EventData[]{ EventData.Create("test", "hello 3") });

            Assert.Equal(3, version);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task AppendToWrongVersionThrows(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            await Assert.ThrowsAsync<DBConcurrencyException>(async () =>
            {
                _ = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
                await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 2") });
            });
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task AppendAndLoad(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            var versionV1 = await store.AppendToStreamAsync(streamName, 0,
                new EventData[]{ EventData.Create("t", "hello 1") });
            var ev1 = store.LoadEventStreamAsync(streamName, 0);
            var events1 = await ev1.ToListAsync();
            var loadVersion1 = events1.Last().Version;

            var versionV2 = await store.AppendToStreamAsync(streamName, loadVersion1,
                new EventData[]{ EventData.Create("y", "hello 2") });
            var ev2 = store.LoadEventStreamAsync(streamName, 0);
            var events2 = await ev2.ToListAsync();
            var loadVersion2 = events2.Last().Version;

            Assert.Single(events1);
            Assert.Equal(1, loadVersion1);
            Assert.Equal(versionV1, loadVersion1);
            Assert.Equal(2, events2.Count);
            Assert.Equal(2, loadVersion2);
            Assert.Equal(versionV2, loadVersion2);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task AppendAndLoadChunk(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;

            var eventsToAppend = Enumerable.Range(0, 30)
                .Select((i,x) => EventData.Create("t", $"hello {i+1}"))
                .ToArray();

            var appendVersion = await store.AppendToStreamAsync(streamName, 0,
                eventsToAppend);
            var events = store.LoadEventStreamAsync(streamName, 0);
            var eventsList = await events.ToArrayAsync();
            var streamVersion = eventsList.Last().Version;

            Assert.Equal(30, eventsToAppend.Length);
            Assert.Equal(30, appendVersion);
            Assert.Equal(30, eventsList.Last().Version);
            Assert.Equal(30, eventsList.Length);
            Assert.Equal(30, streamVersion);
        }

        [Fact]
        public async Task BugHunt()
        {
            await store.AppendToStreamAsync(streamName, 0, new EventData[]
            {
                EventData.Create("test","hello 1"),
                //EventData.Create { Data = "hello 2" },
            });

            var stream = store.LoadEventStreamAsync(streamName, 1);
            await stream.CountAsync();
        }
    }
}
