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
            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal("hey", stream.Events.First().EventAs<TestEvent>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true }).Title);
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
            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal("hey", stream.Events.First().EventAs<Envelope<TestEvent>>(new JsonSerializerOptions { PropertyNameCaseInsensitive = true }).Event.Title);
        }

        [Theory]
        [InlineData(DaprEventStore.SliceMode.Off)]
        [InlineData(DaprEventStore.SliceMode.TwoPhased)]
        [InlineData(DaprEventStore.SliceMode.Transactional)]
        public async Task LoadReturnsVersion(DaprEventStore.SliceMode sliceMode)
        {
            store.Mode = sliceMode;
            _ = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(1, stream.Version);
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

            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(3, stream.Version);
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

            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(4, stream.Version);
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

            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(4, stream.Version);
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

            var stream = await store.LoadEventStreamAsync(streamName, 2);

            Assert.Equal(4, stream.Events.Last().Version);
            Assert.Equal(3, stream.Events.Count());
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
            var streamV1 = await store.LoadEventStreamAsync(streamName, 0);
            var versionV2 = await store.AppendToStreamAsync(streamName, streamV1.Version,
                new EventData[]{ EventData.Create("y", "hello 2") });
            var streamV2 = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(1, streamV1.Version);
            Assert.Equal(versionV1, streamV1.Version);
            Assert.Equal(2, streamV2.Version);
            Assert.Equal(versionV2, streamV2.Version);
        }

        [Fact]
        public async Task BugHunt()
        {
            await store.AppendToStreamAsync(streamName, 0, new EventData[]
            {
                EventData.Create("test","hello 1"),
                //EventData.Create { Data = "hello 2" },
            });

            var stream = await store.LoadEventStreamAsync(streamName, 1);
        }
    }
}
