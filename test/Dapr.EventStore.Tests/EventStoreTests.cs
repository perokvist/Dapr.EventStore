using Dapr.Client;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging.Abstractions;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
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
            //Environment.SetEnvironmentVariable("DAPR_GRPC_PORT", "50000");
            var inDapr = Environment.GetEnvironmentVariable("DAPR_GRPC_PORT") != null;

            if (inDapr)
            {
                client = new DaprClientBuilder().Build();
                store = new DaprEventStore(client, NullLogger<DaprEventStore>.Instance)
                {
                    StoreName = "localcosmos",
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
        public async Task EtagBugAsync()
        {
            var store = "localcosmos";
            var key = Guid.NewGuid().ToString().Substring(0, 5);
            await client.SaveStateAsync(store, key, EventData.Create("test", "testing", 1));
            var (value, etag) = await client.GetStateAndETagAsync<EventData>(store, key);
            await client.TrySaveStateAsync(store, key, value with { Version = 2 }, etag);
        }


        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;
            _ = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(1, stream.Version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadMutipleReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            await store.AppendToStreamAsync(streamName, 1, new EventData[]{ EventData.Create("test", "hello 2") });
            await store.AppendToStreamAsync(streamName, 2, new EventData[]{ EventData.Create("test", "hello 3") });

            var stream = await store.LoadEventStreamAsync(streamName, 0);

            Assert.Equal(3, stream.Version);
        }


        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadArrayReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadMultipleArraysReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadMultipleArraysReturnsVersionInSlice(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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

            var stream = await store.LoadEventStreamAsync(streamName, 1);

            Assert.Equal(4, stream.Events.Last().Version);
            Assert.Equal(3, stream.Events.Count());
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            var version = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });

            Assert.Equal(1, version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendMultipleReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
            await store.AppendToStreamAsync(streamName, 1, new EventData[]{ EventData.Create("test", "hello 2") });
            var version = await store.AppendToStreamAsync(streamName, 2, new EventData[]{ EventData.Create("test", "hello 3") });

            Assert.Equal(3, version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendToWrongVersionThrows(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            await Assert.ThrowsAsync<DBConcurrencyException>(async () =>
            {
                _ = await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 1") });
                await store.AppendToStreamAsync(streamName, 0, new EventData[]{ EventData.Create("test", "hello 2") });
            });
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendAndLoad(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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
