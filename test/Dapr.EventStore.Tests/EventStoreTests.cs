using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapr.EventStore.Tests
{
    public class EventStoreTests
    {
        [Fact]
        public async Task LoadReturnsVersion()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            _ = await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
            var stream = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(1, stream.Version);
        }

        [Fact]
        public async Task LoadMutipleReturnsVersion()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
            await store.AppendToStreamAsync("test", 1, new EventData[] { new EventData { Data = "hello 2" } });
            await store.AppendToStreamAsync("test", 2, new EventData[] { new EventData { Data = "hello 3" } });

            var stream = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(3, stream.Version);
        }

        [Fact]
        public async Task LoadArrayReturnsVersion()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            await store.AppendToStreamAsync("test", 0, new EventData[]
            {
                new EventData { Data = "hello 1" },
                new EventData { Data = "hello 2" },
                new EventData { Data = "hello 3" },
                new EventData { Data = "hello 4" }
            });

            var stream = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(4, stream.Version);
        }

        [Fact]
        public async Task LoadMultipleArraysReturnsVersion()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            await store.AppendToStreamAsync("test", 0, new EventData[]
            {
                new EventData { Data = "hello 1" },
                new EventData { Data = "hello 2" },
            });

            await store.AppendToStreamAsync("test", 2, new EventData[]
            {
                new EventData { Data = "hello 3" },
                new EventData { Data = "hello 4" }
            });

            var stream = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(4, stream.Version);
        }

        [Fact]
        public async Task LoadMultipleArraysReturnsVersionInSlice()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            await store.AppendToStreamAsync("test", 0, new EventData[]
            {
                new EventData { Data = "hello 1" },
                new EventData { Data = "hello 2" },
            });

            await store.AppendToStreamAsync("test", 2, new EventData[]
            {
                new EventData { Data = "hello 3" },
                new EventData { Data = "hello 4" }
            });

            var stream = await store.LoadEventStreamAsync("test", 1);

            Assert.Equal(4, stream.Events.Last().Version);
            Assert.Equal(3, stream.Events.Count());
        }

        [Fact]
        public async Task AppendReturnsVersion()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            var version = await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });

            Assert.Equal(1, version);
        }

        [Fact]
        public async Task AppendMultipleReturnsVersion()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
            await store.AppendToStreamAsync("test", 1, new EventData[] { new EventData { Data = "hello 2" } });
            var version = await store.AppendToStreamAsync("test", 2, new EventData[] { new EventData { Data = "hello 3" } });

            Assert.Equal(3, version);
        }

        [Fact]
        public async Task AppendToWrongVersionThrows()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            await Assert.ThrowsAsync<DBConcurrencyException>(async () =>
            {
                _ = await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
                await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 2" } });
            });
        }

        [Fact]
        public async Task AppendAndLoad()
        {
            var client = new StateTestClient();
            var store = new DaprEventStore(client);

            var versionV1 = await store.AppendToStreamAsync("test", 0,
                new EventData[] { new EventData { Data = "hello 1" } });
            var streamV1 = await store.LoadEventStreamAsync("test", 0);
            var versionV2 = await store.AppendToStreamAsync("test", streamV1.Version,
                new EventData[] { new EventData { Data = "hello 2" } });
            var streamV2 = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(1, streamV1.Version);
            Assert.Equal(versionV1, streamV1.Version);
            Assert.Equal(2, streamV2.Version);
            Assert.Equal(versionV2, streamV2.Version);
        }
    }
}
