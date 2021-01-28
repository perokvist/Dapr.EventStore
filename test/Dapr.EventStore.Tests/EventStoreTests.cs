using Dapr.Client;
using Grpc.Net.Client;
using Microsoft.Extensions.Logging.Abstractions;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Xunit;

namespace Dapr.EventStore.Tests
{
    public class EventStoreTests
    {
        //private readonly DaprClient daprClient;
        private StateTestClient client;
        private readonly DaprEventStore store;

        public EventStoreTests()
        {
            client = new StateTestClient();
            store = new DaprEventStore(client, NullLogger<DaprEventStore>.Instance);
        }

        [Fact]
        public async Task ClientTestAsync()
        {
            await client.SaveStateAsync("testStore", "test", new Widget() { Size = "small", Count = 17, });
        }

        public class Widget
        {
            public string Size { get; set; }

            public int Count { get; set; }
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;
            _ = await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
            var stream = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(1, stream.Version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadMutipleReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
            await store.AppendToStreamAsync("test", 1, new EventData[] { new EventData { Data = "hello 2" } });
            await store.AppendToStreamAsync("test", 2, new EventData[] { new EventData { Data = "hello 3" } });

            var stream = await store.LoadEventStreamAsync("test", 0);

            Assert.Equal(3, stream.Version);
        }


        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadArrayReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadMultipleArraysReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task LoadMultipleArraysReturnsVersionInSlice(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            var version = await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });

            Assert.Equal(1, version);
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendMultipleReturnsVersion(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

            await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
            await store.AppendToStreamAsync("test", 1, new EventData[] { new EventData { Data = "hello 2" } });
            var version = await store.AppendToStreamAsync("test", 2, new EventData[] { new EventData { Data = "hello 3" } });

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
                _ = await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 1" } });
                await store.AppendToStreamAsync("test", 0, new EventData[] { new EventData { Data = "hello 2" } });
            });
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task AppendAndLoad(bool useTransaction)
        {
            store.UseTransaction = useTransaction;

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

        [Fact]
        public async Task BugHunt()
        {
            await store.AppendToStreamAsync("test", 0, new EventData[]
            {
                new EventData { Data = "hello 1" },
                //new EventData { Data = "hello 2" },
            });

            var stream = await store.LoadEventStreamAsync("test", 1);
        }
    }
}
