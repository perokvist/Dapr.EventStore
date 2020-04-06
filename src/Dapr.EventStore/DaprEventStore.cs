using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapr.EventStore
{
    public class DaprEventStore 
    {
        private readonly global::Dapr.Client.DaprClient client;
        public string StoreName { get; set; } = "statestore";

        public DaprEventStore(global::Dapr.Client.DaprClient client)
        {
            this.client = client;
        }

        public async Task<long> AppendToStreamAsync(string streamName, long version, params EventData[] events)
        {
            var head = await client.GetStateEntryAsync<StreamHead>(StoreName, $"{streamName}|head");

            if (head.Value == null)
                head.Value = new StreamHead();

            if (!events.Any())
                return head.Value.Version;

            if (head.Value.Version != version)
            {
                var sliceExists = (await client.GetStateEntryAsync<EventData[]>(StoreName, $"{streamName}|{head.Value.Version}")).Value != null;
                if(sliceExists)
                    throw new DBConcurrencyException($"wrong version - expected {version} but was {head.Value.Version}");
            }

            var newVersion = head.Value.Version + events.Length;
            head.Value.Version = newVersion;
            await head.SaveAsync();
            var versionedEvents = events
                .Select((e, i) => new EventData { Data = e.Data, Version = version + (i + 1)})
                .ToArray();
            await client.SaveStateAsync(StoreName, $"{streamName}|{newVersion}", versionedEvents);
            return newVersion;
        }

        public async Task<(IEnumerable<EventData> Events, long Version)> LoadEventStreamAsync(string streamName, long version)
        {
            var head = await client.GetStateEntryAsync<StreamHead>(StoreName, $"{streamName}|head");

            if (head.Value == null)
                return (Enumerable.Empty<EventData>(), 0);

            var eventSlices = new List<EventData[]>();

            var next = head.Value.Version;
            while (next != 0 && next > version)
            {
                var slice = await client.GetStateAsync<EventData[]>(StoreName, $"{streamName}|{next}");
                next = slice.First().Version - 1;

                if (next < version)
                { 
                    eventSlices.Add(slice.Where(e => e.Version > version).ToArray());
                    break;
                }

                eventSlices.Add(slice);
            }

            var events = eventSlices
                .Reverse<EventData[]>()
                .SelectMany(e => e);

            return (events, events.Last().Version);
        }

        public class StreamHead
        {
            public long Version { get; set; }
        }
    }

    public class EventData
    {
        public string Data { get; set; }
        public long Version { get; internal set; }
    }
}
