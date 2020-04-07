using Microsoft.Extensions.Logging;
using System;
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

        public Task<long> AppendToStreamAsync(string streamName, long version, params EventData[] events)
            => AppendToStreamAsync(
                streamName,
                Concurrency.Match(version),
                events);

        public Task<long> AppendToStreamAsync(string streamName, params EventData[] events)
            => AppendToStreamAsync(
                streamName,
                Concurrency.Ignore(),
                events);

        public async Task<long> AppendToStreamAsync(string streamName, Action<StreamHead> concurrencyGuard, params EventData[] events)
        {
            var head = await client.GetStateEntryAsync<StreamHead>(StoreName, $"{streamName}|head");

            if (head.Value == null)
                head.Value = new StreamHead();

            if (!events.Any())
                return head.Value.Version;

            concurrencyGuard(head.Value);
        
            var newVersion = head.Value.Version + events.Length;
            var versionedEvents = events
                .Select((e, i) => new EventData { Data = e.Data, Version = head.Value.Version + (i + 1) })
                .ToArray();
            await client.SaveStateAsync(StoreName, $"{streamName}|{newVersion}", versionedEvents);
            head.Value.Version = newVersion;
            await head.SaveAsync();
            return newVersion;
        }

        public async Task<(IEnumerable<EventData> Events, long Version)> LoadEventStreamAsync(string streamName, long version)
        {
            var head = await client.GetStateEntryAsync<StreamHead>(StoreName, $"{streamName}|head");

            if (head.Value == null)
                return (Enumerable.Empty<EventData>(), new StreamHead().Version);

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
                .SelectMany(e => e)
                .ToArray();

            return (events, events.Last().Version);
        }

        public class StreamHead
        {
            public long Version { get; set; }
        }

        public class Concurrency
        {
            public static Action<StreamHead> Match(long version) => head =>
            {
                if (head.Version != version)
                    throw new DBConcurrencyException($"wrong version - expected {version} but was {head.Version}");
            };

            public static Action<StreamHead> Ignore() => _ => { };
        }
    }

    public class EventData
    {
        public string Data { get; set; }
        public long Version { get; internal set; }
    }
}
