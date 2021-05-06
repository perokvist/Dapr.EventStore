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
        private readonly ILogger<DaprEventStore> logger;

        public string StoreName { get; set; } = "statestore";

        public enum SliceMode
        {
            Off = 0,
            TwoPhased = 10,
            Transactional = 20
        }

        public SliceMode Mode { get; set; } = SliceMode.Off;

        public Func<string, Dictionary<string, string>> MetaProvider { get; set; } = streamName => new Dictionary<string, string>();

        public DaprEventStore(global::Dapr.Client.DaprClient client, ILogger<DaprEventStore> logger)
        {
            this.client = client;
            this.logger = logger;
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
            var streamHeadKey = Naming.StreamHead(streamName);
            var meta = MetaProvider(streamName);
            var (head, headetag) = await client.GetStateAndETagAsync<StreamHead>(StoreName, streamHeadKey, metadata: meta);

            if (head == null)
                head = new StreamHead();

            if (!events.Any())
                return head.Version;

            concurrencyGuard(head);

            var newVersion = head.Version + events.Length;
            var versionedEvents = events
                .Select((e, i) => new EventData(e.EventId, e.EventName, e.Data, head.Version + (i + 1)))
                .ToArray();

            var sliceKey = Naming.StreamKey(streamName, newVersion);
            var (slice, sliceetag) = await client.GetStateAndETagAsync<EventData[]>(StoreName, sliceKey, metadata: meta);
            if (slice != null)
                throw new DBConcurrencyException($"Event slice {sliceKey} ending with event version {newVersion} already exists");

            head = new StreamHead(newVersion);

            var task = Mode switch
            {
                SliceMode.Off => client.StateTransactionAsync(StoreName, streamName, streamHeadKey, head, headetag, meta, versionedEvents),
                SliceMode.Transactional => client.StateTransactionSliceAsync(StoreName, streamHeadKey, head, headetag, meta, versionedEvents, sliceKey, sliceetag),
                SliceMode.TwoPhased => client.TwoPhasedAsync(StoreName, streamHeadKey, head, headetag, meta, newVersion, versionedEvents, sliceKey, sliceetag),
                _ => throw new Exception("Mode not supported")
            };

            await task;
            return newVersion;
        }

        public async Task<(IEnumerable<EventData> Events, long Version)> LoadEventStreamAsync(string streamName, long version)
        {
            var meta = MetaProvider(streamName);
            var head = await client.GetStateEntryAsync<StreamHead>(StoreName, $"{streamName}|head", metadata: meta);

            if (head.Value == null)
                return (Enumerable.Empty<EventData>(), new StreamHead().Version);

            var eventSlices = new List<EventData[]>();

            if (Mode == SliceMode.Off)
            {
                var (events, v) = client.LoadBulkEventsAsync(StoreName, streamName, version, meta, head);
                return (events.ToEnumerable(), await v());
            }

            return await client.LoadSlicesAsync(StoreName, logger, streamName, version, meta, head, eventSlices);
        }

        public record StreamHead(long Version = 0)
        {
            public StreamHead() : this(0)
            { }
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

    public record EventData(string EventId, string EventName, object Data, long Version = 0)
    {
        public static EventData Create(string EventName, object Data, long Version = 0) => new EventData(Guid.NewGuid().ToString(), EventName, Data, Version);
    }
}
