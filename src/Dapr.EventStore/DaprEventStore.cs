using Dapr.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text.Json;
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
            var streamHeadKey = $"{streamName}|head";
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

            var sliceKey = $"{streamName}|{newVersion}";
            var (slice, sliceetag) = await client.GetStateAndETagAsync<EventData[]>(StoreName, sliceKey, metadata: meta);
            if (slice != null)
                throw new DBConcurrencyException($"Event slice {sliceKey} ending with event version {newVersion} already exists");

            head = new StreamHead(newVersion);

            var task = Mode switch
            {
                SliceMode.Off => StateTransaction2(streamName, streamHeadKey, head, headetag, meta, versionedEvents),
                SliceMode.Transactional => StateTransaction(streamHeadKey, head, headetag, meta, versionedEvents, sliceKey, sliceetag),
                SliceMode.TwoPhased => TwoPhased(streamHeadKey, head, headetag, meta, newVersion, versionedEvents, sliceKey, sliceetag),
                _ => throw new Exception("Mode not supported")
            };

            await task;
            return newVersion;

            async Task StateTransaction(string streamHeadKey, StreamHead head, string headetag, Dictionary<string, string> meta, EventData[] versionedEvents, string sliceKey, string sliceetag)
            {
                var sliceReq = new StateTransactionRequest(sliceKey, JsonSerializer.SerializeToUtf8Bytes(versionedEvents), Client.StateOperationType.Upsert, string.IsNullOrWhiteSpace(sliceetag) ? null : sliceetag, metadata: meta);
                var headReq = new StateTransactionRequest(streamHeadKey, JsonSerializer.SerializeToUtf8Bytes(head), Client.StateOperationType.Upsert, etag: string.IsNullOrWhiteSpace(headetag) ? null : headetag, metadata: meta);
                var reqs = new List<StateTransactionRequest> { sliceReq, headReq };

                await client.ExecuteStateTransactionAsync(StoreName, reqs, meta);
            }

            async Task TwoPhased(string streamHeadKey, StreamHead head, string headetag, Dictionary<string, string> meta, long newVersion, EventData[] versionedEvents, string sliceKey, string sliceetag)
            {
                var sliceWriteSuccess = await client.TrySaveStateAsync(StoreName, sliceKey, versionedEvents, sliceetag, metadata: meta);
                if (!sliceWriteSuccess)
                    throw new DBConcurrencyException($"Error writing events. Event slice {sliceKey} ending with event version {newVersion} already exists");

                var headWriteSuccess = await client.TrySaveStateAsync(StoreName, streamHeadKey, head, headetag, metadata: meta);
                if (!headWriteSuccess)
                    throw new DBConcurrencyException($"stream head {streamHeadKey} have been updated");
            }

            async Task StateTransaction2(string streamName, string streamHeadKey, StreamHead head, string headetag, Dictionary<string, string> meta, EventData[] versionedEvents)
            {
                var eventsReq = versionedEvents.Select(x => new StateTransactionRequest($"{streamName}|{x.Version}", JsonSerializer.SerializeToUtf8Bytes(x), StateOperationType.Upsert, metadata: meta));
                var headReq = new StateTransactionRequest(streamHeadKey, JsonSerializer.SerializeToUtf8Bytes(head), Client.StateOperationType.Upsert, etag: string.IsNullOrWhiteSpace(headetag) ? null : headetag, metadata: meta);
                var reqs = new List<StateTransactionRequest>();
                reqs.AddRange(eventsReq);
                reqs.Add(headReq);

                await client.ExecuteStateTransactionAsync(StoreName, reqs, meta);
            }
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
                var keys = Enumerable
                    .Range(version == default ? 1 : (int)version + 1, (int)(head.Value.Version - version))
                    .Select(x => $"{streamName}|{x}")
                    .ToList();

                if (!keys.Any())
                    return (Enumerable.Empty<EventData>(), new StreamHead().Version);

                var events = (await client.GetBulkStateAsync(StoreName, keys, null, metadata: meta))
                    .Select(x => JsonSerializer.Deserialize<EventData>(x.Value))
                    .OrderBy(x => x.Version);

                return (events, events.LastOrDefault()?.Version ?? head.Value.Version);
            }


            using (logger.BeginScope("Loading head {streamName}. Starting version {version}", head.Key, head.Value.Version))
            {
                var next = head.Value.Version;

                while (next != 0 && next > version)
                {
                    var sliceKey = $"{streamName}|{next}";
                    var slice = await client.GetStateAsync<EventData[]>(StoreName, sliceKey, metadata: meta);

                    logger.LogDebug("Slice {sliceKey} loaded range : {firstVersion} - {lastVersion}", sliceKey, slice.First().Version, slice.Last().Version);
                    next = slice.First().Version - 1;

                    if (next < version)
                    {
                        logger.LogDebug("Version within slice. Next : {next}. Version : {version}", next, version);
                        eventSlices.Add(slice.Where(e => e.Version > version).ToArray());
                        break;
                    }

                    logger.LogDebug("Adding slice. Next : {next}. Version : {version}", next, version);

                    eventSlices.Add(slice);
                }

                logger.LogDebug("Done reading. Got {sliceCount} slices.", eventSlices.Count());

                var events = eventSlices
                    .Reverse<EventData[]>()
                    .SelectMany(e => e)
                    .ToArray();

                return (events, events.LastOrDefault()?.Version ?? head.Value.Version);
            }
        }

        public record StreamHead(long Version = 0);

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
