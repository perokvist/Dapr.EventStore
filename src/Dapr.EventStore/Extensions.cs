﻿using Dapr.Client;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using static Dapr.EventStore.DaprEventStore;

namespace Dapr.EventStore
{
    public static class Extensions
    {
        public static async Task<(IEnumerable<EventData> Events, long Version)> LoadSlicesAsync(
            this DaprClient client, string storeName, ILogger logger,
            string streamName, long version, Dictionary<string, string> meta, StateEntry<StreamHead> head, List<EventData[]> eventSlices)
        {
            using (logger.BeginScope("Loading head {streamName}. Starting version {version}", head.Key, head.Value.Version))
            {
                var next = head.Value.Version;

                while (next != 0 && next >= version)
                {
                    var sliceKey = $"{streamName}|{next}";
                    var slice = await client.GetStateAsync<EventData[]>(storeName, sliceKey, metadata: meta);

                    logger.LogDebug("Slice {sliceKey} loaded range : {firstVersion} - {lastVersion}", sliceKey, slice.First().Version, slice.Last().Version);
                    next = slice.First().Version - 1;

                    if (next < version)
                    {
                        logger.LogDebug("Version within slice. Next : {next}. Version : {version}", next, version);
                        eventSlices.Add(slice.Where(e => e.Version >= version).ToArray());
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

        public static async Task<(IEnumerable<EventData> Events, long Version)> LoadBulkEventsAsync(
            this DaprClient client, string storeName,
            string streamName, long version, Dictionary<string, string> meta, StateEntry<StreamHead> head)
        {
            var keys = Enumerable
                .Range(version == default ? 1 : (int)version, (int)(head.Value.Version - version))
                .Select(x => $"{streamName}|{x}")
                .ToList();

            if (!keys.Any())
                return (Enumerable.Empty<EventData>(), new StreamHead().Version);

            var events = (await client.GetBulkStateAsync(storeName, keys, null, metadata: meta))
                .Select(x => JsonSerializer.Deserialize<EventData>(x.Value))
                .OrderBy(x => x.Version);

            return (events, events.LastOrDefault()?.Version ?? head.Value.Version);
        }


        public static async Task StateTransactionSliceAsync(this DaprClient client,
            string storeName,
            string streamHeadKey, StreamHead head, string headetag, Dictionary<string, string> meta, EventData[] versionedEvents, string sliceKey, string sliceetag)
        {
            var sliceReq = new StateTransactionRequest(sliceKey, JsonSerializer.SerializeToUtf8Bytes(versionedEvents), Client.StateOperationType.Upsert, string.IsNullOrWhiteSpace(sliceetag) ? null : sliceetag, metadata: meta);
            var headReq = new StateTransactionRequest(streamHeadKey, JsonSerializer.SerializeToUtf8Bytes(head), Client.StateOperationType.Upsert, etag: string.IsNullOrWhiteSpace(headetag) ? null : headetag, metadata: meta);
            var reqs = new List<StateTransactionRequest> { sliceReq, headReq };

            await client.ExecuteStateTransactionAsync(storeName, reqs, meta);
        }

        public static async Task TwoPhasedAsync(this DaprClient client,
            string storeName,
            string streamHeadKey, StreamHead head, string headetag, Dictionary<string, string> meta, long newVersion, EventData[] versionedEvents, string sliceKey, string sliceetag)
        {
            var sliceWriteSuccess = await client.TrySaveStateAsync(storeName, sliceKey, versionedEvents, sliceetag, metadata: meta);
            if (!sliceWriteSuccess)
                throw new DBConcurrencyException($"Error writing events. Event slice {sliceKey} ending with event version {newVersion} already exists");

            var headWriteSuccess = await client.TrySaveStateAsync(storeName, streamHeadKey, head, headetag, metadata: meta);
            if (!headWriteSuccess)
                throw new DBConcurrencyException($"stream head {streamHeadKey} have been updated");
        }

        public static async Task StateTransactionAsync(this DaprClient client,
            string storeName,
            string streamName, string streamHeadKey, StreamHead head, string headetag, Dictionary<string, string> meta, EventData[] versionedEvents)
        {
            var eventsReq = versionedEvents.Select(x => new StateTransactionRequest($"{streamName}|{x.Version}", JsonSerializer.SerializeToUtf8Bytes(x), StateOperationType.Upsert, metadata: meta));
            var headReq = new StateTransactionRequest(streamHeadKey, JsonSerializer.SerializeToUtf8Bytes(head), Client.StateOperationType.Upsert, etag: string.IsNullOrWhiteSpace(headetag) ? null : headetag, metadata: meta);
            var reqs = new List<StateTransactionRequest>();
            reqs.AddRange(eventsReq);
            reqs.Add(headReq);

            await client.ExecuteStateTransactionAsync(storeName, reqs, meta);
        }

    }
}