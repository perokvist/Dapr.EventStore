using Dapr.Actors.Runtime;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;

namespace Dapr.Actors.EventStore
{
    public class StateManagerEventStore
    {
        private readonly IActorStateManager stateManager;
        public StateManagerEventStore(IActorStateManager stateManager)
        {
            this.stateManager = stateManager;
        }

        public async Task<long> AppendToStreamAsync(string streamName, params EventData[] events)
        {
            var streamKey = $"{streamName}|head";
            var head = await stateManager.GetOrAddStateAsync(streamKey, new StreamHead());

            if (!events.Any())
                return head.Version;

            var newVersion = head.Version + events.Length;
            var versionedEvents = events
                .Select((e, i) => new EventData { Data = e.Data, Version = head.Version + (i + 1) })
                .ToArray();

            var sliceKey = $"{streamName}|{newVersion}";

            await stateManager.AddStateAsync(sliceKey, versionedEvents);
            head.Version = newVersion;
            await stateManager.SetStateAsync(streamKey, head);

            await stateManager.SaveStateAsync(); //TODO switch

            return newVersion;
        }

        public async Task<(IEnumerable<EventData> Events, long Version)> LoadEventStreamAsync(string streamName, long version)
        {
            var streamKey = $"{streamName}|head";

            var head = await stateManager.GetStateAsync<StreamHead>(streamKey);

            if (head == null)
                return (Enumerable.Empty<EventData>(), new StreamHead().Version);

            var eventSlices = new List<EventData[]>();

            var next = head.Version;
            while (next != 0 && next > version)
            {
                var slice = await stateManager.GetStateAsync<EventData[]>($"{streamName}|{next}");
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

            return (events, events.LastOrDefault()?.Version ?? head.Version);
        }


        public class StreamHead
        {
            public long Version { get; set; }
        }
    }

    public class EventData
    {
        public Guid EventId { get; set; } = Guid.NewGuid();
        public string EventName { get; set; }
        public string Data { get; set; }
        public long Version { get; set; }
    }
}
