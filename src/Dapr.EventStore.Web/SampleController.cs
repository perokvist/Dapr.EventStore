using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace Dapr.EventStore.Web
{
    public class SampleController : ControllerBase
    {
        private readonly DaprEventStore store;
        private readonly ILogger logger;

        public SampleController(DaprEventStore store, ILogger<SampleController> logger)
        {
            this.store = store;
            this.logger = logger;
        }

        [Topic( pubsubName: "in" , name:"sample")]
        [HttpPost("sample")]
        public async Task<ActionResult> Post()
        {
            this.logger.LogInformation("C# got event (pub/sub");
            return Ok(await store.AppendToStreamAsync("sample", EventData.Create("test", "hello")));
        }
    }
    public class SampleEvent
    {
        public string Message { get; set; }
    }
}
