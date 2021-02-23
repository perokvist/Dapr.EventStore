using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dapr.EventStore
{
    public static class Naming
    {
        public static string StreamKey(string streamName, long version) => $"{streamName}|{version}";

        public static string StreamHead(string streamName) => $"{streamName}|head";
    }
}
