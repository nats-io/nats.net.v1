using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.Service;

namespace NATSExamples
{
    class ServiceExample
    {
        class ExampleStatsData : IStatsData
        {
            public readonly string Id;
            public readonly string Text;

            public ExampleStatsData(string id, string text)
            {
                Id = id;
                Text = text;
            }

            public ExampleStatsData(string json)
            {
                if (!string.IsNullOrEmpty(json))
                {
                    int at = json.IndexOf("\"id\"", StringComparison.Ordinal);
                    int vats = json.IndexOf("\"", at + 5, StringComparison.Ordinal);
                    int vate = json.IndexOf("\"", vats + 1, StringComparison.Ordinal);
                    Id = json.Substring(vats + 1, vate - vats - 1);

                    at = json.IndexOf("\"text\"", StringComparison.Ordinal);
                    vats = json.IndexOf("\"", at + 6, StringComparison.Ordinal);
                    vate = json.IndexOf("\"", vats + 1, StringComparison.Ordinal);
                    Text = json.Substring(vats + 1, vate - vats - 1);
                }
            }

            public string ToJson()
            {
                return "{\"id\":\"" + Id + "\",\"text\":\"" + Text + "\"}"; 
            }
        }
        
        private const string EchoService = "EchoService";
        private const string SortService = "SortService";

        static void Main(string[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:4222";
            
            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                StatsDataSupplier sds = () => new ExampleStatsData(RandomId(), RandomText());
                StatsDataDecoder sdd = json =>
                {
                    ExampleStatsData esd = new ExampleStatsData(json);
                    return string.IsNullOrEmpty(esd.Text) ? null : esd;
                };
                
                // create the services
                Service serviceEcho = new ServiceBuilder()
                    .WithConnection(c)
                    .WithName(EchoService)
                    .WithSubject(EchoService)
                    .WithDescription("An Echo Service")
                    .WithVersion("0.0.1")
                    .WithSchemaRequest("echo schema request string/url")
                    .WithSchemaResponse("echo schema response string/url")
                    .WithStatsDataHandlers(sds, sdd)
                    .WithServiceMessageHandler((sender, eArgs) => 
                        ServiceMessage.Reply(c, eArgs.Message, "Echo '" + Encoding.UTF8.GetString(eArgs.Message.Data) + "'"))
                    .Build();
                Console.WriteLine(serviceEcho);

                ServiceBuilder serviceBuilderSort = new ServiceBuilder()
                    .WithConnection(c)
                    .WithName(SortService)
                    .WithSubject(SortService)
                    .WithDescription("A Sort Service")
                    .WithVersion("0.0.2")
                    .WithSchemaRequest("sort schema request string/url")
                    .WithSchemaResponse("sort schema response string/url")
                    .WithServiceMessageHandler((sender, eArgs) =>
                    {
                        byte[] data = eArgs.Message.Data;
                        Array.Sort(data);
                        ServiceMessage.Reply(c, eArgs.Message, "Sort '" + Encoding.UTF8.GetString(data) + "'");
                    });
                
                Service serviceSort = serviceBuilderSort.Build();
                Service serviceAnotherSort = serviceBuilderSort.Build();
                Console.WriteLine(serviceSort);
                Console.WriteLine(serviceAnotherSort);
                
                // ----------------------------------------------------------------------------------------------------
                // Start the services
                // ----------------------------------------------------------------------------------------------------
                Task<bool> taskEcho = serviceEcho.StartService();
                Task<bool> taskSort = serviceSort.StartService();
                Task<bool> taskAnotherSort = serviceAnotherSort.StartService();
                
                // ----------------------------------------------------------------------------------------------------
                // Call the services
                // ----------------------------------------------------------------------------------------------------
                string request = RandomText();
                Msg reply = c.Request(EchoService, Encoding.UTF8.GetBytes(request), 200);
                string response = Encoding.UTF8.GetString(reply.Data);
                Console.WriteLine("\nCalled " + EchoService + " with [" + request + "] Received [" + response + "]");

                request = RandomText();
                reply = c.Request(SortService, Encoding.UTF8.GetBytes(request), 200);
                response = Encoding.UTF8.GetString(reply.Data);
                Console.WriteLine("Called " + SortService + " with [" + request + "] Received [" + response + "]");

                // ----------------------------------------------------------------------------------------------------
                // discovery
                // ----------------------------------------------------------------------------------------------------
                Discovery discovery = new Discovery(c, 1000, 3);

                // ----------------------------------------------------------------------------------------------------
                // ping discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<Ping> pings = discovery.Ping();
                PrintDiscovery("Ping", "[All]", pings);

                pings = discovery.Ping(EchoService);
                PrintDiscovery("Ping", EchoService, pings);

                string echoId = pings[0].ServiceId;
                Ping ping = discovery.PingForNameAndId(EchoService, echoId);
                PrintDiscovery("Ping", EchoService, echoId, ping);

                pings = discovery.Ping(SortService);
                PrintDiscovery("Ping", SortService, pings);

                string sortId = pings[0].ServiceId;
                ping = discovery.PingForNameAndId(SortService, sortId);
                PrintDiscovery("Ping", SortService, sortId, ping);

                // ----------------------------------------------------------------------------------------------------
                // info discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<Info> infos = discovery.Info();
                PrintDiscovery("Info", "[All]", infos);

                infos = discovery.Info(EchoService);
                PrintDiscovery("Info", EchoService, infos);

                Info info = discovery.InfoForNameAndId(EchoService, echoId);
                PrintDiscovery("Info", EchoService, echoId, info);

                infos = discovery.Info(SortService);
                PrintDiscovery("Info", SortService, infos);

                info = discovery.InfoForNameAndId(SortService, sortId);
                PrintDiscovery("Info", SortService, sortId, info);

                // ----------------------------------------------------------------------------------------------------
                // schema discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<SchemaInfo> schemaInfos = discovery.Schema();
                PrintDiscovery("Schema", "[All]", schemaInfos);

                schemaInfos = discovery.Schema(EchoService);
                PrintDiscovery("Schema", EchoService, schemaInfos);

                SchemaInfo schemaInfo = discovery.SchemaForNameAndId(EchoService, echoId);
                PrintDiscovery("Schema", EchoService, echoId, schemaInfo);

                schemaInfos = discovery.Schema(SortService);
                PrintDiscovery("Schema", SortService, schemaInfos);

                schemaInfo = discovery.SchemaForNameAndId(SortService, sortId);
                PrintDiscovery("Schema", SortService, sortId, schemaInfo);

                // ----------------------------------------------------------------------------------------------------
                // stats discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<Stats> statsList = discovery.Stats(null, sdd);
                PrintDiscovery("Stats", "[All]", statsList);

                statsList = discovery.Stats(EchoService);
                PrintDiscovery("Stats", EchoService, statsList); // will show echo without data decoder

                statsList = discovery.Stats(SortService);
                PrintDiscovery("Stats", SortService, statsList);
                
                // ----------------------------------------------------------------------------------------------------
                // stop the service
                // ----------------------------------------------------------------------------------------------------
                serviceEcho.Stop();
                serviceSort.Stop();
                serviceAnotherSort.Stop();
                Console.WriteLine("\nEcho service done ? " + taskEcho.Wait(1000));
                Console.WriteLine("Sort service done ? " + taskSort.Wait(1000));
                Console.WriteLine("Another Sort service done ? " + taskAnotherSort.Wait(1000));
            }
        }
 
        private static void PrintDiscovery(string action, string serviceName, string serviceId, object o) {
            Console.WriteLine("\n" + action  + " " + serviceName + " " + serviceId + "\n  " + o);
        }
        
        private static void PrintDiscovery<T>(string action, string label, IList<T> objects) {
            Console.WriteLine("\n" + action + " " + label);
            foreach (object o in objects) {
                Console.WriteLine("  " + o);
            }
        }

        private static readonly Random Random = new Random();
        private const string RandomChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789abcdefghijklmnopqrstuvwxyz";
        private const string RandomIdChars = "0123456789abcdef";
        private static string RandomText()
        {
            return new string(Enumerable.Repeat(RandomChars, 20)
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }
        
        private static string RandomId()
        {
            return new string(Enumerable.Repeat(RandomIdChars, 6)
                .Select(s => s[Random.Next(s.Length)]).ToArray());
        }
    }
}