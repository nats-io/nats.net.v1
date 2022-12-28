// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.Service;
using static NATSExamples.JsUtils;

namespace NATSExamples
{
    abstract class ServiceExample
    {
        private const string EchoServiceName = "ECHO_SERVICE";
        private const string SortServiceName = "SORT_SERVICE";
        private const string EchoServiceSubject = "echo";
        private const string SortServiceSubject = "sort";

        public static void Main(string[] args)
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
                    .WithName(EchoServiceName)
                    .WithSubject(EchoServiceSubject)
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
                    .WithName(SortServiceName)
                    .WithSubject(SortServiceSubject)
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
                Msg reply = c.Request(EchoServiceSubject, Encoding.UTF8.GetBytes(request), 200);
                string response = Encoding.UTF8.GetString(reply.Data);
                Console.WriteLine("\nCalled " + EchoServiceSubject + " with [" + request + "] Received [" + response + "]");

                request = RandomText();
                reply = c.Request(SortServiceSubject, Encoding.UTF8.GetBytes(request), 200);
                response = Encoding.UTF8.GetString(reply.Data);
                Console.WriteLine("Called " + SortServiceSubject + " with [" + request + "] Received [" + response + "]");

                // ----------------------------------------------------------------------------------------------------
                // discovery
                // ----------------------------------------------------------------------------------------------------
                Discovery discovery = new Discovery(c, 1000, 3);

                // ----------------------------------------------------------------------------------------------------
                // ping discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<PingResponse> pings = discovery.Ping();
                PrintDiscovery("Ping", "[All]", pings);

                pings = discovery.Ping(EchoServiceName);
                PrintDiscovery("Ping", EchoServiceName, pings);

                string echoId = pings[0].ServiceId;
                PingResponse pingResponse = discovery.PingForNameAndId(EchoServiceName, echoId);
                PrintDiscovery("Ping", EchoServiceName, echoId, pingResponse);

                pings = discovery.Ping(SortServiceName);
                PrintDiscovery("Ping", SortServiceName, pings);

                string sortId = pings[0].ServiceId;
                pingResponse = discovery.PingForNameAndId(SortServiceName, sortId);
                PrintDiscovery("Ping", SortServiceName, sortId, pingResponse);

                // ----------------------------------------------------------------------------------------------------
                // info discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<InfoResponse> infos = discovery.Info();
                PrintDiscovery("Info", "[All]", infos);

                infos = discovery.Info(EchoServiceName);
                PrintDiscovery("Info", EchoServiceName, infos);

                InfoResponse infoResponse = discovery.InfoForNameAndId(EchoServiceName, echoId);
                PrintDiscovery("Info", EchoServiceName, echoId, infoResponse);

                infos = discovery.Info(SortServiceName);
                PrintDiscovery("Info", SortServiceName, infos);

                infoResponse = discovery.InfoForNameAndId(SortServiceName, sortId);
                PrintDiscovery("Info", SortServiceName, sortId, infoResponse);

                // ----------------------------------------------------------------------------------------------------
                // schema discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<SchemaResponse> schemaResponses = discovery.Schema();
                PrintDiscovery("Schema", "[All]", schemaResponses);

                schemaResponses = discovery.Schema(EchoServiceName);
                PrintDiscovery("Schema", EchoServiceName, schemaResponses);

                SchemaResponse schemaResponse = discovery.SchemaForNameAndId(EchoServiceName, echoId);
                PrintDiscovery("Schema", EchoServiceName, echoId, schemaResponse);

                schemaResponses = discovery.Schema(SortServiceName);
                PrintDiscovery("Schema", SortServiceName, schemaResponses);

                schemaResponse = discovery.SchemaForNameAndId(SortServiceName, sortId);
                PrintDiscovery("Schema", SortServiceName, sortId, schemaResponse);

                // ----------------------------------------------------------------------------------------------------
                // stats discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<StatsResponse> statsList = discovery.Stats(null, sdd);
                PrintDiscovery("Stats", "[All]", statsList);

                statsList = discovery.Stats(EchoServiceName);
                PrintDiscovery("Stats", EchoServiceName, statsList); // will show echo without data decoder

                statsList = discovery.Stats(SortServiceName);
                PrintDiscovery("Stats", SortServiceName, statsList);
                
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
    }
    
    public class ExampleStatsData : IStatsData
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
}