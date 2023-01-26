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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using NATS.Client.Service;

namespace NATSExamples
{
    abstract class ServiceExample
    {
        public static void Main(string[] args)
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:4222";

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                // endpoints can be created ahead of time
                // or created directly by the ServiceEndpoint builder.
                Endpoint epEcho = Endpoint.Builder()
                    .WithName("EchoEndpoint")
                    .WithSubject("echo")
                    .WithSchemaRequest("echo schema request info") // optional
                    .WithSchemaResponse("echo schema response info") // optional
                    .Build();

                // Sort is going to be grouped. This will affect the actual subject
                Group sortGroup = new Group("sort");

                // 4 service endpoints. 3 in service 1, 1 in service 2
                // - We will reuse an endpoint definition, so we make it ahead of time
                // - For echo, we could have reused a handler as well, if we wanted to.
                ServiceEndpoint seEcho1 = ServiceEndpoint.Builder()
                    .WithEndpoint(epEcho)
                    .WithHandler((s, a) => HandleEchoMessage(c, a.Message, "S1E")) // see below: handleEchoMessage below
                    .WithStatsDataSupplier(SupplyData)
                    .Build();

                ServiceEndpoint seEcho2 = ServiceEndpoint.Builder()
                    .WithEndpoint(epEcho)
                    .WithHandler((s, a) => HandleEchoMessage(c,  a.Message, "S2E"))
                    .Build();

                // you can make the Endpoint directly on the Service Endpoint Builder
                ServiceEndpoint seSort1A = ServiceEndpoint.Builder()
                    .WithGroup(sortGroup)
                    .WithEndpointName("SortEndpointAscending")
                    .WithEndpointSubject("ascending")
                    .WithEndpointSchemaRequest("sort ascending schema request info") // optional
                    .WithEndpointSchemaResponse("sort ascending schema response info") // optional
                    .WithHandler((s, a) => HandleSortAscending(c,  a.Message, "S1A"))
                    .Build();

                // you can also make an endpoint with a constructor instead of a builder.
                Endpoint endSortD = new Endpoint("SortEndpointDescending", "descending");
                ServiceEndpoint seSort1D = ServiceEndpoint.Builder()
                    .WithGroup(sortGroup)
                    .WithEndpoint(endSortD)
                    .WithHandler((s, a) => HandlerSortDescending(c,  a.Message, "S1D"))
                    .Build();

                // Create the service from service endpoints.
                Service service1 = new ServiceBuilder()
                    .WithConnection(c)
                    .WithName("Service1")
                    .WithApiUrl("Service1 Api Url") // optional
                    .WithDescription("Service1 Description") // optional
                    .WithVersion("0.0.1")
                    .AddServiceEndpoint(seEcho1)
                    .AddServiceEndpoint(seSort1A)
                    .AddServiceEndpoint(seSort1D)
                    .Build();

                Service service2 = new ServiceBuilder()
                    .WithConnection(c)
                    .WithName("Service2")
                    .WithVersion("0.0.1")
                    .AddServiceEndpoint(seEcho2) // another of the echo type
                    .Build();

                Console.WriteLine("\n" + service1);
                Console.WriteLine("\n" + service2);

                // ----------------------------------------------------------------------------------------------------
                // Start the services
                // ----------------------------------------------------------------------------------------------------
                Task<bool> done1 = service1.StartService();
                Task<bool> done2 = service2.StartService();

                // ----------------------------------------------------------------------------------------------------
                // Call the services
                // ----------------------------------------------------------------------------------------------------
                Console.WriteLine();
                string request = null;
                for (int x = 1; x <= 9; x++)
                {
                    // run ping a few times to see it hit different services
                    request = JsUtils.RandomText();
                    string echoSubject = "echo";
                    Msg echoReply = c.Request(echoSubject, Encoding.UTF8.GetBytes(request));
                    string echoResponse = Encoding.UTF8.GetString(echoReply.Data);
                    Console.WriteLine("" + x + ". Called " + echoSubject + " with [" + request + "] Received " + echoResponse);
                }

                // sort subjects are formed this way because the endpoints have groups
                request = JsUtils.RandomText();
                string subject = "sort.ascending";
                Msg reply = c.Request(subject, Encoding.UTF8.GetBytes(request));
                string response = Encoding.UTF8.GetString(reply.Data);
                Console.WriteLine("1. Called " + subject + " with [" + request + "] Received " + response);

                subject = "sort.descending";
                reply = c.Request(subject, Encoding.UTF8.GetBytes(request));
                response = Encoding.UTF8.GetString(reply.Data);
                Console.WriteLine("1. Called " + subject + " with [" + request + "] Received " + response);

                // ----------------------------------------------------------------------------------------------------
                // discovery
                // ----------------------------------------------------------------------------------------------------
                Discovery discovery = new Discovery(c, 1000, 3);

                // ----------------------------------------------------------------------------------------------------
                // ping discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<PingResponse> pingResponses = discovery.Ping();
                PrintDiscovery("Ping", "[All]", pingResponses);

                pingResponses = discovery.Ping("Service1");
                PrintDiscovery("Ping", "Service1", pingResponses);

                pingResponses = discovery.Ping("Service2");
                PrintDiscovery("Ping", "Service2", pingResponses);

                // ----------------------------------------------------------------------------------------------------
                // info discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<InfoResponse> infoResponses = discovery.Info();
                PrintDiscovery("Info", "[All]", infoResponses);

                infoResponses = discovery.Info("Service1");
                PrintDiscovery("Info", "Service1", infoResponses);

                infoResponses = discovery.Info("Service2");
                PrintDiscovery("Info", "Service2", infoResponses);

                // ----------------------------------------------------------------------------------------------------
                // schema discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<SchemaResponse> schemaResponsList = discovery.Schema();
                PrintDiscovery("Schema", "[All]", schemaResponsList);

                schemaResponsList = discovery.Schema("Service1");
                PrintDiscovery("Schema", "Service1", schemaResponsList);

                schemaResponsList = discovery.Schema("Service2");
                PrintDiscovery("Schema", "Service2", schemaResponsList);

                // ----------------------------------------------------------------------------------------------------
                // stats discover variations
                // ----------------------------------------------------------------------------------------------------
                IList<StatsResponse> statsResponseList = discovery.Stats();
                PrintDiscovery("Stats", "[All]", statsResponseList);

                statsResponseList = discovery.Stats("Service1");
                PrintDiscovery("Stats", "Service1", statsResponseList); // will show echo without data decoder

                statsResponseList = discovery.Stats("Service2");
                PrintDiscovery("Stats", "Service2", statsResponseList);

                // ----------------------------------------------------------------------------------------------------
                // stop the service
                // ----------------------------------------------------------------------------------------------------
                service1.Stop();
                service2.Stop();
                Console.WriteLine("\nService 1 done ? " + done1.Result);
                Console.WriteLine("Service 2 done ? " + done2.Result);
            }
        }

        private static string ReplyBody(String label, byte[] data, String handlerId)
        {
            JSONObject o = new JSONObject();
            o[label] = Encoding.UTF8.GetString(data);
            o["hid"] = handlerId;
            return o.ToString();
        }

        private static void HandlerSortDescending(IConnection nc, ServiceMsg smsg, String handlerId) {
            Array.Sort(smsg.Data);
            int len = smsg.Data.Length;
            byte[] descending = new byte[len];
            for (int x = 0; x < len; x++) {
                descending[x] = smsg.Data[len - x - 1];
            }
            smsg.Respond(nc, ReplyBody("sort_descending", descending, handlerId));
        }

        private static void HandleSortAscending(IConnection nc, ServiceMsg smsg, String handlerId) {
            Array.Sort(smsg.Data);
            smsg.Respond(nc, ReplyBody("sort_ascending", smsg.Data, handlerId));
        }

        private static void HandleEchoMessage(IConnection nc, ServiceMsg smsg, String handlerId) {
            smsg.Respond(nc, ReplyBody("echo", smsg.Data, handlerId));
        }

        static string Echo(string data) {
            return "Echo " + data;
        }

        private static string Echo(byte[] data) {
            return "Echo " + Encoding.UTF8.GetString(data);
        }

        private static string SortA(byte[] data) {
            Array.Sort(data);
            return "Sort Ascending " + Encoding.UTF8.GetString(data);
        }

        private static string SortA(string data) {
            return SortA(Encoding.UTF8.GetBytes(data));
        }

        private static string SortD(byte[] data) {
            Array.Sort(data);
            int len = data.Length;
            byte[] descending = new byte[len];
            for (int x = 0; x < len; x++) {
                descending[x] = data[len - x - 1];
            }
            return "Sort Descending " + Encoding.UTF8.GetString(descending);
        }

        private static string SortD(string data) {
            return SortD(Encoding.UTF8.GetBytes(data));
        }

        private static void PrintDiscovery(string action, string label, IList<PingResponse> responses)
        {
            Console.WriteLine("\n" + action + " " + label); 
            foreach (PingResponse r in responses) { Console.WriteLine("  " + r); }
        }

        private static void PrintDiscovery(string action, string label, IList<InfoResponse> responses)
        {
            Console.WriteLine("\n" + action + " " + label); 
            foreach (InfoResponse r in responses) { Console.WriteLine("  " + r); }
        }

        private static void PrintDiscovery(string action, string label, IList<SchemaResponse> responses)
        {
            Console.WriteLine("\n" + action + " " + label); 
            foreach (SchemaResponse r in responses) { Console.WriteLine("  " + r); }
        }

        private static void PrintDiscovery(string action, string label, IList<StatsResponse> responses)
        {
            Console.WriteLine("\n" + action + " " + label); 
            foreach (StatsResponse r in responses) { Console.WriteLine("  " + r); }
        }

        private static int _dataX = -1;
        private static JSONNode SupplyData()
        {
            _dataX++;
            return new ExampleStatsData("s-" + _dataX, _dataX).ToJsonNode();
        }
    }

    class ExampleStatsData : JsonSerializable
    {
        public readonly string SData;
        public readonly int IData;

        public ExampleStatsData(String sData, int iData)
        {
            SData = sData;
            IData = iData;
        }
        
        public override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();
            JsonUtils.AddField(o, "sdata", SData);
            JsonUtils.AddField(o, "idata", IData);
            return o;
        }
    }
}