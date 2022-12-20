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
using IntegrationTests;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using NATS.Client.Service;
using Xunit;
using static UnitTests.TestBase;

namespace IntegrationTestsInternal
{
    class TestStatsData : IStatsData
    {
        public readonly string Id;
        public readonly string Text;

        public TestStatsData(string id, string text)
        {
            Id = id;
            Text = text;
        }

        public TestStatsData(string json)
        {
            JSONNode node = JSON.Parse(json);
            Id = node[ApiConstants.Id];
            Text = node["text"];
        }

        public string ToJson()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, Id);
            JsonUtils.AddField(jso, "text", Text);
            return jso.ToString();
        }

        public override string ToString()
        {
            return ToJson();
        }
    }

    public class TestService : TestSuite<ServiceSuiteContext>
    {
        public TestService(ServiceSuiteContext context) : base(context) { }

        private const string EchoServiceName = "ECHO_SERVICE";
        private const string SortServiceName = "SORT_SERVICE";
        private const string EchoServiceSubject = "echo";
        private const string SortServiceSubject = "sort";

        delegate string InfoVerifier(Info expectedInfo, object o);
        delegate string SchemaInfoVerifier(Info expectedInfo, SchemaInfo expectedSchemaInfo, object o);

        [Fact]
        public void TestServiceWorkflow()
        {
            using (var s = NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (IConnection serviceNc1 = Context.OpenConnection(Context.Server1.Port))
                using (IConnection serviceNc2 = Context.OpenConnection(Context.Server1.Port))
                using (IConnection clientNc = Context.OpenConnection(Context.Server1.Port))
                {
                    StatsDataSupplier sds = () => new TestStatsData($"{GetHashCode()}", "blah error [" + DateTime.Now + "]");
                    StatsDataDecoder sdd = json =>
                    {
                        TestStatsData esd = new TestStatsData(json);
                        return string.IsNullOrEmpty(esd.Text) ? null : esd;
                    };

                    Service echoService1 = EchoServiceCreator(serviceNc1, "echoService1")
                        .WithStatsDataHandlers(sds, sdd)
                        .Build();
                    String echoServiceId1 = echoService1.Id;
                    Task<bool> echoDone1 = echoService1.StartService();

                    Service sortService1 = SortServiceCreator(serviceNc1, "sortService1").Build();
                    String sortServiceId1 = sortService1.Id;
                    Task<bool> sortDone1 = sortService1.StartService();

                    Service echoService2 = EchoServiceCreator(serviceNc2, "echoService2")
                        .WithStatsDataHandlers(sds, sdd)
                        .Build();
                    String echoServiceId2 = echoService2.Id;
                    Task<bool> echoDone2 = echoService2.StartService();

                    Service sortService2 = SortServiceCreator(serviceNc2, "sortService2").Build();
                    String sortServiceId2 = sortService2.Id;
                    Task<bool> sortDone2 = sortService2.StartService();
                    
                    Assert.NotEqual(echoServiceId1, echoServiceId2);
                    Assert.NotEqual(sortServiceId1, sortServiceId2);

                    // service request execution
                    int requestCount = 10;
                    for (int x = 0; x < requestCount; x++) {
                        VerifyServiceExecution(clientNc, EchoServiceName, EchoServiceSubject);
                        VerifyServiceExecution(clientNc, SortServiceName, SortServiceSubject);
                    }

                    Info echoInfo = echoService1.Info;
                    Info sortInfo = sortService1.Info;
                    SchemaInfo echoSchemaInfo = echoService1.SchemaInfo;
                    SchemaInfo sortSchemaInfo = sortService1.SchemaInfo;

                    // discovery - wait at most 500 millis for responses, 5 total responses max
                    Discovery discovery = new Discovery(clientNc, 500, 5);

                    // ping discovery
                    void VerifyPingDiscovery(Info expectedInfo, IList<Ping> pings, params string[] expectedIds) {
                        Assert.Equal(expectedIds.Length, pings.Count);
                        foreach (var p in pings) {
                            if (expectedInfo != null) {
                                Assert.Equal(expectedInfo.Name, p.Name);
                            }
                            Assert.Contains(p.ServiceId, expectedIds);
                        }
                    }
                    VerifyPingDiscovery(null, discovery.Ping(), echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                    VerifyPingDiscovery(echoInfo, discovery.Ping(EchoServiceName), echoServiceId1, echoServiceId2);
                    VerifyPingDiscovery(sortInfo, discovery.Ping(SortServiceName), sortServiceId1, sortServiceId2);
                    VerifyPingDiscovery(echoInfo, new List<Ping>{discovery.PingForNameAndId(EchoServiceName, echoServiceId1)}, echoServiceId1);
                    VerifyPingDiscovery(sortInfo, new List<Ping>{discovery.PingForNameAndId(SortServiceName, sortServiceId1)}, sortServiceId1);
                    VerifyPingDiscovery(echoInfo, new List<Ping>{discovery.PingForNameAndId(EchoServiceName, echoServiceId2)}, echoServiceId2);
                    VerifyPingDiscovery(sortInfo, new List<Ping>{discovery.PingForNameAndId(SortServiceName, sortServiceId2)}, sortServiceId2);

                    // info discovery
                    void VerifyInfoDiscovery(Info expectedInfo, IList<Info> infos, params string[] expectedIds) {
                        Assert.Equal(expectedIds.Length, infos.Count);
                        foreach (var i in infos) {
                            if (expectedInfo != null) {
                                Assert.Equal(expectedInfo.Name, i.Name);
                                Assert.Equal(expectedInfo.Description, i.Description);
                                Assert.Equal(expectedInfo.Version, i.Version);
                                Assert.Equal(expectedInfo.Subject, i.Subject);
                            }
                            Assert.Contains(i.ServiceId, expectedIds);
                        }
                    }
                    VerifyInfoDiscovery(null, discovery.Info(), echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                    VerifyInfoDiscovery(echoInfo, discovery.Info(EchoServiceName), echoServiceId1, echoServiceId2);
                    VerifyInfoDiscovery(sortInfo, discovery.Info(SortServiceName), sortServiceId1, sortServiceId2);
                    VerifyInfoDiscovery(echoInfo, new List<Info>{discovery.InfoForNameAndId(EchoServiceName, echoServiceId1)}, echoServiceId1);
                    VerifyInfoDiscovery(sortInfo, new List<Info>{discovery.InfoForNameAndId(SortServiceName, sortServiceId1)}, sortServiceId1);
                    VerifyInfoDiscovery(echoInfo, new List<Info>{discovery.InfoForNameAndId(EchoServiceName, echoServiceId2)}, echoServiceId2);
                    VerifyInfoDiscovery(sortInfo, new List<Info>{discovery.InfoForNameAndId(SortServiceName, sortServiceId2)}, sortServiceId2);

                    // schema discovery
                    void VerifySchemaDiscovery(SchemaInfo expectedSchemaInfo, IList<SchemaInfo> schemas, params string[] expectedIds) {
                        Assert.Equal(expectedIds.Length, schemas.Count);
                        foreach (var sch in schemas) {
                            if (expectedSchemaInfo != null) {
                                Assert.Equal(expectedSchemaInfo.Name, sch.Name);
                                Assert.Equal(expectedSchemaInfo.Version, sch.Version);
                                Assert.Equal(expectedSchemaInfo.Version, sch.Version);
                                Assert.Equal(expectedSchemaInfo.Schema.Request, sch.Schema.Request);
                                Assert.Equal(expectedSchemaInfo.Schema.Response, sch.Schema.Response);
                            }
                            Assert.Contains(sch.ServiceId, expectedIds);
                        }
                    }
                    VerifySchemaDiscovery(null, discovery.Schema(), echoServiceId1, sortServiceId1, echoServiceId2, sortServiceId2);
                    VerifySchemaDiscovery(echoSchemaInfo, discovery.Schema(EchoServiceName), echoServiceId1, echoServiceId2);
                    VerifySchemaDiscovery(sortSchemaInfo, discovery.Schema(SortServiceName), sortServiceId1, sortServiceId2);
                    VerifySchemaDiscovery(echoSchemaInfo, new List<SchemaInfo>{discovery.SchemaForNameAndId(EchoServiceName, echoServiceId1)}, echoServiceId1);
                    VerifySchemaDiscovery(sortSchemaInfo, new List<SchemaInfo>{discovery.SchemaForNameAndId(SortServiceName, sortServiceId1)}, sortServiceId1);
                    VerifySchemaDiscovery(echoSchemaInfo, new List<SchemaInfo>{discovery.SchemaForNameAndId(EchoServiceName, echoServiceId2)}, echoServiceId2);
                    VerifySchemaDiscovery(sortSchemaInfo, new List<SchemaInfo>{discovery.SchemaForNameAndId(SortServiceName, sortServiceId2)}, sortServiceId2);
                    
                    // stats discovery
                    discovery = new Discovery(clientNc); // coverage for the simple constructor
                    IList<Stats> statsList = discovery.Stats(null, sdd);
                    Assert.Equal(4, statsList.Count);
                    int responseEcho = 0;
                    int responseSort = 0;
                    long requestsEcho = 0;
                    long requestsSort = 0;
                    foreach (Stats st in statsList) {
                        if (st.Name.Equals(EchoServiceName)) {
                            responseEcho++;
                            requestsEcho += st.NumRequests;
                            Assert.NotNull(st.Data);
                            Assert.True(st.Data is TestStatsData);
                        }
                        else {
                            responseSort++;
                            requestsSort += st.NumRequests;
                        }
                    }
                    Assert.Equal(2, responseEcho);
                    Assert.Equal(2, responseSort);
                    Assert.Equal(requestCount, requestsEcho);
                    Assert.Equal(requestCount, requestsSort);

                    // stats one specific instance so I can also test reset
                    Stats stats = discovery.StatsForNameAndId(EchoServiceName, echoServiceId1);
                    Assert.Equal(echoServiceId1, stats.ServiceId);
                    Assert.Equal(echoInfo.Version, stats.Version);

                    // reset stats
                    echoService1.Reset();
                    stats = echoService1.Stats;
                    Assert.Equal(0, stats.NumRequests);
                    Assert.Equal(0, stats.NumErrors);
                    Assert.Equal(0, stats.ProcessingTime);
                    Assert.Equal(0, stats.AverageProcessingTime);
                    Assert.Null(stats.Data);

                    stats = discovery.StatsForNameAndId(EchoServiceName, echoServiceId1);
                    Assert.Equal(0, stats.NumRequests);
                    Assert.Equal(0, stats.NumErrors);
                    Assert.Equal(0, stats.ProcessingTime);
                    Assert.Equal(0, stats.AverageProcessingTime);
                    
                    // shutdown
                    echoService1.Stop(); // drain = true, exception = null
                    sortService1.Stop(true); // drain = true, exception = null
                    echoService2.Stop(false); // drain = false, exception = null
                    sortService2.Stop(false, new Exception()); // drain = true, exception not null

                    echoDone1.Wait();
                    sortDone1.Wait();
                    echoDone2.Wait();
                    Assert.Throws<AggregateException>(() => sortDone2.Wait());
                }
            }
        }

        private static void VerifyDiscovery(Info expectedInfo, SchemaInfo expectedSchemaInfo, IList<object> objects, SchemaInfoVerifier siv, List<String>  expectedIds) {
            Assert.Equal(expectedIds.Count, objects.Count);
            foreach (var o in objects) {
                String id = siv.Invoke(expectedInfo, expectedSchemaInfo, o);
                Assert.Contains(id, expectedIds);
            }
        }

        private static ServiceBuilder EchoServiceCreator(IConnection nc, EventHandler<MsgHandlerEventArgs> handler) {
            return new ServiceBuilder()
                .WithConnection(nc)
                .WithName(EchoServiceName)
                .WithSubject(EchoServiceSubject)
                .WithDescription("An Echo Service")
                .WithVersion("0.0.1")
                .WithSchemaRequest("echo schema request string/url")
                .WithSchemaResponse("echo schema response string/url")
                .WithServiceMessageHandler(handler);
        }

        private static ServiceBuilder EchoServiceCreator(IConnection nc, string id)
        {
            return EchoServiceCreator(nc,
                (sender, args) =>
                    ServiceMessage.Reply(nc, args.Message, Echo(args.Message.Data),
                        new MsgHeader { ["handlerId"] = id }));
        }

        private static ServiceBuilder SortServiceCreator(IConnection nc, string id) {
            return new ServiceBuilder()
                .WithConnection(nc)
                .WithName(SortServiceName)
                .WithSubject(SortServiceSubject)
                .WithDescription("A Sort Service")
                .WithVersion("0.0.2")
                .WithSchemaRequest("sort schema request string/url")
                .WithSchemaResponse("sort schema response string/url")
                .WithServiceMessageHandler((sender, args) => 
                    ServiceMessage.Reply(nc, args.Message, Sort(args.Message.Data), new MsgHeader { ["handlerId"] = id }));
        }
        
        private static void VerifyServiceExecution(IConnection nc, String serviceName, String serviceSubject)
        {
            String request = DateTime.Now.ToLongDateString();
            Msg m = nc.Request(serviceSubject, Encoding.UTF8.GetBytes(request));
            String response = Encoding.UTF8.GetString(m.Data);
            String expected = serviceName.Equals(EchoServiceName) ? Echo(request) : Sort(request);
            Assert.Equal(expected, response);
        }

        private static string Echo(String data) {
            return "Echo " + data;
        }

        private static string Echo(byte[] data) {
            return "Echo " + Encoding.UTF8.GetString(data);
        }

        private static string Sort(byte[] data) {
            Array.Sort(data);
            return "Sort " + Encoding.UTF8.GetString(data);
        }

        private static string Sort(String data) {
            return Sort(Encoding.UTF8.GetBytes(data));
        }
        
        [Fact]
        public void TestHandlerException()
        {
            Context.RunInServer(nc =>
            {
                Service devexService = new ServiceBuilder()
                    .WithConnection(nc)
                    .WithName("HANDLER_EXCEPTION_SERVICE")
                    .WithSubject("HandlerExceptionService")
                    .WithVersion("0.0.1")
                    .WithServiceMessageHandler((s, a) => throw new Exception("handler-problem"))
                    .Build();
                devexService.StartService();

                Msg m = nc.Request("HandlerExceptionService", null);
                Assert.Equal("handler-problem", m.Header[ServiceMessage.NatsServiceError]);
                Assert.Equal("500", m.Header[ServiceMessage.NatsServiceErrorCode]);
                Assert.Equal(1, devexService.Stats.NumRequests);
                Assert.Equal(1, devexService.Stats.NumErrors);
                Assert.Contains("System.Exception: handler-problem", devexService.Stats.LastError);
            });
        }

        [Fact]
        public void TestServiceCreatorValidation()
        {
            Context.RunInServer(nc =>
            {
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(null, (sender, args) => {}).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (EventHandler<MsgHandlerEventArgs>)null).WithVersion("").Build());
                
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithVersion(null).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithVersion(string.Empty).Build());
                
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(null).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(string.Empty).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasSpace).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasPrintable).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasDot).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasStar).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasGt).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasDollar).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasLow).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(Has127).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasFwdSlash).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasBackSlash).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasEquals).Build());
                Assert.Throws<ArgumentException>(() => EchoServiceCreator(nc, (sender, args) => {}).WithName(HasTic).Build());
            });
        }
    }
}