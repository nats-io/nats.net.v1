// Copyright 2022-2023 The NATS Authors
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
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Threading.Tasks;
using IntegrationTests;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.Service;
using UnitTests;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;

namespace IntegrationTestsInternal
{
    [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
    public class TestService : TestSuite<AutoServerSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestService(ITestOutputHelper output, AutoServerSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
        }
        
        const string ServiceName1 = "Service1";
        const string ServiceName2 = "Service2";
        const string EchoEndpointName = "EchoEndpoint";
        const string EchoEndpointSubject = "echo";
        const string SortGroup = "sort";
        const string SortEndpointAscendingName = "SortEndpointAscending";
        const string SortEndpointDescendingName = "SortEndpointDescending";
        const string SortEndpointAscendingSubject = "ascending";
        const string SortEndpointDescendingSubject = "descending";

        [Fact]
        public void TestServiceWorkflow()
        {
            TestServerInfo server = Context.AutoServer();
            using (var s = NATSServer.CreateFastAndVerify(server.Port))
            {
                using (IConnection serviceNc1 = Context.OpenConnection(server.Port))
                using (IConnection serviceNc2 = Context.OpenConnection(server.Port))
                using (IConnection clientNc = Context.OpenConnection(server.Port))
                {
                    Endpoint endEcho = Endpoint.Builder()
                        .WithName(EchoEndpointName)
                        .WithSubject(EchoEndpointSubject)
                        .Build();

                    Endpoint endSortA = Endpoint.Builder()
                        .WithName(SortEndpointAscendingName)
                        .WithSubject(SortEndpointAscendingSubject)
                        .Build();

                    // constructor coverage
                    Endpoint endSortD = new Endpoint(SortEndpointDescendingName, SortEndpointDescendingSubject);

                    // sort is going to be grouped
                    Group sortGroup = new Group(SortGroup);

                    ServiceEndpoint seEcho1 = ServiceEndpoint.Builder()
                        .WithEndpoint(endEcho)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc1, Echo(args.Message.Data)); })
                        .WithStatsDataSupplier(TestServiceObjects.SupplyData)
                        .Build();

                    ServiceEndpoint seSortA1 = ServiceEndpoint.Builder()
                        .WithGroup(sortGroup)
                        .WithEndpoint(endSortA)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc1, SortA(args.Message.Data)); })
                        .Build();

                    ServiceEndpoint seSortD1 = ServiceEndpoint.Builder()
                        .WithGroup(sortGroup)
                        .WithEndpoint(endSortD)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc1, SortD(args.Message.Data)); })
                        .Build();

                    ServiceEndpoint seEcho2 = ServiceEndpoint.Builder()
                        .WithEndpoint(endEcho)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc2, Echo(args.Message.Data)); })
                        .WithStatsDataSupplier(TestServiceObjects.SupplyData)
                        .Build();

                    // build variations
                    ServiceEndpoint seSortA2 = ServiceEndpoint.Builder()
                        .WithGroup(sortGroup)
                        .WithEndpointName(endSortA.Name)
                        .WithEndpointSubject(endSortA.Subject)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc2, SortA(args.Message.Data)); })
                        .Build();

                    ServiceEndpoint seSortD2 = ServiceEndpoint.Builder()
                        .WithGroup(sortGroup)
                        .WithEndpointName(endSortD.Name)
                        .WithEndpointSubject(endSortD.Subject)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc2, SortD(args.Message.Data)); })
                        .Build();

                    Service service1 = new ServiceBuilder()
                        .WithName(ServiceName1)
                        .WithVersion("1.0.0")
                        .WithConnection(serviceNc1)
                        .AddServiceEndpoint(seEcho1)
                        .AddServiceEndpoint(seSortA1)
                        .AddServiceEndpoint(seSortD1)
                        .Build();
                    String serviceId1 = service1.Id;
                    Task<bool> serviceDone1 = service1.StartService();

                    Service service2 = new ServiceBuilder()
                        .WithName(ServiceName2)
                        .WithVersion("1.0.0")
                        .WithConnection(serviceNc2)
                        .AddServiceEndpoint(seEcho2)
                        .AddServiceEndpoint(seSortA2)
                        .AddServiceEndpoint(seSortD2)
                        .Build();
                    String serviceId2 = service2.Id;
                    Task<bool> serviceDone2 = service2.StartService();

                    Assert.NotEqual(serviceId1, serviceId2);

                    // service request execution
                    int requestCount = 10;
                    for (int x = 0; x < requestCount; x++)
                    {
                        VerifyServiceExecution(clientNc, EchoEndpointName, EchoEndpointSubject, null);
                        VerifyServiceExecution(clientNc, SortEndpointAscendingName, SortEndpointAscendingSubject, sortGroup);
                        VerifyServiceExecution(clientNc, SortEndpointDescendingName, SortEndpointDescendingSubject, sortGroup);
                    }

                    PingResponse pingResponse1 = service1.PingResponse;
                    PingResponse pingResponse2 = service2.PingResponse;
                    InfoResponse infoResponse1 = service1.InfoResponse;
                    InfoResponse infoResponse2 = service2.InfoResponse;
                    StatsResponse statsResponse1 = service1.GetStatsResponse();
                    StatsResponse statsResponse2 = service2.GetStatsResponse();
                    EndpointResponse[] endpointResponseArray1 = new EndpointResponse[]
                    {
                        service1.GetEndpointStats(EchoEndpointName),
                        service1.GetEndpointStats(SortEndpointAscendingName),
                        service1.GetEndpointStats(SortEndpointDescendingName)
                    };
                    EndpointResponse[] endpointResponseArray2 = new EndpointResponse[]
                    {
                        service2.GetEndpointStats(EchoEndpointName),
                        service2.GetEndpointStats(SortEndpointAscendingName),
                        service2.GetEndpointStats(SortEndpointDescendingName)
                    };
                    Assert.Null(service1.GetEndpointStats("notAnEndpoint"));

                    Assert.Equal(serviceId1, pingResponse1.Id);
                    Assert.Equal(serviceId2, pingResponse2.Id);
                    Assert.Equal(serviceId1, infoResponse1.Id);
                    Assert.Equal(serviceId2, infoResponse2.Id);
                    Assert.Equal(serviceId1, statsResponse1.Id);
                    Assert.Equal(serviceId2, statsResponse2.Id);

                    // this relies on the fact that I load the endpoints up in the service
                    // in the same order and the json list comes back ordered
                    // expecting 10 responses across each endpoint between 2 services
                    for (int x = 0; x < 3; x++)
                    {
                        Assert.Equal(requestCount,
                            endpointResponseArray1[x].NumRequests
                            + endpointResponseArray2[x].NumRequests);
                        Assert.Equal(requestCount,
                            statsResponse1.EndpointStatsList[x].NumRequests
                            + statsResponse2.EndpointStatsList[x].NumRequests);
                    }

                    // discovery - wait at most 500 millis for responses, 5 total responses max
                    Discovery discovery = new Discovery(clientNc, 500, 5);

                    // ping discovery
                    void VerifyPingDiscoveries(IList<PingResponse> responses, params PingResponse[] expectedResponses) {
                        Assert.Equal(expectedResponses.Length, responses.Count);
                        foreach (PingResponse r in responses)
                        {
                            // ReSharper disable once CoVariantArrayConversion
                            PingResponse exp = (PingResponse)Find(expectedResponses, r);
                            Assert.NotNull(exp);
                            VerifyServiceResponseFields(r, exp);
                        }
                    }
                    VerifyPingDiscoveries(discovery.Ping(), pingResponse1, pingResponse2);
                    VerifyPingDiscoveries(discovery.Ping(ServiceName1), pingResponse1);
                    VerifyPingDiscoveries(discovery.Ping(ServiceName2), pingResponse2);
                    VerifyServiceResponseFields(discovery.PingForNameAndId(ServiceName1, serviceId1), pingResponse1);
                    Assert.Null(discovery.PingForNameAndId(ServiceName1, "badId"));
                    Assert.Null(discovery.PingForNameAndId("bad", "badId"));

                    // info discovery
                    void VerifyInfoDiscovery(InfoResponse r, InfoResponse exp) {
                        VerifyServiceResponseFields(r, exp);
                        Assert.Equal(exp.Description, r.Description);
                        Assert.Equal(exp.Subjects, r.Subjects);
                    }
                    void VerifyInfoDiscoveries(IList<InfoResponse> responses, params InfoResponse[] expectedResponses)
                    {
                        Assert.Equal(expectedResponses.Length, responses.Count);
                        foreach (InfoResponse r in responses)
                        {
                            // ReSharper disable once CoVariantArrayConversion
                            InfoResponse exp = (InfoResponse)Find(expectedResponses, r);
                            Assert.NotNull(exp);
                            VerifyInfoDiscovery(r, exp);
                        }
                    }
                    VerifyInfoDiscoveries(discovery.Info(), infoResponse1, infoResponse2);
                    VerifyInfoDiscoveries(discovery.Info(ServiceName1), infoResponse1);
                    VerifyInfoDiscoveries(discovery.Info(ServiceName2), infoResponse2);
                    VerifyInfoDiscovery(discovery.InfoForNameAndId(ServiceName1, serviceId1), infoResponse1);
                    Assert.Null(discovery.InfoForNameAndId(ServiceName1, "badId"));
                    Assert.Null(discovery.InfoForNameAndId("bad", "badId"));
                    
                    // stats discovery
                    void VerifyStatsDiscovery(StatsResponse r, StatsResponse exp) {
                        VerifyServiceResponseFields(r, exp);
                        Assert.Equal(exp.Started, r.Started);
                        for (int x = 0; x < 3; x++) {
                            EndpointResponse es = exp.EndpointStatsList[x];
                            if (!es.Name.Equals(EchoEndpointName)) {
                                // echo endpoint has data that will vary
                                Assert.Equal(es, r.EndpointStatsList[x]);
                            }
                        }
                    }
                    void VerifyStatsDiscoveries(IList<StatsResponse> responses, params StatsResponse[] expectedResponses)
                    {
                        Assert.Equal(expectedResponses.Length, responses.Count);
                        foreach (StatsResponse r in responses)
                        {
                            // ReSharper disable once CoVariantArrayConversion
                            StatsResponse exp = (StatsResponse)Find(expectedResponses, r);
                            Assert.NotNull(exp);
                            VerifyStatsDiscovery(r, exp);
                        }
                    }
                    VerifyStatsDiscoveries(discovery.Stats(), statsResponse1, statsResponse2);
                    VerifyStatsDiscoveries(discovery.Stats(ServiceName1), statsResponse1);
                    VerifyStatsDiscoveries(discovery.Stats(ServiceName2), statsResponse2);
                    VerifyStatsDiscovery(discovery.StatsForNameAndId(ServiceName1, serviceId1), statsResponse1);
                    Assert.Null(discovery.StatsForNameAndId(ServiceName1, "badId"));
                    Assert.Null(discovery.StatsForNameAndId("bad", "badId"));

                    // shutdown
                    service1.Stop();
                    Assert.True(serviceDone1.Result);
                    service2.Stop(new Exception("Testing stop(Exception e)"));
                    AggregateException ae = Assert.Throws<AggregateException>(() => serviceDone2.Result);
                    Assert.Contains("Testing stop(Exception e)", ae.GetBaseException().Message);
                }
            }
        }
        
        private void VerifyServiceResponseFields(ServiceResponse r, ServiceResponse exp) {
            Assert.Equal(exp.Type, r.Type);
            Assert.Equal(exp.Name, r.Name);
            Assert.Equal(exp.Version, r.Version);
        }
        
        private ServiceResponse Find(ServiceResponse[] expectedResponses, ServiceResponse response) {
            foreach (ServiceResponse sr in expectedResponses) {
                if (response.Id.Equals(sr.Id)) {
                    return sr;
                }
            }
            return null;
        }

        private void VerifyServiceExecution(IConnection nc, string endpointName, string serviceSubject, Group group) {
            string request = DateTime.UtcNow.ToLongDateString(); // just some random text
            string subject = group == null ? serviceSubject : group.Subject + "." + serviceSubject;
            Msg m = nc.Request(subject, Encoding.UTF8.GetBytes(request));
            String response = Encoding.UTF8.GetString(m.Data);
            switch (endpointName) {
                case EchoEndpointName:
                    Assert.Equal(Echo(request), response);
                    break;
                case SortEndpointAscendingName:
                    Assert.Equal(SortA(request), response);
                    break;
                case SortEndpointDescendingName:
                    Assert.Equal(SortD(request), response);
                    break;
            }
        }

        private string Echo(string data) {
            return "Echo " + data;
        }

        private string Echo(byte[] data) {
            return "Echo " + Encoding.UTF8.GetString(data);
        }

        private string SortA(byte[] data) {
            Array.Sort(data);
            return "Sort Ascending " + Encoding.UTF8.GetString(data);
        }

        private string SortA(string data) {
            return SortA(Encoding.UTF8.GetBytes(data));
        }

        private string SortD(byte[] data) {
            Array.Sort(data);
            int len = data.Length;
            byte[] descending = new byte[len];
            for (int x = 0; x < len; x++) {
                descending[x] = data[len - x - 1];
            }
            return "Sort Descending " + Encoding.UTF8.GetString(descending);
        }

        private string SortD(string data) {
            return SortD(Encoding.UTF8.GetBytes(data));
        }

        [Fact]
        public void TestServiceBuilderConstruction()
        {
            string name = Name(Nuid.NextGlobal());
            Connection conn = new Connection(ConnectionFactory.GetDefaultOptions());
            ServiceEndpoint se = ServiceEndpoint.Builder()
                .WithEndpoint(new Endpoint(Name(0)))
                .WithHandler((s, a) => { })
                .Build();

            // minimum valid service
            Service service = Service.Builder().WithConnection(conn).WithName(name).WithVersion("1.0.0").AddServiceEndpoint(se).Build();
            Assert.NotNull(service.ToString()); // coverage
            Assert.NotNull(service.Id);
            Assert.Equal(name, service.Name);
            Assert.Equal(ServiceBuilder.DefaultDrainTimeoutMillis, service.DrainTimeoutMillis);
            Assert.Equal("1.0.0", service.Version);
            Assert.Null(service.Description);

            service = Service.Builder().WithConnection(conn).WithName(name).WithVersion("1.0.0").AddServiceEndpoint(se)
                .WithApiUrl("apiUrl")
                .WithDescription("desc")
                .WithDrainTimeoutMillis(1000)
                .Build();
            Assert.Equal("desc", service.Description);
            Assert.Equal(1000, service.DrainTimeoutMillis);

            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(null));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(string.Empty));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasSpace));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasPrintable));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasDot));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasStar)); // invalid in the middle
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasGt)); // invalid in the middle
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasDollar));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasLow));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(Has127));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasFwdSlash));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasBackSlash));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasEquals));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithName(HasTic));

            Assert.Throws<ArgumentException>(() => Service.Builder().WithVersion(null));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithVersion(string.Empty));
            Assert.Throws<ArgumentException>(() => Service.Builder().WithVersion("not-semver"));

            ArgumentException ae = Assert.Throws<ArgumentException>(
                () => Service.Builder().WithName(name).WithVersion("1.0.0").AddServiceEndpoint(se).Build());
            Assert.Contains("Connection cannot be null or empty", ae.Message);

            ae = Assert.Throws<ArgumentException>(
                () => Service.Builder().WithConnection(conn).WithVersion("1.0.0").AddServiceEndpoint(se).Build());
            Assert.Contains("Name cannot be null or empty", ae.Message);

            ae = Assert.Throws<ArgumentException>(() =>
                Service.Builder().WithConnection(conn).WithName(name).AddServiceEndpoint(se).Build());
            Assert.Contains("Version cannot be null or empty", ae.Message);

            ae = Assert.Throws<ArgumentException>(() =>
                Service.Builder().WithConnection(conn).WithName(name).WithVersion("1.0.0").Build());
            Assert.Contains("Endpoints cannot be null or empty", ae.Message);
        }

        [Fact]
        public void TestHandlerException()
        {
            Context.RunInJsServer(c =>
            {
                ServiceEndpoint exServiceEndpoint = ServiceEndpoint.Builder()
                    .WithEndpointName("exEndpoint")
                    .WithEndpointSubject("exSubject")
                    .WithHandler((s, a) => throw new Exception("handler-problem"))
                    .Build();

                Service exService = new ServiceBuilder()
                    .WithConnection(c)
                    .WithName("ExceptionService")
                    .WithVersion("0.0.1")
                    .AddServiceEndpoint(exServiceEndpoint)
                    .Build();
                exService.StartService();

                Msg m = c.Request("exSubject", null, 1000);
                Assert.Equal("System.Exception: handler-problem", m.Header[ServiceMsg.NatsServiceError]);
                Assert.Equal("500", m.Header[ServiceMsg.NatsServiceErrorCode]);
                StatsResponse sr = exService.GetStatsResponse();
                EndpointResponse es = sr.EndpointStatsList[0];
                Assert.Equal(1, es.NumRequests);
                Assert.Equal(1, es.NumErrors);
                Assert.Equal("System.Exception: handler-problem", es.LastError);
            });
        }

        [Fact]
        public void TestServiceMessage()
        {
            Context.RunInJsServer(nc =>
            {
                InterlockedInt which = new InterlockedInt();
                ServiceEndpoint se = ServiceEndpoint.Builder()
                    .WithEndpointName("testServiceMessage")
                    .WithHandler((s, a) =>
                    {
                        ServiceMsg sm = a.Message;
                        // Coverage // just hitting all the reply variations
                        switch (which.Increment())
                        {
                            case 1:
                                sm.Respond(nc, Encoding.UTF8.GetBytes("1"));
                                break;
                            case 2:
                                sm.Respond(nc, "2");
                                break;
                            case 3:
                                sm.Respond(nc, (JSONNode)new JSONString("3"));
                                break;
                            case 4:
                                sm.Respond(nc, Encoding.UTF8.GetBytes("4"), sm.Header);
                                break;
                            case 5:
                                sm.Respond(nc, "5", sm.Header);
                                break;
                            case 6:
                                sm.Respond(nc, (JSONNode)new JSONString("6"), sm.Header);
                                break;
                            case 7:
                                sm.RespondStandardError(nc, "error", 500);
                                break;
                        }
                    })
                    .Build();
                
                Service service = new ServiceBuilder()
                    .WithConnection(nc)
                    .WithName("testService")
                    .WithVersion("0.0.1")
                    .AddServiceEndpoint(se)
                    .Build();
                service.StartService();
                
                Msg m = nc.Request("testServiceMessage", null, 1000);
                Assert.Equal("1", Encoding.UTF8.GetString(m.Data));
                Assert.False(m.HasHeaders);
                
                m = nc.Request("testServiceMessage", null);
                Assert.Equal("2", Encoding.UTF8.GetString(m.Data));
                Assert.False(m.HasHeaders);

                m = nc.Request("testServiceMessage", null);
                Assert.Equal("\"3\"", Encoding.UTF8.GetString(m.Data));
                Assert.False(m.HasHeaders);

                MsgHeader h = new MsgHeader { { "h", "4" } };
                m = nc.Request(new Msg("testServiceMessage", h, null));
                Assert.Equal("4", Encoding.UTF8.GetString(m.Data));
                Assert.True(m.HasHeaders);
                Assert.Equal("4", m.Header["h"]);

                h = new MsgHeader { { "h", "5" } };
                m = nc.Request(new Msg("testServiceMessage", h, null));
                Assert.Equal("5", Encoding.UTF8.GetString(m.Data));
                Assert.True(m.HasHeaders);
                Assert.Equal("5", m.Header["h"]);

                h = new MsgHeader { { "h", "6" } };
                m = nc.Request(new Msg("testServiceMessage", h, null));
                Assert.Equal("\"6\"", Encoding.UTF8.GetString(m.Data));
                Assert.True(m.HasHeaders);
                Assert.Equal("6", m.Header["h"]);

                m = nc.Request("testServiceMessage", null);
                Assert.Empty(m.Data);
                Assert.True(m.HasHeaders);
                Assert.Equal("error", m.Header[ServiceMsg.NatsServiceError]);
                Assert.Equal("500", m.Header[ServiceMsg.NatsServiceErrorCode]);
            });
        }
        
        [Fact]
        public void TestInboxSupplier() {
            Context.RunInServer(c => {
                Discovery discovery = new Discovery(c, 100, 1);
                bool WasCalled = false;
                discovery.InboxSupplier = () =>
                {
                    WasCalled = true;
                    return "CUSTOM INBOX";
                };
            
                try {
                    discovery.Ping("servicename");
                }
                catch (Exception) {
                    // we know it will throw exception b/c there is no service
                    // running, we just care about it make the call
                }
                Assert.True(WasCalled);
            });
        }
    }
}