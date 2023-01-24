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
    public class TestService : TestSuite<ServiceSuiteContext>
    {
        private readonly ITestOutputHelper output;
        public TestService(ITestOutputHelper output, ServiceSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
        }
        
        const string SERVICE_NAME_1 = "Service1";
        const string SERVICE_NAME_2 = "Service2";
        const string ECHO_ENDPOINT_NAME = "EchoEndpoint";
        const string ECHO_ENDPOINT_SUBJECT = "echo";
        const string SORT_GROUP = "sort";
        const string SORT_ENDPOINT_ASCENDING_NAME = "SortEndpointAscending";
        const string SORT_ENDPOINT_DESCENDING_NAME = "SortEndpointDescending";
        const string SORT_ENDPOINT_ASCENDING_SUBJECT = "ascending";
        const string SORT_ENDPOINT_DESCENDING_SUBJECT = "descending";

        [Fact]
        public void TestServiceWorkflow()
        {
            using (var s = NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using (IConnection serviceNc1 = Context.OpenConnection(Context.Server1.Port))
                using (IConnection serviceNc2 = Context.OpenConnection(Context.Server1.Port))
                using (IConnection clientNc = Context.OpenConnection(Context.Server1.Port))
                {
                    Endpoint endEcho = Endpoint.Builder()
                        .WithName(ECHO_ENDPOINT_NAME)
                        .WithSubject(ECHO_ENDPOINT_SUBJECT)
                        .WithSchemaRequest("echo schema request info") // optional
                        .WithSchemaResponse("echo schema response info") // optional
                        .Build();

                    Endpoint endSortA = Endpoint.Builder()
                        .WithName(SORT_ENDPOINT_ASCENDING_NAME)
                        .WithSubject(SORT_ENDPOINT_ASCENDING_SUBJECT)
                        .WithSchemaRequest("sort ascending schema request info") // optional
                        .WithSchemaResponse("sort ascending schema response info") // optional
                        .Build();

                    // constructor coverage
                    Endpoint endSortD = new Endpoint(SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT);

                    // sort is going to be grouped
                    Group sortGroup = new Group(SORT_GROUP);

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
                        .WithEndpointSchemaRequest(endSortA.Schema.Request)
                        .WithEndpointSchemaResponse(endSortA.Schema.Response)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc2, SortA(args.Message.Data)); })
                        .Build();

                    ServiceEndpoint seSortD2 = ServiceEndpoint.Builder()
                        .WithGroup(sortGroup)
                        .WithEndpointName(endSortD.Name)
                        .WithEndpointSubject(endSortD.Subject)
                        .WithHandler((source, args) => { args.Message.Respond(serviceNc2, SortD(args.Message.Data)); })
                        .Build();

                    Service service1 = new ServiceBuilder()
                        .WithName(SERVICE_NAME_1)
                        .WithVersion("1.0.0")
                        .WithConnection(serviceNc1)
                        .AddServiceEndpoint(seEcho1)
                        .AddServiceEndpoint(seSortA1)
                        .AddServiceEndpoint(seSortD1)
                        .Build();
                    String serviceId1 = service1.Id;
                    Task<bool> serviceDone1 = service1.StartService();

                    Service service2 = new ServiceBuilder()
                        .WithName(SERVICE_NAME_2)
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
                        VerifyServiceExecution(clientNc, ECHO_ENDPOINT_NAME, ECHO_ENDPOINT_SUBJECT, null);
                        VerifyServiceExecution(clientNc, SORT_ENDPOINT_ASCENDING_NAME, SORT_ENDPOINT_ASCENDING_SUBJECT, sortGroup);
                        VerifyServiceExecution(clientNc, SORT_ENDPOINT_DESCENDING_NAME, SORT_ENDPOINT_DESCENDING_SUBJECT, sortGroup);
                    }

                    PingResponse pingResponse1 = service1.PingResponse;
                    PingResponse pingResponse2 = service2.PingResponse;
                    InfoResponse infoResponse1 = service1.InfoResponse;
                    InfoResponse infoResponse2 = service2.InfoResponse;
                    SchemaResponse schemaResponse1 = service1.SchemaResponse;
                    SchemaResponse schemaResponse2 = service2.SchemaResponse;
                    StatsResponse statsResponse1 = service1.GetStatsResponse();
                    StatsResponse statsResponse2 = service2.GetStatsResponse();
                    EndpointStats[] endpointStatsArray1 = new EndpointStats[]
                    {
                        service1.GetEndpointStats(ECHO_ENDPOINT_NAME),
                        service1.GetEndpointStats(SORT_ENDPOINT_ASCENDING_NAME),
                        service1.GetEndpointStats(SORT_ENDPOINT_DESCENDING_NAME)
                    };
                    EndpointStats[] endpointStatsArray2 = new EndpointStats[]
                    {
                        service2.GetEndpointStats(ECHO_ENDPOINT_NAME),
                        service2.GetEndpointStats(SORT_ENDPOINT_ASCENDING_NAME),
                        service2.GetEndpointStats(SORT_ENDPOINT_DESCENDING_NAME)
                    };
                    Assert.Null(service1.GetEndpointStats("notAnEndpoint"));

                    Assert.Equal(serviceId1, pingResponse1.Id);
                    Assert.Equal(serviceId2, pingResponse2.Id);
                    Assert.Equal(serviceId1, infoResponse1.Id);
                    Assert.Equal(serviceId2, infoResponse2.Id);
                    Assert.Equal(serviceId1, schemaResponse1.Id);
                    Assert.Equal(serviceId2, schemaResponse2.Id);
                    Assert.Equal(serviceId1, statsResponse1.Id);
                    Assert.Equal(serviceId2, statsResponse2.Id);

                    // this relies on the fact that I load the endpoints up in the service
                    // in the same order and the json list comes back ordered
                    // expecting 10 responses across each endpoint between 2 services
                    for (int x = 0; x < 3; x++)
                    {
                        Assert.Equal(requestCount,
                            endpointStatsArray1[x].NumRequests
                            + endpointStatsArray2[x].NumRequests);
                        Assert.Equal(requestCount,
                            statsResponse1.EndpointStatsList[x].NumRequests
                            + statsResponse2.EndpointStatsList[x].NumRequests);
                    }
                }
            }
        }

        private void VerifyServiceExecution(IConnection nc, string endpointName, string serviceSubject, Group group) {
            string request = DateTime.UtcNow.ToLongDateString(); // just some random text
            string subject = group == null ? serviceSubject : group.Subject + "." + serviceSubject;
            Msg m = nc.Request(subject, Encoding.UTF8.GetBytes(request));
            String response = Encoding.UTF8.GetString(m.Data);
            switch (endpointName) {
                case ECHO_ENDPOINT_NAME:
                    Assert.Equal(Echo(request), response);
                    break;
                case SORT_ENDPOINT_ASCENDING_NAME:
                    Assert.Equal(SortA(request), response);
                    break;
                case SORT_ENDPOINT_DESCENDING_NAME:
                    Assert.Equal(SortD(request), response);
                    break;
            }
        }

        private static string Echo(string data) {
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

        [Fact]
        public void TestServiceBuilderConstruction()
        {
            Connection conn = new Connection(ConnectionFactory.GetDefaultOptions());
            ServiceEndpoint se = ServiceEndpoint.Builder()
                .WithEndpoint(new Endpoint(Name(0)))
                .WithHandler((s, a) => { })
                .Build();

            // minimum valid service
            Service service = Service.Builder().WithConnection(conn).WithName(NAME).WithVersion("1.0.0").AddServiceEndpoint(se).Build();
            Assert.NotNull(service.ToString()); // coverage
            Assert.NotNull(service.Id);
            Assert.Equal(NAME, service.Name);
            Assert.Equal(ServiceBuilder.DefaultDrainTimeoutMillis, service.DrainTimeoutMillis);
            Assert.Equal("1.0.0", service.Version);
            Assert.Null(service.Description);
            Assert.Null(service.ApiUrl);

            service = Service.Builder().WithConnection(conn).WithName(NAME).WithVersion("1.0.0").AddServiceEndpoint(se)
                .WithApiUrl("apiUrl")
                .WithDescription("desc")
                .WithDrainTimeoutMillis(1000)
                .Build();
            Assert.Equal("desc", service.Description);
            Assert.Equal("apiUrl", service.ApiUrl);
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
                () => Service.Builder().WithName(NAME).WithVersion("1.0.0").AddServiceEndpoint(se).Build());
            Assert.Contains("Connection cannot be null or empty", ae.Message);

            ae = Assert.Throws<ArgumentException>(
                () => Service.Builder().WithConnection(conn).WithVersion("1.0.0").AddServiceEndpoint(se).Build());
            Assert.Contains("Name cannot be null or empty", ae.Message);

            ae = Assert.Throws<ArgumentException>(() =>
                Service.Builder().WithConnection(conn).WithName(NAME).AddServiceEndpoint(se).Build());
            Assert.Contains("Version cannot be null or empty", ae.Message);

            ae = Assert.Throws<ArgumentException>(() =>
                Service.Builder().WithConnection(conn).WithName(NAME).WithVersion("1.0.0").Build());
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
                EndpointStats es = sr.EndpointStatsList[0];
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
    }
}