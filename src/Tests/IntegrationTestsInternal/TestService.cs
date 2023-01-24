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
using System.Text;
using IntegrationTests;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.Service;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;

namespace IntegrationTestsInternal
{
    public class TestService : TestSuite<ServiceSuiteContext>
    {
        private readonly ITestOutputHelper output;
        public TestService(ITestOutputHelper output, ServiceSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
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
    }
}