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
using System.Threading;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using NATS.Client.Service;
using Xunit;

namespace UnitTests
{
    public class TestServiceObjects : TestBase
    {
        [Fact]
        public void TestEndpointConstruction()
        {
            Endpoint e = new Endpoint(NAME);
            Assert.Equal(NAME, e.Name);
            Assert.Equal(NAME, e.Subject);
            Assert.Null(e.Schema);
            Assert.Equal(e, Endpoint.Builder().WithEndpoint(e).Build());

            e = new Endpoint(NAME, SUBJECT);
            Assert.Equal(NAME, e.Name);
            Assert.Equal(SUBJECT, e.Subject);
            Assert.Null(e.Schema);
            Assert.Equal(e, Endpoint.Builder().WithEndpoint(e).Build());

            e = new Endpoint(NAME, SUBJECT, "schema-request", null);
            Assert.Equal(NAME, e.Name);
            Assert.Equal(SUBJECT, e.Subject);
            Assert.Equal("schema-request", e.Schema.Request);
            Assert.Null(e.Schema.Response);
            Assert.Equal(e, Endpoint.Builder().WithEndpoint(e).Build());

            e = new Endpoint(NAME, SUBJECT, null, "schema-response");
            Assert.Equal(NAME, e.Name);
            Assert.Equal(SUBJECT, e.Subject);
            Assert.Null(e.Schema.Request);
            Assert.Equal("schema-response", e.Schema.Response);
            Assert.Equal(e, Endpoint.Builder().WithEndpoint(e).Build());

            e = Endpoint.Builder()
                .WithName(NAME)
                .WithSubject(SUBJECT)
                .WithSchemaRequest("schema-request")
                .WithSchemaResponse("schema-response")
                .Build();
            Assert.Equal(NAME, e.Name);
            Assert.Equal(SUBJECT, e.Subject);
            Assert.Equal("schema-request", e.Schema.Request);
            Assert.Equal("schema-response", e.Schema.Response);
            Assert.Equal(e, Endpoint.Builder().WithEndpoint(e).Build());

            e = Endpoint.Builder()
                .WithName(NAME).WithSubject(SUBJECT)
                .WithSchema(e.Schema)
                .Build();
            Assert.Equal(NAME, e.Name);
            Assert.Equal(SUBJECT, e.Subject);
            Assert.Equal("schema-request", e.Schema.Request);
            Assert.Equal("schema-response", e.Schema.Response);

            String j = e.ToJsonString();
            Assert.StartsWith("{", j);
            Assert.Contains("\"name\":\"name\"", j);
            Assert.Contains("\"subject\":\"subject\"", j);
            Assert.Contains("\"schema\":{", j);
            Assert.Contains("\"request\":\"schema-request\"", j);
            Assert.Contains("\"response\":\"schema-response\"", j);
            Assert.Equal(JsonUtils.ToKey(typeof(Endpoint)) +j, e.ToString());

            e = Endpoint.Builder()
                .WithName(NAME).WithSubject(SUBJECT)
                .WithSchema(null)
                .Build();
            Assert.Equal(NAME, e.Name);
            Assert.Equal(SUBJECT, e.Subject);
            Assert.Null(e.Schema);

            // some subject testing
            e = new Endpoint(NAME, "foo.>");
            Assert.Equal("foo.>", e.Subject);
            e = new Endpoint(NAME, "foo.*");
            Assert.Equal("foo.*", e.Subject);

            Assert.Throws<ArgumentException>(() => Endpoint.Builder().Build());

            // many names are bad
            Assert.Throws<ArgumentException>(() => new Endpoint((string)null));
            Assert.Throws<ArgumentException>(() => new Endpoint(string.Empty));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasSpace));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasPrintable));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasDot));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasStar)); // invalid in the middle
            Assert.Throws<ArgumentException>(() => new Endpoint(HasGt)); // invalid in the middle
            Assert.Throws<ArgumentException>(() => new Endpoint(HasDollar));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasLow));
            Assert.Throws<ArgumentException>(() => new Endpoint(Has127));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasFwdSlash));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasBackSlash));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasEquals));
            Assert.Throws<ArgumentException>(() => new Endpoint(HasTic));

            // fewer subjects are bad
            Assert.Throws<ArgumentException>(() => new Endpoint(NAME, HasSpace));
            Assert.Throws<ArgumentException>(() => new Endpoint(NAME, HasLow));
            Assert.Throws<ArgumentException>(() => new Endpoint(NAME, Has127));
            Assert.Throws<ArgumentException>(() => new Endpoint(NAME, "foo.>.bar")); // gt is not last segment
        }

        [Fact]
        public void TestSchemaConstruction()
        {
            Schema s1 = new Schema("request", "response");
            Assert.Equal("request", s1.Request);
            Assert.Equal("response", s1.Response);

            Assert.Null(Schema.OptionalInstance(null, ""));
            Assert.Null(Schema.OptionalInstance("", null));
            Assert.Null(Schema.OptionalInstance(null));

            Schema s2 = new Schema("request", null);
            Assert.Equal("request", s2.Request);
            Assert.Null(s2.Response);

            s2 = new Schema(null, "response");
            Assert.Null(s2.Request);
            Assert.Equal("response", s2.Response);

            s2 = new Schema(s1.ToJsonNode());
            Assert.Equal(s1, s2);

            s2 = Schema.OptionalInstance(s1.ToJsonNode());
            Assert.Equal(s1, s2);

            String j = s1.ToJsonString();
            Assert.StartsWith("{", j);
            Assert.Contains("\"request\":\"request\"", j);
            Assert.Contains("\"response\":\"response\"", j);
            String s = s1.ToString();
            Assert.StartsWith(JsonUtils.ToKey(typeof(Schema)), s);
            Assert.Contains("\"request\":\"request\"", s);
            Assert.Contains("\"response\":\"response\"", s);
        }

        [Fact]
        public void TestEndpointStatsConstruction()
        {
            JSONNode data = new JSONString("data");
            DateTime dt = DateTime.UtcNow;
            EndpointStats es = new EndpointStats("name", "subject", 0, 0, 0, null, null, dt);
            Assert.Equal("name", es.Name);
            Assert.Equal("subject", es.Subject);
            Assert.Null(es.LastError);
            Assert.Null(es.Data);
            Assert.Equal(0, es.NumRequests);
            Assert.Equal(0, es.NumErrors);
            Assert.Equal(0, es.ProcessingTime);
            Assert.Equal(0, es.AverageProcessingTime);
            Assert.Equal(dt, es.Started);

            es = new EndpointStats("name", "subject", 2, 4, 10, "lastError", data, dt);
            Assert.Equal("name", es.Name);
            Assert.Equal("subject", es.Subject);
            Assert.Equal("lastError", es.LastError);
            Assert.Equal("\"data\"", es.Data.ToString());
            Assert.Equal(2, es.NumRequests);
            Assert.Equal(4, es.NumErrors);
            Assert.Equal(10, es.ProcessingTime);
            Assert.Equal(5, es.AverageProcessingTime);
            Assert.Equal(dt, es.Started);

            String j = es.ToJsonString();
            Assert.StartsWith("{", j);
            Assert.Contains("\"name\":\"name\"", j);
            Assert.Contains("\"subject\":\"subject\"", j);
            Assert.Contains("\"last_error\":\"lastError\"", j);
            Assert.Contains("\"data\":\"data\"", j);
            Assert.Contains("\"num_requests\":2", j);
            Assert.Contains("\"num_errors\":4", j);
            Assert.Contains("\"processing_time\":10", j);
            Assert.Contains("\"average_processing_time\":5", j);
            Assert.Equal(JsonUtils.ToKey(typeof(EndpointStats)) + j, es.ToString());
        }

        [Fact]
        public void TestGroupConstruction()
        {
            Group g1 = new Group(Subject(1));
            Group g2 = new Group(Subject(2));
            Group g3 = new Group(Subject(3));
            Assert.Equal(Subject(1), g1.Name);
            Assert.Equal(Subject(1), g1.Subject);
            Assert.Equal(Subject(2), g2.Name);
            Assert.Equal(Subject(2), g2.Subject);
            Assert.Equal(Subject(3), g3.Name);
            Assert.Equal(Subject(3), g3.Subject);
            Assert.Null(g1.Next);
            Assert.Null(g2.Next);
            Assert.Null(g3.Next);

            Assert.Equal(g1, g1.AppendGroup(g2));
            Assert.NotNull(g1.Next);
            Assert.Equal(Subject(2), g1.Next.Name);
            Assert.Null(g2.Next);
            Assert.Equal(Subject(1), g1.Name);
            Assert.Equal(Subject(1) + "." + Subject(2), g1.Subject);
            Assert.Equal(Subject(2), g2.Name);
            Assert.Equal(Subject(2), g2.Subject);

            Assert.Equal(g1, g1.AppendGroup(g3));
            Assert.NotNull(g1.Next);
            Assert.Equal(Subject(2), g1.Next.Name);
            Assert.NotNull(g1.Next.Next);
            Assert.Equal(Subject(3), g1.Next.Next.Name);
            Assert.Equal(Subject(1), g1.Name);
            Assert.Equal(Subject(1) + "." + Subject(2) + "." + Subject(3), g1.Subject);

            g1 = new Group("foo.*");
            Assert.Equal("foo.*", g1.Name);

            Assert.Throws<ArgumentException>(() => new Group(HasSpace));
            Assert.Throws<ArgumentException>(() => new Group(HasLow));
            Assert.Throws<ArgumentException>(() => new Group(Has127));
            Assert.Throws<ArgumentException>(() => new Group("foo.>")); // gt is last segment
            Assert.Throws<ArgumentException>(() => new Group("foo.>.bar")); // gt is not last segment
        }

        [Fact]
        public void TestServiceEndpointConstruction()
        {
            Group g1 = new Group(Subject(1));
            Group g2 = new Group(Subject(2)).AppendGroup(g1);
            Endpoint e1 = new Endpoint(Name(100), Subject(100));
            Endpoint e2 = new Endpoint(Name(200), Subject(200));
            EventHandler<ServiceMsgHandlerEventArgs> smh = (sender, args) => {};
            Func<JSONNode> sds = () => null;

            ServiceEndpoint se = ServiceEndpoint.Builder()
                .WithEndpoint(e1)
                .WithHandler(smh)
                .WithStatsDataSupplier(sds)
                .Build();
            Assert.Null(se.Group);
            Assert.Equal(e1, se.Endpoint);
            Assert.Equal(e1.Name, se.Name);
            Assert.Equal(e1.Subject, se.Subject);
            Assert.Equal(smh, se.Handler);
            Assert.Equal(sds, se.StatsDataSupplier);

            se = ServiceEndpoint.Builder()
                .WithGroup(g1)
                .WithEndpoint(e1)
                .WithHandler(smh)
                .Build();
            Assert.Equal(g1, se.Group);
            Assert.Equal(e1, se.Endpoint);
            Assert.Equal(e1.Name, se.Name);
            Assert.Equal(g1.Subject + "." + e1.Subject, se.Subject);
            Assert.Null(se.StatsDataSupplier);

            se = ServiceEndpoint.Builder()
                .WithGroup(g2)
                .WithEndpoint(e1)
                .WithHandler(smh)
                .Build();
            Assert.Equal(g2, se.Group);
            Assert.Equal(e1, se.Endpoint);
            Assert.Equal(e1.Name, se.Name);
            Assert.Equal(g2.Subject + "." + e1.Subject, se.Subject);

            se = ServiceEndpoint.Builder()
                .WithEndpoint(e1)
                .WithEndpoint(e2)
                .WithHandler(smh)
                .Build();
            Assert.Equal(e2, se.Endpoint);
            Assert.Equal(e2.Name, se.Name);
            Assert.Equal(e2.Subject, se.Subject);

            se = ServiceEndpoint.Builder()
                .WithEndpoint(e1)
                .WithEndpointName(e2.Name)
                .WithHandler(smh)
                .Build();
            Assert.Equal(e2.Name, se.Name);
            Assert.Equal(e1.Subject, se.Subject);

            se = ServiceEndpoint.Builder()
                .WithEndpoint(e1)
                .WithEndpointSubject(e2.Subject)
                .WithHandler(smh)
                .Build();
            Assert.Equal(e1.Name, se.Name);
            Assert.Equal(e2.Subject, se.Subject);

            ArgumentException ae = Assert.Throws<ArgumentException>(() => ServiceEndpoint.Builder().Build());
            Assert.Contains("Endpoint", ae.Message);

            ae = Assert.Throws<ArgumentException>(() => ServiceEndpoint.Builder().WithEndpoint(e1).Build());
            Assert.Contains("Handler", ae.Message);
        }

        [Fact]
        public void TestUtilToDiscoverySubject()
        {
            Assert.Equal("$SRV.PING", Service.ToDiscoverySubject(Service.SrvPing, null, null));
            Assert.Equal("$SRV.PING.myservice", Service.ToDiscoverySubject(Service.SrvPing, "myservice", null));
            Assert.Equal("$SRV.PING.myservice.123", Service.ToDiscoverySubject(Service.SrvPing, "myservice", "123"));
        }

        [Fact]
        public void TestServiceResponsesConstruction()
        {
            PingResponse pr1 = new PingResponse("id", "name", "0.0.0");
            PingResponse pr2 = new PingResponse(pr1.ToJsonNode().ToString());
            ValidateApiInOutPingResponse(pr1);
            ValidateApiInOutPingResponse(pr2);
            ArgumentException ae = Assert.Throws<ArgumentException>(() => new TestServiceResponses(pr1.ToJsonNode()));
            Assert.Contains("Invalid type", ae.Message);

            ae = Assert.Throws<ArgumentException>(() => new TestServiceResponses("{[bad json"));
            Assert.Contains("Type cannot be null", ae.Message);

            String json1 = "{\"id\":\"id\",\"name\":\"name\",\"version\":\"0.0.0\"}";
            ae = Assert.Throws<ArgumentException>(() => new TestServiceResponses(json1));
            Assert.Contains("Type cannot be null", ae.Message);

            String json2 = "{\"name\":\"name\",\"version\":\"0.0.0\",\"type\":\"io.nats.micro.v1.test_response\"}";
            ae = Assert.Throws<ArgumentException>(() => new TestServiceResponses(json2));
            Assert.Contains("Id cannot be null", ae.Message);

            String json3 = "{\"id\":\"id\",\"version\":\"0.0.0\",\"type\":\"io.nats.micro.v1.test_response\"}";
            ae = Assert.Throws<ArgumentException>(() => new TestServiceResponses(json3));
            Assert.Contains("Name cannot be null", ae.Message);

            String json4 = "{\"id\":\"id\",\"name\":\"name\",\"type\":\"io.nats.micro.v1.test_response\"}";
            ae = Assert.Throws<ArgumentException>(() => new TestServiceResponses(json4));
            Assert.Contains("Version cannot be null", ae.Message);

            IList<String> slist = new List<string>();
            slist.Add("subject1");
            slist.Add("subject2");
            InfoResponse ir1 = new InfoResponse("id", "name", "0.0.0", "desc", slist);
            InfoResponse ir2 = new InfoResponse(ir1.ToJsonString());
            ValidateApiInOutInfoResponse(ir1);
            ValidateApiInOutInfoResponse(ir2);

            IList<Endpoint> endpoints = new List<Endpoint>();
            endpoints.Add(new Endpoint("endName0", "endSubject0", "endSchemaRequest0", "endSchemaResponse0"));
            endpoints.Add(new Endpoint("endName1", "endSubject1", "endSchemaRequest1", "endSchemaResponse1"));
            SchemaResponse sch1 = new SchemaResponse("id", "name", "0.0.0", "apiUrl", endpoints);
            SchemaResponse sch2 = new SchemaResponse(sch1.ToJsonString());
            ValidateApiInOutSchemaResponse(sch1);
            ValidateApiInOutSchemaResponse(sch2);

            DateTime serviceStarted = DateTime.UtcNow;
            DateTime[] endStarteds = new DateTime[2];
            Thread.Sleep(100);
            endStarteds[0] = DateTime.UtcNow;
            Thread.Sleep(100);
            endStarteds[1] = DateTime.UtcNow;

            IList<EndpointStats> statsList = new List<EndpointStats>();
            JSONNode[] data = new JSONNode[] { SupplyData(), SupplyData() };
            statsList.Add(new EndpointStats("endName0", "endSubject0", 1000, 0, 10000, "lastError0", data[0],
                endStarteds[0]));
            statsList.Add(new EndpointStats("endName1", "endSubject1", 2000, 10, 10000, "lastError1", data[1],
                endStarteds[1]));

            StatsResponse stat1 = new StatsResponse(pr1, serviceStarted, statsList);
            StatsResponse stat2 = new StatsResponse(stat1.ToJsonString());
            ValidateApiInOutStatsResponse(stat1, serviceStarted, endStarteds, data);
            ValidateApiInOutStatsResponse(stat2, serviceStarted, endStarteds, data);
        }
        
        private static void ValidateApiInOutStatsResponse(StatsResponse stat, DateTime serviceStarted, DateTime[] endStarteds, JSONNode[] data)
        {
            ValidateApiInOutServiceResponse(stat, StatsResponse.ResponseType);
            Assert.Equal(serviceStarted, stat.Started);
            Assert.Equal(2, stat.EndpointStatsList.Count);
            for (int x = 0; x < 2; x++) {
                EndpointStats e = stat.EndpointStatsList[x];
                Assert.Equal("endName" + x, e.Name);
                Assert.Equal("endSubject" + x, e.Subject);
                long nr = x * 1000 + 1000;
                long errs = x * 10;
                long avg = 10000 / nr;
                Assert.Equal(nr, e.NumRequests);
                Assert.Equal(errs, e.NumErrors);
                Assert.Equal(10000, e.ProcessingTime);
                Assert.Equal(avg, e.AverageProcessingTime);
                Assert.Equal("lastError" + x, e.LastError);
                Assert.Equal(new TestStatsData(data[x]), new TestStatsData(e.Data));
                Assert.Equal(endStarteds[x], e.Started);
            }
        }

        private static void ValidateApiInOutSchemaResponse(SchemaResponse r)
        {
            ValidateApiInOutServiceResponse(r, SchemaResponse.ResponseType);
            Assert.Equal("apiUrl", r.ApiUrl);
            Assert.Equal(2, r.Endpoints.Count);
            for (int x = 0; x < 2; x++) {
                Endpoint e = r.Endpoints[x];
                Assert.Equal("endName" + x, e.Name);
                Assert.Equal("endSubject" + x, e.Subject);
                Assert.Equal("endSchemaRequest" + x, e.Schema.Request);
                Assert.Equal("endSchemaResponse" + x, e.Schema.Response);
            }
        }

        private static void ValidateApiInOutInfoResponse(InfoResponse r)
        {
            ValidateApiInOutServiceResponse(r, InfoResponse.ResponseType);
            Assert.Equal("desc", r.Description);
            Assert.Equal(2, r.Subjects.Count);
            Assert.True(r.Subjects.Contains("subject1"));
            Assert.True(r.Subjects.Contains("subject2"));
        }

        private static void ValidateApiInOutPingResponse(PingResponse r)
        {
            ValidateApiInOutServiceResponse(r, PingResponse.ResponseType);
        }

        private static void ValidateApiInOutServiceResponse(ServiceResponse r, string type)
        {
            Assert.Equal(type, r.Type);
            Assert.Equal("id", r.Id);
            Assert.Equal("name", r.Name);
            Assert.Equal("0.0.0", r.Version);
            string j = r.ToJsonString();
            Assert.StartsWith("{", j);
            Assert.Contains("\"type\":\"" + type + "\"", j);
            Assert.Contains("\"name\":\"name\"", j);
            Assert.Contains("\"id\":\"id\"", j);
            Assert.Contains("\"version\":\"0.0.0\"", j);
            Assert.Equal(JsonUtils.ToKey(r.GetType()) + j, r.ToString());
        }

        class TestStatsData : JsonSerializable
        {
            public string Sdata { get; }
            public int Idata { get; }

            public TestStatsData(string sdata, int idata)
            {
                Sdata = sdata;
                Idata = idata;
            }

            public TestStatsData(string json) : this(JSON.Parse(json)) {}

            public TestStatsData(JSONNode node)
            {
                Sdata = node["sdata"];
                Idata = node["idata"].AsInt;
            }

            internal override JSONNode ToJsonNode()
            {
                JSONObject jso = new JSONObject();
                JsonUtils.AddField(jso, "sdata", Sdata);
                JsonUtils.AddField(jso, "idata", Idata);
                return jso;
            }

            protected bool Equals(TestStatsData other)
            {
                return Sdata == other.Sdata && Idata == other.Idata;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;
                return Equals((TestStatsData)obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return ((Sdata != null ? Sdata.GetHashCode() : 0) * 397) ^ Idata;
                }
            }
        }

        private static int _dataX = -1;
        private static JSONNode SupplyData()
        {
            _dataX++;
            return new TestStatsData("s-" + _dataX, _dataX).ToJsonNode();
        }
    }

    class TestServiceResponses : ServiceResponse
    {
        const string ResponseType = "io.nats.micro.v1.test_response";
        
        public TestServiceResponses(string id, string name, string version) : base(ResponseType, id, name, version) {}

        public TestServiceResponses(ServiceResponse template) : base(ResponseType, template) {}

        internal TestServiceResponses(string json) : this(JSON.Parse(json)) {}

        public TestServiceResponses(JSONNode node) : base(ResponseType, node) {}

        internal override JSONNode ToJsonNode()
        {
            return BaseJsonObject();
        }
    }
}
