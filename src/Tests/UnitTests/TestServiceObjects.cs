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

using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using NATS.Client.Service;
using Xunit;

namespace UnitTests
{
    public class TestServiceObjects
    {
        [Fact]
        public void TestToDiscoverySubject()
        {
            Assert.Equal("$SRV.PING", ServiceUtil.ToDiscoverySubject("PING", null, null));
            Assert.Equal("$SRV.PING.myservice", ServiceUtil.ToDiscoverySubject("PING", "myservice", null));
            Assert.Equal("$SRV.PING.myservice.123", ServiceUtil.ToDiscoverySubject("PING", "myservice", "123"));
        }

        [Fact]
        public void TestApiJsonInOut()
        {
            Ping pr1 = new Ping("{\"name\":\"ServiceName\",\"id\":\"serviceId\"}");
            Ping pr2 = new Ping(pr1.ToJsonNode().ToString());
            Assert.Equal("ServiceName", pr1.Name);
            Assert.Equal("serviceId", pr1.ServiceId);
            Assert.Equal(pr1.Name, pr2.Name);
            Assert.Equal(pr1.ServiceId, pr2.ServiceId);

            Info ir1 = new Info("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"description\":\"desc\",\"version\":\"0.0.1\",\"subject\":\"ServiceSubject\"}");
            Info ir2 = new Info(ir1.ToJsonNode().ToString());
            Assert.Equal("ServiceName", ir1.Name);
            Assert.Equal("serviceId", ir1.ServiceId);
            Assert.Equal("desc", ir1.Description);
            Assert.Equal("0.0.1", ir1.Version);
            Assert.Equal("ServiceSubject", ir1.Subject);
            Assert.Equal(ir1.Name, ir2.Name);
            Assert.Equal(ir1.ServiceId, ir2.ServiceId);
            Assert.Equal(ir1.Description, ir2.Description);
            Assert.Equal(ir1.Version, ir2.Version);
            Assert.Equal(ir1.Subject, ir2.Subject);

            SchemaInfo sr1 = new SchemaInfo("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\",\"schema\":{\"request\":\"rqst\",\"response\":\"rspns\"}}");
            SchemaInfo sr2 = new SchemaInfo(sr1.ToJsonNode().ToString());
            Assert.Equal("ServiceName", sr1.Name);
            Assert.Equal("serviceId", sr1.ServiceId);
            Assert.Equal("0.0.1", sr1.Version);
            Assert.Equal("rqst", sr1.Schema.Request);
            Assert.Equal("rspns", sr1.Schema.Response);
            Assert.Equal(sr1.Name, sr2.Name);
            Assert.Equal(sr1.ServiceId, sr2.ServiceId);
            Assert.Equal(sr1.Version, sr2.Version);
            Assert.Equal(sr1.Schema.Request, sr2.Schema.Request);
            Assert.Equal(sr1.Schema.Response, sr2.Schema.Response);

            sr1 = new SchemaInfo("{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\"}");
            sr2 = new SchemaInfo(sr1.ToJsonNode().ToString());
            Assert.Equal("ServiceName", sr1.Name);
            Assert.Equal("serviceId", sr1.ServiceId);
            Assert.Equal("0.0.1", sr1.Version);
            Assert.Equal(sr1.Name, sr2.Name);
            Assert.Equal(sr1.ServiceId, sr2.ServiceId);
            Assert.Equal(sr1.Version, sr2.Version);
            Assert.Null(sr1.Schema);
            Assert.Null(sr2.Schema);

            StatsDataDecoder sdd = json => new TestStatsData(json);
            
            string statsJson = "{\"name\":\"ServiceName\",\"id\":\"serviceId\",\"version\":\"0.0.1\",\"num_requests\":1,\"num_errors\":2,\"last_error\":\"npe\",\"total_processing_time\":3,\"average_processing_time\":4,\"data\":{\"id\":\"user id\",\"last_error\":\"user last error\"}}";
            Stats stats1 = new Stats(statsJson, sdd);
            Stats stats2 = new Stats(stats1.ToJsonNode().ToString(), sdd);
            Assert.Equal("ServiceName", stats1.Name);
            Assert.Equal("serviceId", stats1.ServiceId);
            Assert.Equal("0.0.1", stats1.Version);
            Assert.Equal(stats1.Name, stats2.Name);
            Assert.Equal(stats1.ServiceId, stats2.ServiceId);
            Assert.Equal(stats1.Version, stats2.Version);
            Assert.Equal(1, stats1.NumRequests);
            Assert.Equal(1, stats2.NumRequests);
            Assert.Equal(2, stats1.NumErrors);
            Assert.Equal(2, stats2.NumErrors);
            Assert.Equal("npe", stats1.LastError);
            Assert.Equal("npe", stats2.LastError);
            Assert.Equal(3, stats1.TotalProcessingTime);
            Assert.Equal(3, stats2.TotalProcessingTime);
            Assert.Equal(4, stats1.AverageProcessingTime);
            Assert.Equal(4, stats2.AverageProcessingTime);
            Assert.True(stats1.Data is TestStatsData);
            Assert.True(stats2.Data is TestStatsData);
            TestStatsData data1 = (TestStatsData)stats1.Data;
            TestStatsData data2 = (TestStatsData)stats2.Data;
            Assert.Equal("user id", data1.Id);
            Assert.Equal("user id", data2.Id);
            Assert.Equal("user last error", data1.LastError);
            Assert.Equal("user last error", data2.LastError);
        }
    }
    
    class TestStatsData : IStatsData {
        public string Id { get; }
        public string LastError { get; }

        public TestStatsData(string id, string lastError)
        {
            Id = id;
            LastError = lastError;
        }
        
        public TestStatsData(string json) : this(JSON.Parse(json)) {}

        public TestStatsData(JSONNode node)
        {
            Id = node[ApiConstants.Id];
            LastError = node[ApiConstants.LastError];
        }

        public string ToJson()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, Id);
            JsonUtils.AddField(jso, ApiConstants.LastError, LastError);
            return jso.ToString();
        }
    }

}