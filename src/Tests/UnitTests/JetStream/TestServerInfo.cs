// Copyright 2020 The NATS Authors
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
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestServerInfo : TestBase
    {
        readonly string _json = ReadDataFile("ServerInfoJson.txt");

        [Fact]
        public void JsonIsReadProperly()
        {
            ServerInfo info = new ServerInfo(_json);
            Assert.Equal("serverId", info.ServerId);
            Assert.Equal("serverName", info.ServerName);
            Assert.Equal("1.2.3", info.Version);
            Assert.Equal("go0.0.0", info.GoVersion);
            Assert.Equal("host", info.Host);
            Assert.Equal(7777, info.Port);
            Assert.True(info.AuthRequired);
            Assert.True(info.TlsRequired);
            Assert.True(info.HeadersSupported);
            Assert.Equal(100_000_000_000L, info.MaxPayload);
            Assert.Equal(1, info.ProtocolVersion);
            Assert.True(info.LameDuckMode);
            Assert.True(info.JetStreamAvailable);
            Assert.Equal(42, info.ClientId);
            Assert.Equal("127.0.0.1", info.ClientIp);
            Assert.Equal("cluster", info.Cluster);
            Assert.Equal(2, info.ConnectURLs.Length);
            Assert.Equal("url0", info.ConnectURLs[0]);
            Assert.Equal("url1", info.ConnectURLs[1]);
            Assert.Equal("YWJjZGVmZ2hpamtsbW5vcHFyc3R1dnd4eXo", info.Nonce);
        }
        
        [Fact]
        public void ServerVersionComparisonsWork()
        {
            ServerInfo info = new ServerInfo(_json);
            ServerInfo info234 = new ServerInfo(_json.Replace("1.2.3", "2.3.4"));
            ServerInfo info235 = new ServerInfo(_json.Replace("1.2.3", "2.3.5"));
            ServerInfo info235Beta2 = new ServerInfo(_json.Replace("1.2.3", "2.3.5-beta.2"));
            Assert.True(info.IsOlderThanVersion("2.3.4"));
            Assert.True(info234.IsOlderThanVersion("2.3.5"));
            Assert.True(info235.IsOlderThanVersion("2.3.5-beta.2"));
            Assert.True(info.IsSameVersion("1.2.3"));
            Assert.True(info234.IsSameVersion("2.3.4"));
            Assert.True(info235.IsSameVersion("2.3.5"));
            Assert.True(info235Beta2.IsSameVersion("2.3.5-beta.2"));
            Assert.False(info235.IsSameVersion("2.3.4"));
            Assert.False(info235Beta2.IsSameVersion("2.3.5"));
            Assert.True(info234.IsNewerVersionThan("1.2.3"));
            Assert.True(info235.IsNewerVersionThan("2.3.4"));
            Assert.True(info235Beta2.IsNewerVersionThan("2.3.5"));

            Assert.True(info234.IsNewerVersionThan("not-a-number"));
            Assert.False(info234.IsNewerVersionThan("2.3.5"));
            Assert.False(info235.IsOlderThanVersion("2.3.4"));
        }

        [Fact]
        public void EmptyUrlParsedProperly()
        {        
            String json = "INFO {" +
                          "\"server_id\":\"myserver\"" + "," +
                          "\"connect_urls\":[\"one\", \"\"]" +
                          "}";
            ServerInfo info = new ServerInfo(json);
            Assert.Equal("myserver", info.ServerId);
            
            // The empty string gets dropped
            Assert.Collection(info.ConnectURLs,
                item => Assert.Equal("one", item));
        }

        [Fact]
        public void IPV6InBracketsParsedProperly()
        {
            String json = "INFO {" +
                          "\"server_id\":\"myserver\"" + "," +
                          "\"connect_urls\":[\"one:4222\", \"[a:b:c]:4222\", \"[d:e:f]:4223\"]" + "," +
                          "\"max_payload\":100000000000" +
                          "}";
            ServerInfo info = new ServerInfo(json);
            Assert.Equal("myserver", info.ServerId);
            Assert.Equal(3, info.ConnectURLs.Length);
            Assert.Equal("one:4222", info.ConnectURLs[0]);
            Assert.Equal("[a:b:c]:4222", info.ConnectURLs[1]);
            Assert.Equal("[d:e:f]:4223", info.ConnectURLs[2]);
        }

        [Fact]
        public void EncodingInString() {
            String json = "INFO {" +
                          "\"server_id\":\"\\\\\\b\\f\\n\\r\\t\"" + "," +
                          "\"go\":\"my\\u0021go\"" + "," +
                          "\"host\":\"my\\\\host\"" + "," +
                          "\"version\":\"1.1.1\\t1\"" +
                          "}";
            ServerInfo info = new ServerInfo(json);
            Assert.Equal("\\\b\f\n\r\t", info.ServerId);
            Assert.Equal("my!go", info.GoVersion);
            Assert.Equal("my\\host", info.Host);
            Assert.Equal("1.1.1\t1", info.Version);
        }
    }
}
