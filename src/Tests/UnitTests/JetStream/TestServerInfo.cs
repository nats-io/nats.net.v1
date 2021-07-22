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
        [Fact]
        public void JsonIsReadProperly()
        {
            string json = ReadDataFile("ServerInfoJson.txt");

            ServerInfo info = new ServerInfo(json);
            Assert.Equal("serverId", info.ServerId);
            Assert.Equal("serverName", info.ServerName);
            Assert.Equal("0.0.0", info.Version);
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
