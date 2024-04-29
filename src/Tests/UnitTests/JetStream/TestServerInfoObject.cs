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

using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestServerInfoObject : TestBase
    {
        public TestServerInfoObject() {}

        readonly string json = ReadDataFile("ServerInfoJson.txt");

        [Fact]
        public void JsonIsReadProperly()
        {
            ServerInfo info = new ServerInfo(json);
            Assert.Equal("serverId", info.ServerId);
            Assert.Equal("serverName", info.ServerName);
            Assert.Equal("1.2.3", info.Version);
            Assert.Equal("go0.0.0", info.GoVersion);
            Assert.Equal("host", info.Host);
            Assert.Equal(7777, info.Port);
            Assert.True(info.AuthRequired);
            Assert.True(info.TlsRequired);
            Assert.True(info.TlsAvailable);
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
            ServerInfo si234 = new ServerInfo(json.Replace("1.2.3", "2.3.4"));
            ServerInfo si235A1 = new ServerInfo(json.Replace("1.2.3", "2.3.5-alpha.1"));
            ServerInfo si235A2 = new ServerInfo(json.Replace("1.2.3", "2.3.5-alpha-2"));
            ServerInfo si235B1 = new ServerInfo(json.Replace("1.2.3", "v2.3.5-beta.1"));
            ServerInfo si235B2 = new ServerInfo(json.Replace("1.2.3", "v2.3.5-beta-2"));
            ServerInfo si235 = new ServerInfo(json.Replace("1.2.3", "2.3.5"));

            ServerInfo[] infos = new ServerInfo[]
                { si234, si235A1, si235A2, si235B1, si235B2, si235 };
            for (int i = 0; i < infos.Length; i++)
            {
                ServerInfo si = infos[i];
                for (int j = 0; j < infos.Length; j++)
                {
                    string v2 = new ServerVersion(infos[j].Version).ToString();
                    if (i == j)
                    {
                        Assert.True(si.IsSameVersion(v2));
                        Assert.True(si.IsSameOrOlderThanVersion(v2));
                        Assert.True(si.IsSameOrNewerThanVersion(v2));
                        Assert.False(si.IsNewerVersionThan(v2));
                        Assert.False(si.IsOlderThanVersion(v2));
                    }
                    else
                    {
                        Assert.False(si.IsSameVersion(v2));
                        if (i < j)
                        {
                            Assert.True(si.IsOlderThanVersion(v2));
                            Assert.True(si.IsSameOrOlderThanVersion(v2));
                            Assert.False(si.IsNewerVersionThan(v2));
                            Assert.False(si.IsSameOrNewerThanVersion(v2));
                        }
                        else
                        {
                            // i > j
                            Assert.False(si.IsOlderThanVersion(v2));
                            Assert.False(si.IsSameOrOlderThanVersion(v2));
                            Assert.True(si.IsNewerVersionThan(v2));
                            Assert.True(si.IsSameOrNewerThanVersion(v2));
                        }
                    }
                }
            }

            Assert.True(si234.IsNewerVersionThan("not-a-number.2.3"));
            Assert.True(si234.IsNewerVersionThan("1.not-a-number.3"));
            Assert.True(si234.IsNewerVersionThan("1.2.not-a-number"));

            ServerInfo siPadded1 = new ServerInfo(json.Replace("1.2.3", "1.20.30"));
            ServerInfo siPadded2 = new ServerInfo(json.Replace("1.2.3", "40.500.6000"));
            Assert.True(siPadded1.IsSameVersion("1.20.30"));
            Assert.True(siPadded2.IsSameVersion("40.500.6000"));
            Assert.True(siPadded2.IsNewerVersionThan(siPadded1.Version));
            Assert.True(siPadded1.IsOlderThanVersion(siPadded2.Version));
        }

        [Fact]
        public void EmptyUrlParsedProperly()
        {        
            string json = "INFO {" +
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
            string json = "INFO {" +
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
            string json = "INFO {" +
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
