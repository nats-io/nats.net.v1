// Copyright 2015-2018 The NATS Authors
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
using NATS.Client;
using Xunit;
using Xunit.Abstractions;
using static NATS.Client.Defaults;

namespace UnitTests
{
    public class TestOptions
    {
        private readonly ITestOutputHelper output;

        public TestOptions(ITestOutputHelper output)
        {
            this.output = output;
        }
        
        private Options GetDefaultOptions() => ConnectionFactory.GetDefaultOptions();

        [Fact]
        public void TestBadOptionTimeoutConnect()
        {
            var opts = GetDefaultOptions();

            Assert.ThrowsAny<Exception>(() => opts.Timeout = -1);
        }

        [Fact]
        public void TestBadOptionSubscriptionBatchSize()
        {
            var opts = GetDefaultOptions();

            Assert.ThrowsAny<ArgumentException>(() => opts.SubscriptionBatchSize = -1);

            Assert.ThrowsAny<ArgumentException>(() => opts.SubscriptionBatchSize = 0);
        }

        [Theory]
        [InlineData("")]
        [InlineData("\r")]
        [InlineData("\n")]
        [InlineData("\t")]
        [InlineData("Test")]
        [InlineData(".Test.")]
        public void TestBadCustomPrefix(string customPrefix)
        {
            var opts = GetDefaultOptions();

            Assert.ThrowsAny<ArgumentException>(() => opts.CustomInboxPrefix = customPrefix);
        }

        [Theory]
        [InlineData("Test.")]
        [InlineData("Test.SubTest.")]
        [InlineData("_Test.")]
        [InlineData("_Test.SubTest.")]
        public void TestOkCustomPrefix(string customPrefix)
        {
            var opts = GetDefaultOptions();

            opts.CustomInboxPrefix = customPrefix;
        }

        [Theory]
        [InlineData("http://localhost:4222")]
        [InlineData("HTTP://localhost:4222")]
        [InlineData("https://localhost:4222")]
        [InlineData("HTTPS://localhost:4222")]
        [InlineData("file://localhost:4222")]
        [InlineData("ftp://localhost:4222")]
        public void TestBadUrlProtocol(string invalidUrl)
        {
            Assert.Throws<ArgumentException>(() => GetDefaultOptions().Url = invalidUrl);
        }

        [Theory]
        [InlineData("nats://localhost:4222")]
        [InlineData("NATS://localhost:4222")]
        [InlineData("tls://localhost:4222")]
        [InlineData("TLS://localhost:4222")]
        [InlineData("localhost:4222")]
        [InlineData("")]
        [InlineData(null)]
        public void TestOkUrlProtocol(string okUrl)
        {
            GetDefaultOptions().Url = okUrl;
        }
        
        [Fact]
        public void TestBadServers()
        {
            var invalidServers = new[]
            {
                "http://localhost:4222",
                "HTTPS://localhost:4222",
                "file://localhost:4222",
                "ftp://localhost:4222"
            };
            
            Assert.Throws<ArgumentException>(() => GetDefaultOptions().Servers = invalidServers);
        }

        [Fact]
        public void TestOkServers()
        {
            var okServers = new[]
            {
                "nats://localhost:4222",
                "NATS://localhost:4222",
                "tls://localhost:4222",
                "TLS://localhost:4222",
                "",
                "null",
            };
            GetDefaultOptions().Servers = okServers;
        }

        [Fact]
        public void TestDefaultHandler()
        {
            // making sure there is no null pointer
            ConnEventArgs cea = new ConnEventArgs(null, null);
            DefaultClosedEventHandler().Invoke(null, cea);
            DefaultServerDiscoveredEventHandler().Invoke(null, cea);
            DefaultDisconnectedEventHandler().Invoke(null, cea);
            DefaultReconnectedEventHandler().Invoke(null, cea);
            DefaultLameDuckModeEventHandler().Invoke(null, cea);
            DefaultLameDuckModeEventHandler().Invoke(null, cea);
            DefaultClosedEventHandler().Invoke(null, null);
            DefaultServerDiscoveredEventHandler().Invoke(null, null);
            DefaultDisconnectedEventHandler().Invoke(null, null);
            DefaultReconnectedEventHandler().Invoke(null, null);
            DefaultLameDuckModeEventHandler().Invoke(null, null);
            DefaultLameDuckModeEventHandler().Invoke(null, null);

            ErrEventArgs eea = new ErrEventArgs(null, null, null);
            DefaultAsyncErrorEventHandler().Invoke(null, eea);
            DefaultAsyncErrorEventHandler().Invoke(null, null);

            HeartbeatAlarmEventArgs haea = new HeartbeatAlarmEventArgs(null, null, 0U, 0U);
            DefaultHeartbeatAlarmEventHandler().Invoke(null, haea);
            DefaultHeartbeatAlarmEventHandler().Invoke(null, null);

            UnhandledStatusEventArgs usea = new UnhandledStatusEventArgs(null, null, null);
            DefaultUnhandledStatusEventHandler().Invoke(null, usea);
            DefaultUnhandledStatusEventHandler().Invoke(null, null);

            FlowControlProcessedEventArgs fcpea = new FlowControlProcessedEventArgs(null, null, null, FlowControlSource.Heartbeat);
            DefaultFlowControlProcessedEventHandler().Invoke(null, fcpea);
            DefaultFlowControlProcessedEventHandler().Invoke(null, null);
        }

        [Fact]
        public void TestNatsUri() {
            string[] schemes = new string[] { "nats", "tls", null, "unk"};
            bool[] secures = new bool[]     { false,  true,  false, false};
            string[] hosts = new string[]{"host", "1.2.3.4", null};
            bool[] ips = new bool[]      {false,  true,      false};
            int?[] ports = new int?[]{1122, null};
            string[] userInfos = new string[]{null, "u:p"};
            for (int e = 0; e < schemes.Length; e++) {
                string scheme = schemes[e];
                for (int h = 0; h < hosts.Length; h++) {
                    string host = hosts[h];
                    foreach (int? port in ports) {
                        foreach (string userInfo in userInfos) {
                            StringBuilder sb = new StringBuilder();
                            string expectedScheme;
                            if (scheme == null) {
                                expectedScheme = "nats";
                            }
                            else {
                                expectedScheme = scheme;
                                sb.Append(scheme).Append("://");
                            }
                            if (userInfo != null) {
                                sb.Append(userInfo).Append("@");
                            }
                            if (host != null) {
                                sb.Append(host);
                            }
                            int expectedPort;
                            if (port == null) {
                                expectedPort = NatsUri.DefaultPort;
                            }
                            else {
                                expectedPort = port.Value;
                                sb.Append(":").Append(expectedPort);
                            }
                            if (host == null || "unk".Equals(scheme)) {
                                Assert.Throws<UriFormatException>(() => new NatsUri(sb.ToString()));
                            }
                            else {
                                NatsUri uri1 = new NatsUri(sb.ToString());
                                NatsUri uri2 = new NatsUri(uri1.Uri);
                                Assert.Equal(uri1, uri2);
                                checkCreate(uri1, secures[e], ips[h], expectedScheme, host, expectedPort, userInfo);
                                checkCreate(uri2, secures[e], ips[h], expectedScheme, host, expectedPort, userInfo);
                            }
                        }
                    }
                }
            }
        }

        private static void checkCreate(NatsUri uri, bool secure, bool ip, string scheme, string host, int port, string userInfo) {
            Assert.Equal(secure, uri.Secure);
            Assert.Equal(scheme, uri.Scheme);
            Assert.Equal(host, uri.Host);
            Assert.Equal(port, uri.Port);
            if (userInfo == null)
            {
             Assert.Empty(uri.UserInfo);   
            }
            else
            {
                Assert.Equal(userInfo, uri.UserInfo);
            }
            string expectedUri = userInfo == null
                ? scheme + "://" + host + ":" + port
                : scheme + "://" + userInfo + "@" + host + ":" + port;
            Assert.Equal(expectedUri, uri.ToString());
            Assert.Equal(expectedUri.Replace(host, "rehost"), uri.ReHost("rehost").ToString());
            Assert.Equal(ip, uri.HostIsIpAddress);
        }

    }
}