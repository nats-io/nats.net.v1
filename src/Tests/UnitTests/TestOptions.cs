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
using NATS.Client;
using Xunit;
using static NATS.Client.Defaults;

namespace UnitTests
{
    public class TestOptions
    {
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
    }
}