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

using System.Linq;
using NATS.Client;
using Xunit;

namespace UnitTests
{
    public class TestServerPool
    {
        static readonly string[] BootstrapUrls = {
            "nats://a:4222", "nats://b:4222", "nats://c:4222", "nats://d:4222"
        };
        static readonly NatsUri[] BootstrapUrlWrappers = {
            new NatsUri(BootstrapUrls[0]),  new NatsUri(BootstrapUrls[1]), new NatsUri(BootstrapUrls[2]), new NatsUri(BootstrapUrls[3])
        };
        
        static readonly string[] DiscoveredUrls = {
            "nats://a:4222", "nats://b:4222", "nats://c:4222", "nats://d:4222",
            "nats://z:4222", "nats://y:4222", "nats://x:4222", "nats://w:4222"
        };
        static readonly NatsUri[] DiscoveredUrlWrappers = {
            new NatsUri(DiscoveredUrls[0]),  new NatsUri(DiscoveredUrls[1]), new NatsUri(DiscoveredUrls[2]), new NatsUri(DiscoveredUrls[3]),
            new NatsUri(DiscoveredUrls[4]),  new NatsUri(DiscoveredUrls[5]), new NatsUri(DiscoveredUrls[6]), new NatsUri(DiscoveredUrls[7])
        };

        [Fact]
        public void TestDefault()
        {
            IServerListProvider slp = new NatsServerListProvider(Opts(true, true, true, false));
            var poolUrls = slp.GetServersToTry(null);
            Assert.True(poolUrls.Count == 1);
            Assert.Equal(poolUrls[0].ToString(), Defaults.Url);
        }

        [Fact]
        public void TestRandomization()
        {
            IServerListProvider slp = new NatsServerListProvider(Opts(true, true, true, false));
            var poolUrls = slp.GetServersToTry(null);
            Assert.True(poolUrls.Count == 4);
            Assert.False(poolUrls.SequenceEqual(BootstrapUrlWrappers));
        }

        [Fact]
        public void TestNoRandomization()
        {
            IServerListProvider slp = new NatsServerListProvider(Opts(true, false, true, false));
            var poolUrls = slp.GetServersToTry(null);
            Assert.True(poolUrls.Count == 4);
            Assert.False(poolUrls.SequenceEqual(BootstrapUrlWrappers));
        }

        [Fact]
        public void TestIncludeDiscovered()
        {
            IServerListProvider slp = new NatsServerListProvider(Opts(true, true, true, false));
            var poolUrls = slp.GetServersToTry(null);
            Assert.True(poolUrls.Count == 8);
        }

        [Fact]
        public void TestIgnoreDiscovered()
        {
            IServerListProvider slp = new NatsServerListProvider(Opts(true, true, false, false));
            var poolUrls = slp.GetServersToTry(null);
            Assert.True(poolUrls.Count == 4);
        }

        [Fact]
        public void TestCurrentServer()
        {
            IServerListProvider slp = new NatsServerListProvider(Opts(true, true, true, false));
            var poolUrls = slp.GetServersToTry(BootstrapUrlWrappers[0]);
            Assert.True(poolUrls.Count == 4);
            Assert.Equal(BootstrapUrlWrappers[0], poolUrls[3]);
            Assert.Equal(BootstrapUrlWrappers[1], poolUrls[0]);
            Assert.Equal(BootstrapUrlWrappers[2], poolUrls[1]);
            Assert.Equal(BootstrapUrlWrappers[3], poolUrls[2]);
        }

        private static Options Opts(bool bootstrap, bool randomize, bool includeDiscoveredServers,
            bool resolveHostnames)
        {
            Options opt = new Options();
            if (bootstrap)
            {
                opt.Servers = BootstrapUrls;
            }
            opt.NoRandomize = !randomize;
            opt.IgnoreDiscoveredServers = !includeDiscoveredServers;
            // opt.ResolveHostnames = resolveHostnames;
            return opt;
        }
    }
}