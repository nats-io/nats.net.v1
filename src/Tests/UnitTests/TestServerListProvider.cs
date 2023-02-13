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
    public class TestServerListProvider
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

        [Fact]
        public void TestDefault()
        {
            IServerListProvider slp = SetupProvider(null, null, true, true, false);
            var uris = slp.GetServersToTry(null);
            Assert.True(uris.Count == 1);
            Assert.Equal(uris[0].ToString(), Defaults.Url);
        }

        [Fact]
        public void TestRandomization()
        {
            IServerListProvider slp = SetupProvider(BootstrapUrls, null, true, true, false);
            var uris = slp.GetServersToTry(null);
            Assert.True(uris.Count == 4);
        }

        [Fact]
        public void TestNoRandomization()
        {
            IServerListProvider slp = SetupProvider(BootstrapUrls, null, false, true, false);
            var uris = slp.GetServersToTry(null);
            Assert.True(uris.Count == 4);
            Assert.True(uris.SequenceEqual(BootstrapUrlWrappers));
        }

        [Fact]
        public void TestIncludeDiscovered()
        {
            IServerListProvider slp = SetupProvider(BootstrapUrls, DiscoveredUrls, true, true, false);
            var uris = slp.GetServersToTry(null);
            Assert.True(uris.Count == 8);
        }

        [Fact]
        public void TestIgnoreDiscovered()
        {
            IServerListProvider slp = SetupProvider(BootstrapUrls, DiscoveredUrls, true, false, false);
            var uris = slp.GetServersToTry(null);
            Assert.True(uris.Count == 4);
        }

        [Fact]
        public void TestCurrentServer()
        {
            IServerListProvider slp = SetupProvider(BootstrapUrls, null, false, true, false);
            var uris = slp.GetServersToTry(BootstrapUrlWrappers[0]);
            Assert.True(uris.Count == 4);
            Assert.Equal(BootstrapUrlWrappers[0], uris[3]);
            Assert.Equal(BootstrapUrlWrappers[1], uris[0]);
            Assert.Equal(BootstrapUrlWrappers[2], uris[1]);
            Assert.Equal(BootstrapUrlWrappers[3], uris[2]);
        }

        private static NatsServerListProvider SetupProvider(string[] servers, string[] discovered, bool randomize, bool includeDiscoveredServers, bool resolveHostnames)
        {
            Options opt = new Options();
            if (servers != null)
            {
                opt.Servers = servers;
            }

            opt.NoRandomize = !randomize;
            opt.IgnoreDiscoveredServers = !includeDiscoveredServers;
            // opt.ResolveHostnames = resolveHostnames;

            NatsServerListProvider nslp = new NatsServerListProvider();
            nslp.Initialize(opt);
            if (discovered != null)
            {
                nslp.AcceptDiscoveredServers(discovered);
            }
            return nslp;
        }
    }
}