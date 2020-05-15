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
        static readonly string[] startUrls = {
            "nats://a:4222", "nats://b:4222", "nats://c:4222", "nats://d:4222",
            "nats://e:4222", "nats://f:4222", "nats://g:4222", "nats://h:4222",
            "nats://i:4222", "nats://j:4222", "nats://k:4222", "nats://l:4222"
        };

        [Fact]
        public void TestDefault()
        {
            var sp = new ServerPool();
            sp.Setup(new Options());

            var poolUrls = sp.GetServerList(false);
            Assert.True(poolUrls.Length == 1);
            Assert.Equal(poolUrls[0], Defaults.Url);
        }

        [Fact]
        public void TestBasicRandomization()
        {
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Servers = startUrls;

            for (int i = 0; i < 10; i++)
            {
                var sp = new ServerPool();
                sp.Setup(opts);

                var poolUrls = sp.GetServerList(false);
                Assert.True(poolUrls.Length == startUrls.Length);
                Assert.False(poolUrls.SequenceEqual(startUrls));
            }
        }

        [Fact]
        public void TestIdempotency()
        {
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Servers = startUrls;

            var sp = new ServerPool();
            sp.Setup(opts);

            var poolUrls = sp.GetServerList(false);
            Assert.True(poolUrls.Length == startUrls.Length);

            sp.Add(startUrls, true);
            Assert.True(poolUrls.Length == startUrls.Length);
        }

        [Fact]
        public void TestNoRandomization()
        {
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Servers = startUrls;
            opts.NoRandomize = true;

            for (int i = 0; i < 10; i++)
            {
                var sp = new ServerPool();
                sp.Setup(opts);

                var poolUrls = sp.GetServerList(false);
                Assert.True(poolUrls.Length == startUrls.Length);
                Assert.True(poolUrls.SequenceEqual(startUrls));
            }

            for (int i = 0; i < 10; i++)
            {
                var sp = new ServerPool();
                sp.Setup(opts);

                var poolUrls = sp.GetServerList(false);
                Assert.True(poolUrls.Length == startUrls.Length);
                Assert.True(poolUrls.SequenceEqual(startUrls));

                string[] impUrls = {
                    "nats://impA:4222", "nats://impB:4222", "nats://impC:4222", "nats://impD:4222",
                    "nats://impE:4222", "nats://impF:4222", "nats://impG:4222", "nats://impH:4222",
                };
                sp.Add(impUrls, true);
                Assert.True(poolUrls.SequenceEqual(startUrls));
            }
        }

        [Fact]
        public void TestImplicitRandomization()
        {
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = null;
            opts.Servers = startUrls;

            var sp = new ServerPool();
            sp.Setup(opts);

            string[] impUrls = {
                "nats://impA:4222", "nats://impB:4222", "nats://impC:4222", "nats://impD:4222",
                "nats://impE:4222", "nats://impF:4222", "nats://impG:4222", "nats://impH:4222",
            };
            sp.Add(impUrls, true);

            var poolUrls = sp.GetServerList(false);

            // Ensure length is OK and that we have randomized the list
            Assert.True(poolUrls.Length == startUrls.Length + impUrls.Length);
            Assert.False(poolUrls.SequenceEqual(startUrls));

            // Ensure implicit urls aren't placed at the end of the list.
            int i;
            for (i = 0; i < startUrls.Length; i++)
            {
                if (poolUrls[i].Contains("imp"))
                    break;
            }
            Assert.True(i != startUrls.Length);
        }
    }
}