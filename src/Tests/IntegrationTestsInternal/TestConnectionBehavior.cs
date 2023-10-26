// Copyright 2022-2023 The NATS Authors
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
using IntegrationTests;
using NATS.Client;
using NATS.Client.Internals;
using Xunit;

namespace IntegrationTestsInternal
{
    public class TestConnectionBehavior : TestSuite<ConnectionBehaviorSuite>
    {
        public TestConnectionBehavior(ConnectionBehaviorSuite context) : base(context) {}

        [Fact]
        public void TestReconnectOnConnect()
        {
            _testReconnectOnConnect(true);
        }

        [Fact]
        public void TestNoReconnectOnConnect()
        {
            _testReconnectOnConnect(false);
        }

        private void _testReconnectOnConnect(bool reconnectOnConnect)
        {
            InterlockedInt disconnects = new InterlockedInt();
            InterlockedInt reconnects = new InterlockedInt();

            using (NATSServer s1 = NATSServer.CreateWithConfig(Context.Server1.Port, "auth.conf"),
                   _ = NATSServer.CreateWithConfig(Context.Server2.Port, "auth.conf"))
            {
                Options opts = Context.GetTestOptions();
                opts.Servers = new[]
                {
                    $"nats://username:passwordx@localhost:{Context.Server1.Port}",
                    $"nats://username:passwordx@localhost:{Context.Server2.Port}"
                };
                TestServerPool pool = new TestServerPool();
                opts.ServerProvider = pool;
                opts.NoRandomize = true;
                opts.MaxReconnect = 2;
                opts.DisconnectedEventHandler += (sender, e) => disconnects.Increment();
                opts.ReconnectedEventHandler += (sender, e) => reconnects.Increment();

                try
                {
                    using (IConnection c = Context.ConnectionFactory.CreateConnection(opts, reconnectOnConnect))
                    {
                        c.Close();
                    }
                }
                catch (Exception e)
                {
                    if (reconnectOnConnect)
                    {
                        Assert.False(true, $"Exception not expected {e.Message}");
                    }
                }

                Thread.Sleep(100); // ensure events get handled
                if (reconnectOnConnect)
                {
                    Assert.Equal(2, disconnects.Read());
                    Assert.Equal(1, reconnects.Read());
                    Assert.Equal(1, pool.setupCallCount);
                    Assert.Equal(3, pool.selectNextServerCallCount);
                }
                else
                {
                    Assert.Equal(0, disconnects.Read());
                    Assert.Equal(0, reconnects.Read());
                    Assert.Equal(1, pool.setupCallCount);
                    Assert.Equal(0, pool.selectNextServerCallCount);
                }
            }
        }
    }

    internal class TestServerPool : ServerPool
    {
        internal int setupCallCount = 0;
        internal int selectNextServerCallCount = 0;

        public LinkedList<Srv> SrvList => sList;
        
        public override void Setup(Options opts)
        {
            setupCallCount++;
            base.Setup(opts);
        }

        public override Srv SelectNextServer(int maxReconnect)
        {
            if (++selectNextServerCallCount == 3)
            {
                foreach (Srv srv in SrvList)
                {
                    srv.SetUrl(srv.Url.ToString().Replace("passwordx", "password"));
                }
            }
            return base.SelectNextServer(maxReconnect);
        }
    }
}
