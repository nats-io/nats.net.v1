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
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Linq;
using Xunit;

namespace IntegrationTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestCluster : TestSuite<ClusterSuiteContext>
    {
        public TestCluster(ClusterSuiteContext context) : base(context) { }

        [Fact]
        public void TestServersOption()
        {
            IConnection c = null;
            ConnectionFactory cf = Context.ConnectionFactory;
            Options o = Context.GetTestOptions();
            o.NoRandomize = true;
            o.Servers = Context.GetTestServersUrls();

            Assert.ThrowsAny<NATSNoServersException>(() => cf.CreateConnection(o));
            
            // Make sure we can connect to first server if running
            using (NATSServer ns = NATSServer.Create(Context.Server1.Port))
            {
                c = cf.CreateConnection(o);
                Assert.Equal(Context.Server1.Url, c.ConnectedUrl);
                c.Close();
            }

            // make sure we can connect to a non-first server.
            using (NATSServer ns = NATSServer.Create(Context.Server6.Port))
            {
                c = cf.CreateConnection(o);
                Assert.Equal(Context.Server6.Url, c.ConnectedUrl);
                c.Close();
            }
        }

        [Fact]
        public void TestAuthServers()
        {
            Options opts = Context.GetTestOptions();
            opts.NoRandomize = true;
            opts.Servers = new [] { Context.Server1.Url, Context.Server3.Url };
            opts.Timeout = 5000;

            using (NATSServer as1 = NATSServer.CreateWithConfig(Context.Server1.Port, "auth.conf"),
                              as2 = NATSServer.CreateWithConfig(Context.Server3.Port, "auth.conf"))
            {
                Assert.ThrowsAny<NATSException>(() => Context.ConnectionFactory.CreateConnection(opts));

                // Test that we can connect to a subsequent correct server.
                var authServers = new[] {
                    Context.Server1.Url,
		            $"nats://username:password@localhost:{Context.Server3.Port}"};

                opts.Servers = authServers;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.Equal(authServers[1], c.ConnectedUrl);
                }
            }
        }

        [Fact]
        public void TestBasicClusterReconnect()
        {
            Options opts = Context.GetTestOptions();
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;
            opts.NoRandomize = true;
            opts.Servers = new [] { Context.Server1.Url, Context.Server3.Url };

            Object disconnectLock = new Object();
            opts.DisconnectedEventHandler += (sender, args) =>
            {
                // Suppress any additional calls
                opts.DisconnectedEventHandler = null;
                lock (disconnectLock)
                {
                    Monitor.Pulse(disconnectLock);
                }
            };

            Object reconnectLock = new Object();

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                // Suppress any additional calls
                lock (reconnectLock)
                {
                    Monitor.Pulse(reconnectLock);
                }
            };

            opts.Timeout = 1000;

            using (NATSServer s1 = NATSServer.Create(Context.Server1.Port),
                              s2 = NATSServer.Create(Context.Server3.Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Stopwatch reconnectSw = new Stopwatch();

                    lock (disconnectLock)
                    {
                        s1.Shutdown();
                        Assert.True(Monitor.Wait(disconnectLock, 20000));
                    }

                    reconnectSw.Start();

                    lock (reconnectLock)
                    {
                        Assert.True(Monitor.Wait(reconnectLock, 20000));
                    }

                    Assert.Equal(c.ConnectedUrl, Context.Server3.Url);

                    reconnectSw.Stop();

                    // Make sure we did not wait on reconnect for default time.
                    // Reconnect should be fast since it will be a switch to the
                    // second server and not be dependent on server restart time.
                    // TODO:  .NET connect timeout is exceeding long compared to
                    // GO's.  Look shortening it, or living with it.
                    //if (reconnectSw.ElapsedMilliseconds > opts.ReconnectWait)
                    //{
                    //   Assert.Fail("Reconnect time took to long: {0} millis.",
                    //        reconnectSw.ElapsedMilliseconds);
                    //}
                }
            }
        }

        [Fact]
        public void TestServerDiscoveredHandler()
        {
            IConnection c = null;
            ConnectionFactory cf = Context.ConnectionFactory;
            Options o = Context.GetTestOptions();

            o.NoRandomize = true;
            o.Servers = Context.GetTestServersUrls();

            bool serverDiscoveredCalled = false;
            o.ServerDiscoveredEventHandler += (sender, e) =>
            {
                serverDiscoveredCalled = true;
            };

            string seedServerArgs = $@"-p {Context.Server1.Port} -cluster {Context.ClusterServer1.Url}";
            string secondClusterMemberArgs = $@"-p {Context.Server2.Port} -cluster {Context.ClusterServer2.Url} -routes {Context.ClusterServer1.Url}";

            // create the seed server for a cluster...
            using (NATSServer ns1 = NATSServer.Create(seedServerArgs))
            {
                // ...then connect to it...
                using (c = cf.CreateConnection(o))
                {
                    Assert.Equal(Context.Server1.Url,c.ConnectedUrl);

                    // ...then while connected, start up a second server...
                    using (NATSServer ns2 = NATSServer.Create(secondClusterMemberArgs))
                    {
                        // ...waiting up to 30 seconds for the second server to start...
                        for (int ii = 0; ii < 6; ii++)
                        {
                            Thread.Sleep(5000);

                            // ...taking an early out if we detected the startup...
                            if (serverDiscoveredCalled)
                                break;
                        }

                        // ...and by then we should have received notification of
                        // its awakening.
                        Assert.True(serverDiscoveredCalled);
                    }
                }
            }
        }

        private class SimClient
        {
            IConnection c;
            Object mu = new Object();

            public string ConnectedUrl
            {
                get { return c.ConnectedUrl; }
            }

            public void waitForReconnect()
            {
                lock (mu)
                {
                    Monitor.Wait(mu);
                }
            }

            public void Connect(string[] servers)
            {
                Options opts = ConnectionFactory.GetDefaultOptions();
                opts.Servers = servers;
                c = new ConnectionFactory().CreateConnection(opts);
                opts.ReconnectedEventHandler = (sender, args) =>
                {
                    lock (mu)
                    {
                        Monitor.Pulse(mu);
                    }
                };
            }

            public void close()
            {
                c.Close();
            }
        }


        [Fact(Skip = "WorkInProgress")]
        public void TestHotSpotReconnect()
        {
            int numClients = 10;
            SimClient[] clients = new SimClient[100];

            Options opts = Context.GetTestOptions();
            opts.Servers = Context.GetTestServersUrls();

            NATSServer s1 = NATSServer.Create(Context.Server1.Port);
            Task[] waitgroup = new Task[numClients];


            for (int i = 0; i < numClients; i++)
            {
                clients[i] = new SimClient();
                waitgroup[i] = Task.Run(() => {
                    clients[i].Connect(Context.GetTestServersUrls());
                    clients[i].waitForReconnect();
                });
            }


            NATSServer s3 = NATSServer.Create(Context.Server3.Port);
            NATSServer s5 = NATSServer.Create(Context.Server5.Port);

            s1.Shutdown();
            Task.WaitAll(waitgroup);

            int s3Count = 0;
            int s5Count = 0;
            int unknown = 0;

            for (int i = 0; i < numClients; i++)
            {
                if (Context.Server3.Url.Equals(clients[i].ConnectedUrl))
                    s3Count++;
                else if (Context.Server5.Url.Equals(clients[i].ConnectedUrl))
                    s5Count++;
                else
                    unknown++;
            }

            Assert.True(unknown == 0);
            int delta = Math.Abs(s3Count - s5Count);
            int range = numClients / 30;

            Assert.False(delta > range, string.Format("Connected clients to servers out of range: {0}/{0}", delta, range));

        }

        [Fact]
        public void TestProperReconnectDelay()
        {
            Object mu = new Object();
            Options opts = Context.GetTestOptions();
            opts.Servers = Context.GetTestServersUrls();
            opts.NoRandomize = true;

            bool disconnectHandlerCalled = false;
            opts.DisconnectedEventHandler = (sender, args) =>
            {
                opts.DisconnectedEventHandler = null;
                disconnectHandlerCalled = true;
                lock (mu)
                {
                    disconnectHandlerCalled = true;
                    Monitor.Pulse(mu);
                }
            };

            bool closedCbCalled = false;
            opts.ClosedEventHandler = (sender, args) =>
            {
                closedCbCalled = true;
            };

            using (NATSServer s1 = NATSServer.Create(Context.Server1.Port))
            {
                IConnection c = Context.ConnectionFactory.CreateConnection(opts);

                lock (mu)
                {
                    s1.Shutdown();
                    // wait for disconnect
                    Assert.True(Monitor.Wait(mu, 10000));


                    // Wait, want to make sure we don't spin on
                    //reconnect to non-existant servers.
                    Thread.Sleep(1000);

                    Assert.False(closedCbCalled);
                    Assert.True(disconnectHandlerCalled);
                    Assert.True(c.State == ConnState.RECONNECTING);
                }

            }
        }

        [Fact]
        public void TestProperFalloutAfterMaxAttempts()
        {
            Options opts = Context.GetTestOptions();

            Object dmu = new Object();
            Object cmu = new Object();

            opts.Servers = Context.GetTestServersShortListUrls();
            opts.NoRandomize = true;
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 25; // millis
            opts.Timeout = 500;

            bool disconnectHandlerCalled = false;

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                lock (dmu)
                {
                    disconnectHandlerCalled = true;
                    Monitor.Pulse(dmu);
                }
            };

            bool closedHandlerCalled = false;
            opts.ClosedEventHandler = (sender, args) =>
            {
                lock (cmu)
                {
                    closedHandlerCalled = true;
                    Monitor.Pulse(cmu);
                }
            };

            using (NATSServer s1 = NATSServer.Create(Context.TestServersShortList[0].Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    s1.Shutdown();

                    lock (dmu)
                    {
                        if (!disconnectHandlerCalled)
                            Assert.True(Monitor.Wait(dmu, 20000));
                    }

                    lock (cmu)
                    {
                        if (!closedHandlerCalled)
                            Assert.True(Monitor.Wait(cmu, 60000));
                    }

                    Assert.True(disconnectHandlerCalled);
                    Assert.True(closedHandlerCalled);
                    Assert.True(c.IsClosed());
                }
            }
        }

        [Fact]
        public void TestProperFalloutAfterMaxAttemptsWithAuthMismatch()
        {
            Options opts = Context.GetTestOptions();

            Object dmu = new Object();
            Object cmu = new Object();

            opts.Servers = new [] {
                Context.Server8.Url,
                Context.Server1.Url
            };

            opts.NoRandomize = true;
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 25; // millis
            opts.Timeout = 1000;

            bool disconnectHandlerCalled = false;

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                lock (dmu)
                {
                    disconnectHandlerCalled = true;
                    Monitor.Pulse(dmu);
                }
            };

            bool closedHandlerCalled = false;
            opts.ClosedEventHandler = (sender, args) =>
            {
                lock (cmu)
                {
                    closedHandlerCalled = true;
                    Monitor.Pulse(cmu);
                }
            };

            using (NATSServer
                   s1 = NATSServer.Create(Context.Server8.Port),
                   s2 = NATSServer.CreateWithConfig(Context.Server1.Port, "tls_verify.conf"))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    s1.Shutdown();

                    lock (dmu)
                    {
                        if (!disconnectHandlerCalled)
                            Assert.True(Monitor.Wait(dmu, 20000));
                    }

                    lock (cmu)
                    {
                        if (!closedHandlerCalled)
                            Assert.True(Monitor.Wait(cmu, 600000));
                    }

                    Assert.True(c.Stats.Reconnects != opts.MaxReconnect);

                    Assert.True(disconnectHandlerCalled);
                    Assert.True(closedHandlerCalled);
                    Assert.True(c.IsClosed());
                }
            }
        }

        [Fact]
        public void TestTimeoutOnNoServers()
        {
            Options opts = Context.GetTestOptions();
            Object dmu = new Object();
            Object cmu = new Object();

            opts.Servers = Context.GetTestServersShortListUrls();
            opts.NoRandomize = true;
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 100; // millis

            bool disconnectHandlerCalled = false;
            bool closedHandlerCalled = false;

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                lock (dmu)
                {
                    disconnectHandlerCalled = true;
                    Monitor.Pulse(dmu);
                }
            };

            opts.ClosedEventHandler = (sender, args) =>
            {
                lock (cmu)
                {
                    closedHandlerCalled = true;
                    Monitor.Pulse(cmu);
                }
            };

            using (NATSServer s1 = NATSServer.Create(Context.TestServersShortList[0].Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    s1.Shutdown();

                    lock (dmu)
                    {
                        if (!disconnectHandlerCalled)
                           Assert.True(Monitor.Wait(dmu, 20000));
                    }

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    lock (cmu)
                    {
                        if (!closedHandlerCalled)
                            Assert.True(Monitor.Wait(cmu, 60000));
                    }

                    sw.Stop();

                    int expected = opts.MaxReconnect * opts.ReconnectWait;

                    // .NET has long connect times, so revisit this after
                    // a connect timeout has been added.
                    //Assert.IsTrue(sw.ElapsedMilliseconds < (expected + 500));

                    Assert.True(disconnectHandlerCalled);
                    Assert.True(closedHandlerCalled);
                    Assert.True(c.IsClosed());
                }
            }
        }

        [Fact(Skip = "WorkInProgress")]
        private void TestPingReconnect()
        {
            /// Work in progress
            int RECONNECTS = 4;

            Options opts = Context.GetTestOptions();
            Object mu = new Object();

            opts.Servers = Context.GetTestServersShortListUrls();
            opts.NoRandomize = true;
            opts.ReconnectWait = 200;
            opts.PingInterval = 50;
            opts.MaxPingsOut = -1;
            opts.Timeout = 1000;


            Stopwatch disconnectedTimer = new Stopwatch();

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                disconnectedTimer.Reset();
                disconnectedTimer.Start();
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                lock (mu)
                {
                    args.Conn.Opts.MaxPingsOut = 500;
                    disconnectedTimer.Stop();
                    Monitor.Pulse(mu);
                }
            };

            using (NATSServer s1 = NATSServer.Create(Context.TestServersShortList[0].Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    s1.Shutdown();
                    for (int i = 0; i < RECONNECTS; i++)
                    {
                        lock (mu)
                        {
                            Assert.True(Monitor.Wait(mu, 100000));
                        }
                    }
                }
            }
        }

    } // class

} // namespace

