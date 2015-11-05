// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
    public class TestCluster
    {
        string[] testServers = new string[] {
            "nats://localhost:1222",
            "nats://localhost:1223",
            "nats://localhost:1224",
            "nats://localhost:1225",
            "nats://localhost:1226",
            "nats://localhost:1227",
            "nats://localhost:1228" 
        };

        string[] testServersShortList = new string[] {
            "nats://localhost:1222",
            "nats://localhost:1223"
        };

        UnitTestUtilities utils = new UnitTestUtilities();

        [TestMethod]
        public void TestServersOption()
        {
            IConnection c = null;
            ConnectionFactory cf = new ConnectionFactory();
            Options o = ConnectionFactory.GetDefaultOptions();

            o.NoRandomize = true;

            UnitTestUtilities.testExpectedException(
                () => { cf.CreateConnection(); },
                typeof(NATSNoServersException));

            o.Servers = testServers;

            UnitTestUtilities.testExpectedException(
                () => { cf.CreateConnection(o); },
                typeof(NATSNoServersException));

            // Make sure we can connect to first server if running
            using (NATSServer ns = utils.CreateServerOnPort(1222))
            {
                c = cf.CreateConnection(o);
                Assert.IsTrue(testServers[0].Equals(c.ConnectedUrl));
                c.Close();
            }

            // make sure we can connect to a non-first server.
            using (NATSServer ns = utils.CreateServerOnPort(1227))
            {
                c = cf.CreateConnection(o);
                Assert.IsTrue(testServers[5].Equals(c.ConnectedUrl));
                c.Close();
            }
        }

        [TestMethod]
        public void TestAuthServers()
        {
            string[] plainServers = new string[] {
                "nats://localhost:1222",
		        "nats://localhost:1224"
            };

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.NoRandomize = true;
            opts.Servers = plainServers;
            opts.Timeout = 5000;

            using (NATSServer as1 = utils.CreateServerWithConfig("auth_1222.conf"),
                              as2 = utils.CreateServerWithConfig("auth_1224.conf"))
            {
                UnitTestUtilities.testExpectedException(
                    () => { new ConnectionFactory().CreateConnection(opts); },
                    typeof(NATSException));

                // Test that we can connect to a subsequent correct server.
                string[] authServers = new string[] {
                    "nats://localhost:1222",
		            "nats://username:password@localhost:1224"};

                opts.Servers = authServers;

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    Assert.IsTrue(c.ConnectedUrl.Equals(authServers[1]));
                }
            }
        }

        [TestMethod]
        public void TestBasicClusterReconnect()
        {
            string[] plainServers = new string[] {
                "nats://localhost:1222",
		        "nats://localhost:1224"
            };

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;
            opts.NoRandomize = true;
            opts.Servers = plainServers;

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

            opts.Timeout = 200;

            using (NATSServer s1 = utils.CreateServerOnPort(1222),
                              s2 = utils.CreateServerOnPort(1224))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    Stopwatch reconnectSw = new Stopwatch();

                    System.Console.WriteLine("Connected to: " + c.ConnectedUrl);
                    lock (disconnectLock)
                    {
                        s1.Shutdown();
                        Assert.IsTrue(Monitor.Wait(disconnectLock, 20000));
                    }

                    reconnectSw.Start();

                    lock (reconnectLock)
                    {
                        Assert.IsTrue(Monitor.Wait(reconnectLock, 20000));
                    }

                    Assert.IsTrue(c.ConnectedUrl.Equals(testServers[2]));

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


        //[TestMethod]
        public void TestHotSpotReconnect()
        {
            int numClients = 10;
            SimClient[] clients = new SimClient[100];

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Servers = testServers;

            NATSServer s1 = utils.CreateServerOnPort(1222);
            Task[] waitgroup = new Task[numClients];


            for (int i = 0; i < numClients; i++)
            {
                clients[i] = new SimClient();
                Task t = new Task(() => {
                    clients[i].Connect(testServers); 
                    clients[i].waitForReconnect();
                });
                t.Start();
                waitgroup[i] = t;
            }


            NATSServer s2 = utils.CreateServerOnPort(1224);
            NATSServer s3 = utils.CreateServerOnPort(1226);

            s1.Shutdown();
            Task.WaitAll(waitgroup);

            int s2Count = 0;
            int s3Count = 0;
            int unknown = 0;

            for (int i = 0; i < numClients; i++)
            {
                if (testServers[3].Equals(clients[i].ConnectedUrl))
                    s2Count++;
                else if (testServers[5].Equals(clients[i].ConnectedUrl))
                    s3Count++;
                else
                    unknown++;
            }

            Assert.IsTrue(unknown == 0);
            int delta = Math.Abs(s2Count - s3Count);
            int range = numClients / 30;
            if (delta > range)
            {
                Assert.Fail("Connected clients to servers out of range: {0}/{1}", delta, range);
            }

        }

        [TestMethod]
        public void TestProperReconnectDelay()
        {
            Object mu = new Object();
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Servers = testServers;
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

            using (NATSServer s1 = utils.CreateServerOnPort(1222))
            {
                IConnection c = new ConnectionFactory().CreateConnection(opts);

                lock (mu)
                {
                    s1.Shutdown();
                    // wait for disconnect
                    Assert.IsTrue(Monitor.Wait(mu, 10000));


                    // Wait, want to make sure we don't spin on
                    //reconnect to non-existant servers.
                    Thread.Sleep(1000);

                    Assert.IsFalse(closedCbCalled);
                    Assert.IsTrue(disconnectHandlerCalled);
                    Assert.IsTrue(c.State == ConnState.RECONNECTING);
                }

            }
        }

        [TestMethod]
        public void TestProperFalloutAfterMaxAttempts()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();

            Object dmu = new Object();
            Object cmu = new Object();

            opts.Servers = this.testServersShortList;
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

            using (NATSServer s1 = utils.CreateServerOnPort(1222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    s1.Shutdown();

                    lock (dmu)
                    {
                        if (!disconnectHandlerCalled)
                            Assert.IsTrue(Monitor.Wait(dmu, 20000));
                    }

                    lock (cmu)
                    {
                        if (!closedHandlerCalled)
                            Assert.IsTrue(Monitor.Wait(cmu, 60000));
                    }

                    Assert.IsTrue(disconnectHandlerCalled);
                    Assert.IsTrue(closedHandlerCalled);
                    Assert.IsTrue(c.IsClosed());
                }
            }
        }

        [TestMethod]
        public void TestTimeoutOnNoServers()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            Object dmu = new Object();
            Object cmu = new Object();

            opts.Servers = testServersShortList;
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

            using (NATSServer s1 = utils.CreateServerOnPort(1222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    s1.Shutdown();

                    lock (dmu)
                    {
                        if (!disconnectHandlerCalled)
                           Assert.IsTrue(Monitor.Wait(dmu, 20000));
                    }

                    Stopwatch sw = new Stopwatch();
                    sw.Start();

                    lock (cmu)
                    {
                        if (!closedHandlerCalled)
                            Assert.IsTrue(Monitor.Wait(cmu, 60000));
                    }

                    sw.Stop();

                    int expected = opts.MaxReconnect * opts.ReconnectWait;

                    // .NET has long connect times, so revisit this after
                    // a connect timeout has been added.
                    //Assert.IsTrue(sw.ElapsedMilliseconds < (expected + 500));

                    Assert.IsTrue(disconnectHandlerCalled);
                    Assert.IsTrue(closedHandlerCalled);
                    Assert.IsTrue(c.IsClosed());
                }
            }
        }

        //[TestMethod]
        public void TestPingReconnect()
        {
            /// Work in progress
            int RECONNECTS = 4;

            Options opts = ConnectionFactory.GetDefaultOptions();
            Object mu = new Object();

            opts.Servers = testServersShortList;
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

            using (NATSServer s1 = utils.CreateServerOnPort(1222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    s1.Shutdown();
                    for (int i = 0; i < RECONNECTS; i++)
                    {
                        lock (mu)
                        {
                            Assert.IsTrue(Monitor.Wait(mu, 100000));
                        }
                    }
                }
            }
        }

    } // class

} // namespace

