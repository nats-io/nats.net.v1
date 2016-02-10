// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;
using System.Threading;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
    public class TestConnection
    {

        UnitTestUtilities utils = new UnitTestUtilities();

        private TestContext testContextInstance;
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        [TestInitialize()]
        public void Initialize()
        {
            UnitTestUtilities.CleanupExistingServers();
            utils.StartDefaultServer();
        }

        [TestCleanup()]
        public void Cleanup()
        {
            utils.StopDefaultServer();
        }

        [TestMethod]
        public void TestConnectionStatus()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            Assert.AreEqual(ConnState.CONNECTED, c.State);
            c.Close();
            Assert.AreEqual(ConnState.CLOSED, c.State);
        }

        [TestMethod]
        public void TestCloseHandler()
        {
            bool closed = false;

            Options o = ConnectionFactory.GetDefaultOptions();
            o.ClosedEventHandler += (sender, args) => { 
                closed = true; };
            IConnection c = new ConnectionFactory().CreateConnection(o);
            c.Close();
            Thread.Sleep(1000);
            Assert.IsTrue(closed);

            // now test using.
            closed = false;
            using (c = new ConnectionFactory().CreateConnection(o)) { };
            Thread.Sleep(1000);
            Assert.IsTrue(closed);
        }

        [TestMethod]
        public void TestCloseDisconnectedHandler()
        {
            bool disconnected = false;
            Object mu = new Object();

            Options o = ConnectionFactory.GetDefaultOptions();
            o.AllowReconnect = false;
            o.DisconnectedEventHandler += (sender, args) => {
                lock (mu)
                {
                    disconnected = true;
                    Monitor.Pulse(mu);
                }
            };

            IConnection c = new ConnectionFactory().CreateConnection(o);
            lock (mu)
            {
                c.Close();
                Monitor.Wait(mu, 20000);
            }
            Assert.IsTrue(disconnected);

            // now test using.
            disconnected = false;
            lock (mu)
            {
                using (c = new ConnectionFactory().CreateConnection(o)) { };
                Monitor.Wait(mu, 20000);
            }
            Assert.IsTrue(disconnected);
        }

        [TestMethod]
        public void TestServerStopDisconnectedHandler()
        {
            bool disconnected = false;
            Object mu = new Object();

            Options o = ConnectionFactory.GetDefaultOptions();
            o.AllowReconnect = false;
            o.DisconnectedEventHandler += (sender, args) =>
            {
                lock (mu)
                {
                    disconnected = true;
                    Monitor.Pulse(mu);
                }
            };

            IConnection c = new ConnectionFactory().CreateConnection(o);
            lock (mu)
            {
                utils.bounceDefaultServer(1000);
                Monitor.Wait(mu, 10000);
            }
            c.Close();
            Assert.IsTrue(disconnected);
        }

        [TestMethod]
        public void TestClosedConnections()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            ISyncSubscription s = c.SubscribeSync("foo");

            c.Close();

            // While we can annotate all the exceptions in the test framework,
            // just do it manually.
            UnitTestUtilities.testExpectedException(
                () => { c.Publish("foo", null); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.Publish(new Msg("foo")); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeAsync("foo"); },
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeSync("foo"); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeAsync("foo", "bar"); },
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.SubscribeSync("foo", "bar"); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { c.Request("foo", null); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.NextMessage(); },
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.NextMessage(100); },
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.Unsubscribe(); }, 
                typeof(NATSConnectionClosedException));

            UnitTestUtilities.testExpectedException(
                () => { s.AutoUnsubscribe(1); }, 
                typeof(NATSConnectionClosedException));
        }

        [TestMethod]
        public void TestConnectVerbose()
        {

            Options o = ConnectionFactory.GetDefaultOptions();
            o.Verbose = true;

            IConnection c = new ConnectionFactory().CreateConnection(o);
            c.Close();
        }

        internal class ConditionalObj
        {
            Object objLock = new Object();
            bool   completed = false;

            internal void wait(int timeout)
            {
                lock (objLock)
                {
                    if (completed)
                        return;
                    
                    Assert.IsTrue(Monitor.Wait(objLock, timeout));
                }
            }

            internal void reset()
            {
                lock (objLock)
                {
                    completed = false;
                }
            }

            internal void notify()
            {
                lock (objLock)
                {
                    completed = true;
                    Monitor.Pulse(objLock);
                }
            }
        }

        [TestMethod]
        public void TestCallbacksOrder()
        {
            bool firstDisconnect = true;

            long orig = DateTime.Now.Ticks;

            long dtime1 = orig;
            long dtime2 = orig;
            long rtime = orig;
            long atime1 = orig;
            long atime2 = orig;
            long ctime = orig;

            ConditionalObj reconnected = new ConditionalObj();
            ConditionalObj closed      = new ConditionalObj();
            ConditionalObj asyncErr1   = new ConditionalObj();
            ConditionalObj asyncErr2   = new ConditionalObj();
            ConditionalObj recvCh      = new ConditionalObj();
            ConditionalObj recvCh1     = new ConditionalObj();
            ConditionalObj recvCh2     = new ConditionalObj();

            using (NATSServer s = utils.CreateServerWithConfig(TestContext, "auth_1222.conf"))
            {
                Options o = ConnectionFactory.GetDefaultOptions();

                o.DisconnectedEventHandler += (sender, args) =>
                {
                    Thread.Sleep(100);
                    if (firstDisconnect)
                    {
                        System.Console.WriteLine("First disconnect.");
                        firstDisconnect = false;
                        dtime1 = DateTime.Now.Ticks;
                    }
                    else
                    {
                        System.Console.WriteLine("Second disconnect.");
                        dtime2 = DateTime.Now.Ticks;
                    }
                };

                o.ReconnectedEventHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Reconnected.");
                    Thread.Sleep(50);
                    rtime = DateTime.Now.Ticks;
                    reconnected.notify();
                };

                o.AsyncErrorEventHandler += (sender, args) =>
                {
                    if (args.Subscription.Subject.Equals("foo"))
                    {
                        System.Console.WriteLine("Error handler foo.");
                        Thread.Sleep(200);
                        atime1 = DateTime.Now.Ticks;
                        asyncErr1.notify();
                    }
                    else
                    {
                        System.Console.WriteLine("Error handler bar.");
                        atime2 = DateTime.Now.Ticks;
                        asyncErr2.notify();
                    }
                };

                o.ClosedEventHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Closed handler.");
                    ctime = DateTime.Now.Ticks;
                    closed.notify();
                };

                o.ReconnectWait = 50;
                o.NoRandomize = true;
                o.Servers = new string[] { "nats://localhost:4222", "nats:localhost:1222" };
                o.SubChannelLength = 1;

                using (IConnection nc = new ConnectionFactory().CreateConnection(o),
                       ncp = new ConnectionFactory().CreateConnection())
                {
                    utils.StopDefaultServer();

                    Thread.Sleep(1000);

                    utils.StartDefaultServer();

                    reconnected.wait(3000);

                    EventHandler<MsgHandlerEventArgs> eh = (sender, args) =>
                    {
                        System.Console.WriteLine("Received message on subject: " + args.Message.Subject);
                        recvCh.notify();
                        if (args.Message.Subject.Equals("foo"))
                        {
                            recvCh1.notify();
                        }
                        else
                        { 
                            recvCh2.notify();
                        }
                    };

                    IAsyncSubscription sub1 = nc.SubscribeAsync("foo", eh);
                    IAsyncSubscription sub2 = nc.SubscribeAsync("bar", eh);

                    nc.Flush();

                    ncp.Publish("foo", System.Text.Encoding.UTF8.GetBytes("hello"));
                    ncp.Publish("bar", System.Text.Encoding.UTF8.GetBytes("hello"));
                    ncp.Flush();

                    recvCh.wait(3000);

                    for (int i = 0; i < 3; i++)
                    {
                        ncp.Publish("foo", System.Text.Encoding.UTF8.GetBytes("hello"));
                        ncp.Publish("bar", System.Text.Encoding.UTF8.GetBytes("hello"));
                    }

                    ncp.Flush();

                    asyncErr1.wait(3000);
                    asyncErr2.wait(3000);

                    utils.StopDefaultServer();

                    Thread.Sleep(1000);
                    closed.reset();
                    nc.Close();

                    closed.wait(3000);
                }


                if (dtime1 == orig || dtime2 == orig || rtime == orig || 
                    atime1 == orig || atime2 == orig || ctime == orig)
                {
                    System.Console.WriteLine("Error = callback didn't fire: {0}\n{1}\n{2}\n{3}\n{4}\n{5}\n",
                        dtime1, dtime2, rtime, atime1, atime2, ctime);
                    throw new Exception("Callback didn't fire.");
                }

                if (rtime < dtime1 || dtime2 < rtime || atime2 < atime1|| ctime < atime2) 
                {
                    System.Console.WriteLine("Wrong callback order:{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n",
                        dtime1, rtime, atime1, atime2, dtime2, ctime);
                    throw new Exception("Invalid callback order.");
 	            }
            }
        }
        
        /// NOT IMPLEMENTED:
        /// TestServerSecureConnections
        /// TestErrOnConnectAndDeadlock
        /// TestErrOnMaxPayloadLimit

    } // class

} // namespace
