// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using NATS.Client;
using System.Threading;
using Xunit;
using System.Diagnostics;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestConnection : IDisposable
    {
        UnitTestUtilities utils = new UnitTestUtilities();
        
        public TestConnection()
        {
            UnitTestUtilities.CleanupExistingServers();
            utils.StartDefaultServer();
        }
        
        public void Dispose()
        {
            utils.StopDefaultServer();
        }
        
        [Fact]
        public void TestConnectionStatus()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            Assert.Equal(ConnState.CONNECTED, c.State);
            c.Close();
            Assert.Equal(ConnState.CLOSED, c.State);
        }

        [Fact]
        public void TestCloseHandler()
        {
            bool closed = false;

            Options o = ConnectionFactory.GetDefaultOptions();
            o.ClosedEventHandler += (sender, args) => { 
                closed = true; };
            IConnection c = new ConnectionFactory().CreateConnection(o);
            c.Close();
            Thread.Sleep(1000);
            Assert.True(closed);

            // now test using.
            closed = false;
            using (c = new ConnectionFactory().CreateConnection(o)) { };
            Thread.Sleep(1000);
            Assert.True(closed);
        }

        [Fact]
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
            Assert.True(disconnected);

            // now test using.
            disconnected = false;
            lock (mu)
            {
                using (c = new ConnectionFactory().CreateConnection(o)) { };
                Monitor.Wait(mu, 20000);
            }
            Assert.True(disconnected);
        }

        [Fact]
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
            Assert.True(disconnected);
        }

        [Fact]
        public void TestClosedConnections()
        {
            IConnection c = new ConnectionFactory().CreateConnection();
            ISyncSubscription s = c.SubscribeSync("foo");

            c.Close();

            // While we can annotate all the exceptions in the test framework,
            // just do it manually.
            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.Publish("foo", null));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.Publish(new Msg("foo")));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.SubscribeAsync("foo"));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.SubscribeSync("foo"));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.SubscribeAsync("foo", "bar"));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.SubscribeSync("foo", "bar"));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => c.Request("foo", null));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => s.NextMessage());

            Assert.ThrowsAny<NATSConnectionClosedException>(() => s.NextMessage(100));

            Assert.ThrowsAny<NATSConnectionClosedException>(() => s.Unsubscribe());

            Assert.ThrowsAny<NATSConnectionClosedException>(() => s.AutoUnsubscribe(1));
        }

        [Fact]
        public void TestConnectVerbose()
        {

            Options o = ConnectionFactory.GetDefaultOptions();
            o.Verbose = true;

            IConnection c = new ConnectionFactory().CreateConnection(o);
            c.Close();
        }

        //[Fact]
        // This test works locally, but fails in AppVeyor some of the time
        // TODO:  Work to identify why this happens...
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

            using (NATSServer s = utils.CreateServerWithConfig("auth_1222.conf"))
            {
                Options o = ConnectionFactory.GetDefaultOptions();

                o.DisconnectedEventHandler += (sender, args) =>
                {
                    Thread.Sleep(100);
                    if (firstDisconnect)
                    {
                        firstDisconnect = false;
                        dtime1 = DateTime.Now.Ticks;
                    }
                    else
                    {
                        dtime2 = DateTime.Now.Ticks;
                    }
                };

                o.ReconnectedEventHandler += (sender, args) =>
                {
                    Thread.Sleep(100);
                    rtime = DateTime.Now.Ticks;
                    reconnected.notify();
                };

                o.AsyncErrorEventHandler += (sender, args) =>
                {
                    if (args.Subscription.Subject.Equals("foo"))
                    {
                        Thread.Sleep(200);
                        atime1 = DateTime.Now.Ticks;
                        asyncErr1.notify();
                    }
                    else
                    {
                        atime2 = DateTime.Now.Ticks;
                        asyncErr2.notify();
                    }
                };

                o.ClosedEventHandler += (sender, args) =>
                {
                    ctime = DateTime.Now.Ticks;
                    closed.notify();
                };

                o.ReconnectWait = 50;
                o.NoRandomize = true;
                o.Servers = new string[] { "nats://localhost:4222", "nats://localhost:1222" };
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
                    Console.WriteLine("Error = callback didn't fire: {0}\n{1}\n{2}\n{3}\n{4}\n{5}\n",
                        dtime1, dtime2, rtime, atime1, atime2, ctime);
                    throw new Exception("Callback didn't fire.");
                }

                if (rtime < dtime1 || dtime2 < rtime || atime2 < atime1|| ctime < atime2) 
                {
                    Console.WriteLine("Wrong callback order:{0}\n{1}\n{2}\n{3}\n{4}\n{5}\n",
                        dtime1, rtime, atime1, atime2, dtime2, ctime);
                    throw new Exception("Invalid callback order.");
 	            }
            }
        }

        [Fact]
        public void TestConnectionMemoryLeak()
        {
            ConnectionFactory cf = new ConnectionFactory();
            var sw = Stopwatch.StartNew();

            GC.Collect();

            long memStart = Process.GetCurrentProcess().PrivateMemorySize64;

            while (sw.ElapsedMilliseconds < 10000)
            {
                using (IConnection conn = cf.CreateConnection()) { }
            }

            // allow the last dispose to finish.
            Thread.Sleep(500);

            GC.Collect();

            double memGrowthPercent = 100 * (
                ((double)(Process.GetCurrentProcess().PrivateMemorySize64 - memStart))
                    / (double)memStart);

            Assert.True(memGrowthPercent < 20.0);
        }

        [Fact]
        public void TestConnectionCloseAndDispose()
        {
            // test that dispose code works after a connection
            // has been closed and cleaned up.
            using (var c = new ConnectionFactory().CreateConnection())
            {
                c.Close();
                Thread.Sleep(500);
            }

            // attempt to test that dispose works while the connection close
            // has passed off work to cleanup the callback scheduler, etc.
            using (var c = new ConnectionFactory().CreateConnection())
            {
                c.Close();
                Thread.Sleep(500);
            }

            // Check that dispose is idempotent.
            using (var c = new ConnectionFactory().CreateConnection())
            {
                c.Dispose();
            }
        }

        [Fact]
        public void TestUserPassTokenOptions()
        {
            using (new NATSServer("-p 4444 --auth foo"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();

                opts.Url = "nats://localhost:4444";
                opts.Token = "foo";
                var c = new ConnectionFactory().CreateConnection(opts);
                c.Close();

                opts.Token = "garbage";
                Assert.Throws<NATSConnectionException>(() => { new ConnectionFactory().CreateConnection(opts); });
            }

            using (new NATSServer("-p 4444 --user foo --pass b@r"))
            {
                Options opts = ConnectionFactory.GetDefaultOptions();

                opts.Url = "nats://localhost:4444";
                opts.User = "foo";
                opts.Password = "b@r";
                var c = new ConnectionFactory().CreateConnection(opts);
                c.Close();

                opts.Password = "garbage";
                Assert.Throws<NATSConnectionException>(() => { new ConnectionFactory().CreateConnection(opts); });

                opts.User = "baz";
                opts.Password = "bar";
                Assert.Throws<NATSConnectionException>(() => { new ConnectionFactory().CreateConnection(opts); });
            }
        }

        /// NOT IMPLEMENTED:
        /// TestServerSecureConnections
        /// TestErrOnConnectAndDeadlock
        /// TestErrOnMaxPayloadLimit

    } // class

} // namespace
