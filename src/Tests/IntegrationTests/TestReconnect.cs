﻿// Copyright 2015-2018 The NATS Authors
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
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;
using Xunit;

namespace IntegrationTests
{
    public class TestReconnect : TestSuite<ReconnectSuiteContext>
    {
        public TestReconnect(ReconnectSuiteContext context) : base(context) { }

        private Options getReconnectOptions()
        {
            Options o = Context.GetTestOptionsWithDefaultTimeout(Context.Server1.Port);
            o.AllowReconnect = true;
            o.MaxReconnect = 10;
            o.ReconnectWait = 100;

            return o;
        }

        [Fact]
        public void TestReconnectDisallowedFlags()
        {
            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.AllowReconnect = false;

            Object testLock = new Object();

            opts.ClosedEventHandler = (sender, args) =>
            {
                lock(testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            using (NATSServer ns = NATSServer.Create(Context.Server1.Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    lock (testLock)
                    {
                        ns.Shutdown();
                        Assert.True(Monitor.Wait(testLock, 1000));
                    }
                }
            }
        }

        [Fact]
        public void TestReconnectAllowedFlags()
        {
            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;

            AutoResetEvent Closed = new AutoResetEvent(false);
            AutoResetEvent Disconnected = new AutoResetEvent(false);

            opts.DisconnectedEventHandler = (sender, args) => Disconnected.Set();
            opts.ClosedEventHandler = (sender, args) => Closed.Set();

            using (NATSServer ns = NATSServer.Create(Context.Server1.Port))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    ns.Shutdown();
                    Assert.True(Disconnected.WaitOne(1000));
                    Assert.False(Closed.WaitOne(1000));
                    Assert.True(c.State == ConnState.RECONNECTING);
                    c.Opts.ClosedEventHandler = null;
                }
            }
        }

        [Fact]
        public void TestBasicReconnectFunctionality()
        {
            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;

            AutoResetEvent Disconnected = new AutoResetEvent(false);
            AutoResetEvent Reconnected = new AutoResetEvent(false);
            AutoResetEvent MessageArrived = new AutoResetEvent(false);

            Object testLock = new Object();
            Object msgLock = new Object();

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                Disconnected.Set();
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                Reconnected.Set();
            };

            using (var ns1 = NATSServer.Create(Context.Server1.Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    using (var s = c.SubscribeAsync("foo"))
                    {
                        s.MessageHandler += (sender, args) =>
                        {
                            MessageArrived.Set();
                        };

                        s.Start();
                        c.Flush();

                        lock (testLock)
                        {
                            ns1.Shutdown();
                            Assert.True(Disconnected.WaitOne(100000));
                        }

                        c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));

                        // restart the server.
                        using (NATSServer.Create(Context.Server1.Port))
                        {
                            Assert.True(Reconnected.WaitOne(20000));
                            Assert.True(c.Stats.Reconnects == 1);

                            c.Flush(5000);

                            Assert.True(MessageArrived.WaitOne(20000));
                        }
                    }
                }
            }
        }

        int received = 0;

        [Fact]
        public void TestExtendedReconnectFunctionality()
        {
            Options opts = getReconnectOptions();

            Object msgLock = new Object();
            AutoResetEvent disconnectedEvent = new AutoResetEvent(false);
            AutoResetEvent reconnectedEvent = new AutoResetEvent(false);

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                disconnectedEvent.Set();
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                reconnectedEvent.Set();
            };

            byte[] payload = Encoding.UTF8.GetBytes("bar");
            using (var ns1 = NATSServer.Create(Context.Server1.Port))
            {
                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    using (var s1 = c.SubscribeAsync("foo"))
                    {
                        s1.MessageHandler += incrReceivedMessageHandler;
                        
                        using (var s2 = c.SubscribeAsync("foobar"))
                        {
                            s2.MessageHandler += incrReceivedMessageHandler;

                            s1.Start();
                            s2.Start();

                            received = 0;

                            c.Publish("foo", payload);
                            c.Flush();

                            ns1.Shutdown();
                            // server is stopped here.

                            Assert.True(disconnectedEvent.WaitOne(20000));

                            // subscribe to bar while connected.
                            using (var s3 = c.SubscribeAsync("bar"))
                            {
                                s3.MessageHandler += incrReceivedMessageHandler;
                                s3.Start();

                                // Unsub foobar while disconnected
                                s2.Unsubscribe();

                                c.Publish("foo", payload);
                                c.Publish("bar", payload);

                                // server is restarted here...
                                using (NATSServer.Create(Context.Server1.Port))
                                {
                                    // wait for reconnect
                                    Assert.True(reconnectedEvent.WaitOne(60000));

                                    c.Publish("foobar", payload);
                                    c.Publish("foo", payload);

                                    using (IAsyncSubscription s4 = c.SubscribeAsync("done"))
                                    {
                                        AutoResetEvent doneEvent = new AutoResetEvent(false);
                                        s4.MessageHandler += (sender, args) => { doneEvent.Set(); };

                                        s4.Start();

                                        c.Publish("done", payload);
                                        Assert.True(doneEvent.WaitOne(4000));
                                    }
                                } // NATSServer   
                            }    
                        }
                    }

                    Assert.Equal(4, received);
                }
            }
        }

        private void incrReceivedMessageHandler(object sender,
            MsgHandlerEventArgs args)
        {
            Interlocked.Increment(ref received);
        }

        Dictionary<int, bool> results = new Dictionary<int, bool>();

        void checkResults(int numSent)
        {
            lock (results)
            {
                for (int i = 0; i < numSent; i++)
                {
                    Assert.True(results.ContainsKey(i),
                        string.Format("Received incorrect number of messages, {0} for seq: {1}", results[i], i));
                }

                results.Clear();
            }
        }

        void sendAndCheckMsgs(IConnection ec, string subject, int numToSend)
        {
            for (int i = 0; i < numToSend; i++)
            {
                ec.Publish(subject, Encoding.UTF8.GetBytes(Convert.ToString(i)));
            }
            ec.Flush();

            Thread.Sleep(500);

            checkResults(numToSend);
        }

        [Fact]
        public void TestQueueSubsOnReconnect()
        {
            AutoResetEvent reconnectEvent = new AutoResetEvent(false);
            Options opts = getReconnectOptions();
            opts.MaxReconnect = 32;

            string subj = "foo.bar";
            string qgroup = "workers";

            opts.ReconnectedEventHandler += (sender, args) =>
            {
                reconnectEvent.Set();
            };

            using(NATSServer ns = NATSServer.Create(Context.Server1.Port))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    EventHandler<MsgHandlerEventArgs> eh = (sender, args) =>
                    {
                        int seq = Convert.ToInt32(Encoding.UTF8.GetString(args.Message.Data));

                        lock (results)
                        {
                            if (results.ContainsKey(seq) == false)
                                results.Add(seq, true);
                        }
                    };

                    // Create Queue Subscribers
                    c.SubscribeAsync(subj, qgroup, eh);
                    c.SubscribeAsync(subj, qgroup, eh);

                    c.Flush();

                    sendAndCheckMsgs(c, subj, 10);
                    
                    ns.Shutdown();
                    
                    // give the OS time to shut it down.
                    Thread.Sleep(1000);

                    // start back up
                    using (NATSServer.Create(Context.Server1.Port))
                    {
                        // wait for reconnect
                        Assert.True(reconnectEvent.WaitOne(6000));

                        sendAndCheckMsgs(c, subj, 10);
                    }
                }
            }
        }

        [Fact]
        public void TestClose()
        {
            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.AllowReconnect = true;
            opts.MaxReconnect = 60;

            using (NATSServer s1 = NATSServer.Create(Context.Server1.Port))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.False(c.IsClosed());

                    s1.Shutdown();

                    Thread.Sleep(100);
                    Assert.False(c.IsClosed(), string.Format("Invalid state, expecting not closed, received: {0}", c.State));

                    using (NATSServer s2 = NATSServer.Create(Context.Server1.Port))
                    {
                        Thread.Sleep(1000);
                        Assert.False(c.IsClosed());

                        c.Close();
                        Assert.True(c.IsClosed());
                    }
                }
            }
        }

        [Fact]
        public void TestIsReconnectingAndStatus()
        {
            bool disconnected = false;
            object disconnectedLock = new object();

            bool reconnected = false;
            object reconnectedLock = new object();

            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.AllowReconnect = true;
            opts.MaxReconnect = 10000;
            opts.ReconnectWait = 100;

            opts.DisconnectedEventHandler += (sender, args) => 
            {
                lock (disconnectedLock)
                {
                    disconnected = true;
                    Monitor.Pulse(disconnectedLock);
                }
            };

            opts.ReconnectedEventHandler += (sender, args) => 
            {
                lock (reconnectedLock)
                {
                    reconnected = true;
                    Monitor.Pulse(reconnectedLock);
                }
            };

            using (NATSServer s = NATSServer.Create(Context.Server1.Port))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {

                    Assert.True(c.State == ConnState.CONNECTED);
                    Assert.True(c.IsReconnecting() == false);
                    
                    s.Shutdown();
                
                    lock (disconnectedLock)
                    {
                        if (!disconnected)
                            Assert.True(Monitor.Wait(disconnectedLock, 10000));
                    }

                    Assert.True(c.State == ConnState.RECONNECTING);
                    Assert.True(c.IsReconnecting() == true);

                    // restart the server
                    using (NATSServer.Create(Context.Server1.Port))
                    {
                        lock (reconnectedLock)
                        {
                            // may have reconnected, if not, wait
                            if (!reconnected)
                                Assert.True(Monitor.Wait(reconnectedLock, 10000));
                        }

                        Assert.True(c.IsReconnecting() == false);
                        Assert.True(c.State == ConnState.CONNECTED);

                        c.Close();
                    }

                    Assert.True(c.IsReconnecting() == false);
                    Assert.True(c.State == ConnState.CLOSED);
                }
            }
        }


        [Fact]
        public void TestReconnectVerbose()
        {
            // an exception stops and fails the test.
            Object reconnectLock = new Object();
            bool   reconnected = false;

            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.Verbose = true;

            opts.ReconnectedEventHandler += (sender, args) =>
            {
                lock (reconnectLock)
                {
                    reconnected = true;
                    Monitor.Pulse(reconnectLock);
                }
            };

            using (NATSServer s = NATSServer.Create(Context.Server1.Port))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    c.Flush();
                    
                    s.Shutdown();
                    
                    using ( NATSServer.Create(Context.Server1.Port))
                    {
                        lock (reconnectLock)
                        {
                            if (!reconnected)
                                Monitor.Wait(reconnectLock, 5000);
                        }

                        c.Flush();
                    }
                }
            }
        }

        [Fact]
        public void TestReconnectBufferProperty()
        {
            var opts = ConnectionFactory.GetDefaultOptions();
            opts.ReconnectBufferSize = Options.ReconnectBufferDisabled;
            opts.ReconnectBufferSize = Options.ReconnectBufferSizeUnbounded;
            opts.ReconnectBufferSize = 1024 * 1024;
            Assert.Throws<ArgumentOutOfRangeException>(() => { opts.ReconnectBufferSize = -2; });
        }

        [Fact]
        public void TestReconnectBufferDisabled()
        {
            AutoResetEvent disconnected = new AutoResetEvent(false);
            AutoResetEvent reconnected = new AutoResetEvent(false);

            var opts = Context.GetTestOptions(Context.Server1.Port);
            opts.ReconnectBufferSize = Options.ReconnectBufferDisabled;
            opts.DisconnectedEventHandler = (obj, args) => { disconnected.Set(); };
            opts.ReconnectedEventHandler = (obj, args) => { reconnected.Set(); };

            using (var server = NATSServer.Create(Context.Server1.Port))
            {
                // Create our client connections.
                using (var c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (var s = c.SubscribeSync("foo"))
                    {
                        server.Shutdown();
                        
                        // wait until we're disconnected.
                        Assert.True(disconnected.WaitOne(5000));

                        // Publish a message.
                        Assert.Throws<NATSReconnectBufferException>( () => { c.Publish("foo", null);  });

                        using (NATSServer.Create(Context.Server1.Port))
                        {
                            // wait for the client to reconnect.
                            Assert.True(reconnected.WaitOne(20000));

                            // Check that we do not receive a message.
                            Assert.Throws<NATSTimeoutException>(() => { s.NextMessage(1000); });
                            
                            c.Close();
                        }
                    }
                }
            }
        }

        [Fact]
        public void TestReconnectBufferBoundary()
        {
            AutoResetEvent disconnected = new AutoResetEvent(false);

            var opts = Context.GetTestOptions(Context.Server1.Port);
            opts.ReconnectBufferSize = 32; // 32 bytes
            opts.DisconnectedEventHandler = (obj, args) => { disconnected.Set(); };
            EventHandler<MsgHandlerEventArgs> eh = (obj, args) => { /* NOOP */ };

            using (var server = NATSServer.Create(Context.Server1.Port))
            {
                using (var c = new ConnectionFactory().CreateConnection(opts))
                {
                    using ( c.SubscribeAsync("foo", eh))
                    {
                        server.Shutdown();
             
                        // wait until we're disconnected.
                        Assert.True(disconnected.WaitOne(5000));

                        // PUB foo 25\r\n<...> = 30 so first publish should be OK, 2nd publish
                        // should fail.
                        byte[] payload = new byte[18];
                        c.Publish("foo", payload);
                        Assert.Throws<NATSReconnectBufferException>(() => c.Publish("foo", payload));

                        c.Close();
                    }    
                }
            }
        }

        [Fact]
        public void TestReconnectWaitJitter()
        {
            AutoResetEvent reconnected = new AutoResetEvent(false);
            Stopwatch sw = new Stopwatch();

            var opts = Context.GetTestOptions(Context.Server1.Port);
            opts.ReconnectWait = 100;
            opts.SetReconnectJitter(500, 0);
            opts.ReconnectedEventHandler = (obj, args) => {
                sw.Stop();
                reconnected.Set();
            };

            using (var s = NATSServer.Create(Context.Server1.Port))
            {
                // Create our client connections.
                using (new ConnectionFactory().CreateConnection(opts))
                {
                    sw.Start();
                    s.Bounce(50);
                    Assert.True(reconnected.WaitOne(5000));
                }
            }
            // We should wait at least the reconnect wait + random up to 500ms.
            // Account for a bit of variation since we rely on the reconnect
            // handler which is not invoked in place.
            long elapsed = sw.ElapsedMilliseconds;
            Assert.True(elapsed > 100);
            Assert.True(elapsed < 800);
        }

        [Fact]
        public void TestReconnectWaitBreakOnClose()
        {
            var opts = Context.GetTestOptions(Context.Server1.Port);
            opts.ReconnectWait = 30000;

            using (var s = NATSServer.Create(Context.Server1.Port))
            {
                // Create our client connections.
                using (var c = new ConnectionFactory().CreateConnection(opts))
                {
                    Stopwatch sw = Stopwatch.StartNew();
                    s.Shutdown();

                    // Wait a bit for the reconnect loop to go into wait mode.
                    Thread.Sleep(250);

                    // Close the connection to break waiting
                    c.Close();
                    sw.Stop();

                    // It should be around 400 ms max, but only need to check that
                    // it's less than 30000.
                    Assert.True(sw.ElapsedMilliseconds < 30000);
                }
            }
        }

        [Fact]
        public void TestCustomReconnectDelay()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            var opts = Context.GetTestOptions(Context.Server1.Port);
            opts.ReconnectDelayHandler = (obj, args) => ev.Set();

            using (var s = NATSServer.Create(Context.Server1.Port))
            {
                using (new ConnectionFactory().CreateConnection(opts))
                {
                    s.Shutdown();
                    Assert.True(ev.WaitOne(10000));
                }
            }
        }

        [Fact]
        public void TestReconnectDelayJitterOptions()
        {
            var opts = Context.GetTestOptions(Context.Server1.Port);
            Assert.True(opts.ReconnectJitter == Defaults.ReconnectJitter);
            Assert.True(opts.ReconnectJitterTLS == Defaults.ReconnectJitterTLS);
        }
        
        
        [Fact]
        public void TestMaxReconnectOnConnect()
        {
            Options opts = Context.GetTestOptions(Context.Server1.Port);
            opts.AllowReconnect = true;
            opts.MaxReconnect = 60;

            CountdownEvent latch = new CountdownEvent(1);
            IConnection connection = null;
            Thread t = new Thread(() =>
            {
                Assert.Null(connection);
                Thread.Sleep(2000);
                Assert.Null(connection);
                
                using (NATSServer s1 = NATSServer.Create(Context.Server1.Port))
                {
                    latch.Wait(2000);
                    Assert.NotNull(connection);
                    Assert.Equal(ConnState.CONNECTED, connection.State);
                }
            });
            t.Start();

            connection = Context.ConnectionFactory.CreateConnection(opts, true);
            latch.Signal();

            t.Join(5000);
        }
    }

    public class TestPublishErrorsDuringReconnect : TestSuite<PublishErrorsDuringReconnectSuiteContext>
    {
        public TestPublishErrorsDuringReconnect(PublishErrorsDuringReconnectSuiteContext context)
            : base(context) { }

        [Fact]
        public void ConnectionShouldNotBecomeClosed()
        {
            Options opts = Context.GetTestOptions(Context.Server1.Port);

            AutoResetEvent connectedEv = new AutoResetEvent(false);
            using (var server = NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                Task t = Task.Factory.StartNew(() =>
                    {
                        connectedEv.WaitOne(10000);

                        Random r = new Random();

                        // increase this count for a longer running test.
                        for (int i = 0; i < 10; i++)
                        {
                            server.Bounce(r.Next(500));
                        }
                    },
                    CancellationToken.None,
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                    TaskScheduler.Default);

                byte[] payload = Encoding.UTF8.GetBytes("hello");
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    connectedEv.Set();

                    while (t.IsCompleted == false)
                    {
                        try
                        {
                            c.Publish("foo", payload);
                        }
                        catch (Exception e)
                        {
                            Assert.IsNotType<NATSConnectionClosedException>(e);
                            Assert.False(c.IsClosed());
                        }
                    }
                }
            }
        }
    }
}
