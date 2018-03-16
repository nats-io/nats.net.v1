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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Generic;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestReconnect
    {

        private Options reconnectOptions = getReconnectOptions();

        private static Options getReconnectOptions()
        {
            Options o = ConnectionFactory.GetDefaultOptions();
            o.Url = "nats://localhost:22222";
            o.AllowReconnect = true;
            o.MaxReconnect = 10;
            o.ReconnectWait = 100;

            return o;
        }

        UnitTestUtilities utils = new UnitTestUtilities();

        [Fact]
        public void TestReconnectDisallowedFlags()
        {
            Options opts = utils.DefaultTestOptions;
            opts.Url = "nats://localhost:22222";
            opts.AllowReconnect = false;

            Object testLock = new Object();

            opts.ClosedEventHandler = (sender, args) =>
            {
                lock(testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            using (NATSServer ns = utils.CreateServerOnPort(22222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
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
            Options opts = utils.DefaultTestOptions;
            opts.Url = "nats://localhost:22222";
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;

            Object testLock = new Object();

            opts.ClosedEventHandler = (sender, args) =>
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            using (NATSServer ns = utils.CreateServerOnPort(22222))
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    lock (testLock)
                    {
                        ns.Shutdown();
                        Assert.False(Monitor.Wait(testLock, 1000));
                    }

                    Assert.True(c.State == ConnState.RECONNECTING);
                    c.Opts.ClosedEventHandler = null;
                }
            }
        }

        [Fact]
        public void TestBasicReconnectFunctionality()
        {
            Options opts = utils.DefaultTestOptions;
            opts.Url = "nats://localhost:22222";
            opts.MaxReconnect = 2;
            opts.ReconnectWait = 1000;

            Object testLock = new Object();
            Object msgLock = new Object();

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                lock (testLock)
                {
                    Monitor.Pulse(testLock);
                }
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                // NOOP
            };

            NATSServer ns = utils.CreateServerOnPort(22222);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IAsyncSubscription s = c.SubscribeAsync("foo");
                s.MessageHandler += (sender, args) =>
                {
                    lock (msgLock)
                    {
                        Monitor.Pulse(msgLock);   
                    }
                };

                s.Start();
                c.Flush();

                lock (testLock)
                {
                    ns.Shutdown();
                    Assert.True(Monitor.Wait(testLock, 100000));
                }

                c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));

                // restart the server.
                using (ns = utils.CreateServerOnPort(22222))
                {
                    lock (msgLock)
                    {
                        c.Flush(50000);
                        Assert.True(Monitor.Wait(msgLock, 10000));
                    }

                    Assert.True(c.Stats.Reconnects == 1);
                }
            }
        }

        int received = 0;

        [Fact]
        public void TestExtendedReconnectFunctionality()
        {
            Options opts = reconnectOptions;

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
            NATSServer ns = utils.CreateServerOnPort(22222);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IAsyncSubscription s1 = c.SubscribeAsync("foo");
                IAsyncSubscription s2 = c.SubscribeAsync("foobar");

                s1.MessageHandler += incrReceivedMessageHandler;
                s2.MessageHandler += incrReceivedMessageHandler;

                s1.Start();
                s2.Start();

                received = 0;

	            c.Publish("foo", payload);
                c.Flush();

                ns.Shutdown();
                // server is stopped here.

                Assert.True(disconnectedEvent.WaitOne(20000));

                // subscribe to bar while connected.
                IAsyncSubscription s3 = c.SubscribeAsync("bar");
                s3.MessageHandler += incrReceivedMessageHandler;
                s3.Start();

                // Unsub foobar while disconnected
                s2.Unsubscribe();

                c.Publish("foo", payload);
                c.Publish("bar", payload);

                // server is restarted here...
                using (NATSServer ts = utils.CreateServerOnPort(22222))
                {
                    // wait for reconnect
                    Assert.True(reconnectedEvent.WaitOne(60000));

                    c.Publish("foobar", payload);
                    c.Publish("foo", payload);

                    using (IAsyncSubscription s4 = c.SubscribeAsync("done"))
                    {
                        AutoResetEvent doneEvent = new AutoResetEvent(false);
                        s4.MessageHandler += (sender, args) =>
                        {
                            doneEvent.Set();
                        };

                        s4.Start();

                        c.Publish("done", payload);
                        Assert.True(doneEvent.WaitOne(4000));
                    }
                } // NATSServer
                
                Assert.Equal(4, received);
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
            Options opts = reconnectOptions;
            IConnection c;

            string subj = "foo.bar";
            string qgroup = "workers";

            opts.ReconnectedEventHandler += (sender, args) =>
            {
                reconnectEvent.Set();
            };

            using(NATSServer ns = utils.CreateServerOnPort(22222))
            {
                c = new ConnectionFactory().CreateConnection(opts);

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
            }
            // server should stop...

            // give the OS time to shut it down.
            Thread.Sleep(1000);

            // start back up
            using (NATSServer ns = utils.CreateServerOnPort(22222))
            {
                // wait for reconnect
                Assert.True(reconnectEvent.WaitOne(6000));

                sendAndCheckMsgs(c, subj, 10);
            }
        }

        [Fact]
        public void TestClose()
        {
            Options opts = utils.DefaultTestOptions;
            opts.Url = "nats://localhost:22222";
            opts.AllowReconnect = true;
            opts.MaxReconnect = 60;

            using (NATSServer s1 = utils.CreateServerOnPort(22222))
            {
                IConnection c = new ConnectionFactory().CreateConnection(opts);
                Assert.False(c.IsClosed());
                
                s1.Shutdown();

                Thread.Sleep(100);
                Assert.False(c.IsClosed(), string.Format("Invalid state, expecting not closed, received: {0}", c.State));
                
                using (NATSServer s2 = utils.CreateServerOnPort(22222))
                {
                    Thread.Sleep(1000);
                    Assert.False(c.IsClosed());
                
                    c.Close();
                    Assert.True(c.IsClosed());
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


            IConnection c = null;

            Options opts = utils.DefaultTestOptions;
            opts.Url = "nats://localhost:22222";
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

            using (NATSServer s = utils.CreateServerOnPort(22222))
            {
                c = new ConnectionFactory().CreateConnection(opts);

                Assert.True(c.State == ConnState.CONNECTED);
                Assert.True(c.IsReconnecting() == false);
            }
            // server stops here...

            lock (disconnectedLock)
            {
                if (!disconnected)
                    Assert.True(Monitor.Wait(disconnectedLock, 10000));
            }

            Assert.True(c.State == ConnState.RECONNECTING);
            Assert.True(c.IsReconnecting() == true);

            // restart the server
            using (NATSServer s = utils.CreateServerOnPort(22222))
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


        [Fact]
        public void TestReconnectVerbose()
        {
            // an exception stops and fails the test.
            IConnection c = null;

            Object reconnectLock = new Object();
            bool   reconnected = false;

            Options opts = utils.DefaultTestOptions;
            opts.Verbose = true;

            opts.ReconnectedEventHandler += (sender, args) =>
            {
                lock (reconnectLock)
                {
                    reconnected = true;
                    Monitor.Pulse(reconnectLock);
                }
            };

            using (NATSServer s = utils.CreateServerOnPort(4222))
            {
                c = new ConnectionFactory().CreateConnection(opts);
                c.Flush();

                // exit the block and enter a new server block - this
                // restarts the server.
            }

            using (NATSServer s = utils.CreateServerOnPort(4222))
            {
                lock (reconnectLock)
                {
                    if (!reconnected)
                        Monitor.Wait(reconnectLock, 5000);
                }

                c.Flush();
            }
        }

        [Fact]
        public void TestPublishErrorsDuringReconnect()
        {
            AutoResetEvent connectedEv = new AutoResetEvent(false);
            using (var server = new NATSServer())
            {

                Task t = new Task(() =>
                {
                    connectedEv.WaitOne(10000);

                    Random r = new Random();

                    // increase this count for a longer running test.
                    for (int i = 0; i < 10; i++)
                    {
                        server.Bounce(r.Next(500));
                    }
                }, TaskCreationOptions.LongRunning);
                t.Start();

                byte[] payload = Encoding.UTF8.GetBytes("hello");
                using (var c = utils.DefaultTestConnection)
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
    } // class

} // namespace
