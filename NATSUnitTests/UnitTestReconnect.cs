// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NATS.Client;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Collections.Generic;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [TestClass]
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

        [TestInitialize()]
        public void Initialize()
        {
            UnitTestUtilities.CleanupExistingServers();
        }

        [TestMethod]
        public void TestReconnectDisallowedFlags()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
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
                        Assert.IsTrue(Monitor.Wait(testLock, 1000));
                    }
                }
            }
        }

        [TestMethod]
        public void TestReconnectAllowedFlags()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
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
                        Assert.IsFalse(Monitor.Wait(testLock, 1000));
                    }

                    Assert.IsTrue(c.State == ConnState.RECONNECTING);
                    c.Opts.ClosedEventHandler = null;
                }
            }
        }

        [TestMethod]
        public void TestBasicReconnectFunctionality()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
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
                System.Console.WriteLine("Reconnected");
            };

            NATSServer ns = utils.CreateServerOnPort(22222);

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                IAsyncSubscription s = c.SubscribeAsync("foo");
                s.MessageHandler += (sender, args) =>
                {
                    System.Console.WriteLine("Received message.");
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
                    Assert.IsTrue(Monitor.Wait(testLock, 100000));
                }

                System.Console.WriteLine("Sending message.");
                c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
                System.Console.WriteLine("Done sending message.");
                // restart the server.
                using (ns = utils.CreateServerOnPort(22222))
                {
                    lock (msgLock)
                    {
                        c.Flush(50000);
                        Assert.IsTrue(Monitor.Wait(msgLock, 10000));
                    }

                    Assert.IsTrue(c.Stats.Reconnects == 1);
                }
            }
        }

        int received = 0;

        [TestMethod]
        public void TestExtendedReconnectFunctionality()
        {
            Options opts = reconnectOptions;

            Object disconnectedLock = new Object();
            Object msgLock = new Object();
            Object reconnectedLock = new Object();

            opts.DisconnectedEventHandler = (sender, args) =>
            {
                System.Console.WriteLine("Disconnected.");
                lock (disconnectedLock)
                {
                    Monitor.Pulse(disconnectedLock);
                }
            };

            opts.ReconnectedEventHandler = (sender, args) =>
            {
                System.Console.WriteLine("Reconnected.");
                lock (reconnectedLock)
                {
                    Monitor.Pulse(reconnectedLock);
                }
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

                lock(disconnectedLock)
                {
                    ns.Shutdown();
                    // server is stopped here.

                    Assert.IsTrue(Monitor.Wait(disconnectedLock, 20000));
                }

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
                    lock (reconnectedLock)
                    {
                        Assert.IsTrue(Monitor.Wait(reconnectedLock, 60000));
                    }

                    c.Publish("foobar", payload);
                    c.Publish("foo", payload);

                    using (IAsyncSubscription s4 = c.SubscribeAsync("done"))
                    {
                        Object doneLock = new Object();
                        s4.MessageHandler += (sender, args) =>
                        {
                            System.Console.WriteLine("Recieved done message.");
                            lock (doneLock)
                            {
                                Monitor.Pulse(doneLock);
                            }
                        };

                        s4.Start();

                        lock (doneLock)
                        {
                            c.Publish("done", payload);
                            Assert.IsTrue(Monitor.Wait(doneLock, 2000));
                        }
                    }
                } // NATSServer

                if (received != 4)
                {
                    Assert.Fail("Expected 4, received {0}.", received);
                }
            }
        }

        private void incrReceivedMessageHandler(object sender,
            MsgHandlerEventArgs args)
        {
            System.Console.WriteLine("Received message on subject {0}.",
                args.Message.Subject);
            Interlocked.Increment(ref received);
        }

        Dictionary<int, bool> results = new Dictionary<int, bool>();

        void checkResults(int numSent)
        {
            lock (results)
            {
                for (int i = 0; i < numSent; i++)
                {
                    if (results.ContainsKey(i) == false)
                    {
                        Assert.Fail("Received incorrect number of messages, [%d] for seq: %d\n",
                            results[i], i);
                    }
                }

                results.Clear();
            }
        }

        [Serializable]
        class NumberObj
        {
            public  NumberObj(int value)
            {
                v = value;
            }

            public int v;
        }

        void sendAndCheckMsgs(IEncodedConnection ec, string subject, int numToSend)
        {
            for (int i = 0; i < numToSend; i++)
            {
                ec.Publish(subject, new NumberObj(i));
            }
            ec.Flush();

            Thread.Sleep(500);

            checkResults(numToSend);
        }

        [TestMethod]
        public void TestQueueSubsOnReconnect()
        {
            Object reconnectLock = new Object();
            Options opts = reconnectOptions;
            IEncodedConnection ec;

            string subj = "foo.bar";
            string qgroup = "workers";

            opts.ReconnectedEventHandler += (sender, args) =>
            {
                lock (reconnectLock)
                {
                    Monitor.Pulse(reconnectLock);
                }
            };

            using(NATSServer ns = utils.CreateServerOnPort(22222))
            {
                ec = new ConnectionFactory().CreateEncodedConnection(opts);

                EventHandler<EncodedMessageEventArgs> eh = (sender, args) =>
                {
                    int seq = ((NumberObj)args.ReceivedObject).v;

                    lock (results)
                    {
                        if (results.ContainsKey(seq) == false)
                            results.Add(seq, true);
                    }
                };

                // Create Queue Subscribers
	            ec.SubscribeAsync(subj, qgroup, eh);
                ec.SubscribeAsync(subj, qgroup, eh);

                ec.Flush();

                sendAndCheckMsgs(ec, subj, 10);
            }
            // server should stop...

            // give the OS time to shut it down.
            Thread.Sleep(500);

            // start back up
            using (NATSServer ns = utils.CreateServerOnPort(22222))
            {
                // wait for reconnect
                lock (reconnectLock)
                {
                    Assert.IsTrue(Monitor.Wait(reconnectLock, 3000));
                }

                sendAndCheckMsgs(ec, subj, 10);
            }
        }

        [TestMethod]
        public void TestClose()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = "nats://localhost:22222";
            opts.AllowReconnect = true;
            opts.MaxReconnect = 60;

            using (NATSServer s1 = utils.CreateServerOnPort(22222))
            {
                IConnection c = new ConnectionFactory().CreateConnection(opts);
                Assert.IsFalse(c.IsClosed());
                
                s1.Shutdown();

                Thread.Sleep(100);
                if (c.IsClosed())
                {
                    Assert.Fail("Invalid state, expecting not closed, received: "
                        + c.State.ToString());
                }
                
                using (NATSServer s2 = utils.CreateServerOnPort(22222))
                {
                    Thread.Sleep(1000);
                    Assert.IsFalse(c.IsClosed());
                
                    c.Close();
                    Assert.IsTrue(c.IsClosed());
                }
            }
        }

        [TestMethod]
        public void TestIsReconnectingAndStatus()
        {
            bool disconnected = false;
            object disconnectedLock = new object();

            bool reconnected = false;
            object reconnectedLock = new object();


            IConnection c = null;

            Options opts = ConnectionFactory.GetDefaultOptions();
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

                Assert.IsTrue(c.State == ConnState.CONNECTED);
                Assert.IsTrue(c.IsReconnecting() == false);
            }
            // server stops here...

            lock (disconnectedLock)
            {
                if (!disconnected)
                    Assert.IsTrue(Monitor.Wait(disconnectedLock, 10000));
            }

            Assert.IsTrue(c.State == ConnState.RECONNECTING);
            Assert.IsTrue(c.IsReconnecting() == true);

            // restart the server
            using (NATSServer s = utils.CreateServerOnPort(22222))
            {
                lock (reconnectedLock)
                {
                    // may have reconnected, if not, wait
                    if (!reconnected)
                        Assert.IsTrue(Monitor.Wait(reconnectedLock, 10000));
                }

                Assert.IsTrue(c.IsReconnecting() == false);
                Assert.IsTrue(c.State == ConnState.CONNECTED);

                c.Close();
            }

            Assert.IsTrue(c.IsReconnecting() == false);
            Assert.IsTrue(c.State == ConnState.CLOSED);

        }
#if sdlfkjsdflkj

func TestIsReconnectingAndStatus(t *testing.T) {
	ts := startReconnectServer(t)
	// This will kill the last 'ts' server that is created
	defer func() { ts.Shutdown() }()
	disconnectedch := make(chan bool)
	reconnectch := make(chan bool)
	opts := nats.DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = true
	opts.MaxReconnect = 10000
	opts.ReconnectWait = 100 * time.Millisecond

	opts.DisconnectedCB = func(_ *nats.Conn) {
		disconnectedch <- true
	}
	opts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect, verify initial reconnecting state check, then stop the server
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	if nc.IsReconnecting() == true {
		t.Fatalf("IsReconnecting returned true when the connection is still open.")
	}
	if status := nc.Status(); status != nats.CONNECTED {
		t.Fatalf("Status returned %d when connected instead of CONNECTED", status)
	}
	ts.Shutdown()

	// Wait until we get the disconnected callback
	if e := Wait(disconnectedch); e != nil {
		t.Fatalf("Disconnect callback wasn't triggered: %v", e)
	}
	if nc.IsReconnecting() == false {
		t.Fatalf("IsReconnecting returned false when the client is reconnecting.")
	}
	if status := nc.Status(); status != nats.RECONNECTING {
		t.Fatalf("Status returned %d when reconnecting instead of CONNECTED", status)
	}

	ts = startReconnectServer(t)

	// Wait until we get the reconnect callback
	if e := Wait(reconnectch); e != nil {
		t.Fatalf("Reconnect callback wasn't triggered: %v", e)
	}
	if nc.IsReconnecting() == true {
		t.Fatalf("IsReconnecting returned true after the connection was reconnected.")
	}
	if status := nc.Status(); status != nats.CONNECTED {
		t.Fatalf("Status returned %d when reconnected instead of CONNECTED", status)
	}

	// Close the connection, reconnecting should still be false
	nc.Close()
	if nc.IsReconnecting() == true {
		t.Fatalf("IsReconnecting returned true after Close() was called.")
	}
	if status := nc.Status(); status != nats.CLOSED {
		t.Fatalf("Status returned %d after Close() was called instead of CLOSED", status)
	}
}

#endif

    } // class

} // namespace
