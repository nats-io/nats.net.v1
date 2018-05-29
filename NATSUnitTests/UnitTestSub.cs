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
using Xunit;
using System.Collections.Generic;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestSubscriptions
    {
        UnitTestUtilities utils = new UnitTestUtilities();

        [Fact]
        public void TestServerAutoUnsub()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    long received = 0;
                    int max = 10;

                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        s.MessageHandler += (sender, arg) =>
                        {
                            received++;
                        };

                        s.AutoUnsubscribe(max);
                        s.Start();

                        for (int i = 0; i < (max * 2); i++)
                        {
                            c.Publish("foo", Encoding.UTF8.GetBytes("hello"));
                        }
                        c.Flush();

                        Thread.Sleep(500);

                        Assert.Equal(max, received);

                        Assert.False(s.IsValid);
                    }
                }
            }
        }

        [Fact]
        public void TestClientAutoUnsub()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    long received = 0;
                    int max = 10;

                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        s.AutoUnsubscribe(max);

                        for (int i = 0; i < max * 2; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        Thread.Sleep(100);

                        try
                        {
                            while (true)
                            {
                                s.NextMessage(0);
                                received++;
                            }
                        }
                        catch (NATSMaxMessagesException) { /* ignore */ }

                        Assert.True(received == max);
                        Assert.False(s.IsValid);
                    }
                }
            }
        }

        [Fact]
        public void TestCloseSubRelease()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        Stopwatch sw = new Stopwatch();
                        sw.Start();
                        try
                        {
                            new Task(() => { Thread.Sleep(100); c.Close(); }).Start();
                            s.NextMessage(10000);
                        }
                        catch (Exception) { /* ignore */ }

                        sw.Stop();

                        Assert.True(sw.ElapsedMilliseconds < 10000);
                    }
                }
            }
        }

        [Fact]
        public void TestValidSubscriber()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        Assert.True(s.IsValid);

                        try { s.NextMessage(100); }
                        catch (NATSTimeoutException) { }

                        Assert.True(s.IsValid);

                        s.Unsubscribe();

                        Assert.False(s.IsValid);

                        try { s.NextMessage(100); }
                        catch (NATSBadSubscriptionException) { }
                    }
                }
            }
        }

        [Fact]
        public void TestSlowSubscriber()
        {
            Options opts = utils.DefaultTestOptions;
            opts.SubChannelLength = 10;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        Assert.ThrowsAny<Exception>(() =>
                        {
                            for (int i = 0; i < (opts.SubChannelLength + 100); i++)
                            {
                                c.Publish("foo", null);
                            }

                            try
                            {
                                c.Flush();
                            }
                            catch (Exception)
                            {
                            // ignore
                        }

                            s.NextMessage();
                        });
                    }
                }
            }
        }

        [Fact]
        public void TestSlowAsyncSubscriber()
        {
            AutoResetEvent ev = new AutoResetEvent(false);

            Options opts = utils.DefaultTestOptions;
            opts.SubChannelLength = 100;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        Object mu = new Object();

                        s.MessageHandler += (sender, args) =>
                        {
                            // block to back us up.
                            ev.WaitOne(2000);
                        };

                        s.Start();

                        Assert.True(s.PendingByteLimit == Defaults.SubPendingBytesLimit);
                        Assert.True(s.PendingMessageLimit == Defaults.SubPendingMsgsLimit);

                        long pml = 100;
                        long pbl = 1024 * 1024;

                        s.SetPendingLimits(pml, pbl);

                        Assert.True(s.PendingByteLimit == pbl);
                        Assert.True(s.PendingMessageLimit == pml);

                        for (int i = 0; i < (pml + 100); i++)
                        {
                            c.Publish("foo", null);
                        }

                        int flushTimeout = 5000;

                        Stopwatch sw = new Stopwatch();
                        sw.Start();

                        c.Flush(flushTimeout);

                        sw.Stop();

                        ev.Set();

                        Assert.False(sw.ElapsedMilliseconds >= flushTimeout,
                            string.Format("elapsed ({0}) > timeout ({1})",
                                sw.ElapsedMilliseconds, flushTimeout));

                    }
                }
            }
        }

        [Fact]
        public void TestAsyncErrHandler()
        {
            object subLock = new object();
            object testLock = new object();
            IAsyncSubscription s;

            Options opts = utils.DefaultTestOptions;
            opts.SubChannelLength = 10;

            bool handledError = false;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    using (s = c.SubscribeAsync("foo"))
                    {
                        c.Opts.AsyncErrorEventHandler = (sender, args) =>
                        {
                            lock (subLock)
                            {
                                if (handledError)
                                    return;

                                handledError = true;

                                Assert.True(args.Subscription == s);
                                Assert.Contains("Slow", args.Error);

                            // release the subscriber
                            Monitor.Pulse(subLock);
                            }

                        // release the test
                        lock (testLock) { Monitor.Pulse(testLock); }
                        };

                        bool blockedOnSubscriber = false;
                        s.MessageHandler += (sender, args) =>
                        {
                            lock (subLock)
                            {
                                if (blockedOnSubscriber)
                                    return;

                                Assert.True(Monitor.Wait(subLock, 500));
                                blockedOnSubscriber = true;
                            }
                        };

                        s.Start();

                        lock (testLock)
                        {

                            for (int i = 0; i < (opts.SubChannelLength + 100); i++)
                            {
                                c.Publish("foo", null);
                            }

                            try
                            {
                                c.Flush(1000);
                            }
                            catch (Exception)
                            {
                                // ignore - we're testing the error handler, not flush.
                            }

                            Assert.True(Monitor.Wait(testLock, 1000));
                        }
                    }
                }
            }
        }

        [Fact]
        public void TestAsyncSubscriberStarvation()
        {
           AutoResetEvent ev = new AutoResetEvent(false);

            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription helper = c.SubscribeAsync("helper"),
                                              start = c.SubscribeAsync("start"))
                    {
                        helper.MessageHandler += (sender, arg) =>
                        {
                            c.Publish(arg.Message.Reply,
                                Encoding.UTF8.GetBytes("Hello"));
                        };
                        helper.Start();

                        start.MessageHandler += (sender, arg) =>
                        {
                            string responseIB = c.NewInbox();
                            IAsyncSubscription ia = c.SubscribeAsync(responseIB);

                            ia.MessageHandler += (iSender, iArgs) =>
                            {
                                ev.Set();
                            };
                            ia.Start();

                            c.Publish("helper", responseIB,
                                Encoding.UTF8.GetBytes("Help me!"));
                        };

                        start.Start();

                        c.Publish("start", Encoding.UTF8.GetBytes("Begin"));
                        c.Flush();

                        Assert.True(ev.WaitOne(10000));
                    }
                }
            }
        }

        [Fact]
        public void TestAsyncSubscribersOnClose()
        {
            /// basically tests if the subscriber sub channel gets
            /// cleared on a close.
            AutoResetEvent ev = new AutoResetEvent(false);
            int callbacks = 0;
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        s.MessageHandler += (sender, args) =>
                        {
                            callbacks++;
                            ev.WaitOne(10000);
                        };

                        s.Start();

                        for (int i = 0; i < 10; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        Thread.Sleep(500);
                        c.Close();

                        ev.Set();

                        Thread.Sleep(500);

                        Assert.True(callbacks == 1);
                    }
                }
            }
        }

        [Fact]
        public void TestNextMessageOnClosedSub()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    ISyncSubscription s = c.SubscribeSync("foo");
                    s.Unsubscribe();

                    try
                    {
                        s.NextMessage();
                    }
                    catch (NATSBadSubscriptionException) { } // ignore.

                    // any other exceptions will fail the test.
                }
            }
        }

        [Fact]
        public void TestAsyncSubscriptionPending()
        {
            int total = 100;
            int receivedCount = 0;

            AutoResetEvent evSubDone = new AutoResetEvent(false);
            ManualResetEvent evStart = new ManualResetEvent(false);

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    ISubscription s = c.SubscribeAsync("foo", (sender, args) =>
                    {
                        evStart.WaitOne(60000);

                        receivedCount++;
                        if (receivedCount == total)
                        {
                            evSubDone.Set();
                        }
                    });

                    for (int i = 0; i < total; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush();

                    Thread.Sleep(1000);

                    int expectedPendingCount = total - 1;

                    // At least 1 message will be dequeued
                    Assert.True(s.QueuedMessageCount <= expectedPendingCount);

                    Assert.True((s.MaxPendingBytes == (data.Length * total)) ||
                        (s.MaxPendingBytes == (data.Length * expectedPendingCount)));
                    Assert.True((s.MaxPendingMessages == total) ||
                        (s.MaxPendingMessages == expectedPendingCount));
                    Assert.True((s.PendingBytes == (data.Length * total)) ||
                        (s.PendingBytes == (data.Length * expectedPendingCount)));

                    long pendingBytes;
                    long pendingMsgs;

                    s.GetPending(out pendingBytes, out pendingMsgs);
                    Assert.True(pendingBytes == s.PendingBytes);
                    Assert.True(pendingMsgs == s.PendingMessages);

                    long maxPendingBytes;
                    long maxPendingMsgs;
                    s.GetMaxPending(out maxPendingBytes, out maxPendingMsgs);
                    Assert.True(maxPendingBytes == s.MaxPendingBytes);
                    Assert.True(maxPendingMsgs == s.MaxPendingMessages);


                    Assert.True((s.PendingMessages == total) ||
                        (s.PendingMessages == expectedPendingCount));

                    Assert.True(s.Delivered == 1);
                    Assert.True(s.Dropped == 0);

                    evStart.Set();
                    evSubDone.WaitOne(10000);

                    Assert.True(s.QueuedMessageCount == 0);

                    Assert.True((s.MaxPendingBytes == (data.Length * total)) ||
                        (s.MaxPendingBytes == (data.Length * expectedPendingCount)));
                    Assert.True((s.MaxPendingMessages == total) ||
                        (s.MaxPendingMessages == expectedPendingCount));

                    Assert.True(s.PendingMessages == 0);
                    Assert.True(s.PendingBytes == 0);

                    Assert.True(s.Delivered == total);
                    Assert.True(s.Dropped == 0);

                    s.Unsubscribe();

                    Assert.ThrowsAny<Exception>(() => s.MaxPendingBytes);

                    Assert.ThrowsAny<Exception>(() => s.MaxPendingMessages);

                    Assert.ThrowsAny<Exception>(() => s.PendingMessageLimit);

                    Assert.ThrowsAny<Exception>(() => s.PendingByteLimit);

                    Assert.ThrowsAny<Exception>(() => s.SetPendingLimits(1, 10));
                }
            }
        }


        [Fact]
        public void TestAsyncPendingSubscriptionBatchSizeExactlyOne()
        {
            int total = 10;
            int receivedCount = 0;

            AutoResetEvent evSubDone = new AutoResetEvent(false);
            ManualResetEvent evStart = new ManualResetEvent(false);

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (new NATSServer())
            {
                var opts = utils.DefaultTestOptions;
                opts.SubscriptionBatchSize = 1;

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    ISubscription s = c.SubscribeAsync("foo", (sender, args) =>
                    {
                        evStart.WaitOne(60000);

                        receivedCount++;
                        if (receivedCount == total)
                        {
                            evSubDone.Set();
                        }
                    });

                    for (int i = 0; i < total; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush();

                    Thread.Sleep(1000);

                    int expectedPendingCount = total - 1;

                    // Exactly 1 message will be dequeued
                    Assert.True(s.QueuedMessageCount == expectedPendingCount);

                    Assert.True((s.MaxPendingBytes == (data.Length * total)) ||
                        (s.MaxPendingBytes == (data.Length * expectedPendingCount)));
                    Assert.True((s.MaxPendingMessages == total) ||
                        (s.MaxPendingMessages == expectedPendingCount));
                    Assert.True((s.PendingBytes == (data.Length * total)) ||
                        (s.PendingBytes == (data.Length * expectedPendingCount)));

                    long pendingBytes;
                    long pendingMsgs;

                    s.GetPending(out pendingBytes, out pendingMsgs);
                    Assert.True(pendingBytes == s.PendingBytes);
                    Assert.True(pendingMsgs == s.PendingMessages);

                    long maxPendingBytes;
                    long maxPendingMsgs;
                    s.GetMaxPending(out maxPendingBytes, out maxPendingMsgs);
                    Assert.True(maxPendingBytes == s.MaxPendingBytes);
                    Assert.True(maxPendingMsgs == s.MaxPendingMessages);


                    Assert.True((s.PendingMessages == total) ||
                        (s.PendingMessages == expectedPendingCount));

                    Assert.True(s.Delivered == 1);
                    Assert.True(s.Dropped == 0);

                    evStart.Set();
                    evSubDone.WaitOne(10000);

                    Assert.True(s.QueuedMessageCount == 0);

                    Assert.True((s.MaxPendingBytes == (data.Length * total)) ||
                        (s.MaxPendingBytes == (data.Length * expectedPendingCount)));
                    Assert.True((s.MaxPendingMessages == total) ||
                        (s.MaxPendingMessages == expectedPendingCount));

                    Assert.True(s.PendingMessages == 0);
                    Assert.True(s.PendingBytes == 0);

                    Assert.True(s.Delivered == total);
                    Assert.True(s.Dropped == 0);

                    s.Unsubscribe();
                }
            }
        }

        [Fact]
        public void TestSyncSubscriptionPending()
        {
            int total = 100;

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    ISyncSubscription s = c.SubscribeSync("foo");

                    for (int i = 0; i < total; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush();

                    Assert.True(s.QueuedMessageCount == total);

                    Assert.True((s.MaxPendingBytes == (data.Length * total)) ||
                        (s.MaxPendingBytes == (data.Length * total)));
                    Assert.True((s.MaxPendingMessages == total) ||
                        (s.MaxPendingMessages == total));

                    Assert.True(s.Delivered == 0);
                    Assert.True(s.Dropped == 0);

                    for (int i = 0; i < total; i++)
                    {
                        s.NextMessage();
                    }

                    Assert.True(s.QueuedMessageCount == 0);

                    Assert.True((s.MaxPendingBytes == (data.Length * total)) ||
                        (s.MaxPendingBytes == (data.Length * total)));
                    Assert.True((s.MaxPendingMessages == total) ||
                        (s.MaxPendingMessages == total));

                    Assert.True(s.Delivered == total);
                    Assert.True(s.Dropped == 0);

                    s.Unsubscribe();
                }
            }
        }

        [Fact]
        public void TestAsyncSubscriptionPendingDrain()
        {
            int total = 100;

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    ISubscription s = c.SubscribeAsync("foo", (sender, args) => { });

                    for (int i = 0; i < total; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush();

                    while (s.Delivered != total)
                    {
                        Thread.Sleep(50);
                    }

                    Assert.True(s.Dropped == 0);
                    Assert.True(s.PendingBytes == 0);
                    Assert.True(s.PendingMessages == 0);

                    s.Unsubscribe();
                }
            }
        }

        [Fact]
        public void TestSyncSubscriptionPendingDrain()
        {
            int total = 100;

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    ISyncSubscription s = c.SubscribeSync("foo");

                    for (int i = 0; i < total; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush();

                    while (s.Delivered != total)
                    {
                        s.NextMessage(100);
                    }

                    Assert.True(s.Dropped == 0);
                    Assert.True(s.PendingBytes == 0);
                    Assert.True(s.PendingMessages == 0);

                    s.Unsubscribe();
                }
            }
        }

        [Fact]
        public void TestSubDelTaskCountBasic()
        {
            var opts = utils.DefaultTestOptions;

            Assert.Throws<ArgumentOutOfRangeException>(
                () => { opts.SubscriberDeliveryTaskCount = -1; });

            opts.SubscriberDeliveryTaskCount = 2;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    int s1Count = 0;
                    int s2Count = 0;
                    int COUNT = 10;

                    AutoResetEvent ev1 = new AutoResetEvent(false);
                    AutoResetEvent ev2 = new AutoResetEvent(false);

                    IAsyncSubscription s1 = c.SubscribeAsync("foo", (obj, args) =>
                    {
                        s1Count++;
                        if (s1Count == COUNT)
                        {
                            ev1.Set();
                        }
                    });

                    IAsyncSubscription s2 = c.SubscribeAsync("bar", (obj, args) =>
                    {
                        s2Count++;
                        if (s2Count >= COUNT)
                        {
                            ev2.Set();
                        }
                    });

                    for (int i = 0; i < 10; i++)
                    {
                        c.Publish("foo", null);
                        c.Publish("bar", null);
                    }
                    c.Flush();

                    Assert.True(ev1.WaitOne(10000));
                    Assert.True(ev2.WaitOne(10000));
                    s1.Unsubscribe();

                    Assert.True(s1Count == COUNT);
                    Assert.True(s2Count == COUNT);

                    ev2.Reset();

                    c.Publish("bar", null);
                    c.Flush();

                    Assert.True(ev2.WaitOne(10000));
                    Assert.True(s2Count == COUNT + 1);

                    s2.Unsubscribe();
                }
            }
        }

        [Fact]
        public void TestSubDelTaskCountScaling()
        {
            int COUNT = 20000;
            var opts = utils.DefaultTestOptions;
            opts.SubscriberDeliveryTaskCount = 20;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    long recvCount = 0;

                    var subs = new List<IAsyncSubscription>();

                    EventHandler<MsgHandlerEventArgs> eh = (obj, args) =>
                    {
                        Interlocked.Increment(ref recvCount);
                    };

                    for (int i = 0; i < COUNT; i++)
                    {
                        subs.Add(c.SubscribeAsync("foo", eh));
                    }

                    c.Publish("foo", null);
                    c.Flush();

                    while (Interlocked.Read(ref recvCount) != (COUNT))
                    {
                        Thread.Sleep(100);
                    }

                    // ensure we are not creating a thread per subscriber.
                    Assert.True(Process.GetCurrentProcess().Threads.Count < 500);

                    subs.ForEach((s) => { s.Unsubscribe(); });
                }
            }
        }

        [Fact]
        public void TestSubDelTaskCountAutoUnsub()
        {
            var opts = utils.DefaultTestOptions;
            opts.SubscriberDeliveryTaskCount = 2;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    long received = 0;
                    int max = 10;
                    AutoResetEvent ev = new AutoResetEvent(false);

                    using (var s = c.SubscribeAsync("foo", (obj, args) =>
                    {
                        received++;
                        if (received > max)
                            ev.Set();
                    }))
                    {
                        s.AutoUnsubscribe(max);

                        for (int i = 0; i < max * 2; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        // event should never fire.
                        Assert.False(ev.WaitOne(500));

                        // double check
                        Assert.True(received == max);

                        Assert.False(s.IsValid);
                    }
                }
            }
        }

        [Fact]
        public void TestSubDelTaskCountReconnect()
        {
            bool disconnected = false;
            AutoResetEvent reconnectEv = new AutoResetEvent(false);

            var opts = utils.DefaultTestOptions;
            opts.SubscriberDeliveryTaskCount = 2;
            opts.DisconnectedEventHandler = (obj, args) => { disconnected = true;};
            opts.ReconnectedEventHandler = (obj, args) => { reconnectEv.Set(); };

            using (var server = new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    long received = 0;
                    int max = 10;
                    AutoResetEvent ev = new AutoResetEvent(false);

                    using (var s = c.SubscribeAsync("foo", (obj, args) =>
                    {
                        received++;
                        if (received == max)
                            ev.Set();
                    }))
                    {
                        for (int i = 0; i < max / 2; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        // bounce the server, we should reconnect, then
                        // be able to receive messages.
                        server.Bounce(100);

                        Assert.True(reconnectEv.WaitOne(20000));
                        Assert.True(disconnected);

                        for (int i = 0; i < max / 2; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        Assert.True(ev.WaitOne(10000));
                        Assert.True(received == max);
                    }
                }
            }
        }

        [Fact]
        public void TestSubDelTaskCountSlowConsumer()
        {
            AutoResetEvent errorEv = new AutoResetEvent(false);

            var opts = utils.DefaultTestOptions;
            opts.SubscriberDeliveryTaskCount = 1;
            opts.SubChannelLength = 10;

            opts.AsyncErrorEventHandler = (obj, args) => { errorEv.Set(); };

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    AutoResetEvent cbEv = new AutoResetEvent(false);

                    using (var s = c.SubscribeAsync("foo", (obj, args) =>
                    {
                        cbEv.WaitOne();
                    }))
                    {
                        for (int i = 0; i < opts.SubChannelLength * 3; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        // make sure we hit the error.
                        Assert.True(errorEv.WaitOne(30000));

                        // unblock the callback.
                        cbEv.Set();
                    }
                }
            }
        }

        [Fact]
        public void TestSubDelTaskCountWithSyncSub()
        {
            var opts = utils.DefaultTestOptions;
            opts.SubscriberDeliveryTaskCount = 1;

            using (new NATSServer())
            {
                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    ISyncSubscription s = c.SubscribeSync("foo");
                    c.Publish("foo", null);
                    s.NextMessage(10000);
                }
            }
        }
	}
}

