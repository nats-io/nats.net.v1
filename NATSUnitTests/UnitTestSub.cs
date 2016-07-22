// Copyright 2015 Apcera Inc. All rights reserved.

using System;
using NATS.Client;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;
using Xunit;

namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestSubscriptions : IDisposable
    {

        UnitTestUtilities utils = new UnitTestUtilities();

        public TestSubscriptions()
        {
            UnitTestUtilities.CleanupExistingServers();
            utils.StartDefaultServer();
        }

        public void Dispose()
        {
            utils.StopDefaultServer();
        }

        [Fact]
        public void TestServerAutoUnsub()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                long received = 0;
                int max = 10;

                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    s.MessageHandler += (sender, arg) =>
                    {
                        Console.WriteLine("Received msg.");
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

        [Fact]
        public void TestClientAutoUnsub()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
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

        [Fact]
        public void TestCloseSubRelease()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
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

        [Fact]
        public void TestValidSubscriber()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
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

        [Fact]
        public void TestSlowSubscriber()
        {
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.SubChannelLength = 10;

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

        [Fact]
        public void TestSlowAsyncSubscriber()
        {
            ConditionalObj subCond = new ConditionalObj();

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.SubChannelLength = 100;

            using (IConnection c = new ConnectionFactory().CreateConnection(opts))
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    Object mu = new Object();

                    s.MessageHandler += (sender, args) =>
                    {
                        // block to back us up.
                        subCond.wait(2000);
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

                    subCond.notify();

                    Assert.False(sw.ElapsedMilliseconds >= flushTimeout,
                        $"elapsed ({sw.ElapsedMilliseconds}) > timeout ({flushTimeout})");

                }
            }
        }

        [Fact]
        public void TestAsyncErrHandler()
        {
            Object subLock = new Object();
            object testLock = new Object();
            IAsyncSubscription s;


            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.SubChannelLength = 10;

            bool handledError = false;

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

                            Console.WriteLine("Expected Error: " + args.Error);
                            Assert.True(args.Error.Contains("Slow"));

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

                            Console.WriteLine("Subscriber Waiting....");
                            Assert.True(Monitor.Wait(subLock, 500));
                            Console.WriteLine("Subscriber done.");
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

        [Fact]
        public void TestAsyncSubscriberStarvation()
        {
            Object waitCond = new Object();

            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription helper = c.SubscribeAsync("helper"),
                                          start = c.SubscribeAsync("start"))
                {
                    helper.MessageHandler += (sender, arg) =>
                    {
                        Console.WriteLine("Helper");
                        c.Publish(arg.Message.Reply,
                            Encoding.UTF8.GetBytes("Hello"));
                    };
                    helper.Start();

                    start.MessageHandler += (sender, arg) =>
                    {
                        Console.WriteLine("Responsder");
                        string responseIB = c.NewInbox();
                        IAsyncSubscription ia = c.SubscribeAsync(responseIB);

                        ia.MessageHandler += (iSender, iArgs) =>
                        {
                            Console.WriteLine("Internal subscriber.");
                            lock (waitCond) { Monitor.Pulse(waitCond); }
                        };
                        ia.Start();

                        c.Publish("helper", responseIB,
                            Encoding.UTF8.GetBytes("Help me!"));
                    };

                    start.Start();

                    c.Publish("start", Encoding.UTF8.GetBytes("Begin"));
                    c.Flush();

                    lock (waitCond)
                    {
                        Assert.True(Monitor.Wait(waitCond, 2000));
                    }
                }
            }
        }


        [Fact]
        public void TestAsyncSubscribersOnClose()
        {
            /// basically tests if the subscriber sub channel gets
            /// cleared on a close.
            Object waitCond = new Object();
            int callbacks = 0;

            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                {
                    s.MessageHandler += (sender, args) =>
                    {
                        callbacks++;
                        lock (waitCond)
                        {
                            Monitor.Wait(waitCond);
                        }
                    };

                    s.Start();

                    for (int i = 0; i < 10; i++)
                    {
                        c.Publish("foo", null);
                    }
                    c.Flush();

                    Thread.Sleep(500);
                    c.Close();

                    lock (waitCond)
                    {
                        Monitor.Pulse(waitCond);
                    }

                    Thread.Sleep(500);

                    Assert.True(callbacks == 1);
                }
            }
        }

        [Fact]
        public void TestNextMessageOnClosedSub()
        {
            using (IConnection c = new ConnectionFactory().CreateConnection())
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

        [Fact]
        public void TestAsyncSubscriptionPending()
        {
            int total = 100;
            int receivedCount = 0;

            ConditionalObj subDoneCond = new ConditionalObj();
            ConditionalObj startProcessing = new ConditionalObj();

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (IConnection c = new ConnectionFactory().CreateConnection())
            {
                ISubscription s = c.SubscribeAsync("foo", (sender, args) =>
                {
                    startProcessing.wait(60000);

                    receivedCount++;
                    if (receivedCount == total)
                    {
                        subDoneCond.notify();
                    }
                });

                for (int i = 0; i < total; i++)
                {
                    c.Publish("foo", data);
                }
                c.Flush();

                Thread.Sleep(1000);

                int expectedPendingCount = total - 1;

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

                startProcessing.notify();

                subDoneCond.wait(1000);

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

        [Fact]
        public void TestSyncSubscriptionPending()
        {
            int total = 100;

            ConditionalObj subDoneCond = new ConditionalObj();
            ConditionalObj startProcessing = new ConditionalObj();

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (IConnection c = new ConnectionFactory().CreateConnection())
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

        [Fact]
        public void TestAsyncSubscriptionPendingDrain()
        {
            int total = 100;

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (IConnection c = new ConnectionFactory().CreateConnection())
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

        [Fact]
        public void TestSyncSubscriptionPendingDrain()
        {
            int total = 100;

            byte[] data = Encoding.UTF8.GetBytes("0123456789");

            using (IConnection c = new ConnectionFactory().CreateConnection())
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
}

