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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NATS.Client;
using System.Diagnostics;
using Xunit;
using System.Collections.Generic;


namespace NATSUnitTests
{
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    public class TestBasic
    {
        UnitTestUtilities utils = new UnitTestUtilities();

        [Fact]
        public void TestConnectedServer()
        {
            using (new NATSServer())
            {
                IConnection c = utils.DefaultTestConnection;

                string u = c.ConnectedUrl;

                Assert.False(string.IsNullOrWhiteSpace(u), string.Format("Invalid connected url {0}.", u));

                Assert.Equal(Defaults.Url, u);

                c.Close();
                u = c.ConnectedUrl;

                Assert.Null(u);
            }
        }

        [Fact]
        public void TestMultipleClose()
        {
            using (new NATSServer())
            {
                IConnection c = utils.DefaultTestConnection;

                Task[] tasks = new Task[10];

                for (int i = 0; i < 10; i++)
                {

                    tasks[i] = new Task(() => { c.Close(); });
                    tasks[i].Start();
                }

                Task.WaitAll(tasks);
            }
        }

        [Fact]
        public void TestBadOptionTimeoutConnect()
        {
            Options opts = utils.DefaultTestOptions;

            Assert.ThrowsAny<Exception>(() => opts.Timeout = -1);
        }

        [Fact]
        public void TestBadOptionSubscriptionBatchSize()
        {
            Options opts = utils.DefaultTestOptions;

            Assert.ThrowsAny<ArgumentException>(() => opts.SubscriptionBatchSize = -1);

            Assert.ThrowsAny<ArgumentException>(() => opts.SubscriptionBatchSize = 0);
        }

        [Fact]
        public void TestSimplePublish()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    c.Publish("foo", Encoding.UTF8.GetBytes("Hello World!"));
                }
            }
        }

        [Fact]
        public void TestSimplePublishNoData()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    // Null data should succeed
                    c.Publish("foo", null);

                    // Null data with 0 offset and count should succeed
                    c.Publish("foo", null, 0, 0);

                    // Empty data with 0 offset and count should succeed
                    byte[] empty = new byte[0];
                    c.Publish("foo", empty, 0, 0);

                    // Non-empty data with 0 offset and count should succeed
                    byte[] data = new byte[10];
                    c.Publish("foo", data, 0, 0);
                }
            }
        }

        [Fact]
        public void TestPublishDataWithOffsets()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    byte[] data = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
                    c.Publish("foo", data);                     // Should succeed.
                    c.Publish("foo", data, 0, 1);               // Should succeed.
                    c.Publish("foo", data, 4, 3);               // Should succeed.
                    c.Publish("foo", data, data.Length - 1, 1); // Should succeed.

                    // Negative offsets fail
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, -1, 0));
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, -10, 0));

                    // Negative counts fail
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, 0, -1));
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, 0, -100));

                    // Offset + Count should fail if they are out of bounds
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, 0, data.Length + 10)); // good offset, bad count
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, 4, data.Length + 10)); // good offset, bad count
                    Assert.ThrowsAny<ArgumentException>(() => c.Publish("foo", data, data.Length, 1)); // bad offset, bad count
                }
            }
        }

        private bool compare(byte[] p1, byte[] p2)
        {
            // null case
            if (p1 == p2)
                return true;

            if (p1.Length != p2.Length)
                return false;

            for (int i = 0; i < p2.Length; i++)
            {
                if (p1[i] != p2[i])
                    return false;
            }

            return true;
        }

        private bool compare(byte[] payload, Msg m)
        {
            return compare(payload, m.Data);
        }

        // Compares a subset of expected (offset to count) against all of actual
        private bool compare(byte[] expected, int offset, byte[] actual, int count)
        {
            // null case
            if (expected == actual)
                return true;

            if (count != actual.Length)
                return false;

            for (int i = 0; i < actual.Length; i++)
            {
                if (expected[offset + i] != actual[i])
                    return false;
            }

            return true;
        }

        private bool compare(byte[] expected, int offset, Msg received, int count)
        {
            return compare(expected, offset, received.Data, count);
        }

        private bool compare(Msg a, Msg b)
        {
            if (a.Subject.Equals(b.Subject) == false)
                return false;

            if (a.Reply != null && a.Reply.Equals(b.Reply))
            {
                return false;
            }

            return compare(a.Data, b.Data);
        }

        readonly byte[] omsg = Encoding.UTF8.GetBytes("Hello World");
        readonly object mu = new Object();
        IAsyncSubscription asyncSub = null;
        Boolean received = false;

        [Fact]
        public void TestAsyncSubscribe()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        asyncSub = s;
                        s.MessageHandler += CheckReceivedAndValidHandler;
                        s.Start();

                        lock (mu)
                        {
                            received = false;
                            c.Publish("foo", omsg);
                            c.Flush();
                            Monitor.Wait(mu, 30000);
                        }

                        Assert.True(received, "Did not receive message.");
                    }
                }
            }
        }

        private void CheckReceivedAndValidHandler(object sender, MsgHandlerEventArgs args)
        {
            Assert.True(compare(args.Message.Data, omsg), "Messages are not equal.");
            Assert.Equal(asyncSub, args.Message.ArrivalSubcription);

            lock (mu)
            {
                received = true;
                Monitor.Pulse(mu);
            }
        }

        [Fact]
        public void TestSyncSubscribe()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", omsg);
                        Msg m = s.NextMessage(1000);

                        Assert.True(compare(omsg, m), "Messages are not equal.");

                        c.Publish("foo", omsg, 0, omsg.Length);
                        m = s.NextMessage(1000);

                        Assert.True(compare(omsg, m), "Messages are not equal.");

                        c.Publish("foo", omsg, 2, 3);
                        m = s.NextMessage(1000);

                        Assert.True(compare(omsg, 2, m, 3), "Messages are not equal.");
                    }
                }
            }
        }

        [Fact]
        public void TestPubWithReply()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", "reply", omsg);
                        Msg m = s.NextMessage(1000);

                        Assert.True(compare(omsg, m), "Messages are not equal.");

                        c.Publish("foo", "reply", omsg, 0, omsg.Length);
                        m = s.NextMessage(1000);

                        Assert.True(compare(omsg, m), "Messages are not equal.");

                        c.Publish("foo", "reply", omsg, 1, 5);
                        m = s.NextMessage(1000);

                        Assert.True(compare(omsg, 1, m, 5), "Messages are not equal.");
                    }
                }
            }
        }

        [Fact]
        public void TestFlush()
        {
            using (var server = new NATSServer())
            {
                var cf = new ConnectionFactory();
                var opts = utils.DefaultTestOptions;
                opts.AllowReconnect = false;

                var c = cf.CreateConnection(opts);

                using (ISyncSubscription s = c.SubscribeSync("foo"))
                {
                    c.Publish("foo", "reply", omsg);
                    c.Flush();
                }

                // Test a timeout, locally this may actually succeed, 
                // so allow for that.
                // TODO: find a way to debug/pause the server to allow
                // for timeouts.
                try { c.Flush(1); } catch (NATSTimeoutException) {}

                Assert.Throws<ArgumentOutOfRangeException>(() => { c.Flush(-1); });

                // test a closed connection
                c.Close();
                Assert.Throws<NATSConnectionClosedException>(() => { c.Flush(); });

                // test a lost connection
                c = cf.CreateConnection(opts);
                server.Shutdown();
                Thread.Sleep(500);
                Assert.Throws<NATSConnectionClosedException>(() => { c.Flush(); });
            }
        }

        [Fact]
        public void TestQueueSubscriber()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s1 = c.SubscribeSync("foo", "bar"),
                                             s2 = c.SubscribeSync("foo", "bar"))
                    {
                        c.Publish("foo", omsg);
                        c.Flush(1000);

                        Assert.Equal(1, s1.QueuedMessageCount + s2.QueuedMessageCount);

                        // Drain the messages.
                        try { s1.NextMessage(100); }
                        catch (NATSTimeoutException) { }

                        try { s2.NextMessage(100); }
                        catch (NATSTimeoutException) { }

                        int total = 1000;

                        for (int i = 0; i < 1000; i++)
                        {
                            c.Publish("foo", omsg);
                        }
                        c.Flush(1000);

                        Thread.Sleep(1000);

                        int r1 = s1.QueuedMessageCount;
                        int r2 = s2.QueuedMessageCount;

                        Assert.Equal(total, r1 + r2);

                        Assert.False(Math.Abs(r1 - r2) > total * .15, string.Format("Too much variance between {0} and {1}", r1, r2));
                    }
                }
            }
        }

        [Fact]
        public void TestReplyArg()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        s.MessageHandler += ExpectedReplyHandler;
                        s.Start();

                        lock (mu)
                        {
                            received = false;
                            c.Publish("foo", "bar", null);
                            Monitor.Wait(mu, 5000);
                        }
                    }
                }

                Assert.True(received);
            }
        }

        private void ExpectedReplyHandler(object sender, MsgHandlerEventArgs args)
        {
            Assert.Equal("bar", args.Message.Reply);

            lock (mu)
            {
                received = true;
                Monitor.Pulse(mu);
            }
        }

        [Fact]
        public void TestSyncReplyArg()
        {
            using (new NATSServer())
            {

                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", "bar", null);
                        c.Flush(30000);

                        Msg m = s.NextMessage(1000);

                        Assert.Equal("bar", m.Reply);
                    }
                }
            }
        }

        [Fact]
        public void TestUnsubscribe()
        {
            int count = 0;
            int max = 20;

            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        Boolean unsubscribed = false;
                        asyncSub = s;
                        //s.MessageHandler += UnsubscribeAfterCount;
                        s.MessageHandler += (sender, args) =>
                        {
                            count++;
                            if (count == max)
                            {
                                asyncSub.Unsubscribe();
                                lock (mu)
                                {
                                    unsubscribed = true;
                                    Monitor.Pulse(mu);
                                }
                            }
                        };
                        s.Start();

                        max = 20;
                        for (int i = 0; i < max; i++)
                        {
                            c.Publish("foo", null, null);
                        }
                        Thread.Sleep(100);
                        c.Flush();

                        lock (mu)
                        {
                            if (!unsubscribed)
                            {
                                Monitor.Wait(mu, 5000);
                            }
                        }
                    }

                    Assert.Equal(max, count);
                }
            }
        }

        [Fact]
        public void TestDoubleUnsubscribe()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        s.Unsubscribe();

                        Assert.ThrowsAny<Exception>(() => s.Unsubscribe());
                    }
                }
            }
        }

        [Fact]
        public void TestRequestTimeout()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    Assert.ThrowsAny<Exception>(() => c.Request("foo", null, 500));
                }
            }
        }

        [Fact]
        public void TestRequest()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                        s.MessageHandler += (sender, args) =>
                        {
                            c.Publish(args.Message.Reply, response);
                            c.Flush();
                        };

                        s.Start();

                        Msg m = c.Request("foo", Encoding.UTF8.GetBytes("help."), 5000);

                        Assert.True(compare(response, m.Data), "Response isn't valid");
                    }
                }
            }
        }

        [Fact]
        public void TestRequestNoBody()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                        s.MessageHandler += (sender, args) =>
                        {
                            c.Publish(args.Message.Reply, response);
                        };

                        s.Start();

                        Msg m = c.Request("foo", null, 50000);

                        Assert.True(compare(response, m.Data), "Response isn't valid");
                    }
                }
            }
        }


        [Fact]
        public void TestRequestWithOffset()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        byte[] request = Encoding.UTF8.GetBytes("well hello there");
                        byte[] response = Encoding.UTF8.GetBytes("Around the world in eighty days");

                        s.MessageHandler += (sender, args) =>
                        {
                            Assert.True(compare(request, 5, args.Message, 5));

                            c.Publish(args.Message.Reply, response, 11, 5);
                            c.Flush();
                        };

                        s.Start();

                        Msg m = c.Request("foo", request, 5, 5, 5000);

                        Assert.True(compare(response, 11, m.Data, 5), "Response isn't valid");
                    }
                }
            }
        }

        private async void testRequestAsync(bool useOldRequestStyle)
        {
            using (new NATSServer())
            {
                Options opts = utils.DefaultTestOptions;
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                    EventHandler<MsgHandlerEventArgs> eh = (sender, args) =>
                    {
                        c.Publish(args.Message.Reply, response);
                    };

                    using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                    {
                        var tasks = new List<Task>();
                        for (int i = 0; i < 100; i++)
                        {
                            tasks.Add(c.RequestAsync("foo", null));
                        }

                        foreach (Task<Msg> t in tasks)
                        {
                            Msg m = await t;
                            Assert.True(compare(m.Data, response), "Response isn't valid");
                        }
                    }

                    await Assert.ThrowsAsync<NATSBadSubscriptionException>(() => { return c.RequestAsync("", null); });
                }
            }
        }

        private async void testRequestAsyncWithOffsets(bool useOldRequestStyle)
        {
            using (new NATSServer())
            {
                Options opts = utils.DefaultTestOptions;
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    byte[] request = Encoding.UTF8.GetBytes("well hello there");
                    byte[] response = Encoding.UTF8.GetBytes("Around the world in eighty days");

                    EventHandler<MsgHandlerEventArgs> eh = (sender, args) =>
                    {
                        Assert.True(compare(request, 5, args.Message, 5));

                        c.Publish(args.Message.Reply, response, 11, 5);
                    };

                    using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                    {
                        var tasks = new List<Task>();
                        for (int i = 0; i < 100; i++)
                        {
                            tasks.Add(c.RequestAsync("foo", request, 5, 5));
                        }

                        foreach (Task<Msg> t in tasks)
                        {
                            Msg m = await t;
                            Assert.True(compare(response, 11, m.Data, 5), "Response isn't valid");
                        }
                    }

                    await Assert.ThrowsAsync<NATSBadSubscriptionException>(() => { return c.RequestAsync("", null); });
                }
            }
        }

        [Fact]
        public void TestRequestAsync()
        {
            testRequestAsync(useOldRequestStyle: false);
        }

        [Fact]
        public void TestRequestAsync_OldRequestStyle()
        {
            testRequestAsync(useOldRequestStyle: true);
        }

        [Fact]
        public void TestRequestAsyncWithOffsets()
        {
            testRequestAsyncWithOffsets(useOldRequestStyle: false);
        }

        [Fact]
        public void TestRequestAsyncWithOffsets_OldRequestStyle()
        {
            testRequestAsyncWithOffsets(useOldRequestStyle: true);
        }

        private async void testRequestAsyncCancellation(bool useOldRequestStyle)
        {
            using (new NATSServer())
            {
                Options opts = utils.DefaultTestOptions;
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c = new ConnectionFactory().CreateConnection(opts))
                {
                    int responseDelay = 0;

                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                    EventHandler<MsgHandlerEventArgs> eh = (sender, args) =>
                    {
                        if (responseDelay > 0)
                            Thread.Sleep(responseDelay);

                        c.Publish(args.Message.Reply, response);
                    };

                    // test cancellation success.
                    var miscToken = new CancellationTokenSource().Token;
                    using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                    {
                        var tasks = new List<Task>();
                        for (int i = 0; i < 1000; i++)
                        {
                            tasks.Add(c.RequestAsync("foo", null, miscToken));
                        }

                        foreach (Task<Msg> t in tasks)
                        {
                            Msg m = await t;
                            Assert.True(compare(m.Data, response), "Response isn't valid");
                        }
                    }

                    // test timeout, make sure we are somewhat close (for testing on stressed systems).
                    Stopwatch sw = Stopwatch.StartNew();
                    await Assert.ThrowsAsync<NATSTimeoutException>(() => { return c.RequestAsync("no-replier", null, 1000, miscToken); });
                    sw.Stop();
                    Assert.True(Math.Abs(sw.Elapsed.TotalMilliseconds - 1000) < 250);

                    // test early cancellation
                    var cts = new CancellationTokenSource();
                    var ct = cts.Token;
                    cts.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => { return c.RequestAsync("foo", null, cts.Token); });

                    // test cancellation
                    cts = new CancellationTokenSource();
                    var ocex = Assert.ThrowsAnyAsync<OperationCanceledException>(() => { return c.RequestAsync("foo", null, cts.Token); });
                    Thread.Sleep(2000);
                    cts.Cancel();
                    await ocex;

                    // now we want to test a slow replier, to make sure wait logic checking 
                    responseDelay = 500;
                    using (IAsyncSubscription s = c.SubscribeAsync("foo", eh))
                    {
                        await c.RequestAsync("foo", null, miscToken);

                        // test cancellation with a subscriber
                        cts = new CancellationTokenSource();
                        ocex = Assert.ThrowsAnyAsync<OperationCanceledException>(() => { return c.RequestAsync("foo", null, cts.Token); });
                        Thread.Sleep(responseDelay / 2);
                        cts.Cancel();
                        await ocex;
                    }
                }
            }
        }

        [Fact]
        public void TestRequestAsyncCancellation()
        {
            testRequestAsyncCancellation(useOldRequestStyle: false);
        }

        [Fact]
        public void TestRequestAsyncCancellation_OldRequestStyle()
        {
            testRequestAsyncCancellation(useOldRequestStyle: true);
        }

        private async void testRequestAsyncTimeout(bool useOldRequestStyle)
        {
            using (var server = new NATSServer())
            {
                var sw = new Stopwatch();

                var opts = ConnectionFactory.GetDefaultOptions();
                opts.AllowReconnect = false;
                opts.UseOldRequestStyle = useOldRequestStyle;
                var conn = new ConnectionFactory().CreateConnection(opts);

                // success condition
                var sub = conn.SubscribeAsync("foo", (obj, args) => {
                    conn.Publish(args.Message.Reply, new byte[0]);
                });

                sw.Start();
                await conn.RequestAsync("foo", new byte[0], 5000);
                sw.Stop();
                Assert.True(sw.ElapsedMilliseconds < 5000, "Unexpected timeout behavior");
                sub.Unsubscribe();

                // valid connection, but no response
                sw.Restart();
                await Assert.ThrowsAsync<NATSTimeoutException>(() => { return conn.RequestAsync("test", new byte[0], 500); });
                sw.Stop();
                long elapsed = sw.ElapsedMilliseconds;
                Assert.True(elapsed >= 500, string.Format("Unexpected value (should be > 500): {0}", elapsed));
                long variance = elapsed - 500;
#if DEBUG
                Assert.True(variance < 250, string.Format("Invalid timeout variance: {0}", variance));
#else
                Assert.True(variance < 100, string.Format("Invalid timeout variance: {0}", variance));
#endif

                // Test an invalid connection
                server.Shutdown();
                Thread.Sleep(500);
                await Assert.ThrowsAsync<NATSConnectionClosedException>(() => { return conn.RequestAsync("test", new byte[0], 1000); });
            }
        }

        [Fact]
        public void TestRequestAsyncTimeout()
        {
            testRequestAsyncTimeout(useOldRequestStyle: false);
        }

        [Fact]
        public void TestRequestAsyncTimeout_OldRequestStyle()
        {
            testRequestAsyncTimeout(useOldRequestStyle: true);
        }

        class TestReplier
        {
            string replySubject;
            string id;
            int delay;
            private IConnection c;
            private Stopwatch sw;
            Random r;

            public TestReplier(IConnection c, int maxDelay, string id, string replySubject, Stopwatch sw)
            {
                // Save off our data, then carry on.
                this.c = c;
                delay = maxDelay;
                this.sw = sw;
                this.id = id;
                this.replySubject = replySubject;
                this.r = new Random(this.GetHashCode());
            }

            public void process()
            {
                // delay the response to simulate a heavy workload and introduce
                // variability
                Thread.Sleep(r.Next((delay / 5), delay));
                c.Publish(replySubject, Encoding.UTF8.GetBytes("reply"));
                c.Flush();
            }

            public async Task processAsync()
            {
                // delay the response to simulate a heavy workload and introduce
                // variability
                await Task.Delay(r.Next((delay / 5), delay));
                c.Publish(replySubject, Encoding.UTF8.GetBytes("reply"));
                c.Flush();
            }
        }

#if NET45
        // This test method tests mulitiple overlapping requests across many
        // threads.  The responder simulates work, to introduce variablility
        // in the request timing.
        [Fact]
        public void TestRequestSafetyWithThreads()
        {
            testRequestSafetyWithThreads(useOldRequestStyle: false);
        }

        [Fact]
        public void TestRequestSafetyWithThreads_OldRequestStyle()
        {
            testRequestSafetyWithThreads(useOldRequestStyle: true);
        }

        private void testRequestSafetyWithThreads(bool useOldRequestStyle)
        {
            int MAX_DELAY = 1000;
            int TEST_COUNT = 300;

            Stopwatch sw = new Stopwatch();
            byte[] response = Encoding.UTF8.GetBytes("reply");

            ThreadPool.SetMinThreads(300, 300);
            using (new NATSServer())
            {
                Options opts = utils.DefaultTestOptions;
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c1 = new ConnectionFactory().CreateConnection(opts),
                               c2 = new ConnectionFactory().CreateConnection(opts))
                {
                    using (IAsyncSubscription s = c1.SubscribeAsync("foo", (sender, args) =>
                    {
                        // We cannot block this thread... so copy our data, and spawn a thread
                        // to handle a delay and responding.
                        TestReplier t = new TestReplier(c1, MAX_DELAY,
                                Encoding.UTF8.GetString(args.Message.Data),
                                args.Message.Reply,
                                sw);
                        new Thread(() => { t.process(); }).Start();
                    }))
                    {
                        c1.Flush();

                        // use lower level threads over tasks here for predictibility
                        Thread[] threads = new Thread[TEST_COUNT];
                        Random r = new Random();

                        for (int i = 0; i < TEST_COUNT; i++)
                        {
                            threads[i] = new Thread((() =>
                            {
                                // randomly delay for a bit to test potential timing issues.
                                Thread.Sleep(r.Next(100, 500));
                                c2.Request("foo", null, MAX_DELAY * 2);
                            }));
                        }

                        // sleep for one second to allow the threads to initialize.
                        Thread.Sleep(1000);

                        sw.Start();

                        // start all of the threads at the same time.
                        for (int i = 0; i < TEST_COUNT; i++)
                        {
                            threads[i].Start();
                        }

                        // wait for every thread to stop.
                        for (int i = 0; i < TEST_COUNT; i++)
                        {
                            threads[i].Join();
                        }

                        sw.Stop();

                        // check that we didn't process the requests consecutively.
                        Assert.True(sw.ElapsedMilliseconds < (MAX_DELAY * 2));
                    }
                }
            }
        }

        // This test is a useful comparison in determining the difference
        // between threads (above) and tasks and performance.  In some
        // environments, the NATS client will fail here, but succeed in the 
        // comparable test using threads.
        // Do not automatically run, for comparison purposes and future dev.
        //[Fact]
        public void TestRequestSafetyWithTasks()
        {
            testRequestSafetyWithTasks(useOldRequestStyle: false);
        }

        // This test is a useful comparison in determining the difference
        // between threads (above) and tasks and performance.  In some
        // environments, the NATS client will fail here, but succeed in the 
        // comparable test using threads.
        // Do not automatically run, for comparison purposes and future dev.
        //[Fact]
        public void TestRequestSafetyWithTasks_OldRequestStyle()
        {
            testRequestSafetyWithTasks(useOldRequestStyle: true);
        }

        private void testRequestSafetyWithTasks(bool useOldRequestStyle)
        {
            int MAX_DELAY = 1000;
            int TEST_COUNT = 300;

            ThreadPool.SetMinThreads(300, 300);

            Stopwatch sw = new Stopwatch();
            byte[] response = Encoding.UTF8.GetBytes("reply");

            using (new NATSServer())
            {
                Options opts = utils.DefaultTestOptions;
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c1 = new ConnectionFactory().CreateConnection(opts),
                               c2 = new ConnectionFactory().CreateConnection(opts))
                {
                    // Try parallel requests and check the performance.
                    using (IAsyncSubscription s = c1.SubscribeAsync("foo", (sender, args) =>
                    {
                        // We cannot block this NATS thread... so copy our data, and spawn a thread
                        // to handle a delay and responding.
                        TestReplier t = new TestReplier(c1, MAX_DELAY,
                                Encoding.UTF8.GetString(args.Message.Data),
                                args.Message.Reply,
                                sw);
                        Task.Run(async () => { await t.processAsync(); });
                    }))
                    {
                        c1.Flush();

                        // Depending on resources, Tasks can be queueud up for quite while.
                        Task[] tasks = new Task[TEST_COUNT];
                        Random r = new Random();

                        for (int i = 0; i < TEST_COUNT; i++)
                        {
                            tasks[i] = new Task(async () =>
                            {
                                // randomly delay for a bit to test potential timing issues.
                                await Task.Delay(r.Next(100, 500));
                                c2.Request("foo", null, MAX_DELAY * 2);
                            });
                        }

                        sw.Start();

                        // start all of the threads at the same time.
                        for (int i = 0; i < TEST_COUNT; i++)
                        {
                            tasks[i].Start();
                        }

                        Task.WaitAll(tasks);

                        sw.Stop();

                        // check that we didn't process the requests consecutively.
                        Assert.True(sw.ElapsedMilliseconds < (MAX_DELAY * 2));
                    }
                }
            }
        }
#endif

        [Fact]
        public void TestFlushInHandler()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                        s.MessageHandler += (sender, args) =>
                        {
                            c.Flush();

                            lock (mu)
                            {
                                Monitor.Pulse(mu);
                            }
                        };

                        s.Start();

                        lock (mu)
                        {
                            c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
                            Monitor.Wait(mu);
                        }
                    }
                }
            }
        }

        [Fact]
        public void TestReleaseFlush()
        {
            using (new NATSServer())
            {
                IConnection c = utils.DefaultTestConnection;

                for (int i = 0; i < 1000; i++)
                {
                    c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
                }

                new Task(() => { c.Close(); }).Start();
                c.Flush();
            }
        }

        [Fact]
        public void TestCloseAndDispose()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    c.Close();
                }
            }
        }

        [Fact]
        public void TestInbox()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    string inbox = c.NewInbox();
                    Assert.False(string.IsNullOrWhiteSpace(inbox));
                    Assert.StartsWith("_INBOX.", inbox);
                }
            }
        }

        [Fact]
        public void TestStats()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    byte[] data = Encoding.UTF8.GetBytes("The quick brown fox jumped over the lazy dog");
                    int iter = 10;

                    for (int i = 0; i < iter; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush(1000);

                    IStatistics stats = c.Stats;
                    Assert.Equal(iter, stats.OutMsgs);
                    Assert.Equal(iter * data.Length, stats.OutBytes);

                    c.ResetStats();

                    // Test both sync and async versions of subscribe.
                    IAsyncSubscription s1 = c.SubscribeAsync("foo");
                    s1.MessageHandler += (sender, arg) => { };
                    s1.Start();

                    ISyncSubscription s2 = c.SubscribeSync("foo");

                    for (int i = 0; i < iter; i++)
                    {
                        c.Publish("foo", data);
                    }
                    c.Flush(1000);

                    stats = c.Stats;
                    Assert.Equal(2 * iter, stats.InMsgs);
                    Assert.Equal(2 * iter * data.Length, stats.InBytes);
                }
            }
        }

        [Fact]
        public void TestRaceSafeStats()
        {
            using (new NATSServer(false))
            {
                Thread.Sleep(1000);

                using (IConnection c = utils.DefaultTestConnection)
                {

                    new Task(() => { c.Publish("foo", null); }).Start();

                    Thread.Sleep(1000);

                    Assert.Equal(1, c.Stats.OutMsgs);
                }
            }
        }

        [Fact]
        public void TestBadSubject()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    bool exThrown = false;
                    try
                    {
                        c.Publish("", null);
                    }
                    catch (Exception e)
                    {
                        if (e is NATSBadSubscriptionException)
                            exThrown = true;
                    }
                    Assert.True(exThrown);
                }
            }
        }

        [Fact]
        public void TestLargeMessage()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    int msgSize = 51200;
                    byte[] msg = new byte[msgSize];

                    for (int i = 0; i < msgSize; i++)
                        msg[i] = (byte)'A';

                    msg[msgSize - 1] = (byte)'Z';

                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        Object testLock = new Object();

                        s.MessageHandler += (sender, args) =>
                        {
                            lock (testLock)
                            {
                                Monitor.Pulse(testLock);
                            }
                            Assert.True(compare(msg, args.Message.Data));
                        };

                        s.Start();

                        c.Publish("foo", msg);
                        c.Flush(1000);

                        lock (testLock)
                        {
                            Monitor.Wait(testLock, 2000);
                        }
                    }
                }
            }
        }

        [Fact]
        public void TestSendAndRecv()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    using (IAsyncSubscription s = c.SubscribeAsync("foo"))
                    {
                        int received = 0;
                        int count = 1000;

                        s.MessageHandler += (sender, args) =>
                        {
                            Interlocked.Increment(ref received);
                        };

                        s.Start();

                        for (int i = 0; i < count; i++)
                        {
                            c.Publish("foo", null);
                        }
                        c.Flush();

                        Thread.Sleep(500);

                        Assert.Equal(count, received);
                    }
                }
            }
        }


        [Fact]
        public void TestLargeSubjectAndReply()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    String subject = "";
                    for (int i = 0; i < 1024; i++)
                    {
                        subject += "A";
                    }

                    String reply = "";
                    for (int i = 0; i < 1024; i++)
                    {
                        reply += "A";
                    }

                    // 1 MB
                    byte[] data = new byte[1 << 20];

                    using (IAsyncSubscription s = c.SubscribeAsync(subject))
                    {
                        AutoResetEvent ev = new AutoResetEvent(false);
                        string recvSubj = null;
                        string recvReply = null;

                        s.MessageHandler += (sender, args) =>
                        {
                            recvSubj = args.Message.Subject;
                            recvReply = args.Message.Reply;

                            ev.Set();
                        };

                        s.Start();

                        c.Publish(subject, reply, data);
                        c.Flush();

                        Assert.True(ev.WaitOne(10000));
                        Assert.Equal(subject, recvSubj);
                        Assert.Equal(reply, recvReply);
                    }
                }
            }
        }

        [Fact]
        public void TestAsyncSubHandlerAPI()
        {
            using (new NATSServer())
            {
                using (IConnection c = utils.DefaultTestConnection)
                {
                    int received = 0;

                    EventHandler<MsgHandlerEventArgs> h = (sender, args) =>
                    {
                        Interlocked.Increment(ref received);
                    };

                    using (IAsyncSubscription s = c.SubscribeAsync("foo", h))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Thread.Sleep(500);
                    }

                    using (IAsyncSubscription s = c.SubscribeAsync("foo", "bar", h))
                    {
                        c.Publish("foo", null);
                        c.Flush();
                        Thread.Sleep(500);
                    }

                    Assert.Equal(2, received);
                }
            }
        }

        [Fact]
        public void TestUrlArgument()
        {
            string url1 = Defaults.Url;
            string url2 = "nats://localhost:4223";
            string url3 = "nats://localhost:4224";

            string urls = url1 + "," + url2 + "," + url3;

            using (new NATSServer())
            {
                IConnection c = new ConnectionFactory().CreateConnection(urls);
                Assert.Equal(c.Opts.Servers[0],url1);
                Assert.Equal(c.Opts.Servers[1],url2);
                Assert.Equal(c.Opts.Servers[2],url3);

                c.Close();

                urls = url1 + "    , " + url2 + "," + url3;
                c = new ConnectionFactory().CreateConnection(urls);
                Assert.Equal(c.Opts.Servers[0],url1);
                Assert.Equal(c.Opts.Servers[1],url2);
                Assert.Equal(c.Opts.Servers[2],url3);
                c.Close();

                c = new ConnectionFactory().CreateConnection(url1);
                c.Close();
            }
        }

        private bool assureClusterFormed(IConnection c, int serverCount)
        {
            if (c == null)
                return false;

            // wait until the servers are routed and the conn has the updated
            // server list.
            for (int i = 0; i < 20; i++)
            {
                Thread.Sleep(500 * i);
                if (c.Servers.Length == serverCount)
                    break;
            }
            return (c.Servers.Length == serverCount);
        }

        [Fact]
        public void TestAsyncInfoProtocolConnect()
        {
            using (NATSServer s1 = new NATSServer("-a localhost -p 4221 --cluster nats://127.0.0.1:4551 --routes nats://127.0.0.1:4552"),
                              s2 = new NATSServer("-a localhost -p 4222 --cluster nats://127.0.0.1:4552 --routes nats://127.0.0.1:4551"),
                              s3 = new NATSServer("-a localhost -p 4223 --cluster nats://127.0.0.1:4553 --routes nats://127.0.0.1:4551"),
                              s4 = new NATSServer("-a localhost -p 4224 --cluster nats://127.0.0.1:4554 --routes nats://127.0.0.1:4551"),
                              s5 = new NATSServer("-a localhost -p 4225 --cluster nats://127.0.0.1:4555 --routes nats://127.0.0.1:4551"),
                              s6 = new NATSServer("-a localhost -p 4226 --cluster nats://127.0.0.1:4556 --routes nats://127.0.0.1:4551"),
                              s7 = new NATSServer("-a localhost -p 4227 --cluster nats://127.0.0.1:4557 --routes nats://127.0.0.1:4551"))
            {
                var opts = utils.DefaultTestOptions;
                opts.NoRandomize = false;
                opts.Url = "nats://127.0.0.1:4223";

                var c = new ConnectionFactory().CreateConnection(opts);
                Assert.True(assureClusterFormed(c, 7),
                    "Incomplete cluster with server count: " + c.Servers.Length);
                c.Close();

                // Create a new connection to start from scratch, and recieve 
                // the entire server list at once.
                c = new ConnectionFactory().CreateConnection(opts);
                Assert.True(assureClusterFormed(c, 7),
                    "Incomplete cluster with server count: " + c.Servers.Length);

                // Sufficiently test to ensure we don't hit a random false positive
                // - avoid flappers.
                bool different = false;
                for (int i = 0; i < 50; i++)
                {
                    var c2 = new ConnectionFactory().CreateConnection(opts);
                    Assert.True(assureClusterFormed(c, 7),
                        "Incomplete cluster with server count: " + c.Servers.Length);

                    // The first urls should be the same.
                    Assert.Equal(c.Servers[0],c2.Servers[0]);

                    // now check the others are different (randomized)
                    for (int j = 1; j < c.Servers.Length; j++)
                    {
                        if (!c.Servers[j].Equals(c2.Servers[j]))
                        {
                            different = true;
                            break;
                        }
                    }

                    c2.Close();

                    // ensure the two connections are different - that randomization
                    // occurred.
                    if (different)
                        break;
                }

                Assert.True(different, "Connection urls may not be randomized");
                c.Close();
            }
        }

        [Fact]
        public void TestAsyncInfoProtocolUpdate()
        {
            AutoResetEvent evReconnect = new AutoResetEvent(false);
            var opts = utils.DefaultTestOptions;
            opts.Url = "nats://user:pass@127.0.0.1:4223";
            string newUrl = null;

            opts.ReconnectedEventHandler = (obj, args) =>
            {
                newUrl = args.Conn.ConnectedUrl;
                evReconnect.Set();
            };

            // Specify localhost - the 127.0.0.1 should prevent one of the urls
            // from being added - for adding servers, 127.0.0.1 matches localhost.
            using (NATSServer s1 = new NATSServer("-a localhost -p 4223 --cluster nats://127.0.0.1:4555 --routes nats://127.0.0.1:4666"))
            {
                var c = new ConnectionFactory().CreateConnection(opts);

                Assert.True(c.Servers.Length == 1);
                // check that credentials are stripped.
                Assert.Equal("nats://127.0.0.1:4223", c.Servers[0]);

                // build an independent cluster
                using (NATSServer s2 = new NATSServer("-a localhost -p 4224 --cluster nats://127.0.0.1:4666 --routes nats://127.0.0.1:4555"))
                {
                    // wait until the servers are routed and the conn has the updated
                    // server list.
                    assureClusterFormed(c, 2);

                    // Ensure the first server remains in place and has not been
                    // randomized.
                    Assert.Equal("nats://127.0.0.1:4223", c.Servers[0]);
                    Assert.True(c.Servers.Length == 2);
                    Assert.True(c.DiscoveredServers.Length == 1);


                    // sanity check to ensure we can connect to another server.
                    s1.Shutdown();
                    Assert.True(evReconnect.WaitOne(10000));
                    Assert.True(newUrl != null);
                    Assert.Contains("4224", c.ConnectedUrl);
                }

                c.Close();
            }
        }

        private bool listsEqual(string[] l1, string[] l2)
        {
            if (l1.Length != l2.Length)
                return false;

            for (int i = 0; i < l1.Length; i++)
            {
                if (string.Equals(l1[i], l2[i]) == false)
                    return false;
            }

            return true;
        }

        [Fact]
        public void TestServersRandomize()
        {
            var serverList = new string[] {
                "nats://localhost:4222",
                "nats://localhost:2",
                "nats://localhost:3",
                "nats://localhost:4", 
                "nats://localhsot:5", 
                "nats://localhost:6", 
                "nats://localhost:7"
            };

            var opts = utils.DefaultTestOptions;
            opts.Servers = serverList;
            opts.NoRandomize = true;

            using (new NATSServer())
            {
                var c = new ConnectionFactory().CreateConnection(opts);
                Assert.True(listsEqual(serverList, c.Servers));
                c.Close();

                bool wasRandom = false;
                opts.NoRandomize = false;
                for (int i = 0; i < 10; i++)
                {
                    c = new ConnectionFactory().CreateConnection(opts);
                    wasRandom = (listsEqual(serverList, c.Servers) == false);
                    c.Close();

                    if (wasRandom)
                        break;
                }

                Assert.True(wasRandom);

#if skip
            // Although the original intent was that if Opts.Url is
            // set, Opts.Servers is not (and vice versa), the behavior
            // is that Opts.Url is always first, even when randomization
            // is enabled. So make sure that this is still the case.
            opts.Url = "nats://localhost:4222";
            for (int i = 0; i < 5; i++)
            {
                c = new ConnectionFactory().CreateConnection(opts);
                wasRandom = (listsEqual(serverList, c.Servers) == false);
                Assert.True(Equals(serverList[0], c.Servers[0]));
                c.Close();

                if (wasRandom)
                    break;
            }
#endif
            }
        }

    } // class
} // namespace
