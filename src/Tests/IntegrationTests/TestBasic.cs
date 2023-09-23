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
    /// <summary>
    /// Run these tests with the gnatsd auth.conf configuration file.
    /// </summary>
    [Collection(DefaultSuiteContext.CollectionKey)]
    public class TestBasic : TestSuite<DefaultSuiteContext>
    {
        public TestBasic(DefaultSuiteContext context) : base(context) {}

        [Fact]
        public void TestConnectedServer()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (var c = Context.OpenConnection())
                {
                    string u = c.ConnectedUrl;

                    Assert.False(string.IsNullOrWhiteSpace(u), string.Format("Invalid connected url {0}.", u));

                    Assert.Equal(Defaults.Url, u);

                    c.Close();
                    u = c.ConnectedUrl;

                    Assert.Null(u);
                }
            }
        }

        [Fact]
        public void TestMultipleClose()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (var c = Context.OpenConnection())
                {
                    Task[] tasks = new Task[10];

                    for (int i = 0; i < 10; i++)
                    {
                        tasks[i] = Task.Run(() => c.Close());
                    }

                    Task.WaitAll(tasks);
                }
            }
        }

        [Fact]
        public void TestSimplePublish()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    c.Publish("foo", Encoding.UTF8.GetBytes("Hello World!"));
                }
            }
        }

        [Fact]
        public void TestSimplePublishNoData()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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

        private bool Compare(byte[] payload, Msg m)
        {
            return compare(payload, m.Data);
        }

        // Compares a subset of expected (offset to count) against all of actual
        private static bool compare(byte[] expected, int offset, byte[] actual, int count)
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            Assert.Equal(asyncSub, args.Message.ArrivalSubscription);

            lock (mu)
            {
                received = true;
                Monitor.Pulse(mu);
            }
        }

        [Fact]
        public void TestSyncSubscribe()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", omsg);
                        Msg m = s.NextMessage(1000);

                        Assert.True(Compare(omsg, m), "Messages are not equal.");

                        c.Publish("foo", omsg, 0, omsg.Length);
                        m = s.NextMessage(1000);

                        Assert.True(Compare(omsg, m), "Messages are not equal.");

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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", "reply", omsg);
                        Msg m = s.NextMessage(1000);

                        Assert.True(Compare(omsg, m), "Messages are not equal.");

                        c.Publish("foo", "reply", omsg, 0, omsg.Length);
                        m = s.NextMessage(1000);

                        Assert.True(Compare(omsg, m), "Messages are not equal.");

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
            using (var server = NATSServer.CreateFastAndVerify())
            {
                var opts = Context.GetTestOptions();
                opts.AllowReconnect = false;

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    using (ISyncSubscription s = c.SubscribeSync("foo"))
                    {
                        c.Publish("foo", "reply", omsg);
                        c.Flush();
                    }

                    // Test a timeout, locally this may actually succeed, 
                    // so allow for that.
                    // TODO: find a way to debug/pause the server to allow
                    // for timeouts.
                    try
                    {
                        c.Flush(1);
                    }
                    catch (NATSTimeoutException)
                    {
                    }

                    Assert.Throws<ArgumentOutOfRangeException>(() => { c.Flush(-1); });

                    // test a closed connection
                    c.Close();
                    Assert.Throws<NATSConnectionClosedException>(() => { c.Flush(); });
                }

                // test a lost connection
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    server.Shutdown();
                    Thread.Sleep(500);
                    Assert.Throws<NATSConnectionClosedException>(() => { c.Flush(); });
                }
            }
        }

        [Fact]
        public void TestQueueSubscriber()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {

                using (IConnection c = Context.OpenConnection())
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

            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
                            c.Publish("foo", null, null, null);
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    // actually test a timeout, not no-responders.
                    c.SubscribeSync("foo");

                    Assert.Throws<NATSTimeoutException>(() => c.Request("foo", null, 50));
                }

                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = true;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    c.SubscribeSync("foo");
                    Assert.Throws<NATSTimeoutException>(() => c.Request("foo", null, 50));
                }

            }
        }

        [Fact]
        public void TestRequest()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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

        [Fact]
        public void TestRequestNoResponders()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    var sw = Stopwatch.StartNew();
                    Assert.Throws<NATSNoRespondersException>(() => c.Request("foo", null, 10000));
                    sw.Stop();

                    // make sure we didn't also time out.
                    Assert.True(sw.ElapsedMilliseconds < 5000);
                }

                // test old request style.
                opts.UseOldRequestStyle = true;
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    var sw = Stopwatch.StartNew();
                    Assert.Throws<NATSNoRespondersException>(() => c.Request("foo", null, 10000));
                    sw.Stop();

                    // make sure we didn't also time out.
                    Assert.True(sw.ElapsedMilliseconds < 5000);
                }
            }
        }

        private async void testRequestAsync(bool useOldRequestStyle, bool useMsgAPI)
        {
            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");
                    using (c.SubscribeAsync("foo", (obj, args) => args.Message.Respond(response)))
                    {
                        var tasks = new List<Task>();
                        for (int i = 0; i < 100; i++)
                        {
                            if (useMsgAPI)
                            {
                                tasks.Add(c.RequestAsync(new Msg("foo"), 10000));
                            }
                            else
                            {
                                tasks.Add(c.RequestAsync("foo", null, 10000));
                            }
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
            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
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
                            tasks.Add(c.RequestAsync("foo", request, 5, 5, 1000));
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
            testRequestAsync(useOldRequestStyle: false, useMsgAPI: false);
        }

        [Fact]
        public void TestRequestAsync_OldRequestStyle()
        {
            testRequestAsync(useOldRequestStyle: true, useMsgAPI: false);
        }

        [Fact]
        public void TestRequestAsyncMsg()
        {
            testRequestAsync(useOldRequestStyle: false, useMsgAPI: true);
        }

        [Fact]
        public void TestRequestAsyncMsg_OldRequestStyle()
        {
            testRequestAsync(useOldRequestStyle: true, useMsgAPI: true);
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

        private async void testRequestAsyncCancellation(bool useOldRequestStyle, bool useMsgAPI)
        {
            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c = Context.ConnectionFactory.CreateConnection(opts))
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
                            if (useMsgAPI)
                            {
                                tasks.Add(c.RequestAsync(new Msg("foo"), miscToken));
                            }
                            else
                            {
                                tasks.Add(c.RequestAsync("foo", null, miscToken));
                            }
                        }

                        foreach (Task<Msg> t in tasks)
                        {
                            Msg m = await t;
                            Assert.True(compare(m.Data, response), "Response isn't valid");
                        }
                    }

                    // test timeout, make sure we are somewhat close (for testing on stressed systems).
                    Stopwatch sw = Stopwatch.StartNew();
                    await Assert.ThrowsAsync<NATSNoRespondersException>(() => { return c.RequestAsync("no-responder", null, 10000, miscToken); });
                    sw.Stop();
                    Assert.True(sw.Elapsed.TotalMilliseconds < 9000);

                    sw.Reset();
                    sw.Start();
                    c.SubscribeSync("timeout");
                    await Assert.ThrowsAsync<NATSTimeoutException>(() => { return c.RequestAsync("timeout", null, 500, miscToken); });
                    sw.Stop();
                    Assert.True(sw.Elapsed.TotalMilliseconds > 500, "Elapsed millis are: " + sw.ElapsedMilliseconds);

                    
                    // test early cancellation
                    var cts = new CancellationTokenSource();
                    var ct = cts.Token;
                    cts.Cancel();
                    await Assert.ThrowsAnyAsync<OperationCanceledException>(() => { return c.RequestAsync("timeout", null, cts.Token); });

                    // test cancellation
                    cts = new CancellationTokenSource();
                    var ocex = Assert.ThrowsAnyAsync<OperationCanceledException>(() => { return c.RequestAsync("timeout", null, cts.Token); });
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
            testRequestAsyncCancellation(useOldRequestStyle: false, useMsgAPI: false);
            testRequestAsyncCancellation(useOldRequestStyle: false, useMsgAPI: true);
        }

        [Fact]
        public void TestRequestAsyncMsgCancellation()
        {
            testRequestAsyncCancellation(useOldRequestStyle: false, useMsgAPI: true);
        }

        [Fact]
        public void TestRequestAsyncCancellation_OldRequestStyle()
        {
            testRequestAsyncCancellation(useOldRequestStyle: true, useMsgAPI: false);
        }

        [Fact]
        public void TestRequestAsyncMsgCancellation_OldRequestStyle()
        {
            testRequestAsyncCancellation(useOldRequestStyle: true, useMsgAPI: true);
        }

        private async void testRequestAsyncTimeout(bool useOldRequestStyle, bool useMsgAPI)
        {
            using (var server = NATSServer.CreateFastAndVerify())
            {
                var sw = new Stopwatch();

                var opts = Context.GetTestOptionsWithDefaultTimeout();
                opts.AllowReconnect = false;
                opts.UseOldRequestStyle = useOldRequestStyle;
                using (var conn = Context.ConnectionFactory.CreateConnection(opts))
                {
                    // success condition
                    using (var sub = conn.SubscribeAsync("foo", (obj, args) => { args.Message.Respond(new byte[0]); }))
                    {
                        sw.Start();
                        if (useMsgAPI)
                        {
                            await conn.RequestAsync(new Msg("foo", new byte[0]), 5000);
                        }
                        else
                        {
                            await conn.RequestAsync("foo", new byte[0], 5000);
                        }
                        sw.Stop();
                        Assert.True(sw.ElapsedMilliseconds < 5000, "Unexpected timeout behavior");
                        sub.Unsubscribe();
                    }

                    // valid connection, but no response
                    conn.SubscribeSync("test");
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

                    // no responders
                    sw.Restart();
                    await Assert.ThrowsAsync<NATSNoRespondersException>(() => { return conn.RequestAsync("nosubs", new byte[0], 10000); });
                    sw.Stop();
                    Assert.True(sw.ElapsedMilliseconds < 9000);

                    server.Shutdown();
                    Thread.Sleep(500);
                    await Assert.ThrowsAsync<NATSConnectionClosedException>(() => { return conn.RequestAsync("test", new byte[0], 1000); });
                }
            }
        }

        [Fact]
        public void TestRequestAsyncTimeout()
        {
            testRequestAsyncTimeout(useOldRequestStyle: false, useMsgAPI: false);
        }

        [Fact]
        public void TestRequestAsyncMsgTimeout()
        {
            testRequestAsyncTimeout(useOldRequestStyle: false, useMsgAPI: true);
        }

        [Fact]
        public void TestRequestAsyncTimeout_OldRequestStyle()
        {
            testRequestAsyncTimeout(useOldRequestStyle: true, useMsgAPI: false);
        }

        [Fact]
        public void TestRequestAsyncMsgTimeout_OldRequestStyle()
        {
            testRequestAsyncTimeout(useOldRequestStyle: true, useMsgAPI: true);
        }

        private void TestRequestMsgWithHeader(bool useOldStyle)
        {
            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = useOldStyle;

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    bool validHeader = false;
                    byte[] response = Encoding.UTF8.GetBytes("I will help you.");

                    c.SubscribeAsync("foo", (obj, args) =>
                    {
                        var msg = args.Message;
                        if (msg.HasHeaders)
                        {
                            validHeader = "bar".Equals(msg.Header["foo"]);
                        }
                        msg.Respond(response);
                    });

                    Msg rmsg = new Msg();
                    rmsg.Subject = "foo";
                    rmsg.Data = Encoding.UTF8.GetBytes("help!");
                    rmsg.Header["foo"] = "bar";

                    Msg m = c.Request(rmsg, 5000);
                    Assert.True(compare(response, m.Data), "Response #1 isn't valid");

                    Assert.True(validHeader, "Header is not valid");

                    // test both APIs.
                    m = c.Request(rmsg);
                    Assert.True(compare(response, m.Data), "Response #2 isn't valid");
                }
            }
        }

        [Fact]
        public void TestRequestMessageWithHeader()
        {
            TestRequestMsgWithHeader(false);
        }

        [Fact]
        public void TestRequestMessageWithHeader_OldRequestStyle()
        {
            TestRequestMsgWithHeader(true);
        }

        [Fact]
        public void TestRequestMessageVarious()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    c.SubscribeAsync("foo", (obj, args) => args.Message.Respond(null));

                    // test no data
                    Msg m = c.Request(new Msg("foo"), 5000);
                    Assert.False(m.HasHeaders);
                    Assert.True(m.Data.Length == 0);

                    // test that the reply subject is ignored (no timeout)
                    c.Request(new Msg("foo", "bar", null), 5000);
                }
            }
        }

        [Fact]
        public void TestRequestMessageExceptions()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    c.SubscribeSync("foo");

                    // null msg
                    Assert.Throws<ArgumentNullException>(() => c.Request(null));

                    // no subject
                    Assert.Throws<NATSBadSubscriptionException>(() => c.Request(new Msg()));

                    // invalid timeout
                    Assert.Throws<ArgumentException>(() => c.Request(new Msg("foo"), 0));

                    // actual timeout
                    Assert.Throws<NATSTimeoutException>(() => c.Request(new Msg("foo"), 100));

                    // no responders
                    Assert.Throws<NATSNoRespondersException>(() => c.Request("bar", null));
                }
            }
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

#if NET46
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
            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c1 = Context.ConnectionFactory.CreateConnection(opts),
                               c2 = Context.ConnectionFactory.CreateConnection(opts))
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
        [Fact(Skip = "Manual")]
        public void TestRequestSafetyWithTasks()
        {
            testRequestSafetyWithTasks(useOldRequestStyle: false);
        }

        // This test is a useful comparison in determining the difference
        // between threads (above) and tasks and performance.  In some
        // environments, the NATS client will fail here, but succeed in the 
        // comparable test using threads.
        // Do not automatically run, for comparison purposes and future dev.
        [Fact(Skip = "Manual")]
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

            using (NATSServer.CreateFastAndVerify())
            {
                Options opts = Context.GetTestOptions();
                opts.UseOldRequestStyle = useOldRequestStyle;

                using (IConnection c1 = Context.ConnectionFactory.CreateConnection(opts),
                               c2 = Context.ConnectionFactory.CreateConnection(opts))
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (var c = Context.OpenConnection())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        c.Publish("foo", Encoding.UTF8.GetBytes("Hello"));
                    }

                    c.Flush();

                    Task.Run(() => c.Close()).Wait();
                }
            }
        }

        [Fact]
        public void TestCloseAndDispose()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    c.Close();
                }
            }
        }

        [Fact]
        public void TestInbox()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
                    using (var s1 = c.SubscribeAsync("foo"))
                    {
                        s1.MessageHandler += (sender, arg) => { };
                        s1.Start();

                        using (c.SubscribeSync("foo"))
                        {
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
            }
        }

        [Fact]
        public void TestRaceSafeStats()
        {
            using (NATSServer.CreateFast())
            {
                Thread.Sleep(1000);

                using (IConnection c = Context.OpenConnection())
                {
                    Task.Run(() => c.Publish("foo", null));

                    Thread.Sleep(1000);

                    Assert.Equal(1, c.Stats.OutMsgs);
                }
            }
        }

        [Fact]
        public void TestBadSubject()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
                {
                    string subject = "";
                    for (int i = 0; i < 1024; i++)
                    {
                        subject += "A";
                    }

                    string reply = "";
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
            using (NATSServer.CreateFastAndVerify())
            {
                using (IConnection c = Context.OpenConnection())
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
            string url1 = Context.Server2.Url;
            string url2 = Context.Server3.Url;
            string url3 = Context.Server4.Url;

            string urls = url1 + "," + url2 + "," + url3;

            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.Url = urls;
            Assert.Equal(opts.Servers[0], url1);
            Assert.Equal(opts.Servers[1], url2);
            Assert.Equal(opts.Servers[2], url3);
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
            using (NATSServer s1 = NATSServer.Create(Context.Server1.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer1.Port} --routes nats://127.0.0.1:{Context.ClusterServer2.Port}"),
                              s2 = NATSServer.Create(Context.Server2.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer2.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"),
                              s3 = NATSServer.Create(Context.Server3.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer3.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"),
                              s4 = NATSServer.Create(Context.Server4.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer4.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"),
                              s5 = NATSServer.Create(Context.Server5.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer5.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"),
                              s6 = NATSServer.Create(Context.Server6.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer6.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"),
                              s7 = NATSServer.Create(Context.Server7.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer7.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"))
            {
                var opts = Context.GetTestOptions(Context.Server3.Port);
                opts.NoRandomize = false;

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.True(assureClusterFormed(c, 7),
                        "Incomplete cluster with server count: " + c.Servers.Length);
                    c.Close();
                }

                // Create a new connection to start from scratch, and recieve 
                // the entire server list at once.
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.True(assureClusterFormed(c, 7),
                        "Incomplete cluster with server count: " + c.Servers.Length);

                    for (int i = 0; i < 50; i++)
                    {
                        using (var c2 = Context.ConnectionFactory.CreateConnection(opts))
                        {
                            Assert.True(assureClusterFormed(c, 7),
                                "Incomplete cluster with server count: " + c.Servers.Length);

                            // The first urls should be the same.
                            Assert.Equal(c.Servers[0], c2.Servers[0]);

                            c2.Close();
                        }
                    }

                    c.Close();
                }
            }
        }

        [Fact]
        public void TestAsyncInfoProtocolUpdate()
        {
            AutoResetEvent evReconnect = new AutoResetEvent(false);
            var opts = Context.GetTestOptions();
            opts.Url = $"nats://user:pass@127.0.0.1:{Context.Server3.Port}";
            string newUrl = null;

            opts.ReconnectedEventHandler = (obj, args) =>
            {
                newUrl = args.Conn.ConnectedUrl;
                evReconnect.Set();
            };

            // Specify localhost - the 127.0.0.1 should prevent one of the urls
            // from being added - for adding servers, 127.0.0.1 matches localhost.
            using (NATSServer s1 = NATSServer.Create(Context.Server3.Port, $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer5.Port} --routes nats://127.0.0.1:{Context.ClusterServer6.Port}"))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.True(c.Servers.Length == 1);
                    // check that credentials are stripped.
                    Assert.Equal($"nats://127.0.0.1:{Context.Server3.Port}", c.Servers[0]);

                    // build an independent cluster
                    using (NATSServer s2 = NATSServer.Create(Context.Server4.Port,
                        $"-a localhost --cluster nats://127.0.0.1:{Context.ClusterServer6.Port} --routes nats://127.0.0.1:{Context.ClusterServer5.Port}"))
                    {
                        // wait until the servers are routed and the conn has the updated
                        // server list.
                        assureClusterFormed(c, 2);

                        // Ensure the first server remains in place and has not been
                        // randomized.
                        Assert.Equal($"nats://127.0.0.1:{Context.Server3.Port}", c.Servers[0]);
                        Assert.True(c.Servers.Length == 2);
                        Assert.True(c.DiscoveredServers.Length == 1);


                        // sanity check to ensure we can connect to another server.
                        s1.Shutdown();
                        Assert.True(evReconnect.WaitOne(10000));
                        Assert.True(newUrl != null);
                        Assert.Contains(Context.Server4.Port.ToString(), c.ConnectedUrl);
                    }

                    c.Close();
                }
            }
        }

        [Fact]
        public void TestAsyncInfoProtocolPrune()
        {
            var opts = Context.GetTestOptions();
            opts.Url = $"nats://127.0.0.1:{Context.Server1.Port}";

            // Create a cluster of 3 nodes, then take one implicit server away
            // and add another.  The server removed should no longer be in the
            // discovered servers list.
            using (NATSServer s1 = NATSServer.Create(Context.Server1.Port, $"-a 127.0.0.1 --cluster nats://127.0.0.1:{Context.ClusterServer1.Port} --routes nats://127.0.0.1:{Context.ClusterServer2.Port}"),
                              s2 = NATSServer.Create(Context.Server2.Port, $"-a 127.0.0.1 --cluster nats://127.0.0.1:{Context.ClusterServer2.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"),
                              s3 = NATSServer.Create(Context.Server3.Port, $"-a 127.0.0.1 --cluster nats://127.0.0.1:{Context.ClusterServer3.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"))
            {
                // create a test connection to check for cluster formation.  This helps avoid
                // flappers in protocol messages being sent as the cluster forms.
                var tc = Context.ConnectionFactory.CreateConnection(opts);
                Assert.True(assureClusterFormed(tc, 3), "Incomplete cluster with server count: " + tc.Servers.Length);
                tc.Close();

                // Now add event handers to the options
                AutoResetEvent evDS = new AutoResetEvent(false);
                opts.ServerDiscoveredEventHandler = (o, a) => { evDS.Set(); };

                AutoResetEvent evRC = new AutoResetEvent(false);
                opts.ReconnectedEventHandler = (o, a) => { evRC.Set(); };

                // Connect again to test pruning.
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.True(c.Servers.Length == 3, "Unexpected server count");

                    // shutdown server 2
                    s2.Shutdown();

                    evDS.WaitOne(5000);

                    LinkedList<string> discoveredServers = new LinkedList<string>(c.DiscoveredServers);
                    Assert.True(discoveredServers.Count == 1);

                    string expectedServer = $"nats://127.0.0.1:{Context.Server3.Port}";
                    if (!discoveredServers.Contains(expectedServer))
                    {
                        foreach (string s in discoveredServers)
                        {
                            Console.WriteLine("\tDiscovered server:" + expectedServer);
                        }
                        Assert.True(false, "Discovered servers does not contain " + expectedServer);
                    }
                    evDS.Reset();

                    using (NATSServer s4 = NATSServer.Create(Context.Server4.Port,
                        $"-a 127.0.0.1 --cluster nats://127.0.0.1:{Context.ClusterServer4.Port} --routes nats://127.0.0.1:{Context.ClusterServer1.Port}"))
                    {
                        Assert.True(assureClusterFormed(c, 3), "Incomplete cluster with server count: " + c.Servers.Length);

                        // wait for the update with new server to check.
                        Assert.True(evDS.WaitOne(10000));

                        // The server on port 4223 should be pruned out.
                        //
                        // Discovered servers should contain:
                        // ["nats://127.0.0.1:4223",
                        //  "nats://127.0.0.1:4224"]
                        //
                        discoveredServers = new LinkedList<string>(c.DiscoveredServers);
                        Assert.True(discoveredServers.Count == 2);
                        Assert.DoesNotContain($"nats://127.0.0.1:{Context.Server2.Port}", discoveredServers);
                        Assert.Contains($"nats://127.0.0.1:{Context.Server3.Port}", discoveredServers);
                        Assert.Contains($"nats://127.0.0.1:{Context.Server4.Port}", discoveredServers);

                        // shutdown server 1 and wait for reconnect.
                        s1.Shutdown();
                        Assert.True(evRC.WaitOne(10000));
                        // Make sure we did NOT delete our expclitly configured server.
                        LinkedList<string> servers = new LinkedList<string>(c.Servers);
                        Assert.True(servers.Count == 3); // explicit server is still there.
                        Assert.Contains($"nats://127.0.0.1:{Context.Server1.Port}", servers);
                    }

                    c.Close();
                }
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
            var serverList = new[] {
                Context.DefaultServer.Url,
                "nats://localhost:2",
                "nats://localhost:3",
                "nats://localhost:4",
                "nats://localhsot:5",
                "nats://localhost:6",
                "nats://localhost:7"
            };

            var opts = Context.GetTestOptions();
            opts.Servers = serverList;
            opts.NoRandomize = true;

            using (NATSServer.CreateFastAndVerify())
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    Assert.True(listsEqual(serverList, c.Servers));
                    c.Close();
                }

                bool wasRandom = false;
                opts.NoRandomize = false;
                for (int i = 0; i < 10; i++)
                {
                    using (var c = Context.ConnectionFactory.CreateConnection(opts))
                    {
                        wasRandom = (listsEqual(serverList, c.Servers) == false);
                        c.Close();
                    }

                    if (wasRandom)
                        break;
                }

                Assert.True(wasRandom);

#if skip
            // Although the original intent was that if Opts.Url is
            // set, Opts.Servers is not (and vice versa), the behavior
            // is that Opts.Url is always first, even when randomization
            // is enabled. So make sure that this is still the case.
            opts.Url = $"nats://localhost:{Context.DefaultServer.Port}";
            for (int i = 0; i < 5; i++)
            {
                using(var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    wasRandom = (listsEqual(serverList, c.Servers) == false);
                    Assert.True(Equals(serverList[0], c.Servers[0]));
                    c.Close();
                }
                if (wasRandom)
                    break;
            }
#endif
            }
        }

        [Fact]
        public void TestSimpleUrlArgument()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                var o = ConnectionFactory.GetDefaultOptions();

                // simple url connect
                using (var cn = Context.ConnectionFactory.CreateConnection("127.0.0.1"))
                    cn.Close();

                // simple url
                o.Url = "127.0.0.1";
                using (Context.ConnectionFactory.CreateConnection(o)) { }

                // servers with a simple hostname
                o.Servers = new string[] { "127.0.0.1" };
                using (var cn = Context.ConnectionFactory.CreateConnection(o))
                    cn.Close();

                // simple url connect
                using (var cn = Context.ConnectionFactory.CreateConnection("127.0.0.1, localhost"))
                    cn.Close();

                //  url with multiple hosts
                o.Url = "127.0.0.1,localhost";
                using (Context.ConnectionFactory.CreateConnection(o)) { }

                // servers with multiple hosts
                o.Servers = new [] { "127.0.0.1", "localhost" };
                using (var cn = Context.ConnectionFactory.CreateConnection(o))
                    cn.Close();
            }
        }

        [Fact]
        public void TestNoEcho()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                long received = 0;
                var o = ConnectionFactory.GetDefaultOptions();
                o.NoEcho = true;

                using (var c = Context.ConnectionFactory.CreateConnection(o))
                {
                    using (c.SubscribeAsync("foo", (obj, args) => { Interlocked.Increment(ref received); }))
                    {
                        c.Publish("foo", null);
                        c.Flush();

                        // hate sleeping, but with slow CI's, we need to give time to
                        //make sure that message never arrives.
                        Thread.Sleep(1000);
                    }
                }

                Assert.True(Interlocked.Read(ref received) == 0);
            }
        }

        [Fact]
        public void TestServersOption()
        {
            var exception = Record.Exception(() => Context.ConnectionFactory.CreateConnection());

            Assert.IsType<NATSConnectionException>(exception);
        }

        /// <summary>
        /// Temporary test for upcoming jetstream functionality.  The jetstream server must be run
        /// manually and loaded per the issue listed below.  JetStream will remap reply subjects
        /// for requests, so additional code had to be added to handle this.  Future jetstream tests
        /// will cover this and this test should be replaced.
        ///
        /// # Start server with jetstream enabled.
        /// $ ./nats-server -js
        ///
        /// Create the stream
        /// $ jsm str create
        /// ? Stream Name foo
        /// ? Subjects to consume foo.*
        /// ? Storage backend memory
        /// ? Retention Policy Limits
        /// ? Message count limit -1
        /// ? Message size limit -1
        /// ? Maximum message age limit -1
        /// ? Maximum individual message size -1
        ///
        /// Create the server side consumer
        /// $ jsm con create
        /// ? Select a Stream foo
        /// ? Consumer name bar
        /// ? Delivery target
        /// ? Start policy (all, last, 1h, msg sequence) all
        /// ? Filter Stream by subject (blank for all)
        /// ? Maximum Allowed Deliveries -1
        ///
        /// Now manually run this test.
        /// 
        /// </summary>
        [Fact(Skip = "Manual")]
        public void TestJetStreamSubjectHandling()
        {
            // Start a NATS server with experimental -js feature and create a stream and consumer
            // as described in issue https://github.com/nats-io/nats.net/issues/364

            // publish a message into JS
            using (var c = new ConnectionFactory().CreateConnection())
            {
                c.Publish("foo.inc", Encoding.UTF8.GetBytes("hello"));
                c.Request("$JS.STREAM.foo.CONSUMER.bar.NEXT", Encoding.ASCII.GetBytes("1"), 1000);
            }
        }

        [Fact]
        public void TestMessageHeader()
        {
            using (NATSServer.CreateFastAndVerify())
            {
                var o = ConnectionFactory.GetDefaultOptions();

                using (var c = Context.ConnectionFactory.CreateConnection(o))
                {
                    using (var s = c.SubscribeSync("foo"))
                    {
                        // basic header test
                        var m = new Msg("foo");
                        m.Header["key"] = "value";
                        m.Subject = "foo";
                        m.Data = Encoding.UTF8.GetBytes("hello");

                        c.Publish(m);
                        var recvMsg = s.NextMessage(1000);
                        Assert.True(m.HasHeaders);
                        Assert.Equal("value", recvMsg.Header["key"]);

                        // assigning a message header
                        MsgHeader header = new MsgHeader();
                        header["foo"] = "bar";
                        m.Header = header;
                        c.Publish(m);
                        recvMsg = s.NextMessage(1000);
                        Assert.Equal("bar", recvMsg.Header["foo"]);

                        // assigning message header copy constructor
                        m.Header = new MsgHeader(header);
                        c.Publish(m);
                        recvMsg = s.NextMessage(1000);
                        Assert.Equal("bar", recvMsg.Header["foo"]);

                        // publish to the same subject w/o headers.
                        c.Publish("foo", null);
                        recvMsg = s.NextMessage(1000);

                        // reset headers and check that none are received.
                        m.Header = null;
                        c.Publish(m);
                        recvMsg = s.NextMessage(1000);
                        Assert.True(recvMsg.Header.Count == 0);

                        // try empty headers and check that none are received.
                        m.Header = new MsgHeader();
                        c.Publish(m);
                        recvMsg = s.NextMessage(1000);
                        Assert.True(recvMsg.Header.Count == 0);

                        // test with a reply subject
                        m.Header["foo"] = "bar";
                        m.Reply = "reply.subject";
                        c.Publish(m);
                        recvMsg = s.NextMessage(1000);
                        Assert.True(recvMsg.Header.Count == 1);

                    }
                }
            }
        }

        [Fact(Skip = "Manual")]
        public void TestMessageHeaderNoServerSupport()
        {
            //////////////////////////////////////////////////
            // Requires a running server w/o header support //
            //////////////////////////////////////////////////
            using (var c = new ConnectionFactory().CreateConnection())
            {
                Msg m = new Msg();
                m.Header["header"] = "value";
                m.Subject = "foo";

                Assert.Throws<NATSNotSupportedException>(() => c.Publish(m));

                // try w/o headers...
                m.Header = null;
                c.Publish(m);
            }
        }

        [Fact]
        public void TestMaxPayload()
        {
            using (NATSServer.CreateFast(Context.Server1.Port))
            {
                var opts = Context.GetQuietTestOptions(Context.Server1.Port);
                opts.AllowReconnect = false;

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    long maxPayload = c.ServerInfo.MaxPayload;
                    c.Publish("mptest", new byte[maxPayload-1]);
                    c.Publish("mptest", new byte[maxPayload]);
                }
                
                Assert.Throws<NATSMaxPayloadException>(() =>
                {
                    opts.ClientSideLimitChecks = true;
                    using (var c = Context.ConnectionFactory.CreateConnection(opts))
                    {
                        long maxPayload = c.ServerInfo.MaxPayload;
                        for (int x = 1; x < 10; x++)
                        {
                            c.Publish("mptest-ClientSideLimitChecks-true", new byte[maxPayload + x]);
                            Thread.Sleep(100);
                        }
                    }
                });
                
                Assert.Throws<NATSConnectionClosedException>(() =>
                {
                    opts.ClientSideLimitChecks = false;
                    using (var c = Context.ConnectionFactory.CreateConnection(opts))
                    {
                        long maxPayload = c.ServerInfo.MaxPayload;
                        for (int x = 1; x < 10; x++)
                        {
                            c.Publish("mptest-ClientSideLimitChecks-false", new byte[maxPayload + x]);
                            Thread.Sleep(100);
                        }
                    }
                });
            }
        }
    } // class

} // namespace
