// Copyright 2021 The NATS Authors
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
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
using UnitTests;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStreamPushAsync : TestSuite<JetStreamPushAsyncSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestJetStreamPushAsync(ITestOutputHelper output, JetStreamPushAsyncSuiteContext context) : base(context)
        {
            this.output = output;
        }

        [Fact]
        public void TestHandlerSub()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context to receive JetStream messages.
                IJetStream js = c.CreateJetStreamContext();

                // publish some messages
                JsPublish(js, SUBJECT, 10);

                CountdownEvent latch = new CountdownEvent(10);
                int received = 0;

                void TestHandler(object sender, MsgHandlerEventArgs args)
                {
                    received++;

                    if (args.Message.IsJetStream)
                    {
                        args.Message.Ack();
                    }

                    latch.Signal();
                }

                // Subscribe using the handler
                js.PushSubscribeAsync(SUBJECT, TestHandler, false);

                // Wait for messages to arrive using the countdown latch.
                latch.Wait();

                Assert.Equal(10, received);
            });
        }

        [Fact]
        public void TestHandlerAutoAck()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context to receive JetStream messages.
                IJetStream js = c.CreateJetStreamContext();

                // publish some messages
                JsPublish(js, SUBJECT, 10);

                // 1. auto ack true
                CountdownEvent latch1 = new CountdownEvent(10);
                int handlerReceived1 = 0;

                // create our message handler, does not ack
                void Handler1(object sender, MsgHandlerEventArgs args)
                {
                    handlerReceived1++;
                    latch1.Signal();
                }

                // subscribe using the handler, auto ack true
                PushSubscribeOptions pso1 = PushSubscribeOptions.Builder()
                    .WithDurable(Durable(1)).Build();
                js.PushSubscribeAsync(SUBJECT, Handler1, true, pso1);

                // wait for messages to arrive using the countdown latch.
                latch1.Wait();

                Assert.Equal(10, handlerReceived1);

                // check that all the messages were read by the durable
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, pso1);
                AssertNoMoreMessages(sub);

                // 2. auto ack false
                CountdownEvent latch2 = new CountdownEvent(10);
                int handlerReceived2 = 0;

                // create our message handler, also does not ack
                void Handler2(object sender, MsgHandlerEventArgs args)
                {
                    handlerReceived2++;
                    latch2.Signal();
                }

                // subscribe using the handler, auto ack false
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithAckWait(500).Build();
                PushSubscribeOptions pso2 = PushSubscribeOptions.Builder()
                    .WithDurable(Durable(2)).WithConfiguration(cc).Build();
                js.PushSubscribeAsync(SUBJECT, Handler2, false, pso2);

                // wait for messages to arrive using the countdown latch.
                latch2.Wait();
                Assert.Equal(10, handlerReceived2);

                Thread.Sleep(2000); // just give it time for the server to realize the messages are not ack'ed

                // check that we get all the messages again
                sub = js.PushSubscribeSync(SUBJECT, pso2);
                Assert.Equal(10, ReadMessagesAck(sub).Count);
            });
        }

       [Fact]
        public void TestQueueSubWorkflow()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context to receive JetStream messages.
                IJetStream js = c.CreateJetStreamContext();

                // Setup the subscribers
                // - the PushSubscribeOptions can be re-used since all the subscribers are the same
                // - use a concurrent integer to track all the messages received
                // - have a list of subscribers and threads so I can track them
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                InterlockedLong allReceived = new InterlockedLong();
                IList<JsQueueSubscriber> subscribers = new List<JsQueueSubscriber>();
                IList<Thread> subThreads = new List<Thread>();
                for (int id = 1; id <= 3; id++) {
                    // setup the subscription
                    IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, QUEUE, pso);
                    // create and track the runnable
                    JsQueueSubscriber qs = new JsQueueSubscriber(100, js, sub, allReceived);
                    subscribers.Add(qs);
                    // create, track and start the thread
                    Thread t = new Thread(qs.Run);
                    subThreads.Add(t);
                    t.Start();
                }
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // create and start the publishing
                Thread pubThread = new Thread(new JsPublisher(js, 100).Run);
                pubThread.Start();

                // wait for all threads to finish
                pubThread.Join(5000);
                foreach (Thread t in subThreads) {
                    t.Join(5000);
                }

                ISet<String> uniqueDatas = new HashSet<String>();
                // count
                int count = 0;
                foreach (JsQueueSubscriber qs in subscribers) {
                    int r = qs.received;
                    Assert.True(r > 0);
                    count += r;
                    foreach (string s in qs.datas) {
                        Assert.True(uniqueDatas.Add(s));
                    }
                }

                Assert.Equal(100, count);

            });
        }

        [Fact]
        public void TestQueueSubErrors()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context to receive JetStream messages.
                IJetStream js = c.CreateJetStreamContext();

                // create a durable that is not a queue
                PushSubscribeOptions pso1 = PushSubscribeOptions.Builder().WithDurable(Durable(1)).Build();
                js.PushSubscribeSync(SUBJECT, pso1);

                ArgumentException iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, pso1));
                String expected = $"Consumer [{Durable(1)}] is already bound to a subscription.";
                Assert.Equal(expected, iae.Message);

                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(1), pso1));
                expected = $"Existing consumer [{Durable(1)}] is not configured as a queue / deliver group.";
                Assert.Equal(expected, iae.Message);

                PushSubscribeOptions pso21 = PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build();
                js.PushSubscribeSync(SUBJECT, Queue(21), pso21);

                PushSubscribeOptions pso22 = PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build();
                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(22), pso22));
                expected = $"Existing consumer deliver group {Queue(21)} does not match requested queue / deliver group {Queue(22)}.";
                Assert.Equal(expected, iae.Message);

                PushSubscribeOptions pso23 = PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build();
                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, pso23));
                expected = $"Existing consumer [{Durable(2)}] is configured as a queue / deliver group.";
                Assert.Equal(expected, iae.Message);

                PushSubscribeOptions pso3 = PushSubscribeOptions.Builder()
                        .WithDurable(Durable(3))
                        .WithDeliverGroup(Queue(31))
                        .Build();
                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(32), pso3));
                expected = $"Consumer Configuration DeliverGroup [{Queue(31)}] must match the Queue Name [{Queue(32)}] if both are provided.";
                Assert.Equal(expected, iae.Message);
            });
        }
    }
    
    class JsPublisher
    {
        IJetStream js;
        int msgCount;

        public JsPublisher(IJetStream js, int msgCount)
        {
            this.js = js;
            this.msgCount = msgCount;
        }

        public void Run()
        {
            for (int x = 1; x <= msgCount; x++)
            {
                js.Publish(SUBJECT, Encoding.ASCII.GetBytes("Data # " + x));
            }
        }
    }

    class JsQueueSubscriber
    {
        int msgCount;
        IJetStream js;
        IJetStreamPushSyncSubscription sub;
        InterlockedLong allReceived;
        public int received;
        public IList<string> datas;

        public JsQueueSubscriber(int msgCount, IJetStream js, IJetStreamPushSyncSubscription sub, InterlockedLong allReceived)
        {
            this.msgCount = msgCount;
            this.js = js;
            this.sub = sub;
            this.allReceived = allReceived;
            received = 0;
            datas = new List<string>();
        }

        public void Run()
        {
            while (allReceived.Get() < msgCount)
            {
                try
                {
                    Msg msg = sub.NextMessage(500);
                    while (msg != null)
                    {
                        received++;
                        allReceived.Inc();
                        datas.Add(Encoding.UTF8.GetString(msg.Data));
                        msg.Ack();
                        msg = sub.NextMessage(500);
                    }
                }
                catch (NATSTimeoutException e)
                {
                    // timeout is acceptable, means no messages available.
                }
            }
        }
    }
}
