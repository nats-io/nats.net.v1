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
using NATS.Client.Internals;
using NATS.Client.JetStream;
using UnitTests;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStreamPushSync : TestSuite<JetStreamPushSyncSuiteContext>
    {
        public TestJetStreamPushSync(JetStreamPushSyncSuiteContext context) : base(context)
        {
        }

        [Theory]
        [InlineData(null)]
        [InlineData(DELIVER)]
        public void TestJetStreamPushEphemeral(string deliverSubject)
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // publish some messages
                JsPublish(js, SUBJECT, 1, 5);

                // Build our subscription options.
                PushSubscribeOptions options = PushSubscribeOptions.Builder()
                    .WithDeliverSubject(deliverSubject)
                    .Build();

                // Subscription 1
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, options);
                AssertSubscription(sub, STREAM, null, deliverSubject, false);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                IList<Msg> messages1 = ReadMessagesAck(sub);
                int total = messages1.Count;
                ValidateRedAndTotal(5, messages1.Count, 5, total);

                // read again, nothing should be there
                IList<Msg> messages0 = ReadMessagesAck(sub);
                total += messages0.Count;
                ValidateRedAndTotal(0, messages0.Count, 5, total);
                
                sub.Unsubscribe();

                // Subscription 2
                sub = js.PushSubscribeSync(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // read what is available, same messages
                IList<Msg> messages2 = ReadMessagesAck(sub);
                total = messages2.Count;
                ValidateRedAndTotal(5, messages2.Count, 5, total);

                // read again, nothing should be there
                messages0 = ReadMessagesAck(sub);
                total += messages0.Count;
                ValidateRedAndTotal(0, messages0.Count, 5, total);

                AssertSameMessages(messages1, messages2);
            });
        }

        [Theory]
        [InlineData(null)]
        [InlineData(DELIVER)]
        public void TestJetStreamPushDurable(string deliverSubject)
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // publish some messages
                JsPublish(js, SUBJECT, 1, 5);

                // use ackWait so I don't have to wait forever before re-subscribing
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithAckWait(3000).Build();

                // Build our subscription options.
                PushSubscribeOptions options = PushSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .WithConfiguration(cc)
                    .WithDeliverSubject(deliverSubject)
                    .Build();

                // Subscribe.
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, options);
                AssertSubscription(sub, STREAM, DURABLE, deliverSubject, false);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // read what is available
                IList<Msg> messages = ReadMessagesAck(sub);
                int total = messages.Count;
                ValidateRedAndTotal(5, messages.Count, 5, total);

                // read again, nothing should be there
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(0, messages.Count, 5, total);

                sub.Unsubscribe();
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // re-subscribe
                sub = js.PushSubscribeSync(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // read again, nothing should be there
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(0, messages.Count, 5, total);
            });
        }

        [Fact]
        public void TestMessageWithHeadersOnly()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();
                
                // Build our subscription options.
                PushSubscribeOptions options = ConsumerConfiguration.Builder().WithHeadersOnly(true).BuildPushSubscribeOptions();

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                JsPublish(js, SUBJECT, 5);

                IList<Msg> messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);
                Assert.Empty(messages[0].Data);
                Assert.Equal("6", messages[0].Header[JetStreamConstants.MsgSizeHdr]);
            });
        }

        [Fact]
        public void TestAcks()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();
                
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithAckWait(1500).Build();

                // Build our subscription options.
                PushSubscribeOptions options = PushSubscribeOptions.Builder()
                    .WithConfiguration(cc)
                    .Build();

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // NAK
                JsPublish(js, SUBJECT, "NAK", 1);
                
                Msg m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("NAK1", Encoding.ASCII.GetString(m.Data));
                m.Nak();
                
                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("NAK1", Encoding.ASCII.GetString(m.Data));
                m.Ack();
                
                AssertNoMoreMessages(sub);

                // TERM
                JsPublish(js, SUBJECT, "TERM", 1);

                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("TERM1", Encoding.ASCII.GetString(m.Data));
                m.Term();
                
                AssertNoMoreMessages(sub);

                // Ack Wait timeout
                JsPublish(js, SUBJECT, "WAIT", 1);

                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("WAIT1", Encoding.ASCII.GetString(m.Data));
                Thread.Sleep(2000);
                m.Ack();
                
                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("WAIT1", Encoding.ASCII.GetString(m.Data));
                
                // In Progress
                JsPublish(js, SUBJECT, "PRO", 1);
                
                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("PRO1", Encoding.ASCII.GetString(m.Data));
                m.InProgress();
                Thread.Sleep(750);
                m.InProgress();
                Thread.Sleep(750);
                m.InProgress();
                Thread.Sleep(750);
                m.InProgress();
                Thread.Sleep(750);
                m.Ack();
                
                AssertNoMoreMessages(sub);

                // ACK Sync
                JsPublish(js, SUBJECT, "ACKSYNC", 1);
                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal("ACKSYNC1", Encoding.ASCII.GetString(m.Data));
                m.AckSync(DefaultTimeout);
                
                AssertNoMoreMessages(sub);
            });
        }

        [Fact]
        public void TestDeliveryPolicy()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT_STAR);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                string subjectA = SubjectDot("A");
                string subjectB = SubjectDot("B");

                js.Publish(subjectA, DataBytes(1));
                js.Publish(subjectA, DataBytes(2));
                Thread.Sleep(1500);
                js.Publish(subjectA, DataBytes(3));
                js.Publish(subjectB, DataBytes(91));
                js.Publish(subjectB, DataBytes(92));

                // DeliverPolicy.All
                PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverPolicy(DeliverPolicy.All).Build())
                        .Build();
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subjectA, pso);
                Msg m1 = sub.NextMessage(1000);
                AssertMessage(m1, 1);
                Msg m2 = sub.NextMessage(1000);
                AssertMessage(m2, 2);
                Msg m3 = sub.NextMessage(1000);
                AssertMessage(m3, 3);

                // DeliverPolicy.Last
                pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverPolicy(DeliverPolicy.Last).Build())
                        .Build();
                sub = js.PushSubscribeSync(subjectA, pso);
                Msg m = sub.NextMessage(1000);
                AssertMessage(m, 3);
                AssertNoMoreMessages(sub);

                // DeliverPolicy.New - No new messages between subscribe and next message
                pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverPolicy(DeliverPolicy.New).Build())
                        .Build();
                sub = js.PushSubscribeSync(subjectA, pso);
                AssertNoMoreMessages(sub);

                // DeliverPolicy.New - New message between subscribe and next message
                sub = js.PushSubscribeSync(subjectA, pso);
                js.Publish(subjectA, DataBytes(4));
                m = sub.NextMessage(1000);
                AssertMessage(m, 4);

                // DeliverPolicy.ByStartSequence
                pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(ConsumerConfiguration.Builder()
                                .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                                .WithStartSequence(3)
                                .Build())
                        .Build();
                sub = js.PushSubscribeSync(subjectA, pso);
                m = sub.NextMessage(1000);
                AssertMessage(m, 3);
                m = sub.NextMessage(1000);
                AssertMessage(m, 4);

                // DeliverPolicy.ByStartTime
                pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(ConsumerConfiguration.Builder()
                                .WithDeliverPolicy(DeliverPolicy.ByStartTime)
                                .WithStartTime(m3.MetaData.Timestamp.AddSeconds(-1))
                                .Build())
                        .Build();
                sub = js.PushSubscribeSync(subjectA, pso);
                m = sub.NextMessage(1000);
                AssertMessage(m, 3);
                m = sub.NextMessage(1000);
                AssertMessage(m, 4);

                // DeliverPolicy.LastPerSubject
                pso = PushSubscribeOptions.Builder()
                        .WithConfiguration(ConsumerConfiguration.Builder()
                                .WithDeliverPolicy(DeliverPolicy.LastPerSubject)
                                .WithFilterSubject(subjectA)
                                .Build())
                        .Build();
                sub = js.PushSubscribeSync(subjectA, pso);
                m = sub.NextMessage(1000);
                AssertMessage(m, 4);            
            });
        }
        
        private void AssertMessage(Msg m, int i) {
            Assert.NotNull(m);
            Assert.Equal(Data(i), Encoding.UTF8.GetString(m.Data));
        }

       [Fact]
        public void TestQueueSubWorkflow()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateMemoryStream(c, STREAM, SUBJECT);

                // Create our JetStream context.
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

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // create a durable that is not a queue
                PushSubscribeOptions pso1 = PushSubscribeOptions.Builder().WithDurable(Durable(1)).Build();
                js.PushSubscribeSync(SUBJECT, pso1);

                ArgumentException iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, pso1));
                Assert.Contains("[SUB-PB01]", iae.Message);

                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(1), pso1));
                Assert.Contains("[SUB-Q01]", iae.Message);

                PushSubscribeOptions pso21 = PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build();
                js.PushSubscribeSync(SUBJECT, Queue(21), pso21);

                PushSubscribeOptions pso22 = PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build();
                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(22), pso22));
                Assert.Contains("[SUB-Q03]", iae.Message);

                PushSubscribeOptions pso23 = PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build();
                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, pso23));
                Assert.Contains("[SUB-Q02]", iae.Message);

                PushSubscribeOptions pso3 = PushSubscribeOptions.Builder()
                        .WithDurable(Durable(3))
                        .WithDeliverGroup(Queue(31))
                        .Build();
                iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(32), pso3));
                Assert.Contains("[SUB-Q01]", iae.Message);

                // TODO TURN THESE ON AFTER SUB CHANGES DONE
                // ConsumerConfiguration ccFc = ConsumerConfiguration.Builder().WithFlowControl(1000).Build();
                // PushSubscribeOptions pso4 = PushSubscribeOptions.Builder()
                //     .WithDurable(Durable(4))
                //     .WithDeliverGroup(Queue(4))
                //     .WithConfiguration(ccFc)
                //     .Build();
                // iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(4), pso4));
                // Assert.Contains("[SUB-QM01]", iae.Message);
                //
                // ConsumerConfiguration ccHb = ConsumerConfiguration.Builder().WithIdleHeartbeat(1000).Build();
                // PushSubscribeOptions pso5 = PushSubscribeOptions.Builder()
                //     .WithDurable(Durable(5))
                //     .WithDeliverGroup(Queue(5))
                //     .WithConfiguration(ccHb)
                //     .Build();
                // iae = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, Queue(5), pso5));
                // Assert.Contains("[SUB-QM01]", iae.Message);
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
                catch (NATSTimeoutException)
                {
                    // timeout is acceptable, means no messages available.
                }
            }
        }
    }
}
