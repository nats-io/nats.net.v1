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
using System.IO;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using UnitTests;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.ClientExDetail;

namespace IntegrationTests
{
    public class TestJetStreamPushSync : TestSuite<JetStreamPushSyncSuiteContext>
    {
        public static ITestOutputHelper Output;

        public TestJetStreamPushSync(ITestOutputHelper output, JetStreamPushSyncSuiteContext context) : base(context)
        {
            Output = output;
        }

        public class ConsoleWriter : StringWriter
        {
            private ITestOutputHelper output;
            public ConsoleWriter(ITestOutputHelper output)
            {
                this.output = output;
            }

            public override void WriteLine(string m)
            {
                output.WriteLine(m);
            }
        }


        [Theory]
        [InlineData(null)]
        [InlineData(DELIVER)]
        public void TestJetStreamPushEphemeral(string deliverSubject)
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

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
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options normally
                PushSubscribeOptions options1 = PushSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .WithDeliverSubject(deliverSubject)
                    .Build();

                _testPushDurableSubSync(deliverSubject, c, js, () => js.PushSubscribeSync(SUBJECT, options1));
                _testPushDurableSubAsync(js, h => js.PushSubscribeAsync(SUBJECT, h, false, options1));

                // bind long form
                PushSubscribeOptions options2 = PushSubscribeOptions.Builder()
                    .WithStream(STREAM)
                    .WithDurable(DURABLE)
                    .WithBind(true)
                    .WithDeliverSubject(deliverSubject)
                    .Build();
                _testPushDurableSubSync(deliverSubject, c, js, () => js.PushSubscribeSync(null, options2));
                _testPushDurableSubAsync(js, h => js.PushSubscribeAsync(null, h, false, options2));

                // bind short form
                PushSubscribeOptions options3 = PushSubscribeOptions.BindTo(STREAM, DURABLE);
                _testPushDurableSubSync(deliverSubject, c, js, () => js.PushSubscribeSync(null, options3));
                _testPushDurableSubAsync(js, h => js.PushSubscribeAsync(null, h, false, options3));
            });
        }
        
        delegate IJetStreamPushSyncSubscription PushSyncSubSupplier();
        delegate IJetStreamPushAsyncSubscription PushAsyncSubSupplier(EventHandler<MsgHandlerEventArgs> handler);

        private void _testPushDurableSubSync(string deliverSubject, IConnection nc, IJetStream js, PushSyncSubSupplier supplier)
        {
            // publish some messages
            JsPublish(js, SUBJECT, 1, 5);

            IJetStreamPushSyncSubscription sub = supplier.Invoke();
            AssertSubscription(sub, STREAM, DURABLE, deliverSubject, false);

            // read what is available
            IList<Msg> messages = ReadMessagesAck(sub);
            int total = messages.Count;
            ValidateRedAndTotal(5, messages.Count, 5, total);

            // read again, nothing should be there
            messages = ReadMessagesAck(sub);
            total += messages.Count;
            ValidateRedAndTotal(0, messages.Count, 5, total);

            sub.Unsubscribe();
            nc.Flush(1000); // flush outgoing communication with/to the server

            // re-subscribe
            sub = supplier.Invoke();
            nc.Flush(1000); // flush outgoing communication with/to the server

            // read again, nothing should be there
            messages = ReadMessagesAck(sub);
            total += messages.Count;
            ValidateRedAndTotal(0, messages.Count, 5, total);

            sub.Unsubscribe();
            nc.Flush(1000); // flush outgoing communication with/to the server
        }

        private void _testPushDurableSubAsync(IJetStream js, PushAsyncSubSupplier supplier)
        {
            // publish some messages
            JsPublish(js, SUBJECT, 5);

            CountdownEvent latch = new CountdownEvent(5);
            int received = 0;

            void TestHandler(object sender, MsgHandlerEventArgs args)
            {
                received++;
                args.Message.Ack();
                latch.Signal();
            }

            // Subscribe using the handler
            IJetStreamPushAsyncSubscription sub = supplier.Invoke(TestHandler);

            // Wait for messages to arrive using the countdown latch.
            latch.Wait(10000);

            sub.Unsubscribe();

            Assert.Equal(5, received);
        }

        [Fact]
        public void TestMessageWithHeadersOnly()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();
                
                MsgHeader h = new MsgHeader();
                h["foo"] = "bar";
                js.Publish(new Msg(SUBJECT, h, DataBytes(1)));

                // Build our subscription options.
                PushSubscribeOptions options = ConsumerConfiguration.Builder()
                    .WithHeadersOnly(true).BuildPushSubscribeOptions();

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                Msg m = sub.NextMessage(1000);
                Assert.Empty(m.Data);
                Assert.True(m.HasHeaders);
                Assert.Equal("bar", m.Header["foo"]);
                Assert.Equal("6", m.Header[JetStreamConstants.MsgSizeHeader]);

                sub.Unsubscribe();

                // without headers only
                sub = js.PushSubscribeSync(SUBJECT);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server
                m = sub.NextMessage(1000);
                Assert.Equal(6, m.Data.Length);
                Assert.True(m.HasHeaders);
                Assert.Equal("bar", m.Header["foo"]);
                Assert.Null(m.Header[JetStreamConstants.MsgSizeHeader]);
            });
        }

        [Fact]
        public void TestAcks()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

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
        public void TestPushSyncFlowControl()
        {
            InterlockedInt fcps = new InterlockedInt();
            
            Action<Options> optionsModifier = opts =>
            {
                opts.FlowControlProcessedEventHandler = (sender, args) =>
                {
                    fcps.Increment();
                };
            };
            
            Context.RunInJsServer(new TestServerInfo(TestSeedPorts.AutoPort.Increment()), optionsModifier, c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();
                
                byte[] data = new byte[8192];

                int MSG_COUNT = 1000;
                
                for (int x = 100_000; x < MSG_COUNT + 100_000; x++) {
                    byte[] fill = Encoding.ASCII.GetBytes(""+ x);
                    Array.Copy(fill, 0, data, 0, 6);
                    js.Publish(new Msg(SUBJECT, data));
                }
                
                InterlockedInt count = new InterlockedInt();
                HashSet<string> set = new HashSet<string>();
                
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithFlowControl(1000).Build();
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();

                IJetStreamPushSyncSubscription ssub = js.PushSubscribeSync(SUBJECT, pso);
                for (int x = 0; x < MSG_COUNT; x++) {
                    Msg msg = ssub.NextMessage(1000);
                    byte[] fill = new byte[6];
                    Array.Copy(msg.Data, 0, fill, 0, 6);
                    string id = Encoding.ASCII.GetString(fill);
                    if (set.Add(id)) {
                        count.Increment();
                    }
                    msg.Ack();
                }

                Assert.Equal(MSG_COUNT, count.Read());
                Assert.True(fcps.Read() > 0);

                // coverage for subscribe options heartbeat directly
                cc = ConsumerConfiguration.Builder().WithIdleHeartbeat(0).Build();
                pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                js.PushSubscribeSync(SUBJECT, pso);
            });
        }

        [Fact]
        public void TestOrdered() {
            Console.SetOut(new ConsoleWriter(Output));

            Context.RunInJsServer(c =>
            {
                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                CreateMemoryStream(c, STREAM, Subject(1), Subject(2));
                
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithOrdered(true).Build();
                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, QUEUE, pso));
                Assert.Contains(JsSubOrderedNotAllowOnQueues.Id, e.Message);

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(Subject(1), pso);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // set the interceptor for this subscription
                JetStreamOrderedPushSyncSubscription orderedSub = (JetStreamOrderedPushSyncSubscription)sub;
                TestBeforeChannelAddCheck check = new TestBeforeChannelAddCheck();
                orderedSub._____setTestingInterceptor(check.Before());

                // publish after interceptor is set before messages come in
                JsPublish(js, Subject(1), 3);

                // message 1
                Msg m = sub.NextMessage(1000); // use duration version here for coverage
                Assert.Equal(1U, m.MetaData.StreamSequence);
                Assert.Equal(1U, m.MetaData.ConsumerSequence);

                // drop 2
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(1000));

                // message 2
                m = sub.NextMessage(500);
                Assert.Equal(2U, m.MetaData.StreamSequence);
                Assert.Equal(1U, m.MetaData.ConsumerSequence);

                // message 3
                m = sub.NextMessage(500);
                Assert.Equal(3U, m.MetaData.StreamSequence);
                Assert.Equal(2U, m.MetaData.ConsumerSequence);

                // set the interceptor for this subscription
                check = new TestBeforeChannelAddCheck();
                orderedSub._____setTestingInterceptor(check.Before());
                
                // publish after interceptor is set before messages come in
                JsPublish(js, Subject(1), 3);
                
                // message 4
                m = sub.NextMessage(1000); // use duration version here for coverage
                Assert.Equal(4U, m.MetaData.StreamSequence);
                Assert.Equal(3U, m.MetaData.ConsumerSequence);

                // drop 5
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(1000));

                // message 5
                m = sub.NextMessage(500);
                Assert.Equal(5U, m.MetaData.StreamSequence);
                Assert.Equal(1U, m.MetaData.ConsumerSequence);

                // message 6
                m = sub.NextMessage(500);
                Assert.Equal(6U, m.MetaData.StreamSequence);
                Assert.Equal(2U, m.MetaData.ConsumerSequence);

                sub.Unsubscribe();
                
                // ----------------------------------------------------------------------------------------------------
                // THIS IS ACTUALLY TESTING ASYNC SO I DON'T HAVE TO SETUP THE INTERCEPTOR IN OTHER CODE
                // ----------------------------------------------------------------------------------------------------

                CountdownEvent latch = new CountdownEvent(3);
                InterlockedInt received = new InterlockedInt();
                InterlockedLong[] ssFlags = new InterlockedLong[3];
                InterlockedLong[] csFlags = new InterlockedLong[3];

                EventHandler<MsgHandlerEventArgs> handler = (sender, args) =>
                {
                    int i = received.Increment() - 1;
                    ssFlags[i] = new InterlockedLong((long)args.Message.MetaData.StreamSequence);
                    csFlags[i] = new InterlockedLong((long)args.Message.MetaData.ConsumerSequence);
                    latch.Signal();
                };

                IJetStreamPushAsyncSubscription asyncSub = js.PushSubscribeAsync(Subject(2), handler, false, pso);
                
                JetStreamOrderedPushAsyncSubscription orderedAsyncSub = (JetStreamOrderedPushAsyncSubscription)asyncSub;
                check = new TestBeforeChannelAddCheck();
                orderedAsyncSub._____setTestingInterceptor(check.Before());
                                
                // publish after interceptor is set before messages come in
                JsPublish(js, Subject(2), 3);

                // wait for messages to arrive using the countdown latch.
                // make sure we don't wait forever
                latch.Wait(10000);

                Assert.Equal(7, ssFlags[0].Read());
                Assert.Equal(1, csFlags[0].Read());

                Assert.Equal(8, ssFlags[1].Read());
                Assert.Equal(1, csFlags[1].Read());

                Assert.Equal(9, ssFlags[2].Read());
                Assert.Equal(2, csFlags[2].Read());

            });
        }

        class TestBeforeChannelAddCheck
        {
            private readonly InterlockedInt drop = new InterlockedInt();

            // create our message handler, does not ack
            public Func<Msg, Msg> Before() =>
                m =>
                {
                    if (m.HasStatus)
                    {
                        return null;
                    }

                    if (m.IsJetStream)
                    {
                        if (drop.Increment() == 2)
                        {
                            return null;
                        }
                    }

                    return m;
                };
        }
    }
}
