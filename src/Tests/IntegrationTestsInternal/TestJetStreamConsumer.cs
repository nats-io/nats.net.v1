// Copyright 2022-2023 The NATS Authors
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
using System.Threading;
using IntegrationTests;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.ClientExDetail;

namespace IntegrationTestsInternal
{
    public class TestJetStreamConsumer : TestSuite<JetStreamSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestJetStreamConsumer(ITestOutputHelper output, JetStreamSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
        }

        // ------------------------------------------------------------------------------------------
        // this allows me to intercept messages before it gets to the connection queue
        // which is before the messages is available for nextMessage or before
        // it gets dispatched to a handler.
        class OrderedTestDropSimulator : OrderedMessageManager
        {
            public OrderedTestDropSimulator(
                Connection conn, JetStream js, string stream, SubscribeOptions so, ConsumerConfiguration cc,
                bool queueMode, bool syncMode)
                : base(conn, js, stream, so, cc, queueMode, syncMode) {}

            protected override bool BeforeChannelAddCheck(Msg msg)
            {
                if (msg != null && msg.IsJetStream)
                {
                    ulong ss = msg.MetaData.StreamSequence;
                    ulong cs = msg.MetaData.ConsumerSequence;
                    if ((ss == 2 && cs == 2) || (ss == 5 && cs == 4))
                    {
                        return false;
                    }
                }

                return base.BeforeChannelAddCheck(msg);
            }
        }
        
        class HeartbeatErrorSimulator : PushMessageManager
        {
            public readonly CountdownEvent latch;

            public HeartbeatErrorSimulator(
                Connection conn, JetStream js, string stream, SubscribeOptions so, ConsumerConfiguration cc,
                bool queueMode, bool syncMode, CountdownEvent latch)
                : base(conn, js, stream, so, cc, queueMode, syncMode)
            {
                this.latch = latch;
            }

            internal override void HandleHeartbeatError()
            {
                base.HandleHeartbeatError();
                if (latch.CurrentCount > 0)
                {
                    latch.Signal(1);
                }
            }

            protected override bool BeforeChannelAddCheck(Msg msg)
            {
                return false;
            }
        }
        
        class OrderedHeartbeatErrorSimulator : OrderedMessageManager
        {
            public readonly CountdownEvent latch;

            public OrderedHeartbeatErrorSimulator(
                Connection conn, JetStream js, string stream, SubscribeOptions so, ConsumerConfiguration cc,
                bool queueMode, bool syncMode, CountdownEvent latch)
                : base(conn, js, stream, so, cc, queueMode, syncMode)
            {
                this.latch = latch;
            }

            internal override void HandleHeartbeatError()
            {
                base.HandleHeartbeatError();
                if (latch.CurrentCount > 0)
                {
                    latch.Signal(1);
                }
            }

            protected override bool BeforeChannelAddCheck(Msg msg)
            {
                return false;
            }
        }
        
        class PullHeartbeatErrorSimulator : PullMessageManager
        {
            public readonly CountdownEvent latch;

            public PullHeartbeatErrorSimulator(
                Connection conn, SubscribeOptions so, bool syncMode, CountdownEvent latch)
                : base(conn, so, syncMode)
            {
                this.latch = latch;
            }

            internal override void HandleHeartbeatError()
            {
                base.HandleHeartbeatError();
                if (latch.CurrentCount > 0)
                {
                    latch.Signal(1);
                }
            }

            protected override bool BeforeChannelAddCheck(Msg msg)
            {
                return false;
            }
        }

        // Expected consumer sequence numbers
        static ulong[] ExpectedConSeqNums = {1, 1, 2, 3, 1, 2};

        [Fact]
        public void TestOrderedConsumerSync()
        {
            Context.RunInJsServer(c =>
            {
                // Setup
                IJetStream js = c.CreateJetStreamContext();
                string subject = Subject(111);
                CreateMemoryStream(c, Stream(111), subject);

                // Get this in place before any subscriptions are made
                ((JetStream)js)._pushOrderedMessageManagerFactory = 
                    (conn, lJs, lStream, so, cc, queueMode, syncMode) => 
                        new OrderedTestDropSimulator(conn, lJs, lStream, so, cc, queueMode, syncMode);

                // The options will be used in various ways
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithOrdered(true).Build();
                
                // Test queue exception
                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(subject, QUEUE, pso));
                Assert.Contains(JsSubOrderedNotAllowOnQueues.Id, e.Message);

                // Setup sync subscription
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subject, pso);
                Thread.Sleep(1000);

                // Published messages will be intercepted by the OrderedTestDropSimulator
                JsPublish(js, subject, 101, 6);

                ulong expectedStreamSeq = 1;
                while (expectedStreamSeq <= 6) {
                    Msg m = sub.NextMessage(1000);
                    if (m != null) {
                        Assert.Equal(expectedStreamSeq, m.MetaData.StreamSequence);
                        Assert.Equal(ExpectedConSeqNums[expectedStreamSeq-1], m.MetaData.ConsumerSequence);
                        ++expectedStreamSeq;
                    }
                }

                sub.Unsubscribe();
                EnsureNotBound(sub);
            });
        }

        [Fact]
        public void TestOrderedConsumerAsync()
        {
            Context.RunInJsServer(c =>
            {
                // Setup
                IJetStream js = c.CreateJetStreamContext();
                string subject = Subject(222);
                CreateMemoryStream(c, Stream(222), subject);

                // Get this in place before any subscriptions are made
                ((JetStream)js)._pushOrderedMessageManagerFactory = 
                    (conn, lJs, lStream, so, cc, queueMode, syncMode) => 
                        new OrderedTestDropSimulator(conn, lJs, lStream, so, cc, queueMode, syncMode);

                // The options will be used in various ways
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithOrdered(true).Build();
                
                // Test queue exception
                void DummyTestHandler(object sender, MsgHandlerEventArgs args) { }
                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeAsync(subject, QUEUE, DummyTestHandler, false, pso));
                Assert.Contains(JsSubOrderedNotAllowOnQueues.Id, e.Message);
                
                // Setup async subscription
                CountdownEvent latch = new CountdownEvent(6);
                InterlockedInt received = new InterlockedInt();
                InterlockedLong[] ssFlags = new InterlockedLong[6];
                InterlockedLong[] csFlags = new InterlockedLong[6];

                void TestHandler(object sender, MsgHandlerEventArgs args)
                {
                    int i = received.Increment() - 1;
                    ssFlags[i] = new InterlockedLong((long)args.Message.MetaData.StreamSequence);
                    csFlags[i] = new InterlockedLong((long)args.Message.MetaData.ConsumerSequence);
                    latch.Signal();
                }

                js.PushSubscribeAsync(subject, TestHandler, false, pso);
                Thread.Sleep(1000);

                // Published messages will be intercepted by the OrderedTestDropSimulator
                JsPublish(js, subject, 101, 6);

                // wait for the messages
                latch.Wait(TimeSpan.FromMinutes(1));

                // Loop through the messages to make sure I get stream sequence 1 to 6
                for (int idx = 0; idx < 6; idx++)
                {
                    Assert.Equal((ulong)idx + 1, (ulong)ssFlags[idx].Read());
                    Assert.Equal(ExpectedConSeqNums[idx], (ulong)csFlags[idx].Read());
                }
            });
        }

        [Fact]
        public void TestHeartbeatError()
        {
            TestEventHandler handler = new TestEventHandler();
            Action<Options> modifier = options => options.HeartbeatAlarmEventHandler = handler.HeartbeatAlarmHandler;
            Context.RunInJsServer(modifier, c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();
                
                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                    .WithFlowControl(2000).WithIdleHeartbeat(100).Build();

                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                CountdownEvent latch = setupFactory(js);
                IJetStreamSubscription sub = js.PushSubscribeSync(SUBJECT, pso);
                validate(sub, handler, latch);
                
                latch = setupFactory(js);
                sub = js.PushSubscribeAsync(SUBJECT, (sender, a) => {}, false, pso);
                validate(sub, handler, latch);
                
                pso = PushSubscribeOptions.Builder().WithOrdered(true).WithConfiguration(cc).Build();
                latch = setupOrderedFactory(js);
                sub = js.PushSubscribeSync(SUBJECT, pso);
                validate(sub, handler, latch);
                
                latch = setupOrderedFactory(js);
                sub = js.PushSubscribeAsync(SUBJECT, (sender, a) => {}, false, pso);
                validate(sub, handler, latch);

                latch = setupPullFactory(js);
                IJetStreamPullSubscription lsub = js.PullSubscribe(SUBJECT, PullSubscribeOptions.DefaultPullOpts);
                lsub.Pull(PullRequestOptions.Builder(1).WithIdleHeartbeat(100).WithExpiresIn(2000).Build());
                validate(sub, handler, latch);
            });
        }
        
        private static void validate(IJetStreamSubscription sub, TestEventHandler handler, CountdownEvent latch)
        {
            latch.Wait(TimeSpan.FromSeconds(10));
            Assert.Equal(0, latch.CurrentCount);
            Assert.True(handler.HeartbeatAlarmEvents.Count > 0);
            handler.Reset();
        }
        
        private static CountdownEvent setupFactory(IJetStream js)
        {
            CountdownEvent latch = new CountdownEvent(2);
            ((JetStream)js)._pushMessageManagerFactory = 
                (conn, lJs, stream, so, serverCC, qmode, dispatcher) => 
                    new HeartbeatErrorSimulator(conn, lJs, stream, so, serverCC, qmode, dispatcher, latch);
            return latch;
        }
        
        private static CountdownEvent setupOrderedFactory(IJetStream js)
        {
            CountdownEvent latch = new CountdownEvent(2);
            ((JetStream)js)._pushOrderedMessageManagerFactory = 
                (conn, lJs, stream, so, serverCC, qmode, dispatcher) => 
                    new OrderedHeartbeatErrorSimulator(conn, lJs, stream, so, serverCC, qmode, dispatcher, latch);
            return latch;
        }
        
        private static CountdownEvent setupPullFactory(IJetStream js)
        {
            CountdownEvent latch = new CountdownEvent(2);
            ((JetStream)js)._pullMessageManagerFactory = 
                (conn, lJs, stream, so, serverCC, qmode, dispatcher) => 
                    new PullHeartbeatErrorSimulator(conn, so, false, latch);
            return latch;
        }
    }
}
