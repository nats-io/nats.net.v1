// Copyright 2022 The NATS Authors
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
    public class TestOrderedConsumer : TestSuite<JetStreamSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestOrderedConsumer(ITestOutputHelper output, JetStreamSuiteContext context) : base(context)
        {
            this.output = output;
        }

        // ------------------------------------------------------------------------------------------
        // this allows me to intercept messages before it gets to the connection queue
        // which is before the messages is available for nextMessage or before
        // it gets dispatched to a handler.
        class OrderedTestDropSimulator : PushMessageManager
        {
            public OrderedTestDropSimulator(
                Connection conn, SubscribeOptions so, ConsumerConfiguration cc, bool queueMode, bool syncMode)
                : base(conn, so, cc, queueMode, syncMode) { }

            protected override Msg BeforeChannelAddCheck(Msg msg)
            {
                msg = base.BeforeChannelAddCheck(msg);
                if (msg != null && msg.IsJetStream)
                {
                    ulong ss = msg.MetaData.StreamSequence;
                    ulong cs = msg.MetaData.ConsumerSequence;
                    if ((ss == 2 && cs == 2) || (ss == 5 && cs == 4))
                    {
                        return null;
                    }
                }

                return msg;
            }
        }
                
        // Expected consumer sequence numbers
        static ulong[] ExpectedConSeqNums = {1, 1, 2, 3, 1, 2};

        [Fact]
        public void TestOrderedConsumerSync()
        {
            Console.SetOut(new ConsoleWriter(output));

            Context.RunInJsServer(c =>
            {
                // Setup
                IJetStream js = c.CreateJetStreamContext();
                string subject = Subject(111);
                CreateMemoryStream(c, Stream(111), subject);

                // Get this in place before any subscriptions are made
                JetStream.PushMessageManagerFactoryImpl = 
                    (conn, so, cc, queueMode, syncMode) => 
                        new OrderedTestDropSimulator(conn, so, cc, queueMode, syncMode);

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

                ulong streamSeq = 1;
                while (streamSeq < 7) {
                    Msg m = sub.NextMessage(1000);
                    if (m != null) {
                        Assert.Equal(streamSeq, m.MetaData.StreamSequence);
                        Assert.Equal(ExpectedConSeqNums[streamSeq-1], m.MetaData.ConsumerSequence);
                        ++streamSeq;
                    }
                }

                sub.Unsubscribe();
                EnsureNotBound(sub);
            });
        }

        [Fact]
        public void TestOrderedConsumerAsync()
        {
            Console.SetOut(new ConsoleWriter(output));

            Context.RunInJsServer(c =>
            {
                // Setup
                IJetStream js = c.CreateJetStreamContext();
                string subject = Subject(222);
                CreateMemoryStream(c, Stream(222), subject);

                // Get this in place before any subscriptions are made
                JetStream.PushMessageManagerFactoryImpl = 
                    (conn, so, cc, queueMode, syncMode) => 
                        new OrderedTestDropSimulator(conn, so, cc, queueMode, syncMode);

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
                latch.Wait(TimeSpan.FromMinutes(5));

                // Loop through the messages to make sure I get stream sequence 1 to 6
                for (int idx = 0; idx < 6; idx++)
                {
                    Assert.Equal((ulong)idx, (ulong)ssFlags[idx].Read());
                    Assert.Equal(ExpectedConSeqNums[idx], (ulong)csFlags[idx].Read());
                }
            });
        }
        
    }
}
