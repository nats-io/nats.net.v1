// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Diagnostics;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.JetStream.BaseConsumeOptions;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestSimplification : TestSuite<OneServerSuiteContext>
    {
        public TestSimplification(OneServerSuiteContext context) : base(context) {}

        private bool RunTest(ServerInfo si)
        {
            return si.IsSameOrNewerThanVersion("2.9.1");
        }
        
        [Fact]
        public void TestStreamContext()
        {
            Context.RunInJsServer(si => RunTest(si), c => {
                string stream = Stream(Nuid.NextGlobal());
                string subject = Subject(Nuid.NextGlobal());

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
    
                Assert.Throws<NATSJetStreamException>(() => c.CreateStreamContext(stream));
                Assert.Throws<NATSJetStreamException>(() => c.CreateStreamContext(stream, JetStreamOptions.DefaultJsOptions));
                Assert.Throws<NATSJetStreamException>(() => js.CreateStreamContext(stream));
    
                CreateMemoryStream(jsm, stream, subject);
                IStreamContext streamContext = c.CreateStreamContext(stream);
                Assert.Equal(stream, streamContext.StreamName);
                _TestStreamContext(stream, subject, streamContext, js);

                jsm.DeleteStream(stream);
                
                CreateMemoryStream(jsm, stream, subject);
                streamContext = js.CreateStreamContext(stream);
                Assert.Equal(stream, streamContext.StreamName);
                _TestStreamContext(stream, subject, streamContext, js);

            });
        }

        private static void _TestStreamContext(string expectedStreamName, string subject, IStreamContext streamContext, IJetStream js)
        {
            Assert.Throws<NATSJetStreamException>(() => streamContext.CreateConsumerContext(Nuid.NextGlobal()));
            Assert.Throws<NATSJetStreamException>(() => streamContext.DeleteConsumer(Nuid.NextGlobal()));

            string durable = Nuid.NextGlobal();
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(durable).Build();
            IConsumerContext consumerContext = streamContext.CreateOrUpdateConsumer(cc);
            ConsumerInfo ci = consumerContext.GetConsumerInfo();
            Assert.Equal(expectedStreamName, ci.Stream);
            Assert.Equal(durable, ci.Name);

            ci = streamContext.GetConsumerInfo(durable);
            Assert.NotNull(ci);
            Assert.Equal(expectedStreamName, ci.Stream);
            Assert.Equal(durable, ci.Name);

            Assert.Equal(1, streamContext.GetConsumerNames().Count);

            Assert.Equal(1, streamContext.GetConsumers().Count);
            Assert.NotNull(streamContext.CreateConsumerContext(durable));
            streamContext.DeleteConsumer(durable);

            Assert.Throws<NATSJetStreamException>(() => streamContext.CreateConsumerContext(durable));
            Assert.Throws<NATSJetStreamException>(() => streamContext.DeleteConsumer(durable));

            // coverage
            js.Publish(subject, Encoding.UTF8.GetBytes("one"));
            js.Publish(subject, Encoding.UTF8.GetBytes("two"));
            js.Publish(subject, Encoding.UTF8.GetBytes("three"));
            js.Publish(subject, Encoding.UTF8.GetBytes("four"));
            js.Publish(subject, Encoding.UTF8.GetBytes("five"));
            js.Publish(subject, Encoding.UTF8.GetBytes("six"));

            Assert.True(streamContext.DeleteMessage(3));
            Assert.True(streamContext.DeleteMessage(4, true));

            MessageInfo mi = streamContext.GetMessage(1);
            Assert.Equal(1U, mi.Sequence);

            mi = streamContext.GetFirstMessage(subject);
            Assert.Equal(1U, mi.Sequence);

            mi = streamContext.GetLastMessage(subject);
            Assert.Equal(6U, mi.Sequence);

            mi = streamContext.GetNextMessage(3, subject);
            Assert.Equal(5U, mi.Sequence);

            Assert.NotNull(streamContext.GetStreamInfo());
            Assert.NotNull(streamContext.GetStreamInfo(StreamInfoOptions.Builder().Build()));

            streamContext.Purge(PurgeOptions.Builder().WithSequence(5).Build());
            Assert.Throws<NATSJetStreamException>(() => streamContext.GetMessage(1));

            mi = streamContext.GetFirstMessage(subject);
            Assert.Equal(5U, mi.Sequence);

            streamContext.Purge();
            Assert.Throws<NATSJetStreamException>(() => streamContext.GetFirstMessage(subject));
        }

        [Fact]
        public void TestFetch() {
            Context.RunInJsServer(si => RunTest(si), c => {
                string stream = Stream(Nuid.NextGlobal());
                string subject = Subject(Nuid.NextGlobal());

                CreateMemoryStream(c, stream, subject);
                IJetStream js = c.CreateJetStreamContext();
                for (int x = 1; x <= 20; x++) {
                    js.Publish(subject, Encoding.UTF8.GetBytes("test-fetch-msg-" + x));
                }
    
                // 1. Different fetch sizes demonstrate expiration behavior
    
                // 1A. equal number of messages than the fetch size
                _testFetch(stream, "1A", c, 20, 0, 20);
    
                // 1B. more messages than the fetch size
                _testFetch(stream, "1B", c, 10, 0, 10);
    
                // 1C. fewer messages than the fetch size
                _testFetch(stream, "1C", c, 40, 0, 40);
    
                // 1D. simple-consumer-40msgs was created in 1C and has no messages available
                _testFetch(stream, "1D", c, 40, 0, 40);
    
                // 2. Different max bytes sizes demonstrate expiration behavior
                //    - each test message is approximately 150 bytes
    
                // 2A. max bytes is reached before message count
                _testFetch(stream, "2A", c, 0, 750, 20);
    
                // 2B. fetch size is reached before byte count
                _testFetch(stream, "2B", c, 10, 1600, 10);
    
                // 2C. fewer bytes than the byte count
                _testFetch(stream, "2C", c, 0, 3500, 40);
            });
        }
    
        private static void _testFetch(string streamName, string label, IConnection c, int maxMessages, int maxBytes, int testAmount) {
            IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
            IJetStream js = c.CreateJetStreamContext();

            string name = generateConsumerName(maxMessages, maxBytes);
    
            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(name).Build();
            ConsumerInfo ci = jsm.AddOrUpdateConsumer(streamName, cc);
    
            // Consumer[Context]
            IConsumerContext consumerContext = js.CreateConsumerContext(streamName, name);
    
            // Custom consume options
            FetchConsumeOptions.FetchConsumeOptionsBuilder builder = FetchConsumeOptions.Builder().WithExpiresIn(2000);
            if (maxMessages == 0) {
                builder.WithMaxBytes(maxBytes);
            }
            else if (maxBytes == 0) {
                builder.WithMaxMessages(maxMessages);
            }
            else {
                builder.WithMax(maxBytes, maxMessages);
            }
            FetchConsumeOptions fetchConsumeOptions = builder.Build();

            Stopwatch sw = Stopwatch.StartNew();
    
            // create the consumer then use it
            int rcvd = 0;
            using (IFetchConsumer consumer = consumerContext.Fetch(fetchConsumeOptions))
            {
                Msg msg = consumer.NextMessage();
                while (msg != null)
                {
                    ++rcvd;
                    msg.Ack();
                    msg = consumer.NextMessage();
                }

                sw.Stop();
            }

            switch (label) {
                case "1A":
                case "1B":
                case "2B":
                    Assert.Equal(testAmount, rcvd);
                    Assert.True(sw.ElapsedMilliseconds < 100);
                    break;
                case "1C":
                case "1D":
                case "2C":
                    Assert.True(rcvd < testAmount);
                    Assert.True(sw.ElapsedMilliseconds >= 1500);
                    break;
                case "2A":
                    Assert.True(rcvd < testAmount);
                    Assert.True(sw.ElapsedMilliseconds < 100);
                    break;
            }
        }
    
        private static string generateConsumerName(int maxMessages, int maxBytes) {
            return maxBytes == 0
                ? Name(Nuid.NextGlobal()) + "-" + maxMessages + "msgs"
                : Name(Nuid.NextGlobal()) + "-" + maxBytes + "bytes-" + maxMessages + "msgs";
        }
    
        [Fact]
        public void TestIterableConsumer()
        {
            Context.RunInJsServer(si => RunTest(si), c => {
                string streamName = Stream(Nuid.NextGlobal());
                string subject = Subject(Nuid.NextGlobal());
                string durable = Nuid.NextGlobal();

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateMemoryStream(jsm, streamName, subject);

                IJetStream js = c.CreateJetStreamContext();
        
                // Pre define a consumer
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(durable).Build();
                jsm.AddOrUpdateConsumer(streamName, cc);

                // Consumer[Context]
                IConsumerContext consumerContext = js.CreateConsumerContext(streamName, durable);
    
                int stopCount = 500;
                // create the consumer then use it
                using (IIterableConsumer consumer = consumerContext.StartIterate())
                {
                    _testIterable(js, stopCount, consumer, subject);
                }
                    
                // coverage
                IIterableConsumer consumer2 = consumerContext.StartIterate(ConsumeOptions.DefaultConsumeOptions);
                consumer2.Dispose();
            });
        }
    
        // [Fact]
        // public void TestOrderedIterableConsumerBasic() 
        // {
            // Context.RunInJsServer(si => RunTest(si), c => {
                // string streamName = Stream(Nuid.NextGlobal());
                // string subject = Subject(Nuid.NextGlobal());
                // string durable = Nuid.NextGlobal();

                // IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                // CreateMemoryStream(jsm, streamName, subject);
                // IJetStream js = c.CreateJetStreamContext();
                // IStreamContext sc = c.CreateStreamContext(streamName);
    
                // int stopCount = 500;
                // OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().WithFilterSubject(subject);
                // using (IIterableConsumer consumer = sc.StartOrderedIterate(occ)) {
                    // _testIterable(js, stopCount, consumer, subject);
                // }
            // });
        // }

        private static void _testIterable(IJetStream js, int stopCount, IIterableConsumer consumer, string subject)
        {
            InterlockedInt count = new InterlockedInt();
            Thread consumeThread = new Thread(() =>
            {
                try
                {
                    Msg msg;
                    while (count.Read() < stopCount)
                    {
                        msg = consumer.NextMessage(1000);
                        if (msg != null)
                        {
                            msg.Ack();
                            count.Increment();
                        }
                    }

                    Thread.Sleep(50); // allows more messages to come across
                    consumer.Stop(200);

                    msg = consumer.NextMessage(1000);
                    while (msg != null)
                    {
                        msg.Ack();
                        count.Increment();
                        msg = consumer.NextMessage(1000);
                    }
                }
                catch (NATSTimeoutException)
                {
                    // this is expected
                }
            });
            consumeThread.Start();

            Publisher publisher = new Publisher(js, subject, 1);
            Thread pubThread = new Thread(publisher.Run);
            pubThread.Start();

            consumeThread.Join();
            publisher.Stop();
            pubThread.Join();

            Assert.True(count.Read() > 500);
        }

        [Fact]
        public void TestConsumeWithHandler()
        {
            Context.RunInJsServer(si => RunTest(si), c => {
                string streamName = Stream(Nuid.NextGlobal());
                string subject = Subject(Nuid.NextGlobal());
                string durable = Nuid.NextGlobal();

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateMemoryStream(jsm, streamName, subject);
                IJetStream js = c.CreateJetStreamContext();
                JsPublish(js, subject, 2500);
    
                // Pre define a consumer
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(durable).Build();
                jsm.AddOrUpdateConsumer(streamName, cc);
    
                // Consumer[Context]
                IConsumerContext consumerContext = js.CreateConsumerContext(streamName, durable);
    
                CountdownEvent latch = new CountdownEvent(500);
                EventHandler<MsgHandlerEventArgs> handler = (s, e) => {
                    e.Message.Ack();
                    latch.Signal();
                };

                using (IMessageConsumer consumer = consumerContext.StartConsume(handler))
                {
                    latch.Wait(10_000);
                    consumer.Stop(200);
                    Assert.Equal(0, latch.CurrentCount);
                }
            });
        }
    
        [Fact]
        public void TestNext() {
            Context.RunInJsServer(si => RunTest(si), c => {
                string streamName = Stream(Nuid.NextGlobal());
                string subject = Subject(Nuid.NextGlobal());
                string durable = Nuid.NextGlobal();

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateMemoryStream(jsm, streamName, subject);
                IJetStream js = c.CreateJetStreamContext();
                JsPublish(js, subject, 2);
    
                // Pre define a consumer
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(durable).Build();
                jsm.AddOrUpdateConsumer(streamName, cc);
    
                // Consumer[Context]
                IConsumerContext consumerContext = js.CreateConsumerContext(streamName, durable);
    
                Assert.Throws<ArgumentException>(() => consumerContext.Next(1));
                Assert.NotNull(consumerContext.Next(1000));
                Assert.NotNull(consumerContext.Next());
                Assert.Null(consumerContext.Next(1000));
            });
        }
    
        [Fact]
        public void TestCoverage() {
            string stream = Stream(Nuid.NextGlobal());
            string subject = Stream(Nuid.NextGlobal());
            string durable1 = Nuid.NextGlobal();
            string durable2 = Nuid.NextGlobal();
            string durable3 = Nuid.NextGlobal();
            string durable4 = Nuid.NextGlobal();
            string durable5 = Nuid.NextGlobal();
            string durable6 = Nuid.NextGlobal();

            Context.RunInJsServer(si => RunTest(si), c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateMemoryStream(jsm, stream, subject);
                IJetStream js = c.CreateJetStreamContext();
    
                // Pre define a consumer
                jsm.AddOrUpdateConsumer(stream, ConsumerConfiguration.Builder().WithDurable(durable1).Build());
                jsm.AddOrUpdateConsumer(stream, ConsumerConfiguration.Builder().WithDurable(durable2).Build());
                jsm.AddOrUpdateConsumer(stream, ConsumerConfiguration.Builder().WithDurable(durable3).Build());
                jsm.AddOrUpdateConsumer(stream, ConsumerConfiguration.Builder().WithDurable(durable4).Build());
    
                // Stream[Context]
                IStreamContext sctx1 = c.CreateStreamContext(stream);
                c.CreateStreamContext(stream, JetStreamOptions.DefaultJsOptions);
                js.CreateStreamContext(stream);
    
                // Consumer[Context]
                IConsumerContext cctx1 = c.CreateConsumerContext(stream, durable1);
                IConsumerContext cctx2 = c.CreateConsumerContext(stream, durable2, JetStreamOptions.DefaultJsOptions);
                IConsumerContext cctx3 = js.CreateConsumerContext(stream, durable3);
                IConsumerContext cctx4 = sctx1.CreateConsumerContext(durable4);
                IConsumerContext cctx5 = sctx1.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(durable5).Build());
                IConsumerContext cctx6 = sctx1.CreateOrUpdateConsumer(ConsumerConfiguration.Builder().WithDurable(durable6).Build());
    
                closeConsumer(cctx1.StartIterate(), durable1, true);
                closeConsumer(cctx2.StartIterate(ConsumeOptions.DefaultConsumeOptions), durable2, true);
                
                closeConsumer(cctx3.StartConsume((s, e) => {}), durable3, true);
                closeConsumer(cctx4.StartConsume((s, e) => {}, ConsumeOptions.DefaultConsumeOptions), durable4, true);
                
                closeConsumer(cctx5.FetchMessages(1), durable5, false);
                closeConsumer(cctx6.FetchBytes(1000), durable6, false);
            });
        }
    
        private void closeConsumer(IMessageConsumer con, string name, bool doStop) {
            ConsumerInfo ci = con.GetConsumerInformation();
            Assert.Equal(name, ci.Name);
            if (doStop) {
                con.Stop(100);
            }
        }

        [Fact]
        public void TestFetchConsumeOptionsBuilder() {
            FetchConsumeOptions fco = FetchConsumeOptions.Builder().Build();
            Assert.Equal(DefaultMessageCount, fco.MaxMessages);
            Assert.Equal(DefaultExpiresInMillis, fco.ExpiresIn);
            Assert.Equal(DefaultThresholdPercent, fco.ThresholdPercent);
            Assert.Equal(0, fco.MaxBytes);
            Assert.Equal(DefaultExpiresInMillis * MaxIdleHeartbeatPercent / 100, fco.IdleHeartbeat);
    
            fco = FetchConsumeOptions.Builder().WithMaxMessages(1000).Build();
            Assert.Equal(1000, fco.MaxMessages);
            Assert.Equal(0, fco.MaxBytes);
            Assert.Equal(DefaultThresholdPercent, fco.ThresholdPercent);
    
            fco = FetchConsumeOptions.Builder().WithMaxMessages(1000).WithThresholdPercent(50).Build();
            Assert.Equal(1000, fco.MaxMessages);
            Assert.Equal(0, fco.MaxBytes);
            Assert.Equal(50, fco.ThresholdPercent);
    
            fco = FetchConsumeOptions.Builder().WithMax(1000, 100).Build();
            Assert.Equal(100, fco.MaxMessages);
            Assert.Equal(1000, fco.MaxBytes);
            Assert.Equal(DefaultThresholdPercent, fco.ThresholdPercent);
    
            fco = FetchConsumeOptions.Builder().WithMax(1000, 100).WithThresholdPercent(50).Build();
            Assert.Equal(100, fco.MaxMessages);
            Assert.Equal(1000, fco.MaxBytes);
            Assert.Equal(50, fco.ThresholdPercent);
        }

        [Fact]
        public void TestConsumeOptionsBuilder() {
            ConsumeOptions co = ConsumeOptions.Builder().Build();
            Assert.Equal(DefaultMessageCount, co.BatchSize);
            Assert.Equal(DefaultExpiresInMillis, co.ExpiresIn);
            Assert.Equal(DefaultThresholdPercent, co.ThresholdPercent);
            Assert.Equal(0, co.BatchBytes);
            Assert.Equal(DefaultExpiresInMillis * MaxIdleHeartbeatPercent / 100, co.IdleHeartbeat);
    
            co = ConsumeOptions.Builder().WithBatchSize(1000).Build();
            Assert.Equal(1000, co.BatchSize);
            Assert.Equal(0, co.BatchBytes);
            Assert.Equal(DefaultThresholdPercent, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithBatchSize(1000).WithThresholdPercent(50).Build();
            Assert.Equal(1000, co.BatchSize);
            Assert.Equal(0, co.BatchBytes);
            Assert.Equal(50, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithBatchBytes(1000).Build();
            Assert.Equal(DefaultMessageCountWhenBytes, co.BatchSize);
            Assert.Equal(1000, co.BatchBytes);
            Assert.Equal(DefaultThresholdPercent, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithThresholdPercent(0).Build();
            Assert.Equal(DefaultThresholdPercent, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithThresholdPercent(-1).Build();
            Assert.Equal(DefaultThresholdPercent, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithThresholdPercent(-999).Build();
            Assert.Equal(DefaultThresholdPercent, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithThresholdPercent(99).Build();
            Assert.Equal(99, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithThresholdPercent(100).Build();
            Assert.Equal(100, co.ThresholdPercent);
    
            co = ConsumeOptions.Builder().WithThresholdPercent(101).Build();
            Assert.Equal(100, co.ThresholdPercent);
            
            co = ConsumeOptions.Builder().WithExpiresIn(0).Build();
            Assert.Equal(DefaultExpiresInMillis, co.ExpiresIn);
    
            co = ConsumeOptions.Builder().WithExpiresIn(-1).Build();
            Assert.Equal(DefaultExpiresInMillis, co.ExpiresIn);
    
            co = ConsumeOptions.Builder().WithExpiresIn(-999).Build();
            Assert.Equal(DefaultExpiresInMillis, co.ExpiresIn);

            Assert.Throws<ArgumentException>(() => ConsumeOptions.Builder().WithExpiresIn(MinExpiresMills - 1).Build());
        }

        class PullOrderedTestDropSimulator : PullOrderedMessageManager
        {
            public PullOrderedTestDropSimulator(
                Connection conn, JetStream js, string stream, SubscribeOptions so, ConsumerConfiguration cc,
                bool queueMode, bool syncMode)
                : base(conn, js, stream, so, cc, syncMode) {}

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
        
        // [Fact]
        // public void TestOrderedIterable() {
        //     Context.RunInJsServer(si => RunTest(si), c => {
        //         string streamName = Stream(Nuid.NextGlobal());
        //         string subject = Subject(Nuid.NextGlobal());
        //
        //         // Setup
        //         IJetStream js = c.CreateJetStreamContext();
        //         IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
        //
        //         CreateMemoryStream(jsm, streamName, subject);
        //
        //         IStreamContext sc = js.CreateStreamContext(streamName);
        //
        //         // Get this in place before any subscriptions are made
        //         ((JetStream)js)._pullOrderedMessageManagerFactory =
        //             (conn, lJs, lStream, so, cc, queueMode, syncMode) =>
        //                 new PullOrderedTestDropSimulator(conn, lJs, lStream, so, cc, queueMode, syncMode);
        //
        //         // Published messages will be intercepted by the OrderedTestDropSimulator
        //         new Thread(() => {
        //             Thread.Sleep(1000); // give the consumer time to get setup before publishing
        //             JsPublish(js, subject, 101, 6);
        //         }).Start();
        //
        //         OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().WithFilterSubject(subject);
        //         using (IIterableConsumer icon = sc.StartOrderedIterate(occ)) {
        //             // Loop through the messages to make sure I get stream sequence 1 to 6
        //             ulong expectedStreamSeq = 1;
        //             while (expectedStreamSeq <= 6) {
        //                 Msg m = icon.NextMessage(1000);
        //                 if (m != null) {
        //                     Assert.Equal(expectedStreamSeq, m.MetaData.StreamSequence);
        //                     Assert.Equal(TestJetStreamConsumer.ExpectedConSeqNums[expectedStreamSeq-1], m.MetaData.ConsumerSequence);
        //                     ++expectedStreamSeq;
        //                 }
        //             }
        //         }
        //     });
        // }
        //
        // [Fact]
        // public void TestOrderedConsume() {
        //     Context.RunInJsServer(si => RunTest(si), c => {
        //         string streamName = Stream(Nuid.NextGlobal());
        //         string subject = Subject(Nuid.NextGlobal());
        //
        //         // Setup
        //         IJetStream js = c.CreateJetStreamContext();
        //         IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
        //
        //         CreateMemoryStream(jsm, streamName, subject);
        //
        //         IStreamContext sc = js.CreateStreamContext(streamName);
        //
        //         // Get this in place before any subscriptions are made
        //         ((JetStream)js)._pullOrderedMessageManagerFactory =
        //             (conn, lJs, lStream, so, cc, queueMode, syncMode) =>
        //                 new PullOrderedTestDropSimulator(conn, lJs, lStream, so, cc, queueMode, syncMode);
        //
        //         CountdownEvent msgLatch = new CountdownEvent(6);
        //         int received = 0;
        //         ulong[] ssFlags = new ulong[6];
        //         ulong[] csFlags = new ulong[6];
        //         EventHandler<MsgHandlerEventArgs> handler = (s, e) => {
        //             int i = ++received - 1;
        //             ssFlags[i] = e.Message.MetaData.StreamSequence;
        //             csFlags[i] = e.Message.MetaData.ConsumerSequence;
        //             msgLatch.Signal();
        //         };
        //
        //         OrderedConsumerConfiguration occ = new OrderedConsumerConfiguration().WithFilterSubject(subject);
        //         using (IMessageConsumer mcon = sc.StartOrderedConsume(occ, handler)) {
        //             JsPublish(js, subject, 201, 6);
        //
        //             // wait for the messages
        //             msgLatch.Wait(30000);
        //
        //             // Loop through the messages to make sure I get stream sequence 1 to 6
        //             ulong expectedStreamSeq = 1;
        //             while (expectedStreamSeq <= 6) {
        //                 ulong idx = expectedStreamSeq - 1;
        //                 Assert.Equal(expectedStreamSeq, ssFlags[idx]);
        //                 Assert.Equal(TestJetStreamConsumer.ExpectedConSeqNums[idx], csFlags[idx]);
        //                 ++expectedStreamSeq;
        //             }
        //         }
        //     });
        // }
    }
}
