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
using Xunit.Abstractions;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.JetStream.BaseConsumeOptions;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestSimplification : TestSuite<OneServerSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestSimplification(ITestOutputHelper output, OneServerSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
        }
        
        [Fact]
        public void TestStreamContext()
        {
            string stream = Stream(Nuid.NextGlobal());
            string subject = Subject(Nuid.NextGlobal());
            
            Context.RunInJsServer(c => {
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
            IConsumerContext consumerContext = streamContext.AddConsumer(cc);
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
            string stream = Stream(Nuid.NextGlobal());
            string subject = Subject(Nuid.NextGlobal());
            Context.RunInJsServer(c => {
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
    
                // don't test bytes before 2.9.1
                if (c.ServerInfo.IsOlderThanVersion("2.9.1")) {
                    return;
                }
    
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
            IFetchConsumer consumer = consumerContext.Fetch(fetchConsumeOptions);
            int rcvd = 0;
            long bc = 0;
            try
            {
                Msg msg = consumer.NextMessage();
                while (true)
                {
                    bc += msg.ConsumeByteCount;
                    ++rcvd;
                    msg.Ack();
                    msg = consumer.NextMessage();
                }
            }
            catch (NATSTimeoutException) {}
            sw.Stop();
    
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
            string streamName = Stream(Nuid.NextGlobal());
            string subject = Subject(Nuid.NextGlobal());
            string durable = Nuid.NextGlobal();

            Context.RunInJsServer(c => {
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
                IIterableConsumer consumer = consumerContext.consume();

                InterlockedInt count = new InterlockedInt();
                Thread consumeThread = new Thread(() =>
                {
                    Msg msg;
                    while (count.Read() < stopCount)
                    {
                        try
                        {
                            msg = consumer.NextMessage(1000);
                            msg.Ack();
                            count.Increment();
                        }
                        catch (NATSTimeoutException)
                        {
                            // this is expected
                        }
                    }

                    Thread.Sleep(1000); // allows more messages to come across
                    consumer.Stop(200);

                    try
                    {
                        while (true)
                        {
                            msg = consumer.NextMessage(1000);
                            msg.Ack();
                            count.Increment();
                        }
                    }
                    catch (NATSTimeoutException) {}
                });
                consumeThread.Start();

                Publisher publisher = new Publisher(js, subject, 1);
                Thread pubThread = new Thread(publisher.Run);
                pubThread.Start();
    
                consumeThread.Join();
                publisher.Stop();
                pubThread.Join();
    
                Assert.True(count.Read() >= 500);
    
                // coverage
                consumerContext.consume(ConsumeOptions.DefaultConsumeOptions);
                Assert.Throws<ArgumentException>(() => consumerContext.consume((ConsumeOptions)null));
                
            });
        }
    
        [Fact]
        public void TestConsumeWithHandler()
        {
            string streamName = Stream(Nuid.NextGlobal());
            string subject = Subject(Nuid.NextGlobal());
            string durable = Nuid.NextGlobal();

            Context.RunInJsServer(c => {
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
    
                IMessageConsumer consumer = consumerContext.consume(handler);
                latch.Wait(10_000);
                consumer.Stop(200);
                Assert.Equal(0, latch.CurrentCount);
            });
        }
    
        [Fact]
        public void TestNext() {
            string streamName = Stream(Nuid.NextGlobal());
            string subject = Subject(Nuid.NextGlobal());
            string durable = Nuid.NextGlobal();

            Context.RunInJsServer(c => {
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
                Assert.Throws<NATSTimeoutException>(() => consumerContext.Next(1000));
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

            Context.RunInJsServer(c => {
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
                IConsumerContext cctx5 = sctx1.AddConsumer(ConsumerConfiguration.Builder().WithDurable(durable5).Build());
                IConsumerContext cctx6 = sctx1.AddConsumer(ConsumerConfiguration.Builder().WithDurable(durable6).Build());
    
                closeConsumer(cctx1.consume(), durable1, true);
                closeConsumer(cctx2.consume(ConsumeOptions.DefaultConsumeOptions), durable2, true);
                
                closeConsumer(cctx3.consume((s, e) => {}), durable3, true);
                closeConsumer(cctx4.consume((s, e) => {}, ConsumeOptions.DefaultConsumeOptions), durable4, true);
                
                closeConsumer(cctx5.FetchMessages(1), durable5, false);
                closeConsumer(cctx6.FetchBytes(1000), durable6, false);
            });
        }
    
        private void closeConsumer(IMessageConsumer con, string name, bool doStop) {
            ConsumerInfo ci = con.GetConsumerInformation();
            Assert.Equal(name, ci.Name);
            if (doStop) {
                Assert.True(con.Stop(100).Wait(100));
            }
            con.Dispose();
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
    }
}
