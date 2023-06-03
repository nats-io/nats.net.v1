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
    public class TestSimplification : TestSuite<SimplificationSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestSimplification(ITestOutputHelper output, SimplificationSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
        }
        
        [Fact]
        public void TestStreamContext() {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
    
                Assert.Throws<NATSJetStreamException>(() => c.CreateStreamContext(STREAM));
                Assert.Throws<NATSJetStreamException>(() => c.CreateStreamContext(STREAM, JetStreamOptions.DefaultJsOptions));
                Assert.Throws<NATSJetStreamException>(() => js.CreateStreamContext(STREAM));
    
                CreateMemoryStream(jsm, STREAM, SUBJECT);
                IStreamContext streamContext = c.CreateStreamContext(STREAM);
                Assert.Equal(STREAM, streamContext.StreamName);
                _TestStreamContext(streamContext, js);

                jsm.DeleteStream(STREAM);
                
                CreateMemoryStream(jsm, STREAM, SUBJECT);
                streamContext = js.CreateStreamContext(STREAM);
                Assert.Equal(STREAM, streamContext.StreamName);
                _TestStreamContext(streamContext, js);

            });
        }

        private static void _TestStreamContext(IStreamContext streamContext, IJetStream js)
        {
            Assert.Throws<NATSJetStreamException>(() => streamContext.CreateConsumerContext(DURABLE));
            Assert.Throws<NATSJetStreamException>(() => streamContext.DeleteConsumer(DURABLE));

            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
            IConsumerContext consumerContext = streamContext.AddConsumer(cc);
            ConsumerInfo ci = consumerContext.GetConsumerInfo();
            Assert.Equal(STREAM, ci.Stream);
            Assert.Equal(DURABLE, ci.Name);

            ci = streamContext.GetConsumerInfo(DURABLE);
            Assert.NotNull(ci);
            Assert.Equal(STREAM, ci.Stream);
            Assert.Equal(DURABLE, ci.Name);

            Assert.Equal(1, streamContext.GetConsumerNames().Count);

            Assert.Equal(1, streamContext.GetConsumers().Count);
            Assert.NotNull(streamContext.CreateConsumerContext(DURABLE));
            streamContext.DeleteConsumer(DURABLE);

            Assert.Throws<NATSJetStreamException>(() => streamContext.CreateConsumerContext(DURABLE));
            Assert.Throws<NATSJetStreamException>(() => streamContext.DeleteConsumer(DURABLE));

            // coverage
            js.Publish(SUBJECT, Encoding.UTF8.GetBytes("one"));
            js.Publish(SUBJECT, Encoding.UTF8.GetBytes("two"));
            js.Publish(SUBJECT, Encoding.UTF8.GetBytes("three"));
            js.Publish(SUBJECT, Encoding.UTF8.GetBytes("four"));
            js.Publish(SUBJECT, Encoding.UTF8.GetBytes("five"));
            js.Publish(SUBJECT, Encoding.UTF8.GetBytes("six"));

            Assert.True(streamContext.DeleteMessage(3));
            Assert.True(streamContext.DeleteMessage(4, true));

            MessageInfo mi = streamContext.GetMessage(1);
            Assert.Equal(1U, mi.Sequence);

            mi = streamContext.GetFirstMessage(SUBJECT);
            Assert.Equal(1U, mi.Sequence);

            mi = streamContext.GetLastMessage(SUBJECT);
            Assert.Equal(6U, mi.Sequence);

            mi = streamContext.GetNextMessage(3, SUBJECT);
            Assert.Equal(5U, mi.Sequence);

            Assert.NotNull(streamContext.GetStreamInfo());
            Assert.NotNull(streamContext.GetStreamInfo(StreamInfoOptions.Builder().Build()));

            streamContext.Purge(PurgeOptions.Builder().WithSequence(5).Build());
            Assert.Throws<NATSJetStreamException>(() => streamContext.GetMessage(1));

            mi = streamContext.GetFirstMessage(SUBJECT);
            Assert.Equal(5U, mi.Sequence);

            streamContext.Purge();
            Assert.Throws<NATSJetStreamException>(() => streamContext.GetFirstMessage(SUBJECT));
        }

        [Fact]
        public void TestFetch() {
            Context.RunInJsServer(c => {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();
                for (int x = 1; x <= 20; x++) {
                    js.Publish(SUBJECT, Encoding.UTF8.GetBytes("test-fetch-msg-" + x));
                }
    
                // 1. Different fetch sizes demonstrate expiration behavior
    
                // 1A. equal number of messages than the fetch size
                _testFetch("1A", c, 20, 0, 20);
    
                // 1B. more messages than the fetch size
                _testFetch("1B", c, 10, 0, 10);
    
                // 1C. fewer messages than the fetch size
                _testFetch("1C", c, 40, 0, 40);
    
                // 1D. simple-consumer-40msgs was created in 1C and has no messages available
                _testFetch("1D", c, 40, 0, 40);
    
                // don't test bytes before 2.9.1
                if (c.ServerInfo.IsOlderThanVersion("2.9.1")) {
                    return;
                }
    
                // 2. Different max bytes sizes demonstrate expiration behavior
                //    - each test message is approximately 100 bytes
    
                // 2A. max bytes is reached before message count
                _testFetch("2A", c, 0, 750, 20);
    
                // 2B. fetch size is reached before byte count
                _testFetch("2B", c, 10, 1500, 10);
    
                // 2C. fewer bytes than the byte count
                _testFetch("2C", c, 0, 3000, 40);
            });
        }
    
        private static void _testFetch(string label, IConnection c, int maxMessages, int maxBytes, int testAmount) {
            IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
            IJetStream js = c.CreateJetStreamContext();

            string name = generateConsumerName(maxMessages, maxBytes);
    
            // Pre define a consumer
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(name).Build();
            ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
    
            // Consumer[Context]
            IConsumerContext consumerContext = js.CreateConsumerContext(STREAM, name);
    
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
            try
            {
                Msg msg = consumer.NextMessage();
                while (true) {
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
                ? NAME + "-" + maxMessages + "msgs"
                : NAME + "-" + maxBytes + "bytes-" + maxMessages + "msgs";
        }
    
        [Fact]
        public void TestIterableConsumer() {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateDefaultTestStream(jsm);
                IJetStream js = c.CreateJetStreamContext();
        
                // for (int x = 1; x <= 5000; x++) {
                    // js.Publish(SUBJECT, Encoding.UTF8.GetBytes("iterable-" + x));
                // }

                // Pre define a consumer
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                // Consumer[Context]
                IConsumerContext consumerContext = js.CreateConsumerContext(STREAM, DURABLE);
    
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

                Publisher publisher = new Publisher(js, SUBJECT, 1);
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
        public void TestConsumeWithHandler() {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateDefaultTestStream(jsm);
                IJetStream js = c.CreateJetStreamContext();
                JsPublish(js, SUBJECT, 2500);
    
                // Pre define a consumer
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(NAME).Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);
    
                // Consumer[Context]
                IConsumerContext consumerContext = js.CreateConsumerContext(STREAM, NAME);
    
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
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateDefaultTestStream(jsm);
                IJetStream js = c.CreateJetStreamContext();
                JsPublish(js, SUBJECT, 2);
    
                // Pre define a consumer
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(NAME).Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);
    
                // Consumer[Context]
                IConsumerContext consumerContext = js.CreateConsumerContext(STREAM, NAME);
    
                Assert.Throws<ArgumentException>(() => consumerContext.Next(1));
                Assert.NotNull(consumerContext.Next(1000));
                Assert.NotNull(consumerContext.Next());
                Assert.Throws<NATSTimeoutException>(() => consumerContext.Next(1000));
            });
        }
    
        [Fact]
        public void TestCoverage() {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
    
                CreateDefaultTestStream(jsm);
                IJetStream js = c.CreateJetStreamContext();
    
                // Pre define a consumer
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Name(1)).Build());
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Name(2)).Build());
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Name(3)).Build());
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Name(4)).Build());
    
                // Stream[Context]
                IStreamContext sctx1 = c.CreateStreamContext(STREAM);
                c.CreateStreamContext(STREAM, JetStreamOptions.DefaultJsOptions);
                js.CreateStreamContext(STREAM);
    
                // Consumer[Context]
                IConsumerContext cctx1 = c.CreateConsumerContext(STREAM, Name(1));
                IConsumerContext cctx2 = c.CreateConsumerContext(STREAM, Name(2), JetStreamOptions.DefaultJsOptions);
                IConsumerContext cctx3 = js.CreateConsumerContext(STREAM, Name(3));
                IConsumerContext cctx4 = sctx1.CreateConsumerContext(Name(4));
                IConsumerContext cctx5 = sctx1.AddConsumer(ConsumerConfiguration.Builder().WithDurable(Name(5)).Build());
                IConsumerContext cctx6 = sctx1.AddConsumer(ConsumerConfiguration.Builder().WithDurable(Name(6)).Build());
    
                closeConsumer(cctx1.consume(), Name(1), true);
                closeConsumer(cctx2.consume(ConsumeOptions.DefaultConsumeOptions), Name(2), true);
                
                closeConsumer(cctx3.consume((s, e) => {}), Name(3), true);
                closeConsumer(cctx4.consume((s, e) => {}, ConsumeOptions.DefaultConsumeOptions), Name(4), true);
                
                closeConsumer(cctx5.FetchMessages(1), Name(5), false);
                closeConsumer(cctx6.FetchBytes(1000), Name(6), false);
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