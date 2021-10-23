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

using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
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
                CreateDefaultTestStream(c);

                // Create our JetStream context.
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
                CreateDefaultTestStream(c);

                // Create our JetStream context.
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
                IJetStreamPushAsyncSubscription asub = js.PushSubscribeAsync(SUBJECT, Handler1, true, pso1);

                // wait for messages to arrive using the countdown latch.
                latch1.Wait();

                Assert.Equal(10, handlerReceived1);

                asub.Unsubscribe();

                // check that all the messages were read by the durable
                IJetStreamPushSyncSubscription ssub = js.PushSubscribeSync(SUBJECT, pso1);
                AssertNoMoreMessages(ssub);

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
                asub = js.PushSubscribeAsync(SUBJECT, Handler2, false, pso2);

                // wait for messages to arrive using the countdown latch.
                latch2.Wait();
                Assert.Equal(10, handlerReceived2);

                Thread.Sleep(2000); // just give it time for the server to realize the messages are not ack'ed

                asub.Unsubscribe();

                // check that we get all the messages again
                ssub = js.PushSubscribeSync(SUBJECT, pso2);
                Assert.Equal(10, ReadMessagesAck(ssub).Count);
            });
        }
    }
}
