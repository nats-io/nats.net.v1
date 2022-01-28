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
                    args.Message.Ack();
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

        [Fact]
        public void TestDontAutoAckIfUserAcks() {
            string mockAckReply = "mock-ack-reply.";
            
            Context.RunInJsServer(c =>
            {
                CreateMemoryStream(c, STREAM, SUBJECT, mockAckReply + "*");
                
                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // publish some messages
                JsPublish(js, SUBJECT, 2);

                // 1. auto ack true
                CountdownEvent latch = new CountdownEvent(2);
                bool flag = true;

                // create our message handler, does not ack
                void Handler(object sender, MsgHandlerEventArgs args)
                {
                    if (flag)
                    {
                        args.Message.Reply = mockAckReply + "user";
                        args.Message.Ack();
                        flag = false;
                    }
                    args.Message.Reply = mockAckReply + "system";
                    latch.Signal();
                }

                // subscribe using the handler, auto ack true
                js.PushSubscribeAsync(SUBJECT, Handler, true);

                // wait for messages to arrive using the countdown latch.
                latch.Wait();

                IJetStreamPushSyncSubscription ssub = js.PushSubscribeSync(mockAckReply + "*");
                Msg m = ssub.NextMessage(1000);
                Assert.Equal(mockAckReply + "user", m.Subject);
                m = ssub.NextMessage(1000);
                Assert.Equal(mockAckReply + "system", m.Subject);
            });
        }

        [Fact]
        public void TestPushAsyncFlowControl()
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

                int msgCount = 1000;
                
                for (int x = 100_000; x < msgCount + 100_000; x++) {
                    byte[] fill = Encoding.ASCII.GetBytes(""+ x);
                    Array.Copy(fill, 0, data, 0, 6);
                    js.Publish(new Msg(SUBJECT, data));
                }
                
                InterlockedInt count = new InterlockedInt();
                HashSet<string> set = new HashSet<string>();
                
                CountdownEvent latch = new CountdownEvent(msgCount);

                // create our message handler, does not ack
                void Handler(object sender, MsgHandlerEventArgs args)
                {
                    byte[] fill = new byte[6];
                    Array.Copy(args.Message.Data, 0, fill, 0, 6);
                    string id = Encoding.ASCII.GetString(fill);
                    if (set.Add(id)) {
                        count.Increment();
                    }
                    args.Message.Ack();
                    latch.Signal();
                }

                // subscribe using the handler
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithFlowControl(1000).Build();
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                js.PushSubscribeAsync(SUBJECT, Handler, false, pso);

                // wait for messages to arrive using the countdown latch.
                latch.Wait();

                Assert.Equal(msgCount, count.Read());
                Assert.True(fcps.Read() > 0);
            });
        }
    }
}
