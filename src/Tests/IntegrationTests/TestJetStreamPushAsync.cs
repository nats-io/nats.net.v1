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
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStreamPushAsync : TestSuite<JetStreamSuiteContext>
    {
        public TestJetStreamPushAsync(JetStreamSuiteContext context) : base(context) {}

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
                    Interlocked.Increment(ref received);
                    if (args.Message.IsJetStream)
                    {
                        args.Message.Ack();
                    }

                    latch.Signal();
                }


                // Subscribe using the handler
                js.PushSubscribeAsync(SUBJECT, TestHandler);

                // Wait for messages to arrive using the countdown latch.
                latch.Wait();
                
                Assert.Equal(10, received);
            });
        }
    }
}
