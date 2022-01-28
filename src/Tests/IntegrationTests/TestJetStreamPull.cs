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

using System.Collections.Generic;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStreamPull : TestSuite<JetStreamPullSuiteContext>
    {
        public TestJetStreamPull(JetStreamPullSuiteContext context) : base(context)
        {
        }

        [Fact]
        public void TestFetch()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                    .WithAckWait(2500)
                    .Build();
                
                PullSubscribeOptions options = PullSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .WithConfiguration(cc)
                    .Build();

                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                AssertSubscription(sub, STREAM, DURABLE, null, true);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server
                
                IList<Msg> messages = sub.Fetch(10, 3000);
                ValidateRead(0, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "A", 10);
                messages = sub.Fetch(10, 3000);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "B", 20);
                messages = sub.Fetch(10, 3000);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                messages = sub.Fetch(10, 3000);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "C", 5);
                messages = sub.Fetch(10, 3000);
                ValidateRead(5, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "D", 15);
                messages = sub.Fetch(10, 3000);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                messages = sub.Fetch(10, 3000);
                ValidateRead(5, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "E", 10);
                messages = sub.Fetch(10, 3000);
                ValidateRead(10, messages.Count);
                Thread.Sleep(3000);

                messages = sub.Fetch(10, 3000);
                ValidateRead(10, messages.Count);
                AckAll(messages);
            });
        }


        [Fact]
        public void TestBasic()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options. Durable is REQUIRED for pull based subscriptions
                PullSubscribeOptions options = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();

                // Pull Subscribe.
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                AssertSubscription(sub, STREAM, DURABLE, null, true);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // publish some amount of messages, but not entire pull size
                JsPublish(js, SUBJECT, "A", 4);

                // start the pull
                sub.Pull(10);

                // read what is available, expect 4
                IList<Msg> messages = ReadMessagesAck(sub);
                int total = messages.Count;
                ValidateRedAndTotal(4, messages.Count, 4, total);

                // publish some more covering our initial pull and more
                JsPublish(js, SUBJECT, "B", 10);

                // read what is available, expect 6 more
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(6, messages.Count, 10, total);

                // read what is available, should be zero since we didn't re-pull
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(0, messages.Count, 10, total);

                // re-issue the pull
                sub.Pull(10);

                // read what is available, should be 4 left over
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(4, messages.Count, 14, total);

                // publish some more
                JsPublish(js, SUBJECT, "C", 10);

                // read what is available, should be 6 since we didn't finish the last batch
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(6, messages.Count, 20, total);

                // re-issue the pull, but a smaller amount
                sub.Pull(2);

                // read what is available, should be 5 since we changed the pull size
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(2, messages.Count,22, total);

                // re-issue the pull, since we got the full batch size
                sub.Pull(2);

                // read what is available, should be zero since we didn't re-pull
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(2, messages.Count, 24, total);

                // re-issue the pull, any amount there are no messages
                sub.Pull(1);

                // read what is available, there are none
                messages = ReadMessagesAck(sub);
                total += messages.Count;
                ValidateRedAndTotal(0, messages.Count, 24, total);
            });
        }

        [Fact]
        public void TestNoWait()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options. Durable is REQUIRED for pull based subscriptions
                PullSubscribeOptions options = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();

                // Pull Subscribe.
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                AssertSubscription(sub, STREAM, DURABLE, null, true);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // publish 10 messages
                // no wait, batch size 10, there are 10 messages, we will read them all and not trip nowait
                JsPublish(js, SUBJECT, "A", 10);
                sub.PullNoWait(10);
                IList<Msg> messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);
                AssertAllJetStream(messages);

                // publish 20 messages
                // no wait, batch size 10, there are 20 messages, we will read 10
                JsPublish(js, SUBJECT, "B", 20);
                sub.PullNoWait(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);

                // there are still ten messages
                // no wait, batch size 10, there are 20 messages, we will read 10
                sub.PullNoWait(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);

                // publish 5 messages
                // no wait, batch size 10, there are 5 messages, we WILL trip nowait
                JsPublish(js, SUBJECT, "C", 5);
                sub.PullNoWait(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);

                // publish 12 messages
                // no wait, batch size 10, there are more than batch messages we will read 10
                JsPublish(js, SUBJECT, "D", 12);
                sub.PullNoWait(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);

                // 2 messages left
                // no wait, less than batch size will WILL trip nowait
                sub.PullNoWait(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(2, messages.Count);
            });
        }

        [Fact]
        public void TestPullExpires()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options. Durable is REQUIRED for pull based subscriptions
                PullSubscribeOptions options = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();
               
                // Subscribe synchronously.
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                AssertSubscription(sub, STREAM, DURABLE, null, true);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                int expires = 250; // millis

                // publish 10 messages
                JsPublish(js, SUBJECT, "A", 5);
                sub.PullExpiresIn(10, expires);
                IList<Msg> messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);
                AssertAllJetStream(messages);
                Thread.Sleep(expires); // make sure the pull actually expires

                JsPublish(js, SUBJECT, "B", 10);
                sub.PullExpiresIn(10, expires);
                messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);
                Thread.Sleep(expires); // make sure the pull actually expires

                JsPublish(js, SUBJECT, "C", 5);
                sub.PullExpiresIn(10, expires);
                messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);
                AssertAllJetStream(messages);
                Thread.Sleep(expires); // make sure the pull actually expires

                JsPublish(js, SUBJECT, "D", 10);
                sub.Pull(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);

                JsPublish(js, SUBJECT, "E", 5);
                sub.PullExpiresIn(10, expires); // using millis version here
                messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);
                AssertAllJetStream(messages);
                Thread.Sleep(expires); // make sure the pull actually expires

                JsPublish(js, SUBJECT, "F", 10);
                sub.PullNoWait(10);
                messages = ReadMessagesAck(sub);
                Assert.Equal(10, messages.Count);

                JsPublish(js, SUBJECT, "G", 5);
                sub.PullExpiresIn(10, expires); // using millis version here
                messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);
                AssertAllJetStream(messages);

                JsPublish(js, SUBJECT, "H", 10);
                messages = sub.Fetch(10, expires);
                Assert.Equal(10, messages.Count);
                AssertAllJetStream(messages);

                JsPublish(js, SUBJECT, "I", 5);
                sub.PullExpiresIn(10, expires);
                messages = ReadMessagesAck(sub);
                Assert.Equal(5, messages.Count);
                AssertAllJetStream(messages);
                Thread.Sleep(expires); // make sure the pull actually expires
            });
        }

        [Fact]
        public void TestAckNak()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options. Durable is REQUIRED for pull based subscriptions
                PullSubscribeOptions options = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // NAK
                JsPublish(js, SUBJECT, "NAK", 1);

                sub.Pull(1);

                Msg message = sub.NextMessage(1000);
                Assert.NotNull(message);
                Assert.Equal("NAK1", Encoding.ASCII.GetString(message.Data));
                message.Nak();

                sub.Pull(1);
                message = sub.NextMessage(1000);
                Assert.NotNull(message);
                Assert.Equal("NAK1", Encoding.ASCII.GetString(message.Data));
                message.Ack();

                sub.Pull(1);
                AssertNoMoreMessages(sub);
            });
        }

        [Fact]
        public void TestAckTerm()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options. Durable is REQUIRED for pull based subscriptions
                PullSubscribeOptions options = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // TERM
                JsPublish(js, SUBJECT, "TERM", 1);

                sub.Pull(1);
                Msg message = sub.NextMessage(1000);
                Assert.NotNull(message);
                Assert.Equal("TERM1", Encoding.ASCII.GetString(message.Data));
                message.Term();

                sub.Pull(1);
                AssertNoMoreMessages(sub);
            });
        }

        [Fact]
        public void TestAckReplySyncCoverage()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                JsPublish(js, SUBJECT, "COVERAGE", 1);

                Msg message = sub.NextMessage(1000);
                Assert.NotNull(message);
                message.Reply = "$JS.ACK.stream.LS0k4eeN.1.1.1.1627472530542070600.0";

                Assert.Throws<NATSNoRespondersException>(() => message.AckSync(1000));
            });
        }

        [Fact]
        public void TestAckWaitTimeout()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                    .WithAckWait(1500)
                    .Build();
                PullSubscribeOptions pso = PullSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .WithConfiguration(cc)
                    .Build();
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, pso);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server

                // Ack Wait timeout
                JsPublish(js, SUBJECT, "WAIT", 1);

                sub.Pull(1);
                Msg message = sub.NextMessage(1000);
                Assert.NotNull(message);
                Assert.Equal("WAIT1", Encoding.ASCII.GetString(message.Data));
                Thread.Sleep(2000);

                sub.Pull(1);
                message = sub.NextMessage(1000);
                Assert.NotNull(message);
                Assert.Equal("WAIT1", Encoding.ASCII.GetString(message.Data));

                sub.Pull(1);
                AssertNoMoreMessages(sub);
            });
        }
        
        [Fact]
        public void TestDurable()
        {
            Context.RunInJsServer(c =>
            {
                // create the stream.
                CreateDefaultTestStream(c);

                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                // Build our subscription options normally
                PullSubscribeOptions options1 = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                _testDurable(js, () => js.PullSubscribe(SUBJECT, options1));

                // bind long form
                PullSubscribeOptions options2 = PullSubscribeOptions.Builder()
                    .WithStream(STREAM)
                    .WithDurable(DURABLE)
                    .WithBind(true)
                    .Build();
                _testDurable(js, () => js.PullSubscribe(null, options2));

                // bind short form
                PullSubscribeOptions options3 = PullSubscribeOptions.BindTo(STREAM, DURABLE);
                _testDurable(js, () => js.PullSubscribe(null, options3));
            });
        }

        private void _testDurable(IJetStream js, PullSubSupplier supplier)
        {
            JsPublish(js, SUBJECT, 2);

            IJetStreamPullSubscription sub = supplier.Invoke();

            // start the pull
            sub.PullNoWait(4);

            IList<Msg> messages = ReadMessagesAck(sub);
            ValidateRedAndTotal(2, messages.Count, 2, 2);

            sub.Unsubscribe();
        }

        delegate IJetStreamPullSubscription PullSubSupplier();
    }
}
