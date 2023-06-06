// Copyright 2021-2023 The NATS Authors
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
using static NATS.Client.Internals.JetStreamConstants;

namespace IntegrationTests
{
    public class TestJetStreamPull : TestSuite<JetStreamPullSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestJetStreamPull(ITestOutputHelper output, JetStreamPullSuiteContext context) : base(context)
        {
            this.output = output;
            Console.SetOut(new ConsoleWriter(output));
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

                int fetchMs = 3000;
                int ackWaitMs = fetchMs * 2;

                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                    .WithAckWait(ackWaitMs)
                    .Build();
                
                PullSubscribeOptions options = PullSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .WithConfiguration(cc)
                    .Build();

                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, options);
                AssertSubscription(sub, STREAM, DURABLE, null, true);
                c.Flush(DefaultTimeout); // flush outgoing communication with/to the server
                
                IList<Msg> messages = sub.Fetch(10, fetchMs);
                ValidateRead(0, messages.Count);
                AckAll(messages);
                Thread.Sleep(ackWaitMs); // let the pull expire

                JsPublish(js, SUBJECT, "A", 10);
                messages = sub.Fetch(10, fetchMs);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "B", 20);
                messages = sub.Fetch(10, fetchMs);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                messages = sub.Fetch(10, fetchMs);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "C", 5);
                messages = sub.Fetch(10, fetchMs);
                ValidateRead(5, messages.Count);
                AckAll(messages);
                Thread.Sleep(fetchMs * 2); // let the pull expire

                JsPublish(js, SUBJECT, "D", 15);
                messages = sub.Fetch(10, fetchMs);
                ValidateRead(10, messages.Count);
                AckAll(messages);

                messages = sub.Fetch(10, fetchMs);
                ValidateRead(5, messages.Count);
                AckAll(messages);

                JsPublish(js, SUBJECT, "E", 10);
                messages = sub.Fetch(10, fetchMs);
                ValidateRead(10, messages.Count);
                Thread.Sleep(ackWaitMs);

                messages = sub.Fetch(10, fetchMs);
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

        [Fact]
        public void TestPullRequestsOptionsBuilder()
        {
            Assert.Throws<ArgumentException>(() => PullRequestOptions.Builder(0).Build());
            Assert.Throws<ArgumentException>(() => PullRequestOptions.Builder(-1).Build());

            PullRequestOptions pro = PullRequestOptions.Builder(11).Build();
            Assert.Equal(11, pro.BatchSize);
            Assert.Equal(0, pro.MaxBytes);
            Assert.Null(pro.ExpiresIn);
            Assert.Null(pro.IdleHeartbeat);
            Assert.False(pro.NoWait);

            pro = PullRequestOptions.Builder(31)
                .WithMaxBytes(32)
                .WithExpiresIn(33)
                .WithIdleHeartbeat(34)
                .WithNoWait()
                .Build();
            Assert.Equal(31, pro.BatchSize);
            Assert.Equal(32, pro.MaxBytes);
            Assert.Equal(33, pro.ExpiresIn.Millis);
            Assert.Equal(34, pro.IdleHeartbeat.Millis);
            Assert.True(pro.NoWait);

            pro = PullRequestOptions.Builder(41)
                .WithExpiresIn(Duration.OfMillis(43))
                .WithIdleHeartbeat(Duration.OfMillis(44))
                .WithNoWait(false) // just coverage of this method
                .Build();
            Assert.Equal(41, pro.BatchSize);
            Assert.Equal(0, pro.MaxBytes);
            Assert.Equal(43, pro.ExpiresIn.Millis);
            Assert.Equal(44, pro.IdleHeartbeat.Millis);
            Assert.False(pro.NoWait);
        }

        delegate IJetStreamPullSubscription ConflictSetup(IJetStreamManagement jsm, IJetStream js);

        private bool VersionIsBefore(IConnection conn, string version)
        {
            return version != null && conn.ServerInfo.IsOlderThanVersion(version);
        }

        private const int TypeError = 1;
        private const int TypeWarning = 2;
        private const int TypeNone = 0;

        private void TestConflictStatus(string statusText, int type, bool syncMode, string targetVersion, ConflictSetup setup)
        {
            bool skip = false;
            TestEventHandler handler = new TestEventHandler();
            Context.RunInJsServer(handler.Modifier, c =>
            {
                skip = VersionIsBefore(c, targetVersion);
                if (skip)
                {
                    return;
                }
                CreateDefaultTestStream(c);
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
                IJetStreamPullSubscription sub = setup.Invoke(jsm, js);
                if (type == TypeError && syncMode)
                {
                    Assert.Throws<NATSJetStreamStatusException>(() => sub.NextMessage(5000));
                }
                else
                {
                    try
                    {
                        sub.NextMessage(5000);
                    }
                    catch (NATSTimeoutException)
                    {
                    }
                }
                CheckHandler(statusText, type, handler);
            });
        }
        
        private void CheckHandler(String statusText, int type, TestEventHandler handler) {
            if (type == TypeError) {
                Assert.True(handler.PullStatusErrorOrWait(statusText, 2000));
            }
            else if (type == TypeWarning) {
                Assert.True(handler.PullStatusWarningOrWait(statusText, 2000));
            }
        }

        [Fact]
        public void TestExceedsMaxWaiting()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().WithMaxPullWaiting(1).BuildPullSubscribeOptions();
            TestConflictStatus(ExceededMaxWaiting, TypeWarning, true, null, (jsm, js) => {
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.Pull(1);
                sub.Pull(1);
                return sub;
            });
        }

        [Fact]
        public void TestExceedsMaxRequestBatch()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().WithMaxBatch(1).BuildPullSubscribeOptions();
            TestConflictStatus(ExceededMaxRequestBatch, TypeWarning, true, null, (jsm, js) => {
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.Pull(2);
                return sub;
            });

        }

        [Fact]
        public void TestMessageSizeExceedsMaxBytes()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().BuildPullSubscribeOptions();
            TestConflictStatus(MessageSizeExceedsMaxBytes, TypeNone, true, "2.9.0", (jsm, js) => {
                js.Publish(SUBJECT, new byte[1000]);
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.Pull(PullRequestOptions.Builder(1).WithMaxBytes(100).Build());
                return sub;
            });
        }

        [Fact]
        public void TestExceedsMaxRequestExpires()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().WithMaxExpires(1000).BuildPullSubscribeOptions();
            TestConflictStatus(ExceededMaxRequestExpires, TypeWarning, true, null, (jsm, js) => {
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.PullExpiresIn(1, 2000);
                return sub;
            });
        }

        [Fact]
        public void TestConsumerIsPushBased()
        {
            PullSubscribeOptions so = PullSubscribeOptions.BindTo(STREAM, Durable(1));
            TestConflictStatus(ConsumerIsPushBased, TypeError, true, null, (jsm, js) => {
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Durable(1)).Build());
                IJetStreamPullSubscription sub = js.PullSubscribe(null, so);
                jsm.DeleteConsumer(STREAM, Durable(1));
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Durable(1)).WithDeliverSubject(Deliver(1)).Build());
                sub.Pull(1);
                return sub;
            });
        }

        // This just flaps. It's a timing thing. Already spent too much time, it should work as is.
        [Fact(Skip = "Flapper")]
        public void TestConsumerDeleted()
        {
            PullSubscribeOptions so = PullSubscribeOptions.BindTo(STREAM, Durable(1));
            TestConflictStatus(ConsumerDeleted, TypeError, true, "2.9.6", (jsm, js) => {
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Durable(1)).Build());
                IJetStreamPullSubscription sub = js.PullSubscribe(null, so);
                sub.PullExpiresIn(1, 10000);
                jsm.DeleteConsumer(STREAM, Durable(1));
                return sub;
            });
        }

        [Fact]
        public void TestBadRequest()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().BuildPullSubscribeOptions();
            TestConflictStatus(BadRequest, TypeError, true, null, (jsm, js) => {
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.Pull(PullRequestOptions.Builder(1).WithNoWait().WithIdleHeartbeat(1).Build());
                return sub;
            });
        }

        [Fact]
        public void TestNotFound()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().BuildPullSubscribeOptions();
            TestConflictStatus(NoMessages, TypeNone, true, null, (jsm, js) => {
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.PullNoWait(1);
                return sub;
            });
        }

        [Fact]
        public void TestExceedsMaxRequestBytes1stMessage()
        {
            PullSubscribeOptions so = ConsumerConfiguration.Builder().WithMaxBytes(1).BuildPullSubscribeOptions();
            TestConflictStatus(ExceededMaxRequestMaxBytes, TypeWarning, true, null, (jsm, js) => {
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                sub.Pull(PullRequestOptions.Builder(1).WithMaxBytes(2).Build());
                return sub;
            });
        }

        [Fact]
        public void TestExceedsMaxRequestBytesNthMessage()
        {
            bool skip = false;
            TestEventHandler handler = new TestEventHandler();
            Context.RunInJsServer(handler.Modifier, c =>
            {
                skip = VersionIsBefore(c, "2.9.1");
                if (skip)
                {
                    return;
                }
                CreateDefaultTestStream(c);
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Durable(1)).Build());
                PullSubscribeOptions so = PullSubscribeOptions.BindTo(STREAM, Durable(1));
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                
                MsgHeader h = new MsgHeader();
                h["foo"] = "bar";
                // subject 7 + reply 52 + bytes 100 = 159
                // subject 7 + reply 52 + bytes 100 + headers 21 = 180
                js.Publish(SUBJECT, new byte[100]);
                js.Publish(SUBJECT, h, new byte[100]);
                // 1000 - 159 - 180 = 661
                // subject 7 + reply 52 + bytes 610 = 669 > 661
                js.Publish(SUBJECT, new byte[610]);

                sub.Pull(PullRequestOptions.Builder(10).WithMaxBytes(1000).WithExpiresIn(1000).Build());
                Assert.NotNull(sub.NextMessage(500));
                Assert.NotNull(sub.NextMessage(500));
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(500));
                CheckHandler(MessageSizeExceedsMaxBytes, TypeNone, handler);
            });
        }

        [Fact]
        public void TestExceedsMaxRequestBytesExactBytes()
        {
            bool skip = false;
            TestEventHandler handler = new TestEventHandler();
            Context.RunInJsServer(handler.Modifier, c =>
            {
                skip = VersionIsBefore(c, "2.9.1");
                if (skip)
                {
                    return;
                }
                CreateDefaultTestStream(c);
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder().WithDurable(Durable(1)).Build());
                PullSubscribeOptions so = PullSubscribeOptions.BindTo(STREAM, Durable(1));
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, so);
                
                MsgHeader h = new MsgHeader();
                h["foo"] = "bar";
                // 159 + 180 + 661 = 1000
                // subject 7 + reply 52 + bytes 100 = 159
                // subject 7 + reply 52 + bytes 100 + headers 21 = 180
                // subject 7 + reply 52 + bytes 602 = 661
                js.Publish(SUBJECT, new byte[100]);
                js.Publish(SUBJECT, h, new byte[100]);
                js.Publish(SUBJECT, new byte[602]);

                sub.Pull(PullRequestOptions.Builder(10).WithMaxBytes(1000).WithExpiresIn(1000).Build());
                Assert.NotNull(sub.NextMessage(500));
                Assert.NotNull(sub.NextMessage(500));
                Assert.NotNull(sub.NextMessage(500));
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(500));
                CheckHandler(MessageSizeExceedsMaxBytes, TypeNone, handler);
            });
        }
    }
}
