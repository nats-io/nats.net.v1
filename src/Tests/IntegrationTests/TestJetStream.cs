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
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.ClientExDetail;

namespace IntegrationTests
{
    public class TestJetStream : TestSuite<JetStreamSuiteContext>
    {
        public TestJetStream(JetStreamSuiteContext context) : base(context) {}

        [Fact]
        public void TestJetStreamContextCreate()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c); // tries management functions
                c.CreateJetStreamManagementContext().GetAccountStatistics(); // another management
                c.CreateJetStreamContext().Publish(SUBJECT, DataBytes(1));
            });
        }

        [Fact]
        public void TestJetStreamNotEnabled()
        {
            Context.RunInServer(c =>
            {
                Assert.Throws<NATSNoRespondersException>(
                    () => c.CreateJetStreamContext().PushSubscribeSync(SUBJECT));

                Assert.Throws<NATSNoRespondersException>(
                    () => c.CreateJetStreamManagementContext().GetAccountStatistics());
            });
        }

        [Fact]
        public void TestJetStreamPublishDefaultOptions() {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();
                PublishAck ack = JsPublish(js);
                Assert.Equal(1U, ack.Seq);
                
            });
        }

        [Fact]
        public void TestConnectionClosing() {
            Context.RunInJsServer(c =>
            {
                c.Close();
                Assert.Throws<NATSConnectionClosedException>(() => c.CreateJetStreamContext().Publish(new Msg(SUBJECT, null)));
                Assert.Throws<NATSConnectionClosedException>(() => c.CreateJetStreamManagementContext().GetStreamNames());
            });
        }

        [Fact]
        public void TestCreateWithOptionsForCoverage() {
            Context.RunInJsServer(c =>
            {
                JetStreamOptions jso = JetStreamOptions.Builder().Build();
                c.CreateJetStreamContext(jso);
                c.CreateJetStreamManagementContext(jso);
            });
        }

        [Fact]
        public void TestJetStreamSubscribe() {
            Context.RunInJsServer(c =>
            {
                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                CreateDefaultTestStream(jsm);
                JsPublish(js);

                // default ephemeral subscription.
                IJetStreamPushSyncSubscription s = js.PushSubscribeSync(SUBJECT);
                Msg m = s.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal(DATA, Encoding.UTF8.GetString(m.Data));
                IList<String> names = jsm.GetConsumerNames(STREAM);
                Assert.Equal(1, names.Count);

                // default subscribe options // ephemeral subscription.
                s = js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().Build());
                m = s.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal(DATA, Encoding.UTF8.GetString(m.Data));
                names = jsm.GetConsumerNames(STREAM);
                Assert.Equal(2, names.Count);

                // set the stream
                PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                    .WithStream(STREAM).WithDurable(DURABLE).Build();
                s = js.PushSubscribeSync(SUBJECT, pso);
                m = s.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal(DATA, Encoding.UTF8.GetString(m.Data));
                names = jsm.GetConsumerNames(STREAM);
                Assert.Equal(3, names.Count);
                
                // coverage
                js.PushSubscribeSync(SUBJECT);
                js.PushSubscribeSync(SUBJECT, (PushSubscribeOptions)null);
                js.PushSubscribeSync(SUBJECT, QUEUE, null);
                js.PushSubscribeAsync(SUBJECT, (o, a) => {}, false);
                js.PushSubscribeAsync(SUBJECT, (o, a) => {}, false, null);
                js.PushSubscribeAsync(SUBJECT, QUEUE, (o, a) => {}, false, null);
            });
        }

        [Fact]
        public void TestJetStreamSubscribeErrors() {
            Context.RunInJsServer(c =>
            {
                IJetStream js = c.CreateJetStreamContext();

                // stream not found
                PushSubscribeOptions psoInvalidStream = PushSubscribeOptions.Builder().WithStream(STREAM).Build();
                Assert.Throws<NATSJetStreamException>(() => js.PushSubscribeSync(SUBJECT, psoInvalidStream));

                void AssertThrowsForSubject(Func<object> testCode)
                {
                    ArgumentException ae = Assert.Throws<ArgumentException>(testCode);
                    Assert.StartsWith("Subject", ae.Message);
                }
                
                void AssertThrowsForQueue(Func<object> testCode)
                {
                    ArgumentException ae = Assert.Throws<ArgumentException>(testCode);
                    Assert.StartsWith("Queue", ae.Message);
                }
                
                void AssertThrowsForHandler(Func<object> testCode)
                {
                    ArgumentNullException ae = Assert.Throws<ArgumentNullException>(testCode);
                    Assert.Equal("Handler", ae.ParamName);
                }

                // subject
                AssertThrowsForSubject(() => js.PushSubscribeSync(HasSpace));
                AssertThrowsForSubject(() => js.PushSubscribeSync(null, (PushSubscribeOptions)null));
                AssertThrowsForSubject(() => js.PushSubscribeSync(HasSpace, Plain));
                AssertThrowsForSubject(() => js.PushSubscribeSync(null, Plain, null));
                AssertThrowsForSubject(() => js.PushSubscribeAsync(HasSpace, null, false));
                AssertThrowsForSubject(() => js.PushSubscribeAsync(HasSpace, null, false, null));
                AssertThrowsForSubject(() => js.PushSubscribeAsync(HasSpace, Plain, null, false));
                AssertThrowsForSubject(() => js.PushSubscribeAsync(HasSpace, Plain, null, false, null));
                
                // queue
                AssertThrowsForQueue(() => js.PushSubscribeSync(Plain, HasSpace));
                AssertThrowsForQueue(() => js.PushSubscribeSync(Plain, HasSpace, null));
                AssertThrowsForQueue(() => js.PushSubscribeAsync(Plain, HasSpace, null, false));
                AssertThrowsForQueue(() => js.PushSubscribeAsync(Plain, HasSpace, null, false, null));
                
                // handler
                AssertThrowsForHandler(() => js.PushSubscribeAsync(Plain, null, false));
                AssertThrowsForHandler(() => js.PushSubscribeAsync(Plain, null, false, null));
                AssertThrowsForHandler(() => js.PushSubscribeAsync(Plain, Plain, null, false));
                AssertThrowsForHandler(() => js.PushSubscribeAsync(Plain, Plain, null, false, null));

            });
        }

        [Fact]
        public void TestFilterSubjectEphemeral() {
            Context.RunInJsServer(c =>
            {
                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                string subjectWild = SUBJECT + ".*";
                string subjectA = SUBJECT + ".A";
                string subjectB = SUBJECT + ".B";

                // create the stream.
                CreateMemoryStream(c, STREAM, subjectWild);

                JsPublish(js, subjectA, 1);
                JsPublish(js, subjectB, 1);
                JsPublish(js, subjectA, 1);
                JsPublish(js, subjectB, 1);

                // subscribe to the wildcard
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithAckPolicy(AckPolicy.None).Build();
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subjectWild, pso);
                c.Flush(1000);

                Msg m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(1U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(subjectB, m.Subject);
                Assert.Equal(2U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(3U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(subjectB, m.Subject);
                Assert.Equal(4U, m.MetaData.StreamSequence);

                // subscribe to A
                cc = ConsumerConfiguration.Builder().WithFilterSubject(subjectA).WithAckPolicy(AckPolicy.None).Build();
                pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                sub = js.PushSubscribeSync(subjectWild, pso);
                c.Flush(1000);

                m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(1U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(3U, m.MetaData.StreamSequence);
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(1000));

                // subscribe to B
                cc = ConsumerConfiguration.Builder().WithFilterSubject(subjectB).WithAckPolicy(AckPolicy.None).Build();
                pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                sub = js.PushSubscribeSync(subjectWild, pso);
                c.Flush(1000);

                m = sub.NextMessage(1000);
                Assert.Equal(subjectB, m.Subject);
                Assert.Equal(2U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(subjectB, m.Subject);
                Assert.Equal(4U, m.MetaData.StreamSequence);
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(1000));
            });
        }

        [Fact]
        public void TestFilterSubjectDurable() {
            Context.RunInJsServer(c =>
            {
                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                string subjectWild = SUBJECT + ".*";
                string subjectA = SUBJECT + ".A";
                string subjectB = SUBJECT + ".B";

                // create the stream.
                CreateMemoryStream(c, STREAM, subjectWild);

                JsPublish(js, subjectA, 1);
                JsPublish(js, subjectB, 1);
                JsPublish(js, subjectA, 1);
                JsPublish(js, subjectB, 1);

                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithFilterSubject(subjectA).WithAckPolicy(AckPolicy.None).Build();
                PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithDurable(DURABLE).WithConfiguration(cc).Build();
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subjectWild, pso);
                c.Flush(1000);

                Msg m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(1U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(3U, m.MetaData.StreamSequence);
                Assert.Throws<NATSTimeoutException>(() => sub.NextMessage(1000));
                sub.Unsubscribe();

                JsPublish(js, subjectA, 1);
                JsPublish(js, subjectB, 1);
                JsPublish(js, subjectA, 1);
                JsPublish(js, subjectB, 1);
            
                sub = js.PushSubscribeSync(subjectWild, pso);
                c.Flush(1000);

                m = sub.NextMessage(1000);
                Assert.Equal(subjectA, m.Subject);
                Assert.Equal(5U, m.MetaData.StreamSequence);
                m = sub.NextMessage(1000);
                Assert.Equal(7U, m.MetaData.StreamSequence);
                sub.Unsubscribe();

                ConsumerConfiguration cc1 = ConsumerConfiguration.Builder().WithFilterSubject(subjectWild).Build();
                PushSubscribeOptions pso1 = PushSubscribeOptions.Builder().WithDurable(DURABLE).WithConfiguration(cc1).Build();
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(subjectWild, pso1));

                ConsumerConfiguration cc2 = ConsumerConfiguration.Builder().WithFilterSubject(subjectB).Build();
                PushSubscribeOptions pso2 = PushSubscribeOptions.Builder().WithDurable(DURABLE).WithConfiguration(cc2).Build();
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(subjectWild, pso2));
            });
        }

        class JetStreamTestImpl : JetStream
        {
            public JetStreamTestImpl(IConnection connection) : base(connection, null) {}

            protected internal ConsumerInfo TestLookupConsumerInfo(string lookupStream, string lookupConsumer)
            {
                return LookupConsumerInfo(lookupStream, lookupConsumer);
            }
        }

        [Fact]
        public void TestInternalLookupConsumerInfoCoverage() {
            Context.RunInJsServer(c =>
            {
                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                CreateDefaultTestStream(c);

                // - consumer not found
                // - stream does not exist
                JetStreamTestImpl jst = new JetStreamTestImpl(c);
                
                Assert.Null(jst.TestLookupConsumerInfo(STREAM, DURABLE));
                Assert.Throws<NATSJetStreamException>(() => jst.TestLookupConsumerInfo(Stream(999), DURABLE));
            });
        }

        [Fact]
        public void TestBindPush()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                JsPublish(js, SUBJECT, 1, 1);
                PushSubscribeOptions pso = PushSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .Build();
                IJetStreamPushSyncSubscription s = js.PushSubscribeSync(SUBJECT, pso);
                Msg m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(1), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 2, 1);
                pso = PushSubscribeOptions.Builder()
                    .WithStream(STREAM)
                    .WithDurable(DURABLE)
                    .WithBind(true)
                    .Build();
                s = js.PushSubscribeSync(SUBJECT, pso);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(2), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 3, 1);
                pso = PushSubscribeOptions.BindTo(STREAM, DURABLE);
                s = js.PushSubscribeSync(SUBJECT, pso);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(3), Encoding.ASCII.GetString(m.Data));

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithStream(STREAM).WithBind(true).Build());

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithDurable(DURABLE).WithBind(true).Build());

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithStream(String.Empty).WithBind(true).Build());

                Assert.Throws<ArgumentException>(
                () => PushSubscribeOptions.Builder().WithStream(STREAM).WithDurable(String.Empty).WithBind(true).Build());
            });
        }

        [Fact]
        public void TestBindPull()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();
                
                JsPublish(js, SUBJECT, 1, 1);

                PullSubscribeOptions pso = PullSubscribeOptions.Builder()
                    .WithDurable(DURABLE)
                    .Build();
                IJetStreamPullSubscription s = js.PullSubscribe(SUBJECT, pso);
                s.Pull(1);
                Msg m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(1), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 2, 1);
                pso = PullSubscribeOptions.Builder()
                    .WithStream(STREAM)
                    .WithDurable(DURABLE)
                    .WithBind(true)
                    .Build();
                s = js.PullSubscribe(SUBJECT, pso);
                s.Pull(1);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(2), Encoding.ASCII.GetString(m.Data));
                m.Ack();
                s.Unsubscribe();

                JsPublish(js, SUBJECT, 3, 1);
                pso = PullSubscribeOptions.BindTo(STREAM, DURABLE);
                s = js.PullSubscribe(SUBJECT, pso);
                s.Pull(1);
                m = s.NextMessage(1000);
                Assert.NotNull(m);
                Assert.Equal(Data(3), Encoding.ASCII.GetString(m.Data));
            });
        }

        [Fact]
        public void TestBindErrors()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);

                IJetStream js = c.CreateJetStreamContext();
                
                PushSubscribeOptions pushso = PushSubscribeOptions.BindTo(STREAM, DURABLE);
                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, pushso));
                Assert.Contains(JsSubConsumerNotFoundRequiredInBind.Id, e.Message);
                
                PullSubscribeOptions pullso = PullSubscribeOptions.BindTo(STREAM, DURABLE);
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, pullso));
                Assert.Contains(JsSubConsumerNotFoundRequiredInBind.Id, e.Message);
            });
        }

        [Fact]
        public void TestBindDurableDeliverSubject()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);

                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                
                // create a durable push subscriber - has deliver subject
                ConsumerConfiguration ccDurPush = ConsumerConfiguration.Builder()
                        .WithDurable(Durable(1))
                        .WithDeliverSubject(Deliver(1))
                        .Build();
                jsm.AddOrUpdateConsumer(STREAM, ccDurPush);

                // create a durable pull subscriber - notice no deliver subject
                ConsumerConfiguration ccDurPull = ConsumerConfiguration.Builder()
                        .WithDurable(Durable(2))
                        .Build();
                jsm.AddOrUpdateConsumer(STREAM, ccDurPull);

                // try to pull subscribe against a push durable
                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithDurable(Durable(1)).Build()));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPush.Id, e.Message);

                // try to pull bind against a push durable
                e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PullSubscribe(SUBJECT, PullSubscribeOptions.BindTo(STREAM, Durable(1))));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPush.Id, e.Message);

                // this one is okay
                IJetStreamPullSubscription sub = js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithDurable(Durable(2)).Build());
                sub.Unsubscribe(); // so I can re-use the durable

                // try to push subscribe against a pull durable
                e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build()));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPull.Id, e.Message);

                // try to push bind against a pull durable
                e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.BindTo(STREAM, Durable(2))));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPull.Id, e.Message);

                // try to push subscribe but mismatch the deliver subject
                ConsumerConfiguration ccMis = ConsumerConfiguration.Builder().WithDeliverSubject("not-match").Build();
                PushSubscribeOptions psoMis = PushSubscribeOptions.Builder().WithDurable(Durable(1))
                    .WithConfiguration(ccMis).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, psoMis));
                Assert.Contains(JsSubExistingDeliverSubjectMismatch.Id, e.Message);

                // this one is okay
                js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithDurable(Durable(1)).Build());
            });
        }

        [Fact]
        public void TestGetConsumerInfoFromSubscription()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);

                IJetStream js = c.CreateJetStreamContext();

                IJetStreamPushSyncSubscription psync = js.PushSubscribeSync(SUBJECT);
                ConsumerInfo ci = psync.GetConsumerInformation();
                Assert.Equal(STREAM, ci.Stream);

                PullSubscribeOptions pso = PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();
                IJetStreamPullSubscription pull = js.PullSubscribe(SUBJECT, pso);
                ci = pull.GetConsumerInformation();
                Assert.Equal(STREAM, ci.Stream);
            });
        }

        [Fact]
        public void TestConsumerCannotBeModified() {
            Context.RunInJsServer(c =>
            {
                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                CreateDefaultTestStream(jsm);

                ConsumerConfiguration.ConsumerConfigurationBuilder builder = DurBuilder();
                jsm.AddOrUpdateConsumer(STREAM, builder.Build());

                CcbmEx(js, DurBuilder().WithDeliverPolicy(DeliverPolicy.Last));
                CcbmEx(js, DurBuilder().WithDeliverPolicy(DeliverPolicy.New));
                CcbmEx(js, DurBuilder().WithAckPolicy(AckPolicy.None));
                CcbmEx(js, DurBuilder().WithAckPolicy(AckPolicy.All));
                CcbmEx(js, DurBuilder().WithReplayPolicy(ReplayPolicy.Original));

                CcbmEx(js, DurBuilder().WithStartTime(DateTime.Now));
                CcbmEx(js, DurBuilder().WithAckWait(Duration.OfMillis(1)));
                CcbmEx(js, DurBuilder().WithDescription("x"));
                CcbmEx(js, DurBuilder().WithSampleFrequency("x"));
                CcbmEx(js, DurBuilder().WithIdleHeartbeat(Duration.OfMillis(1)));

                CcbmEx(js, DurBuilder().WithStartSequence(5));
                CcbmEx(js, DurBuilder().WithMaxDeliver(5));
                CcbmEx(js, DurBuilder().WithRateLimit(5));
                CcbmEx(js, DurBuilder().WithMaxAckPending(5));

                CcbmOk(js, DurBuilder().WithStartSequence(0));
                CcbmOk(js, DurBuilder().WithMaxDeliver(0));
                CcbmOk(js, DurBuilder().WithMaxDeliver(-1));
                CcbmOk(js, DurBuilder().WithRateLimit(0));
                CcbmOk(js, DurBuilder().WithRateLimit(-1));
                CcbmOk(js, DurBuilder().WithMaxAckPending(20000));
                CcbmOk(js, DurBuilder().WithMaxPullWaiting(0));
                
                ConsumerConfiguration.ConsumerConfigurationBuilder builder2 = ConsumerConfiguration.Builder().WithDurable(Durable(2));
                c.CreateJetStreamManagementContext().AddOrUpdateConsumer(STREAM, builder2.Build());
                CcbmExPull(js, builder2.WithMaxPullWaiting(999));
                CcbmOkPull(js, builder2.WithMaxPullWaiting(512)); // 512 is the default

                jsm.DeleteConsumer(STREAM, DURABLE);
                
                jsm.AddOrUpdateConsumer(STREAM, DurBuilder().WithDescription("desc").WithSampleFrequency("42").Build());
                CcbmOk(js, DurBuilder());
                CcbmEx(js, DurBuilder().WithDescription("x"));
                CcbmEx(js, DurBuilder().WithSampleFrequency("73"));
                
                jsm.DeleteConsumer(STREAM, DURABLE);

                builder = DurBuilder()
                    .WithStartSequence(5)
                    .WithMaxDeliver(6)
                    .WithRateLimit(7)
                    .WithMaxAckPending(8)
                    .WithDeliverPolicy(DeliverPolicy.ByStartSequence);
                
                jsm.AddOrUpdateConsumer(STREAM, builder.Build());

                CcbmEx(js, builder.WithStartSequence(55));
                CcbmEx(js, builder.WithMaxDeliver(66));
                CcbmEx(js, builder.WithRateLimit(77));
                CcbmEx(js, builder.WithMaxAckPending(88));
            });
        }

        private void CcbmOk(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()).Unsubscribe();
        }

        private void CcbmOkPull(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()).Unsubscribe();
        }

        private void CcbmEx(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()));
            Assert.Contains(JsSubExistingConsumerCannotBeModified.Id, e.Message);
        }

        private void CcbmExPull(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()));
            Assert.Contains(JsSubExistingConsumerCannotBeModified.Id, e.Message);
        }

        private ConsumerConfiguration.ConsumerConfigurationBuilder DurBuilder() {
            return ConsumerConfiguration.Builder().WithDurable(DURABLE).WithDeliverSubject(DELIVER);
        }

        [Fact]
        public void TestMoreCreateSubscriptionErrors()
        {
            Context.RunInJsServer(c =>
            {
                // Create our JetStream context.
                IJetStream js = c.CreateJetStreamContext();

                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT));
                Assert.Contains(JsSubNoMatchingStreamForSubject.Id, e.Message);

                // create the stream.
                CreateDefaultTestStream(c);

                // general pull push validation
                ConsumerConfiguration ccCantHave = ConsumerConfiguration.Builder().WithDurable("pulldur").WithDeliverGroup("cantHave").Build();
                PullSubscribeOptions pullCantHaveDlvrGrp = PullSubscribeOptions.Builder().WithConfiguration(ccCantHave).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, pullCantHaveDlvrGrp));
                Assert.Contains(JsSubPullCantHaveDeliverGroup.Id, e.Message);

                ccCantHave = ConsumerConfiguration.Builder().WithDurable("pulldur").WithDeliverSubject("cantHave").Build();
                PullSubscribeOptions pullCantHaveDlvrSub = PullSubscribeOptions.Builder().WithConfiguration(ccCantHave).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, pullCantHaveDlvrSub));
                Assert.Contains(JsSubPullCantHaveDeliverSubject.Id, e.Message);

                ccCantHave = ConsumerConfiguration.Builder().WithMaxPullWaiting(1).Build();
                PushSubscribeOptions pushCantHaveMpw = PushSubscribeOptions.Builder().WithConfiguration(ccCantHave).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, pushCantHaveMpw));
                Assert.Contains(JsSubPushCantHaveMaxPullWaiting.Id, e.Message);

                // create some consumers
                PushSubscribeOptions psoDurNoQ = PushSubscribeOptions.Builder().WithDurable("durNoQ").Build();
                js.PushSubscribeSync(SUBJECT, psoDurNoQ);

                PushSubscribeOptions psoDurYesQ = PushSubscribeOptions.Builder().WithDurable("durYesQ").Build();
                js.PushSubscribeSync(SUBJECT, "yesQ", psoDurYesQ);

                // already bound
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, psoDurNoQ));
                Assert.Contains(JsSubConsumerAlreadyBound.Id, e.Message);

                // queue match
                PushSubscribeOptions qmatch = PushSubscribeOptions.Builder()
                    .WithDurable("qmatchdur").WithDeliverGroup("qmatchq").Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, "qnotmatch", qmatch));
                Assert.Contains(JsSubQueueDeliverGroupMismatch.Id, e.Message);

                // queue vs config
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, "notConfigured", psoDurNoQ));
                Assert.Contains(JsSubExistingConsumerNotQueue.Id, e.Message);

                PushSubscribeOptions psoNoVsYes = PushSubscribeOptions.Builder().WithDurable("durYesQ").Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, psoNoVsYes));
                Assert.Contains(JsSubExistingConsumerIsQueue.Id, e.Message);

                PushSubscribeOptions psoYesVsNo = PushSubscribeOptions.Builder().WithDurable("durYesQ").Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, "qnotmatch", psoYesVsNo));
                Assert.Contains(JsSubExistingQueueDoesNotMatchRequestedQueue.Id, e.Message);

                // flow control heartbeat push / pull
                ConsumerConfiguration ccFc = ConsumerConfiguration.Builder().WithDurable("ccFcDur").WithFlowControl(1000).Build();
                ConsumerConfiguration ccHb = ConsumerConfiguration.Builder().WithDurable("ccHbDur").WithIdleHeartbeat(1000).Build();

                PullSubscribeOptions psoPullCcFc = PullSubscribeOptions.Builder().WithConfiguration(ccFc).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, psoPullCcFc));
                Assert.Contains(JsSubFcHbNotValidPull.Id, e.Message);

                PullSubscribeOptions psoPullCcHb = PullSubscribeOptions.Builder().WithConfiguration(ccHb).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, psoPullCcHb));
                Assert.Contains(JsSubFcHbNotValidPull.Id, e.Message);

                PushSubscribeOptions psoPushCcFc = PushSubscribeOptions.Builder().WithConfiguration(ccFc).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, "cantHaveQ", psoPushCcFc));
                Assert.Contains(JsSubFcHbHbNotValidQueue.Id, e.Message);

                PushSubscribeOptions psoPushCcHb = PushSubscribeOptions.Builder().WithConfiguration(ccHb).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, "cantHaveQ", psoPushCcHb));
                Assert.Contains(JsSubFcHbHbNotValidQueue.Id, e.Message);
            });
        }
    }
}
