﻿// Copyright 2021 The NATS Authors
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
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.ClientExDetail;

namespace IntegrationTests
{
    public class TestJetStream : TestSuite<JetStreamSuiteContext>
    {
        public TestJetStream(JetStreamSuiteContext context) : base(context)
        {
        }

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
        public void TestConnectionClosing()
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(Context.Server1.Port))
            {
                var c = Context.OpenConnection(Context.Server1.Port);
                c.Close();
                Thread.Sleep(100);
                Assert.Throws<NATSConnectionClosedException>(() =>
                    c.CreateJetStreamContext().Publish(new Msg(SUBJECT, null)));
                Assert.Throws<NATSConnectionClosedException>(() =>
                    c.CreateJetStreamManagementContext().GetStreamNames());
            }
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
                bool atLeast290 = c.ServerInfo.IsSameOrNewerThanVersion("2.9.0");
                
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

                // bind with w/o subject
                jsm.AddOrUpdateConsumer(STREAM,
                    ConsumerConfiguration.Builder()
                        .WithDurable(Durable(101))
                        .WithDeliverSubject(Deliver(101))
                        .Build());
                
                PushSubscribeOptions psoBind = PushSubscribeOptions.BindTo(STREAM, Durable(101));
                UnsubscribeEnsureNotBound(js.PushSubscribeSync(null, psoBind));
                UnsubscribeEnsureNotBound(js.PushSubscribeSync("", psoBind));
                UnsubscribeEnsureNotBound(js.PushSubscribeAsync(null, (o, a) => { }, false, psoBind));
                js.PushSubscribeAsync("", (o, a) => { }, false, psoBind);

                jsm.AddOrUpdateConsumer(STREAM,
                    ConsumerConfiguration.Builder()
                        .WithDurable(Durable(102))
                        .WithDeliverSubject(Deliver(102))
                        .WithDeliverGroup(Queue(102))
                        .Build());
                psoBind = PushSubscribeOptions.BindTo(STREAM, Durable(102));
                js.PushSubscribeSync(null, Queue(102), psoBind).Unsubscribe();
                js.PushSubscribeSync("", Queue(102), psoBind).Unsubscribe();
                js.PushSubscribeAsync(null, Queue(102), (o, a) => { }, false, psoBind).Unsubscribe();
                js.PushSubscribeAsync("", Queue(102), (o, a) => { }, false, psoBind);
                
                // test 2.9.0
                if (atLeast290) {
                    ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithName(Name(1)).Build();
                    pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                    IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT, pso);
                    m = sub.NextMessage(1000);
                    Assert.NotNull(m);
                    Assert.Equal(DATA, Encoding.UTF8.GetString(m.Data));
                    ConsumerInfo ci = sub.GetConsumerInformation();
                    Assert.Equal(Name(1), ci.Name);
                    Assert.Equal(Name(1), ci.ConsumerConfiguration.Name);
                    Assert.Empty(ci.ConsumerConfiguration.Durable);
                    
                    cc = ConsumerConfiguration.Builder().WithDurable(Durable(1)).Build();
                    pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                    sub = js.PushSubscribeSync(SUBJECT, pso);
                    m = sub.NextMessage(1000);
                    Assert.NotNull(m);
                    Assert.Equal(DATA, Encoding.UTF8.GetString(m.Data));
                    ci = sub.GetConsumerInformation();
                    Assert.Equal(Durable(1), ci.Name);
                    Assert.Equal(Durable(1), ci.ConsumerConfiguration.Name);
                    Assert.Equal(Durable(1), ci.ConsumerConfiguration.Durable);
                    
                    cc = ConsumerConfiguration.Builder().WithDurable(Name(2)).WithName(Name(2)).Build();
                    pso = PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
                    sub = js.PushSubscribeSync(SUBJECT, pso);
                    m = sub.NextMessage(1000);
                    Assert.NotNull(m);
                    Assert.Equal(DATA, Encoding.UTF8.GetString(m.Data));
                    ci = sub.GetConsumerInformation();
                    Assert.Equal(Name(2), ci.Name);
                    Assert.Equal(Name(2), ci.ConsumerConfiguration.Name);
                    Assert.Equal(Name(2), ci.ConsumerConfiguration.Durable);

                    // test opt out
                    JetStreamOptions jso = JetStreamOptions.Builder().WithOptOut290ConsumerCreate(true).Build();
                    IJetStream jsOptOut = c.CreateJetStreamContext(jso);
                    ConsumerConfiguration ccOptOut = ConsumerConfiguration.Builder().WithName(Name(99)).Build();
                    PushSubscribeOptions psoOptOut = PushSubscribeOptions.Builder().WithConfiguration(ccOptOut).Build();
                    NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => jsOptOut.PushSubscribeSync(SUBJECT, psoOptOut));
                    Assert.Contains(JsConsumerCreate290NotAvailable.Id, e.Message);
                }
            });
        }

        private void UnsubscribeEnsureNotBound(ISubscription sub)
        {
            sub.Unsubscribe();
            sub.Connection.Flush();
            Thread.Sleep(50);
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
        public void TestBindExceptions()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);

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

        private static readonly Random Rndm = new Random();
        
        [Fact]
        public void TestFilterMismatchErrors()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // single subject
                CreateMemoryStream(jsm, STREAM, SUBJECT);

                // will work as SubscribeSubject equals Filter Subject
                SubscribeOk(js, jsm, SUBJECT, SUBJECT);
                SubscribeOk(js, jsm, ">", ">");
                SubscribeOk(js, jsm, "*", "*");

                // will work as SubscribeSubject != empty Filter Subject,
                // b/c Stream has exactly 1 subject and is a match.
                SubscribeOk(js, jsm, "", SUBJECT);

                // will work as SubscribeSubject != Filter Subject of '>'
                // b/c Stream has exactly 1 subject and is a match.
                SubscribeOk(js, jsm, ">", SUBJECT);

                // will not work
                SubscribeEx(js, jsm, "*", SUBJECT);

                // multiple subjects no wildcards
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT, Subject(2));

                // will work as SubscribeSubject equals Filter Subject
                SubscribeOk(js, jsm, SUBJECT, SUBJECT);
                SubscribeOk(js, jsm, ">", ">");
                SubscribeOk(js, jsm, "*", "*");

                // will not work because stream has more than 1 subject
                SubscribeEx(js, jsm, "", SUBJECT);
                SubscribeEx(js, jsm, ">", SUBJECT);
                SubscribeEx(js, jsm, "*", SUBJECT);

                // multiple subjects via '>'
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                // will work, exact matches
                SubscribeOk(js, jsm, SubjectDot("1"), SubjectDot("1"));
                SubscribeOk(js, jsm, ">", ">");

                // will not work because mismatch / stream has more than 1 subject
                SubscribeEx(js, jsm, "", SubjectDot("1"));
                SubscribeEx(js, jsm, ">", SubjectDot("1"));
                SubscribeEx(js, jsm, SUBJECT_GT, SubjectDot("1"));

                // multiple subjects via '*'
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_STAR);

                // will work, exact matches
                SubscribeOk(js, jsm, SubjectDot("1"), SubjectDot("1"));
                SubscribeOk(js, jsm, ">", ">");

                // will not work because mismatch / stream has more than 1 subject
                SubscribeEx(js, jsm, "", SubjectDot("1"));
                SubscribeEx(js, jsm, ">", SubjectDot("1"));
                SubscribeEx(js, jsm, SUBJECT_STAR, SubjectDot("1"));
            });
        }

        private void SubscribeOk(IJetStream js, IJetStreamManagement jsm, string fs, string ss) 
        {
            int i = Rndm.Next(); // just want a unique number
            SetupConsumer(jsm, i, fs);
            js.PushSubscribeSync(ss, ConsumerConfiguration.Builder().WithDurable(Durable(i)).BuildPushSubscribeOptions()).Unsubscribe();
        }

        private void SubscribeEx(IJetStream js, IJetStreamManagement jsm, string fs, string ss) 
        {
            int i = Rndm.Next(); // just want a unique number
            SetupConsumer(jsm, i, fs);
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PushSubscribeSync(ss, ConsumerConfiguration.Builder().WithDurable(Durable(i)).BuildPushSubscribeOptions()));
            Assert.Contains(JsSubSubjectDoesNotMatchFilter.Id, e.Message);
        }

        private void SetupConsumer(IJetStreamManagement jsm, int i, String fs)
        {
            jsm.AddOrUpdateConsumer(STREAM,
                ConsumerConfiguration.Builder().WithDeliverSubject(Deliver(i)).WithDurable(Durable(i)).WithFilterSubject(fs).Build());
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
        public void TestConsumerIsNotModified()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                // test with config in issue 105
                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                    .WithDescription("desc")
                    .WithAckPolicy(AckPolicy.Explicit)
                    .WithDeliverPolicy(DeliverPolicy.All)
                    .WithDeliverSubject(Deliver(1))
                    .WithDeliverGroup(Queue(1))
                    .WithDurable(Durable(1))
                    .WithMaxAckPending(65000)
                    .WithMaxDeliver(5)
                    .WithReplayPolicy(ReplayPolicy.Instant)
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                IJetStream js = c.CreateJetStreamContext();
                
                PushSubscribeOptions pushOpts = PushSubscribeOptions.BindTo(STREAM, Durable(1));
                js.PushSubscribeSync(SUBJECT, Queue(1), pushOpts); // should not throw an error

                // testing numerics
                cc = ConsumerConfiguration.Builder()
                    .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                    .WithDeliverSubject(Deliver(21))
                    .WithDurable(Durable(21))
                    .WithStartSequence(42)
                    .WithMaxDeliver(43)
                    .WithRateLimitBps(44)
                    .WithMaxAckPending(45)
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                pushOpts = PushSubscribeOptions.BindTo(STREAM, Durable(21));
                js.PushSubscribeSync(SUBJECT, pushOpts); // should not throw an error

                cc = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(22))
                    .WithMaxPullWaiting(46)
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                PullSubscribeOptions pullOpts = PullSubscribeOptions.BindTo(STREAM, Durable(22));
                js.PullSubscribe(SUBJECT, pullOpts); // should not throw an error

                // testing DateTime
                cc = ConsumerConfiguration.Builder()
                    .WithDeliverPolicy(DeliverPolicy.ByStartTime)
                    .WithDeliverSubject(Deliver(3))
                    .WithDurable(Durable(3))
                    .WithStartTime(DateTime.UtcNow.AddHours(1))
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                pushOpts = PushSubscribeOptions.BindTo(STREAM, Durable(3));
                js.PushSubscribeSync(SUBJECT, pushOpts); // should not throw an error
                
                // testing boolean and duration
                cc = ConsumerConfiguration.Builder()
                    .WithDeliverSubject(Deliver(4))
                    .WithDurable(Durable(4))
                    .WithFlowControl(1000)
                    .WithHeadersOnly(true)
                    .WithAckWait(2000)
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                pushOpts = PushSubscribeOptions.BindTo(STREAM, Durable(4));
                js.PushSubscribeSync(SUBJECT, pushOpts); // should not throw an error
                
                // testing enums
                cc = ConsumerConfiguration.Builder()
                    .WithDeliverSubject(Deliver(5))
                    .WithDurable(Durable(5))
                    .WithDeliverPolicy(DeliverPolicy.Last)
                    .WithAckPolicy(AckPolicy.None)
                    .WithReplayPolicy(ReplayPolicy.Original)
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                pushOpts = PushSubscribeOptions.BindTo(STREAM, Durable(5));
                js.PushSubscribeSync(SUBJECT, pushOpts); // should not throw an error
            });
        }

        [Fact]
        public void TestSubscribeDurableConsumerMustMatch() {
            Context.RunInJsServer(c =>
            {
                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                CreateDefaultTestStream(jsm);

                // push
                jsm.AddOrUpdateConsumer(STREAM, PushDurableBuilder().Build());

                ChangeExPush(js, PushDurableBuilder().WithDeliverPolicy(DeliverPolicy.Last), "DeliverPolicy");
                ChangeExPush(js, PushDurableBuilder().WithDeliverPolicy(DeliverPolicy.New), "DeliverPolicy");
                ChangeExPush(js, PushDurableBuilder().WithAckPolicy(AckPolicy.None), "AckPolicy");
                ChangeExPush(js, PushDurableBuilder().WithAckPolicy(AckPolicy.All), "AckPolicy");
                ChangeExPush(js, PushDurableBuilder().WithReplayPolicy(ReplayPolicy.Original), "ReplayPolicy");

                ChangeExPush(js, PushDurableBuilder().WithFlowControl(10000), "FlowControl");
                ChangeExPush(js, PushDurableBuilder().WithHeadersOnly(true), "HeadersOnly");

                ChangeExPush(js, PushDurableBuilder().WithStartTime(DateTime.UtcNow), "StartTime");
                ChangeExPush(js, PushDurableBuilder().WithAckWait(Duration.OfMillis(1)), "AckWait");
                ChangeExPush(js, PushDurableBuilder().WithDescription("x"), "Description");
                ChangeExPush(js, PushDurableBuilder().WithSampleFrequency("x"), "SampleFrequency");
                ChangeExPush(js, PushDurableBuilder().WithIdleHeartbeat(Duration.OfMillis(1000)), "IdleHeartbeat");
                ChangeExPush(js, PushDurableBuilder().WithMaxExpires(Duration.OfMillis(1000)), "MaxExpires");
                ChangeExPush(js, PushDurableBuilder().WithInactiveThreshold(Duration.OfMillis(1000)), "InactiveThreshold");

                // value
                ChangeExPush(js, PushDurableBuilder().WithMaxDeliver(1), "MaxDeliver");
                ChangeExPush(js, PushDurableBuilder().WithMaxAckPending(0), "MaxAckPending");
                ChangeExPush(js, PushDurableBuilder().WithAckWait(0), "AckWait");

                // value unsigned
                ChangeExPush(js, PushDurableBuilder().WithStartSequence(1), "StartSequence");
                ChangeExPush(js, PushDurableBuilder().WithRateLimitBps(1), "RateLimitBps");

                // unset doesn't fail because the server provides a value equal to the unset
                ChangeOkPush(js, PushDurableBuilder().WithMaxDeliver(-1));

                // unset doesn't fail because the server does not provide a value
                ChangeOkPush(js, PushDurableBuilder().WithStartSequence(0));
                ChangeOkPush(js, PushDurableBuilder().WithRateLimitBps(0));

                // unset fail b/c the server does set a value that is not equal to the unset or the minimum
                ChangeExPush(js, PushDurableBuilder().WithMaxAckPending(-1), "MaxAckPending");
                ChangeExPush(js, PushDurableBuilder().WithMaxAckPending(0), "MaxAckPending");
                ChangeExPush(js, PushDurableBuilder().WithAckWait(-1), "AckWait");
                ChangeExPush(js, PushDurableBuilder().WithAckWait(0), "AckWait");

                // pull
                jsm.AddOrUpdateConsumer(STREAM, PullDurableBuilder().Build());

                // value
                ChangeExPull(js, PullDurableBuilder().WithMaxPullWaiting(0), "MaxPullWaiting");
                ChangeExPull(js, PullDurableBuilder().WithMaxBatch(0), "MaxBatch");
                ChangeExPull(js, PullDurableBuilder().WithMaxBytes(0), "MaxBytes");

                // unsets fail b/c the server does set a value
                ChangeExPull(js, PullDurableBuilder().WithMaxPullWaiting(-1), "MaxPullWaiting");

                // unset
                ChangeOkPull(js, PullDurableBuilder().WithMaxBatch(-1));

                if (c.ServerInfo.IsNewerVersionThan("2.9.99"))
                {
                    // metadata
                    Dictionary<string, string> metadataA = new Dictionary<string, string>();
                    metadataA["a"] = "A";
                    Dictionary<string, string> metadataB = new Dictionary<string, string>();
                    metadataA["b"] = "B";

                    // metadata server null versus new not null
                    jsm.AddOrUpdateConsumer(STREAM, PushDurableBuilder().Build());
                    ChangeExPush(js, PushDurableBuilder().WithMetadata(metadataA), "Metadata");

                    // metadata server not null versus new null
                    jsm.AddOrUpdateConsumer(STREAM, PushDurableBuilder().WithMetadata(metadataA).Build());
                    ChangeExPush(js, PushDurableBuilder(), "Metadata");

                    // metadata server not null versus new not null but different
                    ChangeExPush(js, PushDurableBuilder().WithMetadata(metadataB), "Metadata");

                    // metadata server not null versus new not null and same
                    ChangeOkPush(js, PushDurableBuilder().WithMetadata(metadataA));
                }                
            });
        }

        private void ChangeOkPush(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()).Unsubscribe();
        }

        private void ChangeOkPull(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()).Unsubscribe();
        }

        private void ChangeExPush(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder, String changedField) {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PushSubscribeSync(SUBJECT, PushSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()));
            _ChangeEx(e, changedField);
        }

        private void ChangeExPull(IJetStream js, ConsumerConfiguration.ConsumerConfigurationBuilder builder, String changedField) {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PullSubscribe(SUBJECT, PullSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()));
            _ChangeEx(e, changedField);
        }

        private void _ChangeEx(NATSJetStreamClientException e, String changedField)
        {
            String msg = e.Message;
            Assert.Contains(JsSubExistingConsumerCannotBeModified.Id, msg);
            Assert.Contains(changedField, msg);
        }

        private ConsumerConfiguration.ConsumerConfigurationBuilder PushDurableBuilder() {
            return ConsumerConfiguration.Builder().WithDurable(PUSH_DURABLE).WithDeliverSubject(DELIVER);
        }

        private ConsumerConfiguration.ConsumerConfigurationBuilder PullDurableBuilder() {
            return ConsumerConfiguration.Builder().WithDurable(PULL_DURABLE);
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
                ConsumerConfiguration ccPush = ConsumerConfiguration.Builder().WithDurable("pulldur").WithDeliverGroup("cantHave").Build();
                PullSubscribeOptions pullCantHaveDlvrGrp = PullSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, pullCantHaveDlvrGrp));
                Assert.Contains(JsSubPullCantHaveDeliverGroup.Id, e.Message);

                ccPush = ConsumerConfiguration.Builder().WithDurable("pulldur").WithDeliverSubject("cantHave").Build();
                PullSubscribeOptions pullCantHaveDlvrSub = PullSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(SUBJECT, pullCantHaveDlvrSub));
                Assert.Contains(JsSubPullCantHaveDeliverSubject.Id, e.Message);

                ccPush = ConsumerConfiguration.Builder().WithMaxPullWaiting(1).Build();
                PushSubscribeOptions pushSo = PushSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, pushSo));
                Assert.Contains(JsSubPushCantHaveMaxPullWaiting.Id, e.Message);

                ccPush = ConsumerConfiguration.Builder().WithMaxPullWaiting(-1).Build();
                pushSo = PushSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                js.PushSubscribeSync(SUBJECT, pushSo);

                ccPush = ConsumerConfiguration.Builder().WithMaxBatch(1).Build();
                pushSo = PushSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, pushSo));

                ccPush = ConsumerConfiguration.Builder().WithMaxBatch(-1).Build();
                pushSo = PushSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                js.PushSubscribeSync(SUBJECT, pushSo);
                Assert.Contains(JsSubPushCantHaveMaxBatch.Id, e.Message);

                ccPush = ConsumerConfiguration.Builder().WithMaxBytes(1).Build();
                pushSo = PushSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, pushSo));

                ccPush = ConsumerConfiguration.Builder().WithMaxBytes(-1).Build();
                pushSo = PushSubscribeOptions.Builder().WithConfiguration(ccPush).Build();
                js.PushSubscribeSync(SUBJECT, pushSo);
                Assert.Contains(JsSubPushCantHaveMaxBytes.Id, e.Message);

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
                Assert.Contains(JsSubFcHbNotValidQueue.Id, e.Message);

                PushSubscribeOptions psoPushCcHb = PushSubscribeOptions.Builder().WithConfiguration(ccHb).Build();
                e = Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(SUBJECT, "cantHaveQ", psoPushCcHb));
                Assert.Contains(JsSubFcHbNotValidQueue.Id, e.Message);
            });
        }
        
    }
}
