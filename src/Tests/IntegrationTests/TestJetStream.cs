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
using System.Threading.Tasks;
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
    
        private void UnsubscribeEnsureNotBound(ISubscription sub)
        {
            sub.Unsubscribe();
            sub.Connection.Flush();
            Thread.Sleep(50);
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
                IList<string> names = jsm.GetConsumerNames(STREAM);
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
                if (AtLeast2_9_0(c)) {
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
    
        [Fact]
        public void TestJetStreamSubscribeLenientSubject() {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();
    
                js.PushSubscribeSync(SUBJECT, (PushSubscribeOptions)null);
                js.PushSubscribeSync(SUBJECT, null, (PushSubscribeOptions)null); // queue name is not required, just a weird way to call this api
                js.PushSubscribeAsync(SUBJECT, (s, e) => {}, false, (PushSubscribeOptions)null);
                js.PushSubscribeAsync(SUBJECT, null, (s, e) => {}, false, (PushSubscribeOptions)null); // queue name is not required, just a weird way to call this api
    
                PushSubscribeOptions pso = ConsumerConfiguration.Builder().WithFilterSubject(SUBJECT).BuildPushSubscribeOptions();
                js.PushSubscribeSync(null, pso);
                js.PushSubscribeSync(null, null, pso);
                js.PushSubscribeAsync(null, (s, e) => {}, false, pso);
                js.PushSubscribeAsync(null, null, (s, e) => {}, false, pso);
    
                PushSubscribeOptions psoF = ConsumerConfiguration.Builder().BuildPushSubscribeOptions();
    
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(null, psoF));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(null, psoF));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(null, null, psoF));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeAsync(null, (s, e) => {}, false, psoF));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeAsync(null, null, (s, e) => {}, false, psoF));
    
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(null, (PushSubscribeOptions)null));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(null, (PushSubscribeOptions)null));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeSync(null, null, (PushSubscribeOptions)null));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeAsync(null, (s, e) => {}, false, (PushSubscribeOptions)null));
                Assert.Throws<NATSJetStreamClientException>(() => js.PushSubscribeAsync(null, null, (s, e) => {}, false, (PushSubscribeOptions)null));
    
                PullSubscribeOptions lso = ConsumerConfiguration.Builder().WithFilterSubject(SUBJECT).BuildPullSubscribeOptions();
                js.PullSubscribe(null, lso);
                js.PullSubscribeAsync(null, (s, e) => {}, lso);
    
                PullSubscribeOptions lsoF = ConsumerConfiguration.Builder().BuildPullSubscribeOptions();
                Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribe(null, lsoF));
                Assert.Throws<NATSJetStreamClientException>(() => js.PullSubscribeAsync(null, (s, e) => {}, lsoF));
    
                Assert.Throws<ArgumentNullException>(() => js.PullSubscribe(null, (PullSubscribeOptions)null));
                Assert.Throws<ArgumentNullException>(() => js.PullSubscribeAsync(null, (s, e) => {}, (PullSubscribeOptions)null));
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

                Exception ex = null;
                foreach (string bad in BadSubjectsOrQueues) {
                    if (string.IsNullOrEmpty(bad))
                    {
                        ex = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(bad));
                        Assert.StartsWith("Subject", ex.Message);
                        NATSJetStreamClientException ce = Assert.Throws<NATSJetStreamClientException>(() =>
                            js.PushSubscribeSync(bad, (PushSubscribeOptions)null));
                        Assert.Contains(JsSubSubjectNeededToLookupStream.Id, ce.Message);

                        Assert.StartsWith("Subject", ex.Message);
                    }
                    else
                    {
                        // subject
                        ex = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(bad));
                        Assert.StartsWith("Subject", ex.Message);
                        ex = Assert.Throws<ArgumentException>(() =>
                            js.PushSubscribeSync(bad, (PushSubscribeOptions)null));
                        Assert.StartsWith("Subject", ex.Message);

                        // queue
                        if (!string.IsNullOrEmpty(bad))
                        {
                            ex = Assert.Throws<ArgumentException>(() => js.PushSubscribeSync(SUBJECT, bad, null));
                            Assert.StartsWith("Queue", ex.Message);
                            ex = Assert.Throws<ArgumentException>(() =>
                                js.PushSubscribeAsync(SUBJECT, bad, (s, e) => { }, false, null));
                            Assert.StartsWith("Queue", ex.Message);
                        }
                    }
                }
    
                // handler
                ex = Assert.Throws<ArgumentNullException>(() => js.PushSubscribeAsync(SUBJECT, null, false));
                Assert.Contains("Handler", ex.Message);
                ex = Assert.Throws<ArgumentNullException>(() => js.PushSubscribeAsync(SUBJECT, null, false, null));
                Assert.Contains("Handler", ex.Message);
                ex = Assert.Throws<ArgumentNullException>(() => js.PushSubscribeAsync(SUBJECT, QUEUE, null, false, null));
                Assert.Contains("Handler", ex.Message);
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
                sub = js.PushSubscribeSync(subjectA, pso);
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
                sub = js.PushSubscribeSync(subjectB, pso);
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

                NATSJetStreamClientException e; 
                    
                e = Assert.Throws<NATSJetStreamClientException>(
                    () => PushSubscribeOptions.Builder().WithStream(STREAM).WithBind(true).Build());
                Assert.Contains(JsSoNameOrDurableRequiredForBind.Id, e.Message);

                Assert.Throws<ArgumentException>(
                    () => PushSubscribeOptions.Builder().WithDurable(DURABLE).WithBind(true).Build());

                Assert.Throws<ArgumentException>(
                    () => PushSubscribeOptions.Builder().WithStream(string.Empty).WithBind(true).Build());

                e = Assert.Throws<NATSJetStreamClientException>(
                    () => PushSubscribeOptions.Builder().WithStream(STREAM).WithDurable(string.Empty).WithBind(true).Build());
                Assert.Contains(JsSoNameOrDurableRequiredForBind.Id, e.Message);
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
                FilterMatchSubscribeOk(js, jsm, SUBJECT, SUBJECT);
                FilterMatchSubscribeOk(js, jsm, ">", ">");
                FilterMatchSubscribeOk(js, jsm, "*", "*");

                // will not work
                FilterMatchSubscribeEx(js, jsm, SUBJECT, "");
                FilterMatchSubscribeEx(js, jsm, SUBJECT, ">");
                FilterMatchSubscribeEx(js, jsm, SUBJECT, "*");

                // multiple subjects no wildcards
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT, Subject(2));

                // will work as SubscribeSubject equals Filter Subject
                FilterMatchSubscribeOk(js, jsm, SUBJECT, SUBJECT);
                FilterMatchSubscribeOk(js, jsm, ">", ">");
                FilterMatchSubscribeOk(js, jsm, "*", "*");

                // will not work because stream has more than 1 subject
                FilterMatchSubscribeEx(js, jsm, SUBJECT, "");
                FilterMatchSubscribeEx(js, jsm, SUBJECT, ">");
                FilterMatchSubscribeEx(js, jsm, SUBJECT, "*");

                // multiple subjects via '>'
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                // will work, exact matches
                FilterMatchSubscribeOk(js, jsm, SubjectDot("1"), SubjectDot("1"));
                FilterMatchSubscribeOk(js, jsm, ">", ">");

                // will not work because mismatch / stream has more than 1 subject
                FilterMatchSubscribeEx(js, jsm, SubjectDot("1"), "");
                FilterMatchSubscribeEx(js, jsm, SubjectDot("1"), ">");
                FilterMatchSubscribeEx(js, jsm, SubjectDot("1"), SUBJECT_GT);

                // multiple subjects via '*'
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_STAR);

                // will work, exact matches
                FilterMatchSubscribeOk(js, jsm, SubjectDot("1"), SubjectDot("1"));
                FilterMatchSubscribeOk(js, jsm, ">", ">");

                // will not work because mismatch / stream has more than 1 subject
                FilterMatchSubscribeEx(js, jsm, SubjectDot("1"), "");
                FilterMatchSubscribeEx(js, jsm, SubjectDot("1"), ">");
                FilterMatchSubscribeEx(js, jsm, SubjectDot("1"), SUBJECT_STAR);
            });
        }

        private void FilterMatchSubscribeOk(IJetStream js, IJetStreamManagement jsm, string subscribeSubject, params string[] filterSubjects) 
        {
            int i = Rndm.Next(); // just want a unique number
            FilterMatchSetupConsumer(jsm, i, filterSubjects);
            js.PushSubscribeSync(subscribeSubject, ConsumerConfiguration.Builder().WithDurable(Durable(i)).BuildPushSubscribeOptions()).Unsubscribe();
        }

        private void FilterMatchSubscribeEx(IJetStream js, IJetStreamManagement jsm, string subscribeSubject, params string[] filterSubjects) 
        {
            int i = Rndm.Next(); // just want a unique number
            FilterMatchSetupConsumer(jsm, i, filterSubjects);
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PushSubscribeSync(subscribeSubject, ConsumerConfiguration.Builder().WithDurable(Durable(i)).BuildPushSubscribeOptions()));
            Assert.Contains(JsSubSubjectDoesNotMatchFilter.Id, e.Message);
        }

        private void FilterMatchSetupConsumer(IJetStreamManagement jsm, int i, params string[] fs)
        {
            jsm.AddOrUpdateConsumer(STREAM,
                ConsumerConfiguration.Builder().WithDeliverSubject(Deliver(i)).WithDurable(Durable(i)).WithFilterSubjects(fs).Build());
        }

        [Fact]
        public void TestBindDurableDeliverSubject()
        {
            Context.RunInJsServer(c =>
            {
                string stream = Stream();
                string subject = Subject();
                CreateMemoryStream(c, stream, subject);

                IJetStream js = c.CreateJetStreamContext();
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                
                // create a durable push subscriber - has deliver subject
                ConsumerConfiguration ccDurPush = ConsumerConfiguration.Builder()
                        .WithDurable(Durable(1))
                        .WithDeliverSubject(Deliver(1))
                        .WithFilterSubject(subject)
                        .Build();
                jsm.AddOrUpdateConsumer(stream, ccDurPush);

                // create a durable pull subscriber - notice no deliver subject
                ConsumerConfiguration ccDurPull = ConsumerConfiguration.Builder()
                        .WithDurable(Durable(2))
                        .Build();
                jsm.AddOrUpdateConsumer(stream, ccDurPull);

                // try to pull subscribe against a push durable
                NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PullSubscribe(subject, PullSubscribeOptions.Builder().WithDurable(Durable(1)).Build()));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPush.Id, e.Message);

                // try to pull bind against a push durable
                e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PullSubscribe(subject, PullSubscribeOptions.BindTo(stream, Durable(1))));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPush.Id, e.Message);

                // try to push subscribe against a pull durable
                e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PushSubscribeSync(subject, PushSubscribeOptions.Builder().WithDurable(Durable(2)).Build()));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPull.Id, e.Message);

                // try to push bind against a pull durable
                e = Assert.Throws<NATSJetStreamClientException>(
                        () => js.PushSubscribeSync(subject, PushSubscribeOptions.BindTo(stream, Durable(2))));
                Assert.Contains(JsSubConsumerAlreadyConfiguredAsPull.Id, e.Message);

                // this one is okay
                js.PushSubscribeSync(subject, PushSubscribeOptions.Builder().WithDurable(Durable(1)).Build());
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
                    .WithFilterSubject(SUBJECT)
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
                    .WithFilterSubject(SUBJECT)
                    .Build();
                jsm.AddOrUpdateConsumer(STREAM, cc);

                pushOpts = PushSubscribeOptions.BindTo(STREAM, Durable(21));
                js.PushSubscribeSync(SUBJECT, pushOpts); // should not throw an error

                cc = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(22))
                    .WithMaxPullWaiting(46)
                    .WithFilterSubject(SUBJECT)
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
                    .WithFilterSubject(SUBJECT)
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
                    .WithFilterSubject(SUBJECT)
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
                    .WithFilterSubject(SUBJECT)
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

                string stream = Stream(); 
                string subject = Subject(); 
                CreateMemoryStream(jsm, stream, subject);

                // push
                string uname = Durable();
                string deliver = Deliver();
                jsm.AddOrUpdateConsumer(stream, PushDurableBuilder(subject, uname, deliver).Build());

                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithDeliverPolicy(DeliverPolicy.Last), "DeliverPolicy");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithDeliverPolicy(DeliverPolicy.New), "DeliverPolicy");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithAckPolicy(AckPolicy.None), "AckPolicy");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithAckPolicy(AckPolicy.All), "AckPolicy");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithReplayPolicy(ReplayPolicy.Original), "ReplayPolicy");

                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithFlowControl(10000), "FlowControl");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithHeadersOnly(true), "HeadersOnly");

                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithStartTime(DateTime.UtcNow), "StartTime");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithAckWait(Duration.OfMillis(1)), "AckWait");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithDescription("x"), "Description");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithSampleFrequency("x"), "SampleFrequency");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithIdleHeartbeat(Duration.OfMillis(1000)), "IdleHeartbeat");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMaxExpires(Duration.OfMillis(1000)), "MaxExpires");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithInactiveThreshold(Duration.OfMillis(1000)), "InactiveThreshold");

                // value
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMaxDeliver(1), "MaxDeliver");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMaxAckPending(0), "MaxAckPending");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithAckWait(0), "AckWait");

                // value unsigned
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithStartSequence(1), "StartSequence");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithRateLimitBps(1), "RateLimitBps");

                // unset doesn't fail because the server provides a value equal to the unset
                ChangeOkPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMaxDeliver(-1));

                // unset doesn't fail because the server does not provide a value
                ChangeOkPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithStartSequence(0));
                ChangeOkPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithRateLimitBps(0));

                // unset fail b/c the server does set a value that is not equal to the unset or the minimum
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMaxAckPending(-1), "MaxAckPending");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMaxAckPending(0), "MaxAckPending");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithAckWait(-1), "AckWait");
                ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithAckWait(0), "AckWait");

                // pull
                string lname = Durable();
                jsm.AddOrUpdateConsumer(stream, PullDurableBuilder(subject, lname).Build());

                // value
                ChangeExPull(js, subject, PullDurableBuilder(subject, lname).WithMaxPullWaiting(0), "MaxPullWaiting");
                ChangeExPull(js, subject, PullDurableBuilder(subject, lname).WithMaxBatch(0), "MaxBatch");
                ChangeExPull(js, subject, PullDurableBuilder(subject, lname).WithMaxBytes(0), "MaxBytes");

                // unsets fail b/c the server does set a value
                ChangeExPull(js, subject, PullDurableBuilder(subject, lname).WithMaxPullWaiting(-1), "MaxPullWaiting");

                // unset
                ChangeOkPull(js, subject, PullDurableBuilder(subject, lname).WithMaxBatch(-1));

                if (c.ServerInfo.IsNewerVersionThan("2.9.99"))
                {
                    // metadata
                    Dictionary<string, string> metadataA = new Dictionary<string, string>();
                    metadataA["a"] = "A";
                    Dictionary<string, string> metadataB = new Dictionary<string, string>();
                    metadataA["b"] = "B";

                    // metadata server null versus new not null
                    jsm.AddOrUpdateConsumer(stream, PushDurableBuilder(subject, uname, deliver).Build());
                    ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMetadata(metadataA), "Metadata");

                    // metadata server not null versus new null
                    jsm.AddOrUpdateConsumer(stream, PushDurableBuilder(subject, uname, deliver).WithMetadata(metadataA).Build());
                    ChangeOkPush(js, subject, PushDurableBuilder(subject, uname, deliver));

                    // metadata server not null versus new not null but different
                    ChangeExPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMetadata(metadataB), "Metadata");

                    // metadata server not null versus new not null and same
                    ChangeOkPush(js, subject, PushDurableBuilder(subject, uname, deliver).WithMetadata(metadataA));
                }                
            });
        }

        private void ChangeOkPush(IJetStream js, string subject, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            js.PushSubscribeSync(subject, PushSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()).Unsubscribe();
        }

        private void ChangeOkPull(IJetStream js, string subject, ConsumerConfiguration.ConsumerConfigurationBuilder builder) {
            js.PullSubscribe(subject, PullSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()).Unsubscribe();
        }

        private void ChangeExPush(IJetStream js, string subject, ConsumerConfiguration.ConsumerConfigurationBuilder builder, string changedField) {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PushSubscribeSync(subject, PushSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()));
            _ChangeEx(e, changedField);
        }

        private void ChangeExPull(IJetStream js, string subject, ConsumerConfiguration.ConsumerConfigurationBuilder builder, string changedField) {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(
                () => js.PullSubscribe(subject, PullSubscribeOptions.Builder().WithConfiguration(builder.Build()).Build()));
            _ChangeEx(e, changedField);
        }

        private void _ChangeEx(NATSJetStreamClientException e, string changedField)
        {
            string msg = e.Message;
            Assert.Contains(JsSubExistingConsumerCannotBeModified.Id, msg);
            Assert.Contains(changedField, msg);
        }

        private ConsumerConfiguration.ConsumerConfigurationBuilder PushDurableBuilder(string subject, string durable, string deliver) {
            return ConsumerConfiguration.Builder().WithDurable(durable)
                .WithDeliverSubject(deliver)
                .WithFilterSubject(subject);
        }

        private ConsumerConfiguration.ConsumerConfigurationBuilder PullDurableBuilder(string subject, string durable) {
            return ConsumerConfiguration.Builder().WithDurable(durable).WithFilterSubject(subject);
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

        [Fact]
        public async Task TestMaxPayloadJs()
        {
            string streamName = "stream-max-payload-test";
            string subject1 = "mptest1";
            string subject2 = "mptest2";
            
            Options opts = ConnectionFactory.GetDefaultOptions();
            opts.AllowReconnect = false;
            opts.Url = $"nats://127.0.0.1:{Context.Server1.Port}";
            NATSServer.QuietOptionsModifier.Invoke(opts);

            ulong expectedSeq = 0;
            using (NATSServer.CreateJetStreamFast(Context.Server1.Port))
            {
                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    try { jsm.DeleteStream(streamName); } catch (NATSJetStreamException) {}
                    jsm.AddStream(StreamConfiguration.Builder()
                        .WithName(streamName)
                        .WithStorageType(StorageType.Memory)
                        .WithSubjects(subject1, subject2)
                        .WithMaxMsgSize(1000)
                        .Build()
                    );

                    IJetStream js = c.CreateJetStreamContext();
                    for (ulong x = 1; x <= 3; x++)
                    {
                        long size = 1000 + (long)x - 2;
                        if (size > 1000)
                        {
                            NATSJetStreamException e = Assert.Throws<NATSJetStreamException>(() => js.Publish(subject1, new byte[size]));
                            Assert.Equal(10054, e.ApiErrorCode);
                        }
                        else
                        {
                            PublishAck pa = js.Publish(subject1, new byte[size]);
                            Assert.Equal(++expectedSeq, pa.Seq);
                        }
                    }
                }
                
                bool receivedDisconnect = false;
                opts.DisconnectedEventHandler = (sender, args) =>
                {
                    receivedDisconnect = true;
                };

                using (var c = Context.ConnectionFactory.CreateConnection(opts))
                {
                    IJetStream js = c.CreateJetStreamContext();
                    for (ulong x = 1; x <= 3; x++)
                    {
                        long size = 1000 + (long)x - 2;
                        if (size > 1000)
                        {
                            try
                            {
                                PublishAck paTask = await js.PublishAsync(subject2, new byte[size]);
                            }
                            catch (NATSJetStreamException j)
                            {
                                Assert.Equal(10054, j.ApiErrorCode);
                            }
                        }
                        else
                        {
                            Task<PublishAck> paTask = js.PublishAsync(subject2, new byte[size]);
                            paTask.Wait(100);
                            PublishAck pa = paTask.Result;
                            Assert.Equal(++expectedSeq, pa.Seq);
                        }
                    }
                }
                Assert.True(receivedDisconnect);
            }
        }
    }
}
