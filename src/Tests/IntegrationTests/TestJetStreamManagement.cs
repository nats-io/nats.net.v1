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
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using static IntegrationTests.JetStreamTestBase;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestJetStreamManagement : TestSuite<JetStreamManagementSuiteContext>
    {
        public TestJetStreamManagement(JetStreamManagementSuiteContext context) : base(context) { }

        [Fact]
        public void TestStreamCreate()
        {
            Context.RunInJsServer(c =>
            {
                DateTime now = DateTime.Now;

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(STREAM)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(Subject(0), Subject(1))
                    .Build();

                StreamInfo si = jsm.AddStream(sc);
                Assert.True(now <= si.Created);

                Assert.NotNull(si.Config);
                sc = si.Config;
                Assert.Equal(STREAM, sc.Name);

                Assert.Equal(2, sc.Subjects.Count);
                Assert.Equal(Subject(0), sc.Subjects[0]);
                Assert.Equal(Subject(1), sc.Subjects[1]);

                Assert.Equal(RetentionPolicy.Limits, sc.RetentionPolicy);
                Assert.Equal(DiscardPolicy.Old, sc.DiscardPolicy);
                Assert.Equal(StorageType.Memory, sc.StorageType);

                Assert.NotNull(si.State);
                Assert.Equal(-1, sc.MaxConsumers);
                Assert.Equal(-1, sc.MaxMsgs);
                Assert.Equal(-1, sc.MaxBytes);
                Assert.Equal(-1, sc.MaxMsgSize);
                Assert.Equal(1, sc.Replicas);

                Assert.Equal(Duration.Zero, sc.MaxAge);
                Assert.Equal(Duration.OfSeconds(120), sc.DuplicateWindow);
                Assert.False(sc.NoAck);
                Assert.Empty(sc.TemplateOwner);

                StreamState ss = si.State;
                Assert.Equal(0, ss.Messages);
                Assert.Equal(0, ss.Bytes);
                Assert.Equal(0, ss.FirstSeq);
                Assert.Equal(0, ss.LastSeq);
                Assert.Equal(0, ss.ConsumerCount);
            });
        }

        [Fact]
        public void TestUpdateStream()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                StreamInfo si = AddTestStream(jsm);
                StreamConfiguration sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(STREAM, sc.Name);
                Assert.NotNull(sc.Subjects);
                Assert.Equal(2, sc.Subjects.Count);
                Assert.Equal(Subject(0), sc.Subjects[0]);
                Assert.Equal(Subject(1), sc.Subjects[1]);
                Assert.Equal(-1, sc.MaxBytes);
                Assert.Equal(-1, sc.MaxMsgSize);
                Assert.Equal(Duration.Zero, sc.MaxAge);
                Assert.Equal(StorageType.Memory, sc.StorageType);
                Assert.Equal(DiscardPolicy.Old, sc.DiscardPolicy);
                Assert.Equal(1, sc.Replicas);
                Assert.False(sc.NoAck);
                Assert.Equal(Duration.OfMinutes(2), sc.DuplicateWindow);
                Assert.Empty(sc.TemplateOwner);

                sc = StreamConfiguration.Builder()
                        .WithName(STREAM)
                        .WithStorageType(StorageType.Memory)
                        .WithSubjects(Subject(0), Subject(1), Subject(2))
                        .WithMaxBytes(43)
                        .WithMaxMsgSize(44)
                        .WithMaxAge(Duration.OfDays(100))
                        .WithDiscardPolicy(DiscardPolicy.New)
                        .WithNoAck(true)
                        .WithDuplicateWindow(Duration.OfMinutes(3))
                        .Build();
                si = jsm.UpdateStream(sc);
                Assert.NotNull(si);

                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(STREAM, sc.Name);
                Assert.NotNull(sc.Subjects);
                Assert.Equal(3, sc.Subjects.Count);
                Assert.Equal(Subject(0), sc.Subjects[0]);
                Assert.Equal(Subject(1), sc.Subjects[1]);
                Assert.Equal(Subject(2), sc.Subjects[2]);
                Assert.Equal(43, sc.MaxBytes);
                Assert.Equal(44, sc.MaxMsgSize);
                Assert.Equal(Duration.OfDays(100), sc.MaxAge);
                Assert.Equal(StorageType.Memory, sc.StorageType);
                Assert.Equal(DiscardPolicy.New, sc.DiscardPolicy);
                Assert.Equal(1, sc.Replicas);
                Assert.True(sc.NoAck);
                Assert.Equal(Duration.OfMinutes(3), sc.DuplicateWindow);
                Assert.Empty(sc.TemplateOwner);

            });
        }
        
        [Fact]
        public void TestAddUpdateStreamInvalids()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                StreamConfiguration scNoName = StreamConfiguration.Builder().Build();
                Assert.Throws<ArgumentNullException>(() => jsm.AddStream(null));
                Assert.Throws<ArgumentException>(() => jsm.AddStream(scNoName));
                Assert.Throws<ArgumentNullException>(() => jsm.UpdateStream(null));
                Assert.Throws<ArgumentException>(() => jsm.UpdateStream(scNoName));

                
                // cannot update non existent stream
                StreamConfiguration sc = GetTestStreamConfiguration();
                // stream not added yet
                Assert.Throws<NATSJetStreamException>(() => jsm.UpdateStream(sc));

                // add the stream
                jsm.AddStream(sc);

                // cannot change MaxConsumers
                StreamConfiguration scMaxCon = GetTestStreamConfigurationBuilder()
                    .WithMaxConsumers(2)
                    .Build();
                Assert.Throws<NATSJetStreamException>(() => jsm.UpdateStream(scMaxCon));

                // cannot change RetentionPolicy
                StreamConfiguration scReten = GetTestStreamConfigurationBuilder()
                    .WithRetentionPolicy(RetentionPolicy.Interest)
                    .Build();
                Assert.Throws<NATSJetStreamException>(() => jsm.UpdateStream(scReten));
            });
        }

        private static StreamInfo AddTestStream(IJetStreamManagement jsm) {
            StreamInfo si = jsm.AddStream(GetTestStreamConfiguration());
            Assert.NotNull(si);
            return si;
        }

        private static StreamConfiguration GetTestStreamConfiguration() {
            return GetTestStreamConfigurationBuilder().Build();
        }

        private static StreamConfiguration.StreamConfigurationBuilder GetTestStreamConfigurationBuilder()
        {
            return StreamConfiguration.Builder()
                .WithName(STREAM)
                .WithStorageType(StorageType.Memory)
                .WithSubjects(Subject(0), Subject(1));
        }

        [Fact]
        public void TestGetStreamInfo()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                Assert.Throws<NATSJetStreamException>(() => jsm.GetStreamInfo(STREAM));
                CreateTestStream(c);

                StreamInfo si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(STREAM, si.Config.Name);
            });
        }

        [Fact]
        public void TestDeleteStream()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                NATSJetStreamException e = 
                    Assert.Throws<NATSJetStreamException>(() => jsm.DeleteStream(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);

                CreateTestStream(c);
                Assert.NotNull(jsm.GetStreamInfo(STREAM));
                Assert.True(jsm.DeleteStream(STREAM));

                e = Assert.Throws<NATSJetStreamException>(() => jsm.GetStreamInfo(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);                

                e = Assert.Throws<NATSJetStreamException>(() => jsm.DeleteStream(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);                
            });
        }

        [Fact]
        public void TestPurgeStream()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateTestStream(c);

                StreamInfo si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(0, si.State.Messages);                

                JsPublish(c, SUBJECT, 1);
                si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(1, si.State.Messages);

                PurgeResponse pr = jsm.PurgeStream(STREAM);
                Assert.True(pr.Success);
                Assert.Equal(1, pr.Purged);
            });
        }

        [Fact]
        public void TestAddDeleteConsumer()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, Subject(0), Subject(1));
                
                List<ConsumerInfo> list = jsm.GetConsumers(STREAM);
                Assert.Empty(list);

                // Assert.Throws<ArgumentException>(() => 
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().Build();
                Assert.Throws<ArgumentException>(() => jsm.AddOrUpdateConsumer(null, cc));
                Assert.Throws<ArgumentNullException>(() => jsm.AddOrUpdateConsumer(STREAM, null));
                Assert.Throws<ArgumentNullException>(() => jsm.AddOrUpdateConsumer(STREAM, cc));

                ConsumerConfiguration cc0 = ConsumerConfiguration.Builder()
                        .WithDurable(Durable(0))
                        .Build();
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc0);
                Assert.Equal(Durable(0), ci.Name);
                Assert.Equal(Durable(0), ci.Configuration.Durable);
                Assert.Empty(ci.Configuration.DeliverSubject);
                
                ConsumerConfiguration cc1 = ConsumerConfiguration.Builder()
                        .WithDurable(Durable(1))
                        .WithDeliverSubject(Deliver(1))
                        .Build();
                ci = jsm.AddOrUpdateConsumer(STREAM, cc1);
                Assert.Equal(Durable(1), ci.Name);
                Assert.Equal(Durable(1), ci.Configuration.Durable);
                Assert.Equal(Deliver(1), ci.Configuration.DeliverSubject);
                
                List<String> consumers = jsm.GetConsumerNames(STREAM);
                Assert.Equal(2, consumers.Count);
                Assert.True(jsm.DeleteConsumer(STREAM, cc1.Durable));
                consumers = jsm.GetConsumerNames(STREAM);
                Assert.Single(consumers);
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteConsumer(STREAM, cc1.Durable));
            });
        }

        [Fact]
        public void TestInvalidConsumerUpdates()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(1))
                    .WithAckPolicy(AckPolicy.Explicit)
                    .WithDeliverSubject(Deliver(1))
                    .WithMaxDeliver(3)
                    .WithFilterSubject(SUBJECT_GT)
                    .Build();

                AssertValidAddOrUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithDeliverSubject(Deliver(2)).Build();
                AssertValidAddOrUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithDeliverPolicy(DeliverPolicy.New).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithAckWait(Duration.OfSeconds(5)).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithFilterSubject(SUBJECT_STAR).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithRateLimit(100).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithMaxAckPending(100).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithIdleHeartbeat(Duration.OfMillis(111)).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithFlowControl(true).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                cc = ConsumerConfiguration.Builder(cc).WithMaxDeliver(4).Build();
                AssertInvalidConsumerUpdate(jsm, cc);
            });
        }

        private void AssertInvalidConsumerUpdate(IJetStreamManagement jsm, ConsumerConfiguration cc) {
            NATSJetStreamException e = Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM, cc));
            // 10013 consumer name already in use
            // 10105 consumer already exists and is still active
            Assert.True(e.ApiErrorCode == 10013 || e.ApiErrorCode == 10105);
        }

        private void AssertValidAddOrUpdate(IJetStreamManagement jsm, ConsumerConfiguration cc) {
            ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
            ConsumerConfiguration cicc = ci.Configuration;
            Assert.Equal(cc.Durable, ci.Name);
            Assert.Equal(cc.Durable, cicc.Durable);
            Assert.Equal(cc.DeliverSubject, cicc.DeliverSubject);
            Assert.Equal(cc.MaxDeliver, cicc.MaxDeliver);
            Assert.Equal(cc.DeliverPolicy, cicc.DeliverPolicy);

            List<String> consumers = jsm.GetConsumerNames(STREAM);
            Assert.Single(consumers);
            Assert.Equal(cc.Durable, consumers[0]);
        }

        [Fact]
        public void TestCreateConsumersWithFilters()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                // plain subject
                CreateMemoryStream(jsm, STREAM, SUBJECT);
                
                ConsumerConfiguration.ConsumerConfigurationBuilder builder = ConsumerConfiguration.Builder().WithDurable(DURABLE);
                jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SUBJECT).Build());
                
                Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM,
                    builder.WithFilterSubject(SubjectDot("not-match")).Build()));

                // wildcard subject
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_STAR);

                jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SubjectDot("A")).Build());
                
                Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM,
                    builder.WithFilterSubject(SubjectDot("not-match")).Build()));

                // gt subject
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SubjectDot("A")).Build());
                
                Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM,
                    builder.WithFilterSubject(SubjectDot("not-match")).Build()));
            });
        }
        

        [Fact]
        public void TestGetConsumerInfo() 
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, SUBJECT);
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteConsumer(STREAM, DURABLE));
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
                Assert.Equal(STREAM, ci.Stream);
                Assert.Equal(DURABLE, ci.Name);
                ci = jsm.GetConsumerInfo(STREAM, DURABLE);
                Assert.Equal(STREAM, ci.Stream);
                Assert.Equal(DURABLE, ci.Name);
                Assert.Throws<NATSJetStreamException>(() => jsm.GetConsumerInfo(STREAM, Durable(999)));
            });
        }

        [Fact]
        public void TestGetConsumers() 
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, Subject(0), Subject(1));

                AddConsumers(jsm, STREAM, 600, "A", null); // getConsumers pages at 256

                List<ConsumerInfo> list = jsm.GetConsumers(STREAM);
                Assert.Equal(600, list.Count);

                AddConsumers(jsm, STREAM, 500, "B", null); // getConsumerNames pages at 1024
                List<string> names = jsm.GetConsumerNames(STREAM);
                Assert.Equal(1100, names.Count);
            });
        }

        private void AddConsumers(IJetStreamManagement jsm, String stream, int count, String durableVary, String filterSubject)
        {
            for (int x = 0; x < count; x++) {
                String dur = Durable(durableVary, x + 1);
                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                        .WithDurable(dur)
                        .WithFilterSubject(filterSubject)
                        .Build();
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(stream, cc);
                Assert.Equal(dur, ci.Name);
                Assert.Equal(dur, ci.Configuration.Durable);
                Assert.Empty(ci.Configuration.DeliverSubject);
            }
        }

        [Fact]
        public void TestGetStreams()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                AddStreams(jsm, 600, 0); // getStreams pages at 256

                List<StreamInfo> list = jsm.GetStreams();
                Assert.Equal(600, list.Count);

                AddStreams(jsm, 500, 600); // getStreamNames pages at 1024
                List<string> names = jsm.GetStreamNames();
                Assert.Equal(1100, names.Count);
            });
        }

        private void AddStreams(IJetStreamManagement jsm, int count, int adj) {
            for (int x = 0; x < count; x++) {
                CreateMemoryStream(jsm, Stream(x + adj), Subject(x + adj));
            }
        }

        [Fact]
        public void TestGetAndDeleteMessage() {
            Context.RunInJsServer(c => {
                CreateMemoryStream(c, STREAM, SUBJECT);
                IJetStream js = c.CreateJetStreamContext();

                MsgHeader h = new MsgHeader();
                h.Add("foo", "bar");

                DateTime beforeCreated = DateTime.Now;
                js.Publish(new Msg(SUBJECT, null, h, DataBytes(1)));
                js.Publish(new Msg(SUBJECT, null));

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                MessageInfo mi = jsm.GetMessage(STREAM, 1);
                Assert.Equal(SUBJECT, mi.Subject);
                Assert.Equal(Data(1), System.Text.Encoding.ASCII.GetString(mi.Data));
                Assert.Equal(1, mi.Seq);
                Assert.True(mi.Time >= beforeCreated);
                Assert.NotNull(mi.Headers);
                Assert.Equal("bar", mi.Headers["foo"]);

                mi = jsm.GetMessage(STREAM, 2);
                Assert.Equal(SUBJECT, mi.Subject);
                Assert.Null(mi.Data);
                Assert.Equal(2, mi.Seq);
                Assert.True(mi.Time >= beforeCreated);
                Assert.Null(mi.Headers);

                Assert.True(jsm.DeleteMessage(STREAM, 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteMessage(STREAM, 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(STREAM, 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(STREAM, 3));
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteMessage(Stream(999), 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(Stream(999), 1));
            });
        }
    }
}
