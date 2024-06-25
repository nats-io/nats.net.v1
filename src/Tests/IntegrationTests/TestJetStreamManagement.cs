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
using NATS.Client.KeyValue;
using Xunit;
using static IntegrationTests.JetStreamTestBase;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestJetStreamManagement : TestSuite<JetStreamManagementSuiteContext>
    {
        public TestJetStreamManagement(JetStreamManagementSuiteContext context) : base(context) {}

        [Fact]
        public void TestStreamCreate()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                String stream = Stream();
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(stream)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(Subject(0), Subject(1))
                    .Build();

                StreamInfo si = jsm.AddStream(sc);
                Assert.NotNull(si.Config);
                sc = si.Config;
                Assert.Equal(stream, sc.Name);

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
                Assert.Equal(-1, sc.MaximumMessageSize);
                Assert.Equal(1, sc.Replicas);
                Assert.Equal(1U, sc.FirstSequence);

                Assert.Equal(Duration.Zero, sc.MaxAge);
                Assert.Equal(Duration.OfSeconds(120), sc.DuplicateWindow);
                Assert.False(sc.NoAck);
                Assert.Empty(sc.TemplateOwner);

                StreamState ss = si.State;
                Assert.Equal(0u, ss.Messages);
                Assert.Equal(0u, ss.Bytes);
                Assert.Equal(0u, ss.FirstSeq);
                Assert.Equal(0u, ss.LastSeq);
                Assert.Equal(0u, ss.ConsumerCount);

                IJetStream js = c.CreateJetStreamContext();
                if (c.ServerInfo.IsNewerVersionThan("2.9.99")) {
                    stream = Stream();
                    sc = StreamConfiguration.Builder()
                        .WithName(stream)
                        .WithStorageType(StorageType.Memory)
                        .WithFirstSequence(42)
                        .WithCompressionOption(CompressionOption.S2)
                        .WithSubjects("test-first-seq").Build();
                    si = jsm.AddStream(sc);
                    Assert.True(si.Timestamp > DateTime.MinValue);
                    Assert.Equal(42U, si.Config.FirstSequence);
                    Assert.Equal(CompressionOption.S2, si.Config.CompressionOption);
                    PublishAck pa = js.Publish("test-first-seq", null);
                    Assert.Equal(42U, pa.Seq);
                }
            });
        }

        [Fact]
        public void TestStreamCreateWithNoSubject() {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(STREAM)
                    .WithStorageType(StorageType.Memory)
                    .Build();

                StreamInfo si = jsm.AddStream(sc);
                sc = si.Config;
                Assert.Equal(STREAM, sc.Name);

                Assert.Single(sc.Subjects);
                Assert.Equal(STREAM, sc.Subjects[0]);

                Assert.Equal(RetentionPolicy.Limits, sc.RetentionPolicy);
                Assert.Equal(DiscardPolicy.Old, sc.DiscardPolicy);
                Assert.Equal(StorageType.Memory, sc.StorageType);

                Assert.NotNull(si.Config);
                Assert.NotNull(si.State);
                Assert.Equal(-1, sc.MaxConsumers);
                Assert.Equal(-1, sc.MaxMsgs);
                Assert.Equal(-1, sc.MaxBytes);
                Assert.Equal(-1, sc.MaximumMessageSize);
                Assert.Equal(1, sc.Replicas);

                Assert.Equal(Duration.Zero, sc.MaxAge);
                Assert.Equal(Duration.OfSeconds(120), sc.DuplicateWindow);
                Assert.False(sc.NoAck);
                Assert.True(string.IsNullOrEmpty(sc.TemplateOwner));

                StreamState ss = si.State;
                Assert.Equal(0U, ss.Messages);
                Assert.Equal(0U, ss.Bytes);
                Assert.Equal(0U, ss.FirstSeq);
                Assert.Equal(0U, ss.LastSeq);
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
                Assert.Equal(-1, sc.MaximumMessageSize);
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
                        .WithMaximumMessageSize(44)
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
                Assert.Equal(43u, sc.MaxBytes);
                Assert.Equal(44, sc.MaximumMessageSize);
                Assert.Equal(Duration.OfDays(100), sc.MaxAge);
                Assert.Equal(StorageType.Memory, sc.StorageType);
                Assert.Equal(DiscardPolicy.New, sc.DiscardPolicy);
                Assert.Equal(1, sc.Replicas);
                Assert.True(sc.NoAck);
                Assert.Equal(Duration.OfMinutes(3), sc.DuplicateWindow);
                Assert.Empty(sc.TemplateOwner);
                
                // allowed to change Allow Direct
                jsm.DeleteStream(STREAM);
                jsm.AddStream(GetTestStreamConfigurationBuilder().WithAllowDirect(false).Build());
                jsm.UpdateStream(GetTestStreamConfigurationBuilder().WithAllowDirect(true).Build());
                jsm.UpdateStream(GetTestStreamConfigurationBuilder().WithAllowDirect(false).Build());
                
                // allowed to change Mirror Direct
                jsm.DeleteStream(STREAM);
                jsm.AddStream(GetTestStreamConfigurationBuilder().WithMirrorDirect(false).Build());
                jsm.UpdateStream(GetTestStreamConfigurationBuilder().WithMirrorDirect(true).Build());
                jsm.UpdateStream(GetTestStreamConfigurationBuilder().WithMirrorDirect(false).Build());
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
                StreamConfiguration scRetention = GetTestStreamConfigurationBuilder()
                    .WithRetentionPolicy(RetentionPolicy.Interest)
                    .Build();
                if (c.ServerInfo.IsOlderThanVersion("2.10.0")) {
                    // cannot change RetentionPolicy
                    Assert.Throws<NATSJetStreamException>(() => jsm.UpdateStream(scRetention));
                }
                else {
                    jsm.UpdateStream(scRetention);
                }
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

                string[] subjects = new string[6];
                for (int x = 0; x < 5; x++) {
                    subjects[x] = Subject(x);
                }
                subjects[5] = "foo.>";
                CreateMemoryStream(jsm, STREAM, subjects);

                IList<PublishAck> packs = new List<PublishAck>();
                IJetStream js = c.CreateJetStreamContext();
                for (int x = 0; x < 5; x++) {
                    JsPublish(js, Subject(x), x + 1);
                    PublishAck pa = JsPublish(js, Subject(x), Data(x + 2));
                    packs.Add(pa);
                    jsm.DeleteMessage(STREAM, pa.Seq);
                }
                JsPublish(js, "foo.bar", 6);

                StreamInfo si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(STREAM, si.Config.Name);
                Assert.Equal(6, si.State.SubjectCount);
                Assert.Equal(0, si.State.Subjects.Count);
                Assert.Equal(5, si.State.DeletedCount);
                Assert.Empty(si.State.Deleted);
                if (c.ServerInfo.IsSameOrOlderThanVersion("2.9.99")) 
                {
                    Assert.Equal(DateTime.MinValue, si.Timestamp);
                }
                else 
                {
                    Assert.NotEqual(DateTime.MinValue, si.Timestamp);
                }
                Assert.Equal(1U, si.Config.FirstSequence);
                
                si = jsm.GetStreamInfo(STREAM, 
                    StreamInfoOptions.Builder()
                        .WithAllSubjects()
                        .WithDeletedDetails()
                        .Build());
                Assert.Equal(STREAM, si.Config.Name);
                Assert.Equal(6, si.State.SubjectCount);
                IList<Subject> list = si.State.Subjects;
                Assert.NotNull(list);
                Assert.Equal(6, list.Count);
                Assert.Equal(5, si.State.DeletedCount);
                Assert.Equal(5, si.State.Deleted.Count);
                Dictionary<string, Subject> map = new Dictionary<string, Subject>();
                foreach (Subject su in list) {
                    map[su.Name] = su;
                }
                for (int x = 0; x < 5; x++)
                {
                    Subject subj = map[Subject(x)];
                    Assert.NotNull(subj);
                    Assert.Equal(x + 1, subj.Count);
                }
                Subject s = map["foo.bar"];
                Assert.NotNull(s);
                Assert.Equal(6, s.Count);

                foreach (PublishAck pa in packs) {
                    Assert.True(si.State.Deleted.Contains(pa.Seq));
                }
            });
        }

        [Fact]
        public void TestGetStreamInfoSubjectPagination()
        {
            Context.RunInJsServer(Context.Server1, "pagination.conf", c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();
                
                long rounds = 101;
                long size = 1000;
                long count = rounds * size;
                
                jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(Stream(1))
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects("s.*.*")
                    .Build());
                
                jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(Stream(2))
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects("t.*.*")
                    .Build());

                for (int x = 1; x <= rounds; x++) {
                    for (int y = 1; y <= size; y++) {
                        js.Publish("s." + x + "." + y, null);
                    }
                }

                for (int y = 1; y <= size; y++) {
                    js.Publish("t.7." + y, null);
                }

                StreamInfo si = jsm.GetStreamInfo(Stream(1));
                ValidateStreamInfo(si.State, 0, 0, count);

                si = jsm.GetStreamInfo(Stream(1), StreamInfoOptions.Builder().WithAllSubjects().Build());
                ValidateStreamInfo(si.State, count, count, count);

                si = jsm.GetStreamInfo(Stream(1), StreamInfoOptions.Builder().WithFilterSubjects("s.7.*").Build());
                ValidateStreamInfo(si.State, size, size, count);

                si = jsm.GetStreamInfo(Stream(1), StreamInfoOptions.Builder().WithFilterSubjects("s.7.1").Build());
                ValidateStreamInfo(si.State, 1L, 1, count);

                si = jsm.GetStreamInfo(Stream(2), StreamInfoOptions.Builder().WithFilterSubjects("t.7.*").Build());
                ValidateStreamInfo(si.State, size, size, size);

                si = jsm.GetStreamInfo(Stream(2), StreamInfoOptions.Builder().WithFilterSubjects("t.7.1").Build());
                ValidateStreamInfo(si.State, 1L, 1, size);

                IList<StreamInfo> infos = jsm.GetStreams();
                Assert.Equal(2, infos.Count);
                si = infos[0];
                if (si.Config.Subjects[0].Equals("s.*.*")) {
                    ValidateStreamInfo(si.State, 0, 0, count);
                    ValidateStreamInfo(infos[1].State, 0, 0, size);
                }
                else {
                    ValidateStreamInfo(si.State, 0, 0, size);
                    ValidateStreamInfo(infos[1].State, 0, 0, count);
                }

                infos = jsm.GetStreams(">");
                Assert.Equal(2, infos.Count);

                infos = jsm.GetStreams("*.7.*");
                Assert.Equal(2, infos.Count);

                infos = jsm.GetStreams("*.7.1");
                Assert.Equal(2, infos.Count);

                infos = jsm.GetStreams("s.7.*");
                Assert.Equal(1, infos.Count);
                Assert.Equal("s.*.*", infos[0].Config.Subjects[0]);

                infos = jsm.GetStreams("t.7.1");
                Assert.Equal(1, infos.Count);
                Assert.Equal("t.*.*", infos[0].Config.Subjects[0]);
            });
        }

        private void ValidateStreamInfo(StreamState streamState, long subjectsList, long filteredCount, long subjectCount) {
            Assert.Equal(subjectsList, streamState.Subjects.Count);
            Assert.Equal(filteredCount, streamState.Subjects.Count);
            Assert.Equal(subjectCount, streamState.SubjectCount);
        }

        [Fact]
        public void TestGetStreamInfoOrNamesPaginationFilter()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                // getStreams pages at 256
                // getStreamNames pages at 1024

                AddStreams(jsm, 300, 0, "x256");

                IList<StreamInfo> list = jsm.GetStreams();
                Assert.Equal(300, list.Count);
                
                IList<string> names = jsm.GetStreamNames();
                Assert.Equal(300, names.Count);

                AddStreams(jsm, 1100, 300, "x1024");
                
                list = jsm.GetStreams();
                Assert.Equal(1400, list.Count);

                names = jsm.GetStreamNames();
                Assert.Equal(1400, names.Count);

                list = jsm.GetStreams("*.x256.*");
                Assert.Equal(300, list.Count);

                names = jsm.GetStreamNames("*.x256.*");
                Assert.Equal(300, names.Count);

                list = jsm.GetStreams("*.x1024.*");
                Assert.Equal(1100, list.Count);

                names = jsm.GetStreamNames("*.x1024.*");
                Assert.Equal(1100, names.Count);
            });
        }

        private void AddStreams(IJetStreamManagement jsm, int count, int adj, string div) {
            for (int x = 0; x < count; x++) {
                CreateMemoryStream(jsm, "stream-" + (x + adj), "sub" + (x + adj) + "." + div + ".*");
            }
        }

        [Fact]
        public void TestGetStreamNamesBySubjectFilter()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                
                CreateMemoryStream(jsm, Stream(1), "foo");
                CreateMemoryStream(jsm, Stream(2), "bar");
                CreateMemoryStream(jsm, Stream(3), "a.a");
                CreateMemoryStream(jsm, Stream(4), "a.b");

                IList<string> list = jsm.GetStreamNames("*");
                AssertStreamNameList(list, 1, 2);

                list = jsm.GetStreamNames(">");
                AssertStreamNameList(list, 1, 2, 3, 4);

                list = jsm.GetStreamNames("*.*");
                AssertStreamNameList(list, 3, 4);

                list = jsm.GetStreamNames("a.>");
                AssertStreamNameList(list, 3, 4);

                list = jsm.GetStreamNames("a.*");
                AssertStreamNameList(list, 3, 4);

                list = jsm.GetStreamNames("foo");
                AssertStreamNameList(list, 1);

                list = jsm.GetStreamNames("a.a");
                AssertStreamNameList(list, 3);

                list = jsm.GetStreamNames("nomatch");
                AssertStreamNameList(list);
            });
        }

        private void AssertStreamNameList(IList<string> list, params int[] ids) {
            Assert.NotNull(list);
            Assert.Equal(ids.Length, list.Count);
            foreach (int id in ids) {
                Assert.Contains(Stream(id), list);
            }
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

                CreateDefaultTestStream(c);
                Assert.NotNull(jsm.GetStreamInfo(STREAM));
                Assert.True(jsm.DeleteStream(STREAM));

                e = Assert.Throws<NATSJetStreamException>(() => jsm.GetStreamInfo(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);                

                e = Assert.Throws<NATSJetStreamException>(() => jsm.DeleteStream(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);                
            });
        }

        [Fact]
        public void TestPurgeStreamAndOptions()
        {
            Context.RunInJsServer(c =>
            {
                Assert.Throws<ArgumentException>(() => PurgeOptions.Builder().WithKeep(1).WithSequence(1).Build());

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                Assert.Throws<NATSJetStreamException>(() => jsm.PurgeStream(STREAM));
                
                CreateMemoryStream(c, STREAM, Subject(1), Subject(2));
                
                StreamInfo si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(0u, si.State.Messages);                

                JsPublish(c, Subject(1), 10);
                si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(10u, si.State.Messages);

                PurgeOptions options = PurgeOptions.Builder().WithKeep(7).Build();
                PurgeResponse pr = jsm.PurgeStream(STREAM, options);
                Assert.True(pr.Success);
                Assert.Equal(3u, pr.Purged);

                options = PurgeOptions.Builder().WithSequence(9).Build();
                pr = jsm.PurgeStream(STREAM, options);
                Assert.True(pr.Success);
                Assert.Equal(5u, pr.Purged);
                si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(2u, si.State.Messages);

                pr = jsm.PurgeStream(STREAM);
                Assert.True(pr.Success);
                Assert.Equal(2u, pr.Purged);
                si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(0u, si.State.Messages);

                JsPublish(c, Subject(1), 10);
                JsPublish(c, Subject(2), 10);
                si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(20u, si.State.Messages);
                jsm.PurgeStream(STREAM, PurgeOptions.WithSubject(Subject(1)));
                si = jsm.GetStreamInfo(STREAM);
                Assert.Equal(10u, si.State.Messages);

                options = PurgeOptions.Builder().WithSubject(Subject(1)).WithSequence(1).Build();
                Assert.Equal(Subject(1), options.Subject);
                Assert.Equal(1u, options.Sequence);

                options = PurgeOptions.Builder().WithSubject(Subject(1)).WithKeep(2).Build();
                Assert.Equal(2u, options.Keep);
            });
        }

        [Fact]
        public void TestAddDeleteConsumer()
        {
            Context.RunInJsServer(c =>
            {
                bool atLeast290 = c.ServerInfo.IsSameOrNewerThanVersion("2.9.0");

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, SubjectDot(">"));
                
                IList<ConsumerInfo> list = jsm.GetConsumers(STREAM);
                Assert.Empty(list);

                ConsumerConfiguration cc = ConsumerConfiguration.Builder().Build();
                
                ArgumentException e = Assert.Throws<ArgumentException>(
                    () => jsm.AddOrUpdateConsumer(null, cc));
                Assert.Contains("Stream cannot be null or empty", e.Message);
                
                e = Assert.Throws<ArgumentNullException>(
                    () => jsm.AddOrUpdateConsumer(STREAM, null));
                Assert.Contains("Value cannot be null", e.Message);
                
                // durable and name can both be null
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
                Assert.NotNull(ci.Name);

                // threshold can be set for durable
                ConsumerConfiguration cc2 = ConsumerConfiguration.Builder().WithDurable(DURABLE).WithInactiveThreshold(10000).Build();
                ci = jsm.AddOrUpdateConsumer(STREAM, cc2);
                Assert.Equal(10000, ci.ConsumerConfiguration.InactiveThreshold.Millis);

                // prep for next part of test
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SubjectDot(">"));

                // with and w/o deliver subject for push/pull
                AddConsumer(jsm, atLeast290, 1, false, null, ConsumerConfiguration.Builder()
                    .WithDurable(Durable(1))
                    .Build());

                AddConsumer(jsm, atLeast290, 2, true, null, ConsumerConfiguration.Builder()
                    .WithDurable(Durable(2))
                    .WithDeliverSubject(Deliver(2))
                    .Build());

                // test delete here
                IList<string> consumers = jsm.GetConsumerNames(STREAM);
                Assert.Equal(2, consumers.Count);
                Assert.True(jsm.DeleteConsumer(STREAM, Durable(1)));
                consumers = jsm.GetConsumerNames(STREAM);
                Assert.Equal(1, consumers.Count);
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteConsumer(STREAM, Durable(1)));

                // some testing of new name
                if (atLeast290) {
                    AddConsumer(jsm, atLeast290, 3, false, null, ConsumerConfiguration.Builder()
                        .WithDurable(Durable(3))
                        .WithName(Durable(3))
                        .Build());

                    AddConsumer(jsm, atLeast290, 4, true, null, ConsumerConfiguration.Builder()
                        .WithDurable(Durable(4))
                        .WithName(Durable(4))
                        .WithDeliverSubject(Deliver(4))
                        .Build());

                    AddConsumer(jsm, atLeast290, 5, false, ">", ConsumerConfiguration.Builder()
                        .WithDurable(Durable(5))
                        .WithFilterSubject(">")
                        .Build());

                    AddConsumer(jsm, atLeast290, 6, false, SubjectDot(">"), ConsumerConfiguration.Builder()
                        .WithDurable(Durable(6))
                        .WithFilterSubject(SubjectDot(">"))
                        .Build());

                    AddConsumer(jsm, atLeast290, 7, false, SubjectDot("foo"), ConsumerConfiguration.Builder()
                        .WithDurable(Durable(7))
                        .WithFilterSubject(SubjectDot("foo"))
                        .Build());
                }
            });
        }

        private static void AddConsumer(IJetStreamManagement jsm, bool atLeast290, int id, bool deliver, string fs, ConsumerConfiguration cc) {
            ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
            Assert.Equal(Durable(id), ci.Name);
            if (atLeast290) {
                Assert.Equal(Durable(id), ci.ConsumerConfiguration.Name);
            }
            Assert.Equal(Durable(id), ci.ConsumerConfiguration.Durable);
            if (fs == null) {
                Assert.Null(ci.ConsumerConfiguration.FilterSubject);
            }
            if (deliver) {
                Assert.Equal(Deliver(id), ci.ConsumerConfiguration.DeliverSubject);
            }
        }

        [Fact]
        public void TestValidConsumerUpdates()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                ConsumerConfiguration cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithDeliverSubject(Deliver(2)).Build();
                AssertValidAddOrUpdate(jsm, cc);

                cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithAckWait(Duration.OfSeconds(5)).Build();
                AssertValidAddOrUpdate(jsm, cc);

                cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithRateLimitBps(100).Build();
                AssertValidAddOrUpdate(jsm, cc);

                cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithMaxAckPending(100).Build();
                AssertValidAddOrUpdate(jsm, cc);

                cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithMaxDeliver(4).Build();
                AssertValidAddOrUpdate(jsm, cc);

                if (c.ServerInfo.IsNewerVersionThan("2.8.4"))
                {
                    cc = PrepForUpdateTest(jsm);
                    cc = ConsumerConfiguration.Builder(cc).WithFilterSubject(SUBJECT_STAR).Build();
                    AssertValidAddOrUpdate(jsm, cc);
                }
            });
        }

        [Fact]
        public void TestInvalidConsumerUpdates()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                ConsumerConfiguration cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithDeliverPolicy(DeliverPolicy.New).Build();
                AssertInvalidConsumerUpdate(jsm, cc);

                if (c.ServerInfo.IsSameOrOlderThanVersion("2.8.4"))
                {
                    cc = PrepForUpdateTest(jsm);
                    cc = ConsumerConfiguration.Builder(cc).WithFilterSubject(SUBJECT_STAR).Build();
                    AssertInvalidConsumerUpdate(jsm, cc);
                }

                cc = PrepForUpdateTest(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithIdleHeartbeat(Duration.OfMillis(111)).Build();
                AssertInvalidConsumerUpdate(jsm, cc);
            });
        }
        
        private ConsumerConfiguration PrepForUpdateTest(IJetStreamManagement jsm)
        {
            try {
                jsm.DeleteConsumer(STREAM, Durable(1));
            }
            catch (Exception) { /* ignore */ }

            ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                .WithDurable(Durable(1))
                .WithAckPolicy(AckPolicy.Explicit)
                .WithDeliverSubject(Deliver(1))
                .WithMaxDeliver(3)
                .WithFilterSubject(SUBJECT_GT)
                .Build();
            AssertValidAddOrUpdate(jsm, cc);
            return cc;
        }

        private void AssertInvalidConsumerUpdate(IJetStreamManagement jsm, ConsumerConfiguration cc) {
            NATSJetStreamException e = Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM, cc));
            Assert.Equal(10012, e.ApiErrorCode);
            Assert.Equal(500, e.ErrorCode);
        }

        private void AssertValidAddOrUpdate(IJetStreamManagement jsm, ConsumerConfiguration cc) {
            ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
            ConsumerConfiguration ciCc = ci.ConsumerConfiguration;
            Assert.Equal(cc.Durable, ci.Name);
            Assert.Equal(cc.Durable, ciCc.Durable);
            Assert.Equal(cc.DeliverSubject, ciCc.DeliverSubject);
            Assert.Equal(cc.MaxDeliver, ciCc.MaxDeliver);
            Assert.Equal(cc.DeliverPolicy, ciCc.DeliverPolicy);

            IList<string> consumers = jsm.GetConsumerNames(STREAM);
            Assert.Single(consumers);
            Assert.Equal(cc.Durable, consumers[0]);
        }

        [Fact]
        public void TestCreateConsumersWithFilters()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                // plain subject
                CreateDefaultTestStream(jsm);
                
                ConsumerConfiguration.ConsumerConfigurationBuilder builder = ConsumerConfiguration.Builder().WithDurable(DURABLE);
                jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SUBJECT).Build());
                IList<ConsumerInfo> cis = jsm.GetConsumers(STREAM);
                Assert.Equal(SUBJECT, cis[0].ConsumerConfiguration.FilterSubject);

                if (c.ServerInfo.IsSameOrOlderThanVersion("2.9.99"))
                {
                    Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM,
                        builder.WithFilterSubject(SubjectDot("not-match")).Build()));
                }
                else
                {
                    // 2.10 and later you can set the filter to something that does not match
                    jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SubjectDot("two-ten-allows-not-matching")).Build());
                    cis = jsm.GetConsumers(STREAM);
                    Assert.Equal(SubjectDot("two-ten-allows-not-matching"),  cis[0].ConsumerConfiguration.FilterSubject);
                }

                // wildcard subject
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_STAR);

                jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SubjectDot("A")).Build());

                if (c.ServerInfo.IsSameOrOlderThanVersion("2.8.4"))
                {
                    Assert.Throws<NATSJetStreamException>(() => jsm.AddOrUpdateConsumer(STREAM,
                        builder.WithFilterSubject(SubjectDot("not-match")).Build()));
                }

                // gt subject
                jsm.DeleteStream(STREAM);
                CreateMemoryStream(jsm, STREAM, SUBJECT_GT);

                jsm.AddOrUpdateConsumer(STREAM, builder.WithFilterSubject(SubjectDot("A")).Build());

                jsm.AddOrUpdateConsumer(STREAM, ConsumerConfiguration.Builder()
                    .WithDurable(Durable(42))
                    .WithFilterSubject(SubjectDot("F"))
                    .Build()
                );
            });
        }
        

        [Fact]
        public void TestGetConsumerInfo() 
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateDefaultTestStream(jsm);
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteConsumer(STREAM, DURABLE));
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc);
                Assert.Equal(STREAM, ci.Stream);
                Assert.Equal(DURABLE, ci.Name);
                ci = jsm.GetConsumerInfo(STREAM, DURABLE);
                Assert.Equal(STREAM, ci.Stream);
                Assert.Equal(DURABLE, ci.Name);
                Assert.Throws<NATSJetStreamException>(() => jsm.GetConsumerInfo(STREAM, Durable(999)));
                if (c.ServerInfo.IsSameOrOlderThanVersion("2.9.99")) 
                {
                    Assert.Equal(DateTime.MinValue, ci.Timestamp);
                }
                else 
                {
                    Assert.NotEqual(DateTime.MinValue, ci.Timestamp);
                }
            });
        }

        [Fact]
        public void TestGetConsumers() 
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, Subject(0), Subject(1));

                AddConsumers(jsm, STREAM, 600, "A", null); // getConsumers pages at 256

                IList<ConsumerInfo> list = jsm.GetConsumers(STREAM);
                Assert.Equal(600, list.Count);

                AddConsumers(jsm, STREAM, 500, "B", null); // getConsumerNames pages at 1024
                IList<string> names = jsm.GetConsumerNames(STREAM);
                Assert.Equal(1100, names.Count);
            });
        }

        private void AddConsumers(IJetStreamManagement jsm, string stream, int count, string durableVary, string filterSubject)
        {
            for (int x = 0; x < count; x++) {
                string dur = Durable(durableVary, x + 1);
                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                        .WithDurable(dur)
                        .WithFilterSubject(filterSubject)
                        .Build();
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(stream, cc);
                Assert.Equal(dur, ci.Name);
                Assert.Equal(dur, ci.ConsumerConfiguration.Durable);
                Assert.Empty(ci.ConsumerConfiguration.DeliverSubject);
            }
        }

        [Fact]
        public void TestDeleteMessage() {
            MessageDeleteRequest mdr = new MessageDeleteRequest(1, true);
            Assert.Equal("{\"seq\":1}", Encoding.UTF8.GetString(mdr.Serialize()));
            Assert.Equal(1U, mdr.Sequence);
            Assert.True(mdr.Erase);
            Assert.False(mdr.NoErase);
            
            mdr = new MessageDeleteRequest(1, false);
            Assert.Equal("{\"seq\":1,\"no_erase\":true}", Encoding.UTF8.GetString(mdr.Serialize()));
            Assert.Equal(1U, mdr.Sequence);
            Assert.False(mdr.Erase);
            Assert.True(mdr.NoErase);

            Context.RunInJsServer(c => {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                MsgHeader h = new MsgHeader { { "foo", "bar" } };

                js.Publish(new Msg(SUBJECT, null, h, DataBytes(1)));
                js.Publish(new Msg(SUBJECT, null));

                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                MessageInfo mi = jsm.GetMessage(STREAM, 1);
                Assert.Equal(SUBJECT, mi.Subject);
                Assert.Equal(Data(1), Encoding.ASCII.GetString(mi.Data));
                Assert.Equal(1U, mi.Sequence);
                Assert.NotNull(mi.Headers);
                Assert.Equal("bar", mi.Headers["foo"]);

                mi = jsm.GetMessage(STREAM, 2);
                Assert.Equal(SUBJECT, mi.Subject);
                Assert.Null(mi.Data);
                Assert.Equal(2U, mi.Sequence);
                Assert.Null(mi.Headers);

                Assert.True(jsm.DeleteMessage(STREAM, 1, false)); // added coverage for use of erase (no_erase) flag.
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteMessage(STREAM, 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(STREAM, 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(STREAM, 3));
                Assert.Throws<NATSJetStreamException>(() => jsm.DeleteMessage(Stream(999), 1));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(Stream(999), 1));
            });
        }

        [Fact]
        public void TestConsumerReplica()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                CreateMemoryStream(jsm, STREAM, Subject(0), Subject(1));

                ConsumerConfiguration cc0 = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(0))
                    .Build();
                ConsumerInfo ci = jsm.AddOrUpdateConsumer(STREAM, cc0);

                // server returns 0 when value is not set
                Assert.Equal(0, ci.ConsumerConfiguration.NumReplicas);
                
                ConsumerConfiguration cc1 = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(0))
                    .WithNumReplicas(1)
                    .Build();
                ci = jsm.AddOrUpdateConsumer(STREAM, cc1);
                Assert.Equal(1, ci.ConsumerConfiguration.NumReplicas);
            });
        }

        [Fact]
        public void TestGetMessage()
        {
            Context.RunInJsServer(c => {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(STREAM)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(Subject(1), Subject(2))
                    .Build();
                StreamInfo si = jsm.AddStream(sc);
                Assert.False(si.Config.AllowDirect);
                
                js.Publish(Subject(1), Encoding.UTF8.GetBytes("s1-q1"));
                js.Publish(Subject(2), Encoding.UTF8.GetBytes("s2-q2"));
                js.Publish(Subject(1), Encoding.UTF8.GetBytes("s1-q3"));
                js.Publish(Subject(2), Encoding.UTF8.GetBytes("s2-q4"));
                js.Publish(Subject(1), Encoding.UTF8.GetBytes("s1-q5"));
                js.Publish(Subject(2), Encoding.UTF8.GetBytes("s2-q6"));

                ValidateGetMessage(jsm);

                sc = StreamConfiguration.Builder(si.Config).WithAllowDirect(true).Build();
                si = jsm.UpdateStream(sc);
                Assert.True(si.Config.AllowDirect);
                ValidateGetMessage(jsm);

                // error case stream doesn't exist
                Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(Stream(999), 1));
            });
        }
        
        private void ValidateGetMessage(IJetStreamManagement jsm) {

            AssertMessageInfo(1, 1, jsm.GetMessage(STREAM, 1));
            AssertMessageInfo(1, 5, jsm.GetLastMessage(STREAM, Subject(1)));
            AssertMessageInfo(2, 6, jsm.GetLastMessage(STREAM, Subject(2)));

            AssertMessageInfo(1, 1, jsm.GetNextMessage(STREAM, 0, Subject(1)));
            AssertMessageInfo(2, 2, jsm.GetNextMessage(STREAM, 0, Subject(2)));
            AssertMessageInfo(1, 1, jsm.GetFirstMessage(STREAM, Subject(1)));
            AssertMessageInfo(2, 2, jsm.GetFirstMessage(STREAM, Subject(2)));

            AssertMessageInfo(1, 1, jsm.GetNextMessage(STREAM, 1, Subject(1)));
            AssertMessageInfo(2, 2, jsm.GetNextMessage(STREAM, 1, Subject(2)));

            AssertMessageInfo(1, 3, jsm.GetNextMessage(STREAM, 2, Subject(1)));
            AssertMessageInfo(2, 2, jsm.GetNextMessage(STREAM, 2, Subject(2)));

            AssertMessageInfo(1, 5, jsm.GetNextMessage(STREAM, 5, Subject(1)));
            AssertMessageInfo(2, 6, jsm.GetNextMessage(STREAM, 5, Subject(2)));

            AssertStatus(10003, Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(STREAM, 0)));
            AssertStatus(10037, Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(STREAM, 9)));
            AssertStatus(10037, Assert.Throws<NATSJetStreamException>(() => jsm.GetLastMessage(STREAM, "not-a-subject")));
            AssertStatus(10037, Assert.Throws<NATSJetStreamException>(() => jsm.GetFirstMessage(STREAM, "not-a-subject")));
            AssertStatus(10037, Assert.Throws<NATSJetStreamException>(() => jsm.GetNextMessage(STREAM, 9, Subject(1))));
            AssertStatus(10037, Assert.Throws<NATSJetStreamException>(() => jsm.GetNextMessage(STREAM, 1, "not-a-subject")));
        }

        private void AssertStatus(int apiErrorCode, NATSJetStreamException e) {
            Assert.Equal(apiErrorCode, e.ApiErrorCode);
        }
        
        private void AssertMessageInfo(int subj, ulong seq, MessageInfo mi) {
            Assert.Equal(STREAM, mi.Stream);
            Assert.Equal(Subject(subj), mi.Subject);
            Assert.Equal(seq, mi.Sequence);
            Assert.Equal("s" + subj + "-q" + seq, Encoding.UTF8.GetString(mi.Data));
        }

        [Fact]
        public void TestMessageGetRequest()
        {
            ValidateMgr(1, null, null, MessageGetRequest.ForSequence(1));
            ValidateMgr(0, "last", null, MessageGetRequest.LastForSubject("last"));
            ValidateMgr(0, null, "first", MessageGetRequest.FirstForSubject("first"));
            ValidateMgr(1, null, "first", MessageGetRequest.NextForSubject(1, "first"));
        }
        
        private void ValidateMgr(ulong seq, string lastBySubject, string nextBySubject, MessageGetRequest mgr) {
            Assert.Equal(seq, mgr.Sequence);
            Assert.Equal(lastBySubject, mgr.LastBySubject);
            Assert.Equal(nextBySubject, mgr.NextBySubject);
            Assert.Equal(seq > 0 && nextBySubject == null, mgr.IsSequenceOnly);
            Assert.Equal(lastBySubject != null, mgr.IsLastBySubject);
            Assert.Equal(nextBySubject != null, mgr.IsNextBySubject);
        }

        [Fact]
        public void TestDirectMessageRepublishedSubject()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                String streamBucketName = "sb-" + Variant(null);
                String subject = Subject();
                String streamSubject = subject + ".>";
                String publishSubject1 = subject + ".one";
                String publishSubject2 = subject + ".two";
                String publishSubject3 = subject + ".three";
                String republishDest = "$KV." + streamBucketName + ".>";

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(streamBucketName)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(streamSubject)
                    .WithRepublish(Republish.Builder().WithSource(">").WithDestination(republishDest).Build())
                    .Build();
                jsm.AddStream(sc);

                KeyValueConfiguration kvc = KeyValueConfiguration.Builder().WithName(streamBucketName).Build();
                c.CreateKeyValueManagementContext().Create(kvc);
                IKeyValue kv = c.CreateKeyValueContext(streamBucketName);

                c.Publish(publishSubject1, Encoding.UTF8.GetBytes("uno"));
                c.CreateJetStreamContext().Publish(publishSubject2, Encoding.UTF8.GetBytes("dos"));
                kv.Put(publishSubject3, "tres");

                KeyValueEntry kve1 = kv.Get(publishSubject1);
                Assert.Equal(streamBucketName, kve1.Bucket);
                Assert.Equal(publishSubject1, kve1.Key);
                Assert.Equal("uno", kve1.ValueAsString());

                KeyValueEntry kve2 = kv.Get(publishSubject2);
                Assert.Equal(streamBucketName, kve2.Bucket);
                Assert.Equal(publishSubject2, kve2.Key);
                Assert.Equal("dos", kve2.ValueAsString());

                KeyValueEntry kve3 = kv.Get(publishSubject3);
                Assert.Equal(streamBucketName, kve3.Bucket);
                Assert.Equal(publishSubject3, kve3.Key);
                Assert.Equal("tres", kve3.ValueAsString());            
            });
        }

        [Fact]
        public void TestPauseConsumer()
        {
            Context.RunInJsServer(AtLeast2_11, c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                string stream = Stream();
                string subject = Subject();
                string con = Name();
                CreateMemoryStream(c, stream, subject);

                IList<ConsumerInfo> list = jsm.GetConsumers(stream);
                Assert.Empty(list);

                // Add a consumer with pause
                DateTime pauseUntil = DateTime.Now.Add(TimeSpan.FromMinutes(2));
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithPauseUntil(pauseUntil).Build();

                ConsumerInfo ci = jsm.AddOrUpdateConsumer(stream, cc);
                Assert.True(ci.Paused);
                Assert.True(ci.PauseRemaining.Millis > 60000);
                Assert.Equal(pauseUntil.ToUniversalTime(), ci.ConsumerConfiguration.PauseUntil.Value);

                // Add a consumer
                string name = Name();
                cc = ConsumerConfiguration.Builder().WithName(name).Build();
                ci = jsm.AddOrUpdateConsumer(stream, cc);
                Assert.NotNull(name);
                Assert.False(ci.Paused);
                Assert.Null(ci.PauseRemaining);

                // Pause
                ConsumerPauseResponse cpre = jsm.PauseConsumer(stream, name, pauseUntil);
                Assert.True(cpre.Paused);
                Assert.True(cpre.PauseRemaining.Millis > 60000);
                Assert.Equal(pauseUntil.ToUniversalTime(), cpre.PauseUntil.Value);
                
                ci = jsm.GetConsumerInfo(stream, name);
                Assert.True(ci.Paused);
                Assert.True(ci.PauseRemaining.Millis > 60000);
                Assert.Equal(pauseUntil.ToUniversalTime(), ci.ConsumerConfiguration.PauseUntil.Value);

                // Resume
                Assert.True(jsm.ResumeConsumer(stream, name));
                ci = jsm.GetConsumerInfo(stream, name);
                Assert.False(ci.Paused);
                Assert.Null(ci.PauseRemaining);
                Assert.False(ci.ConsumerConfiguration.PauseUntil.HasValue);
                
                // Pause again
                cpre = jsm.PauseConsumer(stream, name, pauseUntil);
                Assert.True(cpre.Paused);
                Assert.True(cpre.PauseRemaining.Millis > 60000);
                Assert.Equal(pauseUntil.ToUniversalTime(), cpre.PauseUntil.Value);

                ci = jsm.GetConsumerInfo(stream, name);
                Assert.True(ci.Paused);
                Assert.True(ci.PauseRemaining.Millis > 60000);
                Assert.Equal(pauseUntil.ToUniversalTime(), ci.ConsumerConfiguration.PauseUntil.Value);

                // Resume via pause with no date
                cpre = jsm.PauseConsumer(stream, name, DateTime.MinValue);
                Assert.False(cpre.Paused);
                Assert.Null(cpre.PauseRemaining);
                Assert.False(cpre.PauseUntil.HasValue);

                ci = jsm.GetConsumerInfo(stream, name);
                Assert.False(ci.Paused);
                Assert.Null(cpre.PauseRemaining);
                Assert.False(cpre.PauseUntil.HasValue);
                
                Assert.Throws<NATSJetStreamException>(() => jsm.PauseConsumer(Stream(), name, pauseUntil));
                Assert.Throws<NATSJetStreamException>(() => jsm.PauseConsumer(stream, Name(), pauseUntil));
                Assert.Throws<NATSJetStreamException>(() => jsm.ResumeConsumer(Stream(), name));
                Assert.Throws<NATSJetStreamException>(() => jsm.ResumeConsumer(stream, Name()));
            });
        }
        
        [Fact]
        public void TestCreateConsumerUpdateConsumer()
        {
            Context.RunInJsServer(AtLeast2_9_0, c =>
            {
                string streamPrefix = Variant();
                IJetStreamManagement jsmNew = c.CreateJetStreamManagementContext();
                IJetStreamManagement jsmPre290 = c.CreateJetStreamManagementContext(
                    JetStreamOptions.Builder().WithOptOut290ConsumerCreate(true).Build());
                
                // --------------------------------------------------------
                // New without filter
                // --------------------------------------------------------
                string stream1 = streamPrefix + "-new";
                string name = Name();
                string subject = Name();
                CreateMemoryStream(jsmNew, stream1, subject + ".*");
                
                ConsumerConfiguration cc11 = ConsumerConfiguration.Builder().WithName(name).Build();

                // update no good when not exist
                NATSJetStreamException e = Assert.Throws<NATSJetStreamException>(() => jsmNew.UpdateConsumer(stream1, cc11));
                Assert.Equal(10149, e.ApiErrorCode);

                // initial create ok
                ConsumerInfo ci = jsmNew.CreateConsumer(stream1, cc11);
                Assert.Equal(name, ci.Name);
                Assert.Null(ci.ConsumerConfiguration.FilterSubject);

                // any other create no good
                e = Assert.Throws<NATSJetStreamException>(() => jsmNew.CreateConsumer(stream1, cc11));
                Assert.Equal(10148, e.ApiErrorCode);

                // update ok when exists
                ConsumerConfiguration cc12 = ConsumerConfiguration.Builder().WithName(name).WithDescription(Variant()).Build();
                ci = jsmNew.UpdateConsumer(stream1, cc12);
                Assert.Equal(name, ci.Name);
                Assert.Null(ci.ConsumerConfiguration.FilterSubject);

                // --------------------------------------------------------
                // New with filter subject
                // --------------------------------------------------------
                String stream2 = streamPrefix + "-new-fs";
                name = Name();
                subject = Name();
                String fs1 = subject + ".A";
                String fs2 = subject + ".B";
                CreateMemoryStream(jsmNew, stream2, subject + ".*");

                ConsumerConfiguration cc21 = ConsumerConfiguration.Builder().WithName(name).WithFilterSubject(fs1).Build();

                // update no good when not exist
                e = Assert.Throws<NATSJetStreamException>(() => jsmNew.UpdateConsumer(stream2, cc21));
                Assert.Equal(10149, e.ApiErrorCode);

                // initial create ok
                ci = jsmNew.CreateConsumer(stream2, cc21);
                Assert.Equal(name, ci.Name);
                Assert.Equal(fs1, ci.ConsumerConfiguration.FilterSubject);

                // any other create no good
                e = Assert.Throws<NATSJetStreamException>(() => jsmNew.CreateConsumer(stream2, cc21));
                Assert.Equal(10148, e.ApiErrorCode);

                // update ok when exists
                ConsumerConfiguration cc22 = ConsumerConfiguration.Builder().WithName(name).WithFilterSubjects(fs2).Build();
                ci = jsmNew.UpdateConsumer(stream2, cc22);
                Assert.Equal(name, ci.Name);
                Assert.Equal(fs2, ci.ConsumerConfiguration.FilterSubject);

                // --------------------------------------------------------
                // Pre 290 durable pathway
                // --------------------------------------------------------
                String stream3 = streamPrefix + "-old-durable";
                name = Name();
                subject = Name();
                fs1 = subject + ".A";
                fs2 = subject + ".B";
                String fs3 = subject + ".C";
                CreateMemoryStream(jsmPre290, stream3, subject + ".*");

                ConsumerConfiguration cc31 = ConsumerConfiguration.Builder().WithDurable(name).WithFilterSubject(fs1).Build();

                // update no good when not exist
                e = Assert.Throws<NATSJetStreamException>(() => jsmPre290.UpdateConsumer(stream3, cc31));
                Assert.Equal(10149, e.ApiErrorCode);

                // initial create ok
                ci = jsmPre290.CreateConsumer(stream3, cc31);
                Assert.Equal(name, ci.Name);
                Assert.Equal(fs1, ci.ConsumerConfiguration.FilterSubject);

                // opt out of 209, create on existing ok
                // This is not exactly the same behavior as with the new consumer create api, but it's what the server does
                jsmPre290.CreateConsumer(stream3, cc31);

                ConsumerConfiguration cc32 = ConsumerConfiguration.Builder().WithDurable(name).WithFilterSubject(fs2).Build();
                e = Assert.Throws<NATSJetStreamException>(() => jsmPre290.CreateConsumer(stream3, cc32));
                Assert.Equal(10148, e.ApiErrorCode);

                // update ok when exists
                ConsumerConfiguration cc33 = ConsumerConfiguration.Builder().WithDurable(name).WithFilterSubjects(fs3).Build();
                ci = jsmPre290.UpdateConsumer(stream3, cc33);
                Assert.Equal(name, ci.Name);
                Assert.Equal(fs3, ci.ConsumerConfiguration.FilterSubject);

                // --------------------------------------------------------
                // Pre 290 ephemeral pathway
                // --------------------------------------------------------
                subject = Name();

                String stream4 = streamPrefix + "-old-ephemeral";
                fs1 = subject + ".A";
                CreateMemoryStream(jsmPre290, stream4, subject + ".*");

                ConsumerConfiguration cc4 = ConsumerConfiguration.Builder().WithFilterSubject(fs1).Build();

                // update no good when not exist
                e = Assert.Throws<NATSJetStreamException>(() => jsmPre290.UpdateConsumer(stream4, cc4));
                Assert.Equal(10149, e.ApiErrorCode);

                // initial create ok
                ci = jsmPre290.CreateConsumer(stream4, cc4);
                Assert.Equal(fs1, ci.ConsumerConfiguration.FilterSubject);
            });
        }
    }
}
