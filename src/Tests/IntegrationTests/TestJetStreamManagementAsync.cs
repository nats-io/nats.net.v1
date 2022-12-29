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
using System.Threading.Tasks;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using static IntegrationTests.JetStreamTestBase;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestJetStreamManagementAsync : TestSuite<JetStreamManagementSuiteContext>
    {
        public TestJetStreamManagementAsync(JetStreamManagementSuiteContext context) : base(context) {}

        [Fact]
        public async Task TestStreamCreate()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(STREAM)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(Subject(0), Subject(1))
                    .Build();

                StreamInfo si = await jsm.AddStreamAsync(sc);
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
                Assert.Equal(0u, ss.Messages);
                Assert.Equal(0u, ss.Bytes);
                Assert.Equal(0u, ss.FirstSeq);
                Assert.Equal(0u, ss.LastSeq);
                Assert.Equal(0u, ss.ConsumerCount);
            });
        }

        [Fact]
        public async Task TestStreamCreateWithNoSubject()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(STREAM)
                    .WithStorageType(StorageType.Memory)
                    .Build();

                StreamInfo si = await jsm.AddStreamAsync(sc);
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
                Assert.Equal(-1, sc.MaxMsgSize);
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
        public async Task TestUpdateStream()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                StreamInfo si = await AddTestStream(jsm);
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
                si = await jsm.UpdateStreamAsync(sc);
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
                Assert.Equal(44, sc.MaxMsgSize);
                Assert.Equal(Duration.OfDays(100), sc.MaxAge);
                Assert.Equal(StorageType.Memory, sc.StorageType);
                Assert.Equal(DiscardPolicy.New, sc.DiscardPolicy);
                Assert.Equal(1, sc.Replicas);
                Assert.True(sc.NoAck);
                Assert.Equal(Duration.OfMinutes(3), sc.DuplicateWindow);
                Assert.Empty(sc.TemplateOwner);

                // allowed to change Allow Direct
                await jsm.DeleteStreamAsync(STREAM);
                await jsm.AddStreamAsync(GetTestStreamConfigurationBuilder().WithAllowDirect(false).Build());
                await jsm.UpdateStreamAsync(GetTestStreamConfigurationBuilder().WithAllowDirect(true).Build());
                await jsm.UpdateStreamAsync(GetTestStreamConfigurationBuilder().WithAllowDirect(false).Build());

                // allowed to change Mirror Direct
                await jsm.DeleteStreamAsync(STREAM);
                await jsm.AddStreamAsync(GetTestStreamConfigurationBuilder().WithMirrorDirect(false).Build());
                await jsm.UpdateStreamAsync(GetTestStreamConfigurationBuilder().WithMirrorDirect(true).Build());
                await jsm.UpdateStreamAsync(GetTestStreamConfigurationBuilder().WithMirrorDirect(false).Build());
            });
        }

        [Fact]
        public async Task TestAddUpdateStreamInvalids()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                StreamConfiguration scNoName = StreamConfiguration.Builder().Build();
                await Assert.ThrowsAsync<ArgumentNullException>(async () => await jsm.AddStreamAsync(null));
                await Assert.ThrowsAsync<ArgumentException>(async () => await jsm.AddStreamAsync(scNoName));
                await Assert.ThrowsAsync<ArgumentNullException>(async () => await jsm.UpdateStreamAsync(null));
                await Assert.ThrowsAsync<ArgumentException>(async () => await jsm.UpdateStreamAsync(scNoName));


                // cannot update non existent stream
                StreamConfiguration sc = GetTestStreamConfiguration();
                // stream not added yet
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.UpdateStreamAsync(sc));

                // add the stream
                await jsm.AddStreamAsync(sc);

                // cannot change MaxConsumers
                StreamConfiguration scMaxCon = GetTestStreamConfigurationBuilder()
                    .WithMaxConsumers(2)
                    .Build();
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.UpdateStreamAsync(scMaxCon));

                // cannot change RetentionPolicy
                StreamConfiguration scRetention = GetTestStreamConfigurationBuilder()
                    .WithRetentionPolicy(RetentionPolicy.Interest)
                    .Build();
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.UpdateStreamAsync(scRetention));
            });
        }

        private static async Task<StreamInfo> AddTestStream(IJetStreamManagementAsync jsm) {
            StreamInfo si = await jsm.AddStreamAsync(GetTestStreamConfiguration());
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
        public async Task TestGetStreamInfo()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetStreamInfoAsync(STREAM));

                String[] subjects = new String[6];
                for (int x = 0; x < 5; x++) {
                    subjects[x] = Subject(x);
                }
                subjects[5] = "foo.>";
                await CreateMemoryStreamAsync(jsm, STREAM, subjects);

                IList<PublishAck> packs = new List<PublishAck>();
                IJetStream js = c.CreateJetStreamContext();
                for (int x = 0; x < 5; x++) {
                    JsPublish(js, Subject(x), x + 1);
                    PublishAck pa = JsPublish(js, Subject(x), Data(x + 2));
                    packs.Add(pa);
                    await jsm.DeleteMessageAsync(STREAM, pa.Seq);
                }
                JsPublish(js, "foo.bar", 6);

                StreamInfo si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(STREAM, si.Config.Name);
                Assert.Equal(6, si.State.SubjectCount);
                Assert.Equal(0, si.State.Subjects.Count);
                Assert.Equal(5, si.State.DeletedCount);
                Assert.Empty(si.State.Deleted);

                si = await jsm.GetStreamInfoAsync(STREAM, 
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
        public async Task TestGetStreamInfoSubjectPagination()
        {
            await Context.RunInJsServerAsync(Context.Server1, "pagination.conf", async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                IJetStream js = c.CreateJetStreamContext();

                long rounds = 101;
                long size = 1000;
                long count = rounds * size;

                await jsm.AddStreamAsync(StreamConfiguration.Builder()
                    .WithName(Stream(1))
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects("s.*.*")
                    .Build());

                await jsm.AddStreamAsync(StreamConfiguration.Builder()
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

                StreamInfo si = await jsm.GetStreamInfoAsync(Stream(1));
                ValidateStreamInfo(si.State, 0, 0, count);

                si = await jsm.GetStreamInfoAsync(Stream(1), StreamInfoOptions.Builder().WithAllSubjects().Build());
                ValidateStreamInfo(si.State, count, count, count);

                si = await jsm.GetStreamInfoAsync(Stream(1), StreamInfoOptions.Builder().WithFilterSubjects("s.7.*").Build());
                ValidateStreamInfo(si.State, size, size, count);

                si = await jsm.GetStreamInfoAsync(Stream(1), StreamInfoOptions.Builder().WithFilterSubjects("s.7.1").Build());
                ValidateStreamInfo(si.State, 1L, 1, count);

                si = await jsm.GetStreamInfoAsync(Stream(2), StreamInfoOptions.Builder().WithFilterSubjects("t.7.*").Build());
                ValidateStreamInfo(si.State, size, size, size);

                si = await jsm.GetStreamInfoAsync(Stream(2), StreamInfoOptions.Builder().WithFilterSubjects("t.7.1").Build());
                ValidateStreamInfo(si.State, 1L, 1, size);

                IList<StreamInfo> infos = await jsm.GetStreamsAsync();
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

                infos = await jsm.GetStreamsAsync(">");
                Assert.Equal(2, infos.Count);

                infos = await jsm.GetStreamsAsync("*.7.*");
                Assert.Equal(2, infos.Count);

                infos = await jsm.GetStreamsAsync("*.7.1");
                Assert.Equal(2, infos.Count);

                infos = await jsm.GetStreamsAsync("s.7.*");
                Assert.Equal(1, infos.Count);
                Assert.Equal("s.*.*", infos[0].Config.Subjects[0]);

                infos = await jsm.GetStreamsAsync("t.7.1");
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
        public async Task TestGetStreamInfoOrNamesPaginationFilter()
        {
            await Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                // getStreams pages at 256
                // getStreamNames pages at 1024

                await AddStreamsAsync(jsm, 300, 0, "x256");

                IList<StreamInfo> list = await jsm.GetStreamsAsync();
                Assert.Equal(300, list.Count);

                IList<string> names = await jsm.GetStreamNamesAsync();
                Assert.Equal(300, names.Count);

                await AddStreamsAsync(jsm, 1100, 300, "x1024");

                list = await jsm.GetStreamsAsync();
                Assert.Equal(1400, list.Count);

                names = await jsm.GetStreamNamesAsync();
                Assert.Equal(1400, names.Count);

                list = await jsm.GetStreamsAsync("*.x256.*");
                Assert.Equal(300, list.Count);

                names = await jsm.GetStreamNamesAsync("*.x256.*");
                Assert.Equal(300, names.Count);

                list = await jsm.GetStreamsAsync("*.x1024.*");
                Assert.Equal(1100, list.Count);

                names = await jsm.GetStreamNamesAsync("*.x1024.*");
                Assert.Equal(1100, names.Count);
            });
        }

        private async Task AddStreamsAsync(IJetStreamManagementAsync jsm, int count, int adj, string div) {
            for (int x = 0; x < count; x++) {
                await CreateMemoryStreamAsync(jsm, "stream-" + (x + adj), "sub" + (x + adj) + "." + div + ".*");
            }
        }

        [Fact]
        public void TestGetStreamNamesBySubjectFilter()
        {
            Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                await CreateMemoryStreamAsync(jsm, Stream(1), "foo");
                await CreateMemoryStreamAsync(jsm, Stream(2), "bar");
                await CreateMemoryStreamAsync(jsm, Stream(3), "a.a");
                await CreateMemoryStreamAsync(jsm, Stream(4), "a.b");

                IList<string> list = await jsm.GetStreamNamesAsync("*");
                AssertStreamNameList(list, 1, 2);

                list = await jsm.GetStreamNamesAsync(">");
                AssertStreamNameList(list, 1, 2, 3, 4);

                list = await jsm.GetStreamNamesAsync("*.*");
                AssertStreamNameList(list, 3, 4);

                list = await jsm.GetStreamNamesAsync("a.>");
                AssertStreamNameList(list, 3, 4);

                list = await jsm.GetStreamNamesAsync("a.*");
                AssertStreamNameList(list, 3, 4);

                list = await jsm.GetStreamNamesAsync("foo");
                AssertStreamNameList(list, 1);

                list = await jsm.GetStreamNamesAsync("a.a");
                AssertStreamNameList(list, 3);

                list = await jsm.GetStreamNamesAsync("nomatch");
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
        public async Task TestDeleteStream()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                NATSJetStreamException e = 
                    await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.DeleteStreamAsync(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);

                CreateDefaultTestStream(c);
                Assert.NotNull(await jsm.GetStreamInfoAsync(STREAM));
                Assert.True(await jsm.DeleteStreamAsync(STREAM));

                e = await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetStreamInfoAsync(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);

                e = await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.DeleteStreamAsync(STREAM));
                Assert.Equal(10059, e.ApiErrorCode);
            });
        }

        [Fact]
        public async Task TestPurgeStreamAndOptions()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                Assert.Throws<ArgumentException>(() => PurgeOptions.Builder().WithKeep(1).WithSequence(1).Build());

                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.PurgeStreamAsync(STREAM));

                CreateMemoryStream(c, STREAM, Subject(1), Subject(2));

                StreamInfo si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(0u, si.State.Messages);

                JsPublish(c, Subject(1), 10);
                si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(10u, si.State.Messages);

                PurgeOptions options = PurgeOptions.Builder().WithKeep(7).Build();
                PurgeResponse pr = await jsm.PurgeStreamAsync(STREAM, options);
                Assert.True(pr.Success);
                Assert.Equal(3u, pr.Purged);

                options = PurgeOptions.Builder().WithSequence(9).Build();
                pr = await jsm.PurgeStreamAsync(STREAM, options);
                Assert.True(pr.Success);
                Assert.Equal(5u, pr.Purged);
                si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(2u, si.State.Messages);

                pr = await jsm.PurgeStreamAsync(STREAM);
                Assert.True(pr.Success);
                Assert.Equal(2u, pr.Purged);
                si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(0u, si.State.Messages);

                JsPublish(c, Subject(1), 10);
                JsPublish(c, Subject(2), 10);
                si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(20u, si.State.Messages);
                await jsm.PurgeStreamAsync(STREAM, PurgeOptions.WithSubject(Subject(1)));
                si = await jsm.GetStreamInfoAsync(STREAM);
                Assert.Equal(10u, si.State.Messages);

                options = PurgeOptions.Builder().WithSubject(Subject(1)).WithSequence(1).Build();
                Assert.Equal(Subject(1), options.Subject);
                Assert.Equal(1u, options.Sequence);

                options = PurgeOptions.Builder().WithSubject(Subject(1)).WithKeep(2).Build();
                Assert.Equal(2u, options.Keep);
            });
        }

        [Fact]
        public async Task TestAddDeleteConsumer()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                bool atLeast290 = c.ServerInfo.IsSameOrNewerThanVersion("2.9.0");

                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await CreateMemoryStreamAsync(jsm, STREAM, SubjectDot(">"));

                IList<ConsumerInfo> list = await jsm.GetConsumersAsync(STREAM);
                Assert.Empty(list);

                ConsumerConfiguration cc = ConsumerConfiguration.Builder().Build();

                ArgumentException e = await Assert.ThrowsAsync<ArgumentException>(
                    async () => await jsm.AddOrUpdateConsumerAsync(null, cc));
                Assert.Contains("Stream cannot be null or empty", e.Message);

                e = await Assert.ThrowsAsync<ArgumentNullException>(
                    async () => await jsm.AddOrUpdateConsumerAsync(STREAM, null));
                Assert.Contains("Value cannot be null", e.Message);

                e = await Assert.ThrowsAsync<ArgumentNullException>(
                    async () => await jsm.AddOrUpdateConsumerAsync(STREAM, cc));
                Assert.Contains("Value cannot be null", e.Message);

                // with and w/o deliver subject for push/pull
                await AddConsumerAsync(jsm, atLeast290, 1, false, null, ConsumerConfiguration.Builder()
                    .WithDurable(Durable(1))
                    .Build());

                await AddConsumerAsync(jsm, atLeast290, 2, true, null, ConsumerConfiguration.Builder()
                    .WithDurable(Durable(2))
                    .WithDeliverSubject(Deliver(2))
                    .Build());

                // test delete here
                IList<string> consumers = await jsm.GetConsumerNamesAsync(STREAM);
                Assert.Equal(2, consumers.Count);
                Assert.True(await jsm.DeleteConsumerAsync(STREAM, Durable(1)));
                consumers = await jsm.GetConsumerNamesAsync(STREAM);
                Assert.Equal(1, consumers.Count);
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.DeleteConsumerAsync(STREAM, Durable(1)));

                // some testing of new name
                if (atLeast290) {
                    await AddConsumerAsync(jsm, atLeast290, 3, false, null, ConsumerConfiguration.Builder()
                        .WithDurable(Durable(3))
                        .WithName(Durable(3))
                        .Build());

                    await AddConsumerAsync(jsm, atLeast290, 4, true, null, ConsumerConfiguration.Builder()
                        .WithDurable(Durable(4))
                        .WithName(Durable(4))
                        .WithDeliverSubject(Deliver(4))
                        .Build());

                    await AddConsumerAsync(jsm, atLeast290, 5, false, ">", ConsumerConfiguration.Builder()
                        .WithDurable(Durable(5))
                        .WithFilterSubject(">")
                        .Build());

                    await AddConsumerAsync(jsm, atLeast290, 6, false, SubjectDot(">"), ConsumerConfiguration.Builder()
                        .WithDurable(Durable(6))
                        .WithFilterSubject(SubjectDot(">"))
                        .Build());

                    await AddConsumerAsync(jsm, atLeast290, 7, false, SubjectDot("foo"), ConsumerConfiguration.Builder()
                        .WithDurable(Durable(7))
                        .WithFilterSubject(SubjectDot("foo"))
                        .Build());
                }
            });
        }

        private static async Task AddConsumerAsync(IJetStreamManagementAsync jsm, bool atLeast290, int id, bool deliver, string fs, ConsumerConfiguration cc) {
            ConsumerInfo ci = await jsm.AddOrUpdateConsumerAsync(STREAM, cc);
            Assert.Equal(Durable(id), ci.Name);
            if (atLeast290) {
                Assert.Equal(Durable(id), ci.ConsumerConfiguration.Name);
            }
            Assert.Equal(Durable(id), ci.ConsumerConfiguration.Durable);
            if (fs == null) {
                Assert.Empty(ci.ConsumerConfiguration.FilterSubject);
            }
            if (deliver) {
                Assert.Equal(Deliver(id), ci.ConsumerConfiguration.DeliverSubject);
            }
        }

        [Fact]
        public async Task TestValidConsumerUpdates()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await CreateMemoryStreamAsync(jsm, STREAM, SUBJECT_GT);

                ConsumerConfiguration cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithDeliverSubject(Deliver(2)).Build();
                await AssertValidAddOrUpdateAsync(jsm, cc);

                cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithAckWait(Duration.OfSeconds(5)).Build();
                await AssertValidAddOrUpdateAsync(jsm, cc);

                cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithRateLimitBps(100).Build();
                await AssertValidAddOrUpdateAsync(jsm, cc);

                cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithMaxAckPending(100).Build();
                await AssertValidAddOrUpdateAsync(jsm, cc);

                cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithMaxDeliver(4).Build();
                await AssertValidAddOrUpdateAsync(jsm, cc);

                if (c.ServerInfo.IsNewerVersionThan("2.8.4"))
                {
                    cc = await PrepForUpdateTestAsync(jsm);
                    cc = ConsumerConfiguration.Builder(cc).WithFilterSubject(SUBJECT_STAR).Build();
                    await AssertValidAddOrUpdateAsync(jsm, cc);
                }
            });
        }

        [Fact]
        public async Task TestInvalidConsumerUpdates()
        {
            await Context.RunInJsServerAsync(async c =>
            {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await CreateMemoryStreamAsync(jsm, STREAM, SUBJECT_GT);

                ConsumerConfiguration cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithDeliverPolicy(DeliverPolicy.New).Build();
                await AssertInvalidConsumerUpdateAsync(jsm, cc);

                if (c.ServerInfo.IsSameOrOlderThanVersion("2.8.4"))
                {
                    cc = await PrepForUpdateTestAsync(jsm);
                    cc = ConsumerConfiguration.Builder(cc).WithFilterSubject(SUBJECT_STAR).Build();
                    await AssertInvalidConsumerUpdateAsync(jsm, cc);
                }

                cc = await PrepForUpdateTestAsync(jsm);
                cc = ConsumerConfiguration.Builder(cc).WithIdleHeartbeat(Duration.OfMillis(111)).Build();
                await AssertInvalidConsumerUpdateAsync(jsm, cc);
            });
        }

        private async Task<ConsumerConfiguration> PrepForUpdateTestAsync(IJetStreamManagementAsync jsm)
        {
            try {
                await jsm.DeleteConsumerAsync(STREAM, Durable(1));
            }
            catch (Exception) { /* ignore */ }

            ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                .WithDurable(Durable(1))
                .WithAckPolicy(AckPolicy.Explicit)
                .WithDeliverSubject(Deliver(1))
                .WithMaxDeliver(3)
                .WithFilterSubject(SUBJECT_GT)
                .Build();
            await AssertValidAddOrUpdateAsync(jsm, cc);
            return cc;
        }

        private async Task AssertInvalidConsumerUpdateAsync(IJetStreamManagementAsync jsm, ConsumerConfiguration cc) {
            NATSJetStreamException e = await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.AddOrUpdateConsumerAsync(STREAM, cc));
            Assert.Equal(10012, e.ApiErrorCode);
            Assert.Equal(500, e.ErrorCode);
        }

        private async Task AssertValidAddOrUpdateAsync(IJetStreamManagementAsync jsm, ConsumerConfiguration cc) {
            ConsumerInfo ci = await jsm.AddOrUpdateConsumerAsync(STREAM, cc);
            ConsumerConfiguration ciCc = ci.ConsumerConfiguration;
            Assert.Equal(cc.Durable, ci.Name);
            Assert.Equal(cc.Durable, ciCc.Durable);
            Assert.Equal(cc.DeliverSubject, ciCc.DeliverSubject);
            Assert.Equal(cc.MaxDeliver, ciCc.MaxDeliver);
            Assert.Equal(cc.DeliverPolicy, ciCc.DeliverPolicy);

            IList<string> consumers = await jsm.GetConsumerNamesAsync(STREAM);
            Assert.Single(consumers);
            Assert.Equal(cc.Durable, consumers[0]);
        }

        [Fact]
        public async Task TestCreateConsumersWithFilters()
        {
            await Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                // plain subject
                await CreateDefaultTestStreamAsync(jsm);

                ConsumerConfiguration.ConsumerConfigurationBuilder builder = ConsumerConfiguration.Builder().WithDurable(DURABLE);
                await jsm.AddOrUpdateConsumerAsync(STREAM, builder.WithFilterSubject(SUBJECT).Build());

                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.AddOrUpdateConsumerAsync(STREAM,
                    builder.WithFilterSubject(SubjectDot("not-match")).Build()));

                // wildcard subject
                await jsm.DeleteStreamAsync(STREAM);
                await CreateMemoryStreamAsync(jsm, STREAM, SUBJECT_STAR);

                await jsm.AddOrUpdateConsumerAsync(STREAM, builder.WithFilterSubject(SubjectDot("A")).Build());

                if (c.ServerInfo.IsSameOrOlderThanVersion("2.8.4"))
                {
                    await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.AddOrUpdateConsumerAsync(STREAM,
                        builder.WithFilterSubject(SubjectDot("not-match")).Build()));
                }

                // gt subject
                await jsm.DeleteStreamAsync(STREAM);
                await CreateMemoryStreamAsync(jsm, STREAM, SUBJECT_GT);

                await jsm.AddOrUpdateConsumerAsync(STREAM, builder.WithFilterSubject(SubjectDot("A")).Build());

                await jsm.AddOrUpdateConsumerAsync(STREAM, ConsumerConfiguration.Builder()
                    .WithDurable(Durable(42))
                    .WithFilterSubject(SubjectDot("F"))
                    .Build()
                );
            });
        }

        [Fact]
        public async Task TestGetConsumerInfo() 
        {
            await Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await CreateDefaultTestStreamAsync(jsm);
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.DeleteConsumerAsync(STREAM, DURABLE));
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
                ConsumerInfo ci = await jsm.AddOrUpdateConsumerAsync(STREAM, cc);
                Assert.Equal(STREAM, ci.Stream);
                Assert.Equal(DURABLE, ci.Name);
                ci = await jsm.GetConsumerInfoAsync(STREAM, DURABLE);
                Assert.Equal(STREAM, ci.Stream);
                Assert.Equal(DURABLE, ci.Name);
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetConsumerInfoAsync(STREAM, Durable(999)));
            });
        }

        [Fact]
        public async Task TestGetConsumers() 
        {
            await Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await CreateMemoryStreamAsync(jsm, STREAM, Subject(0), Subject(1));

                await AddConsumersAsync(jsm, STREAM, 600, "A", null); // getConsumers pages at 256

                IList<ConsumerInfo> list = await jsm.GetConsumersAsync(STREAM);
                Assert.Equal(600, list.Count);

                await AddConsumersAsync(jsm, STREAM, 500, "B", null); // getConsumerNames pages at 1024
                IList<string> names = await jsm.GetConsumerNamesAsync(STREAM);
                Assert.Equal(1100, names.Count);
            });
        }

        private async Task AddConsumersAsync(IJetStreamManagementAsync jsm, string stream, int count, string durableVary, string filterSubject)
        {
            for (int x = 0; x < count; x++) {
                String dur = Durable(durableVary, x + 1);
                ConsumerConfiguration cc = ConsumerConfiguration.Builder()
                        .WithDurable(dur)
                        .WithFilterSubject(filterSubject)
                        .Build();
                ConsumerInfo ci = await jsm.AddOrUpdateConsumerAsync(stream, cc);
                Assert.Equal(dur, ci.Name);
                Assert.Equal(dur, ci.ConsumerConfiguration.Durable);
                Assert.Empty(ci.ConsumerConfiguration.DeliverSubject);
            }
        }

        [Fact]
        public async Task TestDeleteMessage() {
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

            await Context.RunInJsServerAsync(async c => {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                MsgHeader h = new MsgHeader { { "foo", "bar" } };

                js.Publish(new Msg(SUBJECT, null, h, DataBytes(1)));
                js.Publish(new Msg(SUBJECT, null));

                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();

                MessageInfo mi = await jsm.GetMessageAsync(STREAM, 1);
                Assert.Equal(SUBJECT, mi.Subject);
                Assert.Equal(Data(1), Encoding.ASCII.GetString(mi.Data));
                Assert.Equal(1U, mi.Sequence);
                Assert.NotNull(mi.Headers);
                Assert.Equal("bar", mi.Headers["foo"]);

                mi = await jsm.GetMessageAsync(STREAM, 2);
                Assert.Equal(SUBJECT, mi.Subject);
                Assert.Null(mi.Data);
                Assert.Equal(2U, mi.Sequence);
                Assert.Null(mi.Headers);

                Assert.True(await jsm.DeleteMessageAsync(STREAM, 1, false)); // added coverage for use of erase (no_erase) flag.
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.DeleteMessageAsync(STREAM, 1));
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetMessageAsync(STREAM, 1));
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetMessageAsync(STREAM, 3));
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.DeleteMessageAsync(Stream(999), 1));
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetMessageAsync(Stream(999), 1));
            });
        }

        [Fact]
        public async Task TestConsumerReplica()
        {
            await Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                await CreateMemoryStreamAsync(jsm, STREAM, Subject(0), Subject(1));

                ConsumerConfiguration cc0 = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(0))
                    .Build();
                ConsumerInfo ci = await jsm.AddOrUpdateConsumerAsync(STREAM, cc0);

                // server returns 0 when value is not set
                Assert.Equal(0, ci.ConsumerConfiguration.NumReplicas);

                ConsumerConfiguration cc1 = ConsumerConfiguration.Builder()
                    .WithDurable(Durable(0))
                    .WithNumReplicas(1)
                    .Build();
                ci = await jsm.AddOrUpdateConsumerAsync(STREAM, cc1);
                Assert.Equal(1, ci.ConsumerConfiguration.NumReplicas);
            });
        }

        [Fact]
        public async Task TestGetMessage()
        {
            await Context.RunInJsServerAsync(async c => {
                IJetStreamManagementAsync jsm = c.CreateJetStreamManagementAsyncContext();
                IJetStream js = c.CreateJetStreamContext();

                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(STREAM)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(Subject(1), Subject(2))
                    .Build();
                StreamInfo si = await jsm.AddStreamAsync(sc);
                Assert.False(si.Config.AllowDirect);

                await js.PublishAsync(Subject(1), Encoding.UTF8.GetBytes("s1-q1"));
                await js.PublishAsync(Subject(2), Encoding.UTF8.GetBytes("s2-q2"));
                await js.PublishAsync(Subject(1), Encoding.UTF8.GetBytes("s1-q3"));
                await js.PublishAsync(Subject(2), Encoding.UTF8.GetBytes("s2-q4"));
                await js.PublishAsync(Subject(1), Encoding.UTF8.GetBytes("s1-q5"));
                await js.PublishAsync(Subject(2), Encoding.UTF8.GetBytes("s2-q6"));

                await ValidateGetMessageAsync(jsm);

                sc = StreamConfiguration.Builder(si.Config).WithAllowDirect(true).Build();
                si = await jsm.UpdateStreamAsync(sc);
                Assert.True(si.Config.AllowDirect);
                await ValidateGetMessageAsync(jsm);

                // error case stream doesn't exist
                await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetMessageAsync(Stream(999), 1));
            });
        }

        private async Task ValidateGetMessageAsync(IJetStreamManagementAsync jsm) {

            AssertMessageInfo(1, 1, await jsm.GetMessageAsync(STREAM, 1));
            AssertMessageInfo(1, 5, await jsm.GetLastMessageAsync(STREAM, Subject(1)));
            AssertMessageInfo(2, 6, await jsm.GetLastMessageAsync(STREAM, Subject(2)));

            AssertMessageInfo(1, 1, await jsm.GetNextMessageAsync(STREAM, 0, Subject(1)));
            AssertMessageInfo(2, 2, await jsm.GetNextMessageAsync(STREAM, 0, Subject(2)));
            AssertMessageInfo(1, 1, await jsm.GetFirstMessageAsync(STREAM, Subject(1)));
            AssertMessageInfo(2, 2, await jsm.GetFirstMessageAsync(STREAM, Subject(2)));

            AssertMessageInfo(1, 1, await jsm.GetNextMessageAsync(STREAM, 1, Subject(1)));
            AssertMessageInfo(2, 2, await jsm.GetNextMessageAsync(STREAM, 1, Subject(2)));

            AssertMessageInfo(1, 3, await jsm.GetNextMessageAsync(STREAM, 2, Subject(1)));
            AssertMessageInfo(2, 2, await jsm.GetNextMessageAsync(STREAM, 2, Subject(2)));

            AssertMessageInfo(1, 5, await jsm.GetNextMessageAsync(STREAM, 5, Subject(1)));
            AssertMessageInfo(2, 6, await jsm.GetNextMessageAsync(STREAM, 5, Subject(2)));

            AssertStatus(10003, await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetMessageAsync(STREAM, 0)));
            AssertStatus(10037, await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetMessageAsync(STREAM, 9)));
            AssertStatus(10037, await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetLastMessageAsync(STREAM, "not-a-subject")));
            AssertStatus(10037, await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetFirstMessageAsync(STREAM, "not-a-subject")));
            AssertStatus(10037, await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetNextMessageAsync(STREAM, 9, Subject(1))));
            AssertStatus(10037, await Assert.ThrowsAsync<NATSJetStreamException>(async () => await jsm.GetNextMessageAsync(STREAM, 1, "not-a-subject")));
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
        
        private void ValidateMgr(ulong seq, String lastBySubject, String nextBySubject, MessageGetRequest mgr) {
            Assert.Equal(seq, mgr.Sequence);
            Assert.Equal(lastBySubject, mgr.LastBySubject);
            Assert.Equal(nextBySubject, mgr.NextBySubject);
            Assert.Equal(seq > 0 && nextBySubject == null, mgr.IsSequenceOnly);
            Assert.Equal(lastBySubject != null, mgr.IsLastBySubject);
            Assert.Equal(nextBySubject != null, mgr.IsNextBySubject);
        }
    }
}
