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

                JetStreamTestBase.JsPublish(c, SUBJECT, 1);
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
                CreateMemoryStream(c, STREAM, Subject(0), Subject(1));
                
                // TODO ...
            });
        }
    }
}
