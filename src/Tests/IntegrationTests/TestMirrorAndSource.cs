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
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestMirrorAndSource : TestSuite<OneServerSuiteContext>
    {
        public TestMirrorAndSource(OneServerSuiteContext context) : base(context)
        {
        }

        [Fact]
        public void TestMirrorBasics()
        {
            string streamName = Stream(Nuid.NextGlobal());
            string mirrorName = Mrrr(Nuid.NextGlobal());
            string subj1 = Subject(Nuid.NextGlobal());
            string subj2 = Subject(Nuid.NextGlobal());
            string subj3 = Subject(Nuid.NextGlobal());

            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                Mirror mirror = Mirror.Builder().WithName(streamName).Build();

                // Create source stream
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(streamName)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(subj1, subj2, subj3)
                    .Build();
                StreamInfo si = jsm.AddStream(sc);
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(streamName, sc.Name);

                // Now create our mirror stream.
                sc = StreamConfiguration.Builder()
                    .WithName(mirrorName)
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(mirror)
                    .Build();
                jsm.AddStream(sc);
                AssertMirror(jsm, mirrorName, streamName, null, null);

                // Send 100 messages.
                JsPublish(js, subj2, 100);

                // Check the state
                AssertMirror(jsm, mirrorName, streamName, 100L, null);

                // Purge the source stream.
                jsm.PurgeStream(streamName);

                JsPublish(js, subj2, 50);

                // Create second mirror
                sc = StreamConfiguration.Builder()
                    .WithName(Mrrr(2))
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(mirror)
                    .Build();
                jsm.AddStream(sc);

                // Check the state
                AssertMirror(jsm, Mrrr(2), streamName, 50L, 101L);

                JsPublish(js, subj3, 100);

                // third mirror checks start seq
                sc = StreamConfiguration.Builder()
                    .WithName(Mrrr(3))
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(Mirror.Builder().WithName(streamName).WithStartSeq(150).Build())
                    .Build();
                jsm.AddStream(sc);

                // Check the state
                AssertMirror(jsm, Mrrr(3), streamName, 101L, 150L);

                // third mirror checks start seq
                DateTime zdt = DateTime.Now.AddHours(-2);
                sc = StreamConfiguration.Builder()
                    .WithName(Mrrr(4))
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(Mirror.Builder().WithName(streamName).WithStartTime(zdt).Build())
                    .Build();
                jsm.AddStream(sc);

                // Check the state
                AssertMirror(jsm, Mrrr(4), streamName, 150L, 101L);
            });
        }

        [Fact]
        public void TestMirrorReading()
        {
            string streamName = Stream(Nuid.NextGlobal());
            string mirrorName = Mrrr(Nuid.NextGlobal());
            string subj1 = Subject(Nuid.NextGlobal());
            string subj2 = Subject(Nuid.NextGlobal());

            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // Create source stream
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(streamName)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(subj1, subj2)
                    .Build();
                StreamInfo si = jsm.AddStream(sc);
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(streamName, sc.Name);

                Mirror mirror = Mirror.Builder().WithName(streamName).Build();

                // Now create our mirror stream.
                sc = StreamConfiguration.Builder()
                    .WithName(mirrorName)
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(mirror)
                    .Build();
                jsm.AddStream(sc);
                AssertMirror(jsm, mirrorName, streamName, null, null);

                // Send messages.
                JsPublish(js, subj1, 10);
                JsPublish(js, subj2, 20);

                AssertMirror(jsm, mirrorName, streamName, 30L, null);

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(subj1);
                IList<Msg> list = ReadMessagesAck(sub);
                Assert.Equal(10, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(streamName, m.MetaData.Stream);
                }

                sub = js.PushSubscribeSync(subj2);
                list = ReadMessagesAck(sub);
                Assert.Equal(20, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(streamName, m.MetaData.Stream);
                }

                PushSubscribeOptions pso = PushSubscribeOptions.ForStream(mirrorName);
                sub = js.PushSubscribeSync(subj1, pso);
                list = ReadMessagesAck(sub);
                Assert.Equal(10, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(mirrorName, m.MetaData.Stream);
                }

                sub = js.PushSubscribeSync(subj2, pso);
                list = ReadMessagesAck(sub);
                Assert.Equal(20, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(mirrorName, m.MetaData.Stream);
                }
            });
        }

        [Fact]
        public void TestMirrorExceptions()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                Mirror mirror = Mirror.Builder().WithName(STREAM).Build();

                StreamConfiguration scEx = StreamConfiguration.Builder()
                    .WithName(Mrrr(99))
                    .WithSubjects(Subject(1))
                    .WithMirror(mirror)
                    .Build();
                Assert.Throws<NATSJetStreamException>(() => jsm.AddStream(scEx));

            });
        }

        [Fact]
        public void TestSourceBasics()
        {
            string s1 = Stream(Nuid.NextGlobal());
            string s2 = Stream(Nuid.NextGlobal());
            string s3 = Stream(Nuid.NextGlobal());
            string s4 = Stream(Nuid.NextGlobal());
            string s5 = Stream(Nuid.NextGlobal());
            string s6 = Stream(Nuid.NextGlobal());
            string src1 = Src(Nuid.NextGlobal());
            string src2 = Src(Nuid.NextGlobal());

            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // Create streams
                StreamInfo si = jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(s1).WithStorageType(StorageType.Memory).Build());
                StreamConfiguration sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(s1, sc.Name);

                si = jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(s2).WithStorageType(StorageType.Memory).Build());
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(s2, sc.Name);

                si = jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(s3).WithStorageType(StorageType.Memory).Build());
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(s3, sc.Name);

                // Populate each one.
                JsPublish(js, s1, 10);
                JsPublish(js, s2, 15);
                JsPublish(js, s3, 25);

                sc = StreamConfiguration.Builder()
                    .WithName(src1)
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(s1).Build(),
                        Source.Builder().WithName(s2).Build(),
                        Source.Builder().WithName(s3).Build())
                    .Build();

                jsm.AddStream(sc);

                AssertSource(jsm, src1, 50L, null);

                sc = StreamConfiguration.Builder()
                    .WithName(src1)
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(s1).Build(),
                        Source.Builder().WithName(s2).Build(),
                        Source.Builder().WithName(s4).Build())
                    .Build();

                jsm.UpdateStream(sc);

                sc = StreamConfiguration.Builder()
                    .WithName(s6)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(s4, s5)
                    .Build();
                jsm.AddStream(sc);

                JsPublish(js, s4, 20);
                JsPublish(js, s5, 20);
                JsPublish(js, s4, 10);

                sc = StreamConfiguration.Builder()
                    .WithName(src2)
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(s6).WithStartSeq(26).Build())
                    .Build();
                jsm.AddStream(sc);
                AssertSource(jsm, src2, 25L, null);

                MessageInfo info = jsm.GetMessage(src2, 1);
                AssertStreamSource(info, s6, 26);

                sc = StreamConfiguration.Builder()
                    .WithName(Src(3))
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(s6).WithStartSeq(11).WithFilterSubject(s4).Build())
                    .Build();
                jsm.AddStream(sc);
                AssertSource(jsm, Src(3), 20L, null);

                info = jsm.GetMessage(Src(3), 1);
                AssertStreamSource(info, s6, 11);
            });
        }
 
        private void AssertSource(IJetStreamManagement jsm, string stream, ulong msgCount, ulong? firstSeq) {
            Thread.Sleep(1000);
            StreamInfo si = jsm.GetStreamInfo(stream);
            AssertConfig(stream, msgCount, firstSeq, si);
        }

        private void AssertMirror(IJetStreamManagement jsm, 
            string stream, string mirroring, ulong? msgCount, ulong? firstSeq) {
            Thread.Sleep(1500);
            StreamInfo si = jsm.GetStreamInfo(stream);

            MirrorInfo msi = si.MirrorInfo;
            Assert.NotNull(msi);
            Assert.Equal(mirroring, msi.Name);

            AssertConfig(stream, msgCount, firstSeq, si);
        }

        private void AssertConfig(string stream, ulong? msgCount, ulong? firstSeq, StreamInfo si) {
            StreamConfiguration sc = si.Config;
            Assert.NotNull(sc);
            Assert.Equal(stream, sc.Name);

            StreamState ss = si.State;
            if (msgCount != null) {
                Assert.Equal(msgCount, ss.Messages);
            }
            if (firstSeq != null) {
                Assert.Equal(firstSeq, ss.FirstSeq);
            }
        }
 
        private void AssertStreamSource(MessageInfo info, string stream, int i) {
            string hval = info.Headers["Nats-Stream-Source"];
            string[] parts = hval.Split(' ');
            Assert.Equal(stream, parts[0]);
            Assert.Equal("" + i, parts[1]);

        }
    }
}