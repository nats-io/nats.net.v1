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
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestMirrorAndSource : TestSuite<MirrorSourceSuiteContext>
    {
        static readonly string S1 = Stream(1);
        static readonly string S2 = Stream(2);
        static readonly string S3 = Stream(3);
        static readonly string S4 = Stream(4);
        static readonly string S5 = Stream(5);
        static readonly string S99 = Stream(99);
        static readonly string U1 = Subject(1);
        static readonly string U2 = Subject(2);
        static readonly string U3 = Subject(3);
        static readonly string M1 = Mrrr(1);
        static readonly string R1 = Src(1);
        static readonly string R2 = Src(2);

        public TestMirrorAndSource(MirrorSourceSuiteContext context) : base(context)
        {
        }

        [Fact]
        public void TestMirrorBasics()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                Mirror mirror = Mirror.Builder().WithName(S1).Build();

                // Create source stream
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(S1)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(U1, U2, U3)
                    .Build();
                StreamInfo si = jsm.AddStream(sc);
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(S1, sc.Name);

                // Now create our mirror stream.
                sc = StreamConfiguration.Builder()
                    .WithName(M1)
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(mirror)
                    .Build();
                jsm.AddStream(sc);
                AssertMirror(jsm, M1, S1, null, null);

                // Send 100 messages.
                JsPublish(js, U2, 100);

                // Check the state
                AssertMirror(jsm, M1, S1, 100L, null);

                // Purge the source stream.
                jsm.PurgeStream(S1);

                JsPublish(js, U2, 50);

                // Create second mirror
                sc = StreamConfiguration.Builder()
                    .WithName(Mrrr(2))
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(mirror)
                    .Build();
                jsm.AddStream(sc);

                // Check the state
                AssertMirror(jsm, Mrrr(2), S1, 50L, 101L);

                JsPublish(js, U3, 100);

                // third mirror checks start seq
                sc = StreamConfiguration.Builder()
                    .WithName(Mrrr(3))
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(Mirror.Builder().WithName(S1).WithStartSeq(150).Build())
                    .Build();
                jsm.AddStream(sc);

                // Check the state
                AssertMirror(jsm, Mrrr(3), S1, 101L, 150L);

                // third mirror checks start seq
                DateTime zdt = DateTime.Now.AddHours(-2);
                sc = StreamConfiguration.Builder()
                    .WithName(Mrrr(4))
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(Mirror.Builder().WithName(S1).WithStartTime(zdt).Build())
                    .Build();
                jsm.AddStream(sc);

                // Check the state
                AssertMirror(jsm, Mrrr(4), S1, 150L, 101L);
            });
        }

        [Fact]
        public void TestMirrorReading()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // Create source stream
                StreamConfiguration sc = StreamConfiguration.Builder()
                    .WithName(S1)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(U1, U2)
                    .Build();
                StreamInfo si = jsm.AddStream(sc);
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(S1, sc.Name);

                Mirror mirror = Mirror.Builder().WithName(S1).Build();

                // Now create our mirror stream.
                sc = StreamConfiguration.Builder()
                    .WithName(M1)
                    .WithStorageType(StorageType.Memory)
                    .WithMirror(mirror)
                    .Build();
                jsm.AddStream(sc);
                AssertMirror(jsm, M1, S1, null, null);

                // Send messages.
                JsPublish(js, U1, 10);
                JsPublish(js, U2, 20);

                AssertMirror(jsm, M1, S1, 30L, null);

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(U1);
                IList<Msg> list = ReadMessagesAck(sub);
                Assert.Equal(10, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(S1, m.MetaData.Stream);
                }

                sub = js.PushSubscribeSync(U2);
                list = ReadMessagesAck(sub);
                Assert.Equal(20, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(S1, m.MetaData.Stream);
                }

                PushSubscribeOptions pso = PushSubscribeOptions.ForStream(M1);
                sub = js.PushSubscribeSync(U1, pso);
                list = ReadMessagesAck(sub);
                Assert.Equal(10, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(M1, m.MetaData.Stream);
                }

                sub = js.PushSubscribeSync(U2, pso);
                list = ReadMessagesAck(sub);
                Assert.Equal(20, list.Count);
                foreach (Msg m in list) {
                    Assert.Equal(M1, m.MetaData.Stream);
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
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                // Create streams
                StreamInfo si = jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(S1).WithStorageType(StorageType.Memory).Build());
                StreamConfiguration sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(S1, sc.Name);

                si = jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(S2).WithStorageType(StorageType.Memory).Build());
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(S2, sc.Name);

                si = jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(S3).WithStorageType(StorageType.Memory).Build());
                sc = si.Config;
                Assert.NotNull(sc);
                Assert.Equal(S3, sc.Name);

                // Populate each one.
                JsPublish(js, S1, 10);
                JsPublish(js, S2, 15);
                JsPublish(js, S3, 25);

                sc = StreamConfiguration.Builder()
                    .WithName(R1)
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(S1).Build(),
                        Source.Builder().WithName(S2).Build(),
                        Source.Builder().WithName(S3).Build())
                    .Build();

                jsm.AddStream(sc);

                AssertSource(jsm, R1, 50L, null);

                sc = StreamConfiguration.Builder()
                    .WithName(R1)
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(S1).Build(),
                        Source.Builder().WithName(S2).Build(),
                        Source.Builder().WithName(S4).Build())
                    .Build();

                jsm.UpdateStream(sc);

                sc = StreamConfiguration.Builder()
                    .WithName(S99)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(S4, S5)
                    .Build();
                jsm.AddStream(sc);

                JsPublish(js, S4, 20);
                JsPublish(js, S5, 20);
                JsPublish(js, S4, 10);

                sc = StreamConfiguration.Builder()
                    .WithName(R2)
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(S99).WithStartSeq(26).Build())
                    .Build();
                jsm.AddStream(sc);
                AssertSource(jsm, R2, 25L, null);

                MessageInfo info = jsm.GetMessage(R2, 1);
                AssertStreamSource(info, S99, 26);

                sc = StreamConfiguration.Builder()
                    .WithName(Src(3))
                    .WithStorageType(StorageType.Memory)
                    .WithSources(Source.Builder().WithName(S99).WithStartSeq(11).WithFilterSubject(S4).Build())
                    .Build();
                jsm.AddStream(sc);
                AssertSource(jsm, Src(3), 20L, null);

                info = jsm.GetMessage(Src(3), 1);
                AssertStreamSource(info, S99, 11);

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