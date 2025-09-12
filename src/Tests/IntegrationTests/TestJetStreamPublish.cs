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
using Subject = NATS.Client.JetStream.Subject;

namespace IntegrationTests
{
    public class TestJetStreamPublish : TestSuite<JetStreamPublishSuiteContext>
    {
        public TestJetStreamPublish(JetStreamPublishSuiteContext context) : base(context) {}

        [Fact]
        public void TestPublishVarieties() {
            Context.RunInJsServer(c =>
            {
                string stream = Stream();
                string subject = Subject();
                
                CreateMemoryStream(c, stream, subject);
                IJetStream js = c.CreateJetStreamContext();

                PublishAck pa = js.Publish(subject, DataBytes(1));
                AssertPublishAck(pa, stream, 1);

                Msg msg = new Msg(subject, DataBytes(2));
                pa = js.Publish(msg);
                AssertPublishAck(pa, stream, 2);

                PublishOptions po = PublishOptions.Builder().Build();
                pa = js.Publish(subject, DataBytes(3), po);
                AssertPublishAck(pa, stream, 3);

                msg = new Msg(subject, DataBytes(4));
                pa = js.Publish(msg, po);
                AssertPublishAck(pa, stream, 4);

                pa = js.Publish(subject, null);
                AssertPublishAck(pa, stream, 5);

                msg = new Msg(subject);
                pa = js.Publish(msg);
                AssertPublishAck(pa, stream, 6);

                pa = js.Publish(subject, null, po);
                AssertPublishAck(pa, stream, 7);

                msg = new Msg(subject);
                pa = js.Publish(msg, po);
                AssertPublishAck(pa, stream, 8);

                MsgHeader h = new MsgHeader { { "foo", "bar9" } };
                pa = js.Publish(subject, h, DataBytes(9));
                AssertPublishAck(pa, stream, 9);

                h = new MsgHeader { { "foo", "bar10" } };
                pa = js.Publish(subject, h, DataBytes(10));
                AssertPublishAck(pa, stream, 10);

                IJetStreamPushSyncSubscription s = js.PushSubscribeSync(subject);
                AssertNextMessage(s, Data(1), null);
                AssertNextMessage(s, Data(2), null);
                AssertNextMessage(s, Data(3), null);
                AssertNextMessage(s, Data(4), null);
                AssertNextMessage(s, null, null); // 5
                AssertNextMessage(s, null, null); // 6
                AssertNextMessage(s, null, null); // 7
                AssertNextMessage(s, null, null); // 8
                AssertNextMessage(s, Data(9), "bar9");
                AssertNextMessage(s, Data(10), "bar10");

                // bad subject
                Assert.Throws<NATSNoRespondersException>(() => js.Publish(Subject(999), null));
            });
        }

        private void AssertNextMessage(IJetStreamPushSyncSubscription s, string data, string header) {
            Msg m = s.NextMessage(DefaultTimeout);
            Assert.NotNull(m);
            if (data == null) {
                Assert.NotNull(m.Data);
                Assert.Empty(m.Data);
            }
            else {
                Assert.Equal(data, Encoding.ASCII.GetString(m.Data));
            }
            if (header != null) {
                Assert.True(m.HasHeaders);
                Assert.Equal(header, m.Header["foo"]);
            }
        }

        [Fact]
        public void TestPublishAsyncVarieties()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                IList<Task<PublishAck>> tasks = new List<Task<PublishAck>>();
                
                tasks.Add(js.PublishAsync(SUBJECT, DataBytes(1)));

                Msg msg = new Msg(SUBJECT, DataBytes(2));
                tasks.Add(js.PublishAsync(msg));

                PublishOptions po = PublishOptions.Builder().Build();
                tasks.Add(js.PublishAsync(SUBJECT, DataBytes(3), po));

                msg = new Msg(SUBJECT, DataBytes(4));
                tasks.Add(js.PublishAsync(msg, po));

                MsgHeader h = new MsgHeader { { "foo", "bar5" } };
                tasks.Add(js.PublishAsync(SUBJECT, h, DataBytes(5)));

                h = new MsgHeader { { "foo", "bar6" } };
                tasks.Add(js.PublishAsync(SUBJECT, h, DataBytes(6), po));

                Thread.Sleep(100); // just make sure all the publish complete

                IList<ulong> seqnos = new List<ulong> {1, 2, 3, 4, 5, 6};
                foreach (var task in tasks)
                {
                    PublishAck pa = task.Result;
                    Assert.Equal(STREAM, pa.Stream);
                    Assert.False(pa.Duplicate);
                    Assert.True(seqnos.Contains(pa.Seq));
                    seqnos.Remove(pa.Seq);
                }

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT);
                for (int x = 1; x <= 6; x++)
                {
                    Msg m = ReadMessageAck(sub);
                    Assert.NotNull(m);
                    Assert.Equal(Data(x), Encoding.UTF8.GetString(m.Data));
                    if (x > 4) {
                        Assert.True(m.HasHeaders);
                        Assert.Equal("bar" + x, m.Header["foo"]);
                    }
                }

                AssertTaskException(js.PublishAsync(Subject(999), null));
                AssertTaskException(js.PublishAsync(new Msg(Subject(999))));

                PublishOptions pox = PublishOptions.Builder().WithExpectedLastMsgId(MessageId(999)).Build();
                AssertTaskException(js.PublishAsync(Subject(999), null, pox));
                AssertTaskException(js.PublishAsync(new Msg(Subject(999)), pox));
                
            });
        }

        private void AssertTaskException(Task<PublishAck> task)
        {
            try
            {
                var ignored = task.Result;
            }
            catch (Exception e)
            {
                Assert.NotNull(e.InnerException as NATSNoRespondersException);
            }
        }

        [Fact]
        public void TestPublishExpectations()
        {
            Context.RunInJsServer(c =>
            {
                string stream1 = Stream();
                string subject1Prefix = Variant();
                string streamSubject = subject1Prefix + ".>";
                string sub11 = subject1Prefix + ".foo.1";
                string sub12 = subject1Prefix + ".foo.2";
                string sub13 = subject1Prefix + ".bar.3";

                CreateMemoryStream(c, stream1, streamSubject);
                IJetStream js = c.CreateJetStreamContext();

                PublishOptions po = PublishOptions.Builder()
                    .WithExpectedStream(stream1)
                    .WithMessageId(MessageId(1))
                    .Build();
                PublishAck pa = js.Publish(sub11, DataBytes(1), po);
                AssertPublishAck(pa, stream1, 1);

                po = PublishOptions.Builder()
                    .WithExpectedLastMsgId(MessageId(1))
                    .WithMessageId(MessageId(2))
                    .Build();
                pa = js.Publish(sub11, DataBytes(2), po);
                AssertPublishAck(pa, stream1, 2);

                po = PublishOptions.Builder()
                    .WithExpectedLastSequence(2)
                    .WithMessageId(MessageId(3))
                    .Build();
                pa = js.Publish(sub11, DataBytes(3), po);
                AssertPublishAck(pa, stream1, 3);

                po = PublishOptions.Builder()
                    .WithExpectedLastSequence(3)
                    .WithMessageId(MessageId(4))
                    .Build();
                pa = js.Publish(sub12, DataBytes(4), po);
                AssertPublishAck(pa, stream1, 4);

                po = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(3)
                    .WithMessageId(MessageId(5))
                    .Build();
                pa = js.Publish(sub11, DataBytes(5), po);
                AssertPublishAck(pa, stream1, 5);

                po = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(4)
                    .WithMessageId(MessageId(6))
                    .Build();
                pa = js.Publish(sub12, DataBytes(6), po);
                AssertPublishAck(pa, stream1, 6);

                PublishOptions po1 = PublishOptions.Builder().WithExpectedStream(Stream(999)).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(sub11, DataBytes(999), po1));

                PublishOptions po2 = PublishOptions.Builder().WithExpectedLastMsgId(MessageId(999)).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(sub11, DataBytes(999), po2));

                PublishOptions po3 = PublishOptions.Builder().WithExpectedLastSequence(999).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(sub11, DataBytes(999), po3));

                PublishOptions po4 = PublishOptions.Builder().WithExpectedLastSubjectSequence(999).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(sub11, DataBytes(999), po4));

                // 0 has meaning to expectedLastSubjectSequence
                string stream2 = Stream();
                string sub22 = Subject();
                CreateMemoryStream(c, stream2, sub22);
                PublishOptions poLss = PublishOptions.Builder().WithExpectedLastSubjectSequence(0).Build();
                pa = js.Publish(sub22, DataBytes(22), poLss);
                AssertPublishAck(pa, stream2, 1U);
               
                NATSJetStreamException e = Assert.Throws<NATSJetStreamException>(() => js.Publish(sub22, DataBytes(999), poLss));
                Assert.Equal(10071, e.ApiErrorCode);

                // 0 has meaning
                stream2 = Stream();
                sub22 = Subject();
                CreateMemoryStream(c, stream2, sub22);
                PublishOptions poLs = PublishOptions.Builder().WithExpectedLastSequence(0).Build();
                pa = js.Publish(sub22, DataBytes(331), poLs);
                AssertPublishAck(pa, stream2, 1);

                stream2 = Stream();
                sub22 = Subject();
                CreateMemoryStream(c, stream2, sub22);
                poLs = PublishOptions.Builder().WithExpectedLastSequence(0).Build();
                pa = js.Publish(sub22, DataBytes(441), poLs);
                AssertPublishAck(pa, stream2, 1);

                // expectedLastSubjectSequenceSubject

                pa = js.Publish(sub13, DataBytes(500));
                AssertPublishAck(pa, stream1, 7);

                PublishOptions poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(5)
                    .Build();
                pa = js.Publish(sub11, DataBytes(501), poLsss);
                AssertPublishAck(pa, stream1, 8);

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(6)
                    .Build();
                pa = js.Publish(sub12, DataBytes(502), poLsss);
                AssertPublishAck(pa, stream1, 9);

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(9)
                    .WithExpectedLastSubjectSequenceSubject(streamSubject)
                    .Build();
                pa = js.Publish(sub12, DataBytes(503), poLsss);
                AssertPublishAck(pa, stream1, 10);

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(10)
                    .WithExpectedLastSubjectSequenceSubject(subject1Prefix + ".foo.*")
                    .Build();
                pa = js.Publish(sub12, DataBytes(504), poLsss);
                AssertPublishAck(pa, stream1, 11);

                Assert.Throws<NATSJetStreamException>(() => js.Publish(sub12, DataBytes(505), poLsss));

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(7)
                    .WithExpectedLastSubjectSequenceSubject(subject1Prefix + ".bar.*")
                    .Build();
                pa = js.Publish(sub13, DataBytes(506), poLsss);
                AssertPublishAck(pa, stream1, 12);

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(12)
                    .WithExpectedLastSubjectSequenceSubject(streamSubject)
                    .Build();
                pa = js.Publish(sub13, DataBytes(507), poLsss);
                AssertPublishAck(pa, stream1, 13);

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequenceSubject("not-even-a-subject")
                    .Build();
                if (AtLeast2_12(c)) {
                    Assert.Throws<NATSJetStreamException>(() => js.Publish(sub13, DataBytes(508), poLsss));
                }
                else {
                    pa = js.Publish(sub13, DataBytes(508), poLsss);
                    AssertPublishAck(pa, stream1, 14);
                }

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSequence(14)
                    .WithExpectedLastSubjectSequenceSubject("not-even-a-subject")
                    .Build();
                if (AtLeast2_12(c)) {
                    Assert.Throws<NATSJetStreamException>(() => js.Publish(sub13, DataBytes(509), poLsss));
                }
                else {
                    pa = js.Publish(sub13, DataBytes(509), poLsss);
                    AssertPublishAck(pa, stream1, 15);
                }

                poLsss = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(15)
                    .WithExpectedLastSubjectSequenceSubject("not-even-a-subject")
                    .Build();
                // JetStreamApiException: wrong last sequence: 0 [10071]
                Assert.Throws<NATSJetStreamException>(() => js.Publish(sub13, DataBytes(510), poLsss));
            });
        }

        [Fact]
        public void TestPublishMiscExceptions()
        {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                // stream supplied and matches
                PublishOptions po = PublishOptions.Builder()
                    .WithStream(STREAM)
                    .Build();
                js.Publish(SUBJECT, DataBytes(999), po);

                // mismatch stream to PO stream
                po = PublishOptions.Builder()
                    .WithStream(Stream(999))
                    .Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(SUBJECT, DataBytes(999), po));

                // invalid subject
                Assert.Throws<NATSNoRespondersException>(() => js.Publish(Subject(999), DataBytes(999)));
            });
        }

        private void AssertPublishAck(PublishAck pa, string stream, ulong seqno) {
            Assert.Equal(stream, pa.Stream);
            Assert.Equal(seqno, pa.Seq);
            Assert.False(pa.Duplicate);
        }

        [Fact]
        public void TestPublishAckJson()
        {
            string json = "{\"stream\":\"sname\", \"seq\":42, \"duplicate\":false}";
            PublishAck pa = new PublishAck(json);
            Assert.Equal("sname", pa.Stream);
            Assert.Equal(42U, pa.Seq);
            Assert.False(pa.Duplicate);
        }

        [Fact] 
        public void TestPublishNoAck()
        {
            Context.RunInJsServer(c => 
            {
                CreateDefaultTestStream(c);

                JetStreamOptions jso = JetStreamOptions.Builder().WithPublishNoAck(true).Build();
                IJetStream js = c.CreateJetStreamContext(jso);
                
                string data1 = "noackdata1";
                string data2 = "noackdata2";

                PublishAck pa = js.Publish(SUBJECT, Encoding.ASCII.GetBytes(data1));
                Assert.Null(pa);

                Task<PublishAck> task = js.PublishAsync(SUBJECT, Encoding.ASCII.GetBytes(data2));
                Assert.Null(task.Result);

                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(SUBJECT);
                Msg m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal(data1, Encoding.ASCII.GetString(m.Data));
                m = sub.NextMessage(DefaultTimeout);
                Assert.NotNull(m);
                Assert.Equal(data2, Encoding.ASCII.GetString(m.Data));
            });
        }

        [Fact] 
        public void TestPublishWithTTL()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                string stream = Stream();
                string subject = Subject();
                jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(stream)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(subject)
                    .WithAllowMessageTtl()
                    .Build()
                );
                
                PublishOptions opts = PublishOptions.Builder().WithMessageTtlSeconds(1).Build();
                PublishAck pa1 = js.Publish(subject, null, opts);
                Assert.NotNull(pa1);

                opts = PublishOptions.Builder().WithMessageTtlNever().Build();
                PublishAck paNever = js.Publish(subject, null, opts);
                Assert.NotNull(paNever);

                MessageInfo mi1 = jsm.GetMessage(stream, pa1.Seq);
                Assert.Equal("1s", mi1.Headers.GetFirst(JetStreamConstants.MsgTtlHdr));

                MessageInfo miNever = jsm.GetMessage(stream, paNever.Seq);
                Assert.Equal("never", miNever.Headers.GetFirst(JetStreamConstants.MsgTtlHdr));

                Thread.Sleep(1200);

                NATSJetStreamException e = Assert.Throws<NATSJetStreamException>(() => jsm.GetMessage(stream, pa1.Seq));
                Assert.Equal(10037, e.ApiErrorCode);
                Assert.NotNull((jsm.GetMessage(stream, paNever.Seq)));
            });
        }

        [Fact] 
        public void TestMsgDeleteMarkerMaxAge()
        {
            Context.RunInJsServer(c =>
            {
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                IJetStream js = c.CreateJetStreamContext();

                string stream = Stream();
                string subject = Subject();
                jsm.AddStream(StreamConfiguration.Builder()
                    .WithName(stream)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(subject)
                    .WithAllowMessageTtl()
                    .WithSubjectDeleteMarkerTtl(Duration.OfSeconds(50))
                    .WithMaxAge(1000)
                    .Build()
                );
                
                PublishOptions opts = PublishOptions.Builder().WithMessageTtlSeconds(1).Build();
                PublishAck pa = js.Publish(subject, null, opts);
                Assert.NotNull(pa);

                Thread.Sleep(1200);

                MessageInfo mi = jsm.GetLastMessage(stream, subject);
                
                Assert.Equal("MaxAge", mi.Headers.GetFirst(JetStreamConstants.NatsMarkerReason));
                Assert.Equal("50s", mi.Headers.GetFirst(JetStreamConstants.MsgTtlHdr));

                Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder()
                    .WithName(stream)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(subject)
                    .WithAllowMessageTtl()
                    .WithSubjectDeleteMarkerTtl(Duration.OfMillis(999))
                    .Build());

                Assert.Throws<ArgumentException>(() => StreamConfiguration.Builder()
                    .WithName(stream)
                    .WithStorageType(StorageType.Memory)
                    .WithSubjects(subject)
                    .WithAllowMessageTtl()
                    .WithSubjectDeleteMarkerTtl(999)
                    .Build());
            });
        }
    }
}
