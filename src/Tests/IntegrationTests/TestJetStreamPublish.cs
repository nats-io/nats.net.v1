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
using NATS.Client.JetStream;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestJetStreamPublish : TestSuite<JetStreamPublishSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestJetStreamPublish(ITestOutputHelper output, JetStreamPublishSuiteContext context) : base(context)
        {
            this.output = output;
        }

        [Fact]
        public void TestPublishVarieties() {
            Context.RunInJsServer(c =>
            {
                CreateDefaultTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                PublishAck pa = js.Publish(SUBJECT, DataBytes(1));
                AssertPublishAck(pa, 1);

                Msg msg = new Msg(SUBJECT, DataBytes(2));
                pa = js.Publish(msg);
                AssertPublishAck(pa, 2);

                PublishOptions po = PublishOptions.Builder().Build();
                pa = js.Publish(SUBJECT, DataBytes(3), po);
                AssertPublishAck(pa, 3);

                msg = new Msg(SUBJECT, DataBytes(4));
                pa = js.Publish(msg, po);
                AssertPublishAck(pa, 4);

                pa = js.Publish(SUBJECT, null);
                AssertPublishAck(pa, 5);

                msg = new Msg(SUBJECT);
                pa = js.Publish(msg);
                AssertPublishAck(pa, 6);

                pa = js.Publish(SUBJECT, null, po);
                AssertPublishAck(pa, 7);

                msg = new Msg(SUBJECT);
                pa = js.Publish(msg, po);
                AssertPublishAck(pa, 8);

                MsgHeader h = new MsgHeader { { "foo", "bar9" } };
                pa = js.Publish(SUBJECT, h, DataBytes(9));
                AssertPublishAck(pa, 9);

                h = new MsgHeader { { "foo", "bar10" } };
                pa = js.Publish(SUBJECT, h, DataBytes(10));
                AssertPublishAck(pa, 10);

                IJetStreamPushSyncSubscription s = js.PushSubscribeSync(SUBJECT);
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
                CreateMemoryStream(c, STREAM, Subject(1), Subject(2));
                IJetStream js = c.CreateJetStreamContext();

                PublishOptions po = PublishOptions.Builder()
                    .WithExpectedStream(STREAM)
                    .WithMessageId(MessageId(1))
                    .Build();
                PublishAck pa = js.Publish(Subject(1), DataBytes(1), po);
                AssertPublishAck(pa, 1);

                po = PublishOptions.Builder()
                    .WithExpectedLastMsgId(MessageId(1))
                    .WithMessageId(MessageId(2))
                    .Build();
                pa = js.Publish(Subject(1), DataBytes(2), po);
                AssertPublishAck(pa, 2);

                po = PublishOptions.Builder()
                    .WithExpectedLastSequence(2)
                    .WithMessageId(MessageId(3))
                    .Build();
                pa = js.Publish(Subject(1), DataBytes(3), po);
                AssertPublishAck(pa, 3);

                po = PublishOptions.Builder()
                    .WithExpectedLastSequence(3)
                    .WithMessageId(MessageId(4))
                    .Build();
                pa = js.Publish(Subject(2), DataBytes(4), po);
                AssertPublishAck(pa, 4);

                po = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(3)
                    .WithMessageId(MessageId(5))
                    .Build();
                pa = js.Publish(Subject(1), DataBytes(5), po);
                AssertPublishAck(pa, 5);

                po = PublishOptions.Builder()
                    .WithExpectedLastSubjectSequence(4)
                    .WithMessageId(MessageId(6))
                    .Build();
                pa = js.Publish(Subject(2), DataBytes(6), po);
                AssertPublishAck(pa, 6);

                PublishOptions po1 = PublishOptions.Builder().WithExpectedStream(Stream(999)).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(Subject(1), DataBytes(999), po1));

                PublishOptions po2 = PublishOptions.Builder().WithExpectedLastMsgId(MessageId(999)).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(Subject(1), DataBytes(999), po2));

                PublishOptions po3 = PublishOptions.Builder().WithExpectedLastSequence(999).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(Subject(1), DataBytes(999), po3));

                PublishOptions po4 = PublishOptions.Builder().WithExpectedLastSubjectSequence(999).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(Subject(1), DataBytes(999), po4));
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

        private void AssertPublishAck(PublishAck pa, ulong seqno) {
            Assert.Equal(STREAM, pa.Stream);
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
    }
}
