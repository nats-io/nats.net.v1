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

using NATS.Client;
using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestUtils;

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
                c.CreateJetStreamContext();
                // TODO
                // c.CreateJetStreamManagementContext().GetAccountStatistics()
            });
        }

        [Fact]
        public void TestJetStreamNotEnabled()
        {
            Context.RunInServer(c =>
            {
                // TODO
                // AssertThrows<???>(() => c.CreateJetStreamContext().Subscribe(SUBJECT));
                // AssertThrows<???>(() => c.CreateJetStreamManagementContext().GetAccountStatistics());
            });
        }

        [Fact]
        public void TestJetStreamSimplePublish()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);

                IJetStream js = c.CreateJetStreamContext();

                PublishAck pa = js.Publish(SUBJECT, DataBytes());
                Assert.True(pa.HasError == false);
                Assert.True(pa.Seq == 1);
                Assert.Equal(STREAM, pa.Stream);
            });
        }

        [Fact]
        public void TestPublishVarieties() {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);
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

                // Subscription s = js.subscribe(SUBJECT);
                // assertNextMessage(s, data(1));
                // assertNextMessage(s, data(2));
                // assertNextMessage(s, data(3));
                // assertNextMessage(s, data(4));
                // assertNextMessage(s, null); // 5
                // assertNextMessage(s, null); // 6
                // assertNextMessage(s, null); // 7
                // assertNextMessage(s, null); // 8
            });
        }

        // private void assertNextMessage(subscription s, String data) throws InterruptedException {
            // Message m = s.nextMessage(DEFAULT_TIMEOUT);
            // assertNotNull(m);
            // if (data == null) {
                // assertNotNull(m.getData());
                // assertEquals(0, m.getData().length);
            // }
            // else {
                // assertEquals(data, new String(m.getData()));
            // }
        // }

        [Fact]
        public void TestPublishExpectations()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);
                IJetStream js = c.CreateJetStreamContext();

                PublishOptions po = PublishOptions.Builder()
                    .WithExpectedStream(STREAM)
                    .WithMessageId(MessageId(1))
                    .Build();
                PublishAck pa = js.Publish(SUBJECT, DataBytes(1), po);
                AssertPublishAck(pa, 1);

                po = PublishOptions.Builder()
                    .WithExpectedLastMsgId(MessageId(1))
                    .WithMessageId(MessageId(2))
                    .Build();
                pa = js.Publish(SUBJECT, DataBytes(2), po);
                AssertPublishAck(pa, 2);

                po = PublishOptions.Builder()
                    .WithExpectedLastSequence(2)
                    .WithMessageId(MessageId(3))
                    .Build();
                pa = js.Publish(SUBJECT, DataBytes(3), po);
                AssertPublishAck(pa, 3);

                PublishOptions po1 = PublishOptions.Builder().WithExpectedStream(Stream(999)).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(SUBJECT, DataBytes(999), po1));

                PublishOptions po2 = PublishOptions.Builder().WithExpectedLastMsgId(MessageId(999)).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(SUBJECT, DataBytes(999), po2));

                PublishOptions po3 = PublishOptions.Builder().WithExpectedLastSequence(999).Build();
                Assert.Throws<NATSJetStreamException>(() => js.Publish(SUBJECT, DataBytes(999), po3));
            });
        }

        [Fact]
        public void TestPublishMiscExceptions()
        {
            Context.RunInJsServer(c =>
            {
                CreateTestStream(c);
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

        private void AssertPublishAck(PublishAck pa, int seqno) {
            Assert.Equal(STREAM, pa.Stream);
            if (seqno != -1) {
                Assert.Equal(seqno, pa.Seq);
            }
            Assert.False(pa.Duplicate);
        }

    }
}
