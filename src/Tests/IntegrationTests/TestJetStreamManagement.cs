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

using NATS.Client.JetStream;
using Xunit;
using static UnitTests.TestBase;

namespace IntegrationTests
{
    public class TestJetStreamManagement : TestSuite<JetStreamManagementSuiteContext>
    {
        public TestJetStreamManagement(JetStreamManagementSuiteContext context) : base(context) { }

        [Fact]
        public void TestBasicStreamManagement()
        {
            Context.RunInJsServer(c =>
                {
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                    var sc = StreamConfiguration.Builder()
                        .WithName(STREAM)
                        .WithStorageType(StorageType.Memory)
                        .WithSubjects(Subject(1)).Build();

                    StreamInfo si = jsm.AddStream(sc);
                    Assert.Equal(STREAM, si.Config.Name);
                    Assert.Equal(Subject(1), si.Config.Subjects[0]);
                    Assert.Equal(0, si.State.Messages);

                    sc = StreamConfiguration.Builder()
                        .WithName(STREAM)
                        .WithStorageType(StorageType.Memory)
                        .WithSubjects(Subject(1), Subject(2))
                        .Build();
                    si = jsm.UpdateStream(sc);
                    Assert.Contains(Subject(1), si.Config.Subjects);
                    Assert.Contains(Subject(2), si.Config.Subjects);

                    si = jsm.GetStreamInfo(STREAM);
                    Assert.Contains(Subject(1), si.Config.Subjects);
                    Assert.Contains(Subject(2), si.Config.Subjects);

                    Assert.True(jsm.DeleteStream(STREAM));
                    Assert.False(jsm.DeleteStream(Stream(999)));

                    Assert.Throws<NATSJetStreamException>(() => jsm.GetStreamInfo(STREAM));
                    Assert.Throws<NATSJetStreamException>(() => jsm.UpdateStream(sc));

                });
        }

        [Fact]
        public void TestBasicConsumerManagement()
        {
            Context.RunInJsServer(c =>
                {
                    IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                    jsm.AddStream(StreamConfiguration.Builder().
                        WithName(STREAM).
                        WithStorageType(StorageType.Memory).
                        WithSubjects(SUBJECT).
                        Build());

                    ConsumerConfiguration cc = ConsumerConfiguration.Builder().
                        WithDeliverSubject(DELIVER).
                        WithDurable(DURABLE).
                        Build();

                    ConsumerInfo ci = jsm.AddConsumer(STREAM, cc);
                    Assert.Equal(DURABLE, ci.Configuration.Durable);
                    Assert.Equal(DELIVER, ci.Configuration.DeliverSubject);

                    ci = jsm.GetConsumerInfo(STREAM, DURABLE);
                    Assert.Equal(DURABLE, ci.Configuration.Durable);
                    Assert.Equal(DELIVER, ci.Configuration.DeliverSubject);

                    Assert.True(jsm.DeleteConsumer(STREAM, DURABLE));
                    Assert.False(jsm.DeleteConsumer(STREAM, DURABLE));
                    Assert.False(jsm.DeleteConsumer(Stream(999), Durable(999)));
                    Assert.Throws<NATSJetStreamException>(() => jsm.GetConsumerInfo(STREAM, DURABLE));
                });
        }
    }
}
