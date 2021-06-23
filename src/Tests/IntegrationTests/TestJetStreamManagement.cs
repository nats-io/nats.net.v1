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

namespace IntegrationTests
{
    public class TestJetStreamManagement : TestSuite<ConnectionSuiteContext>
    {
        public TestJetStreamManagement(ConnectionSuiteContext context) : base(context) { }

        [Fact]
        public void TestBasicStreamManagement()
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();
                var sc = StreamConfiguration.Builder().WithName("foo").WithStorageType(StorageType.Memory).WithSubjects("foo").Build();

                StreamInfo si = jsm.AddStream(sc);
                Assert.Equal("foo", si.Config.Name);
                Assert.Equal("foo", si.Config.Subjects[0]);
                Assert.Equal(0, si.State.Messages);

                sc = StreamConfiguration.Builder().WithName("foo").WithStorageType(StorageType.Memory).WithSubjects("foo", "bar").Build();
                si = jsm.UpdateStream(sc);
                Assert.Contains("foo", si.Config.Subjects);
                Assert.Contains("bar", si.Config.Subjects);

                si = jsm.GetStreamInfo("foo");
                Assert.Contains("foo", si.Config.Subjects);
                Assert.Contains("bar", si.Config.Subjects);

                Assert.True(jsm.DeleteStream("foo"));
                Assert.False(jsm.DeleteStream("garbage"));

                Assert.Throws<NATSJetStreamException>(() => jsm.GetStreamInfo("foo"));
                Assert.Throws<NATSJetStreamException>(() => jsm.UpdateStream(sc));
            }

            // check for failure.
            using (var s = NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                // see if it fails.
                Assert.Throws<NATSJetStreamException>(() => c.CreateJetStreamManagementContext());
            }
        }

        [Fact]
        public void TestBasicConsumerManagement()
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                IJetStreamManagement jsm = c.CreateJetStreamManagementContext();

                jsm.AddStream(StreamConfiguration.Builder().
                    WithName("foo").
                    WithStorageType(StorageType.Memory).
                    WithSubjects("foo").
                    Build());

                ConsumerConfiguration cc = ConsumerConfiguration.Builder().
                    WithDeliverSubject("bar").
                    WithDurable("dur").
                    Build();

                ConsumerInfo ci = jsm.AddConsumer("foo", cc);
                Assert.Equal("dur", ci.Configuration.Durable);
                Assert.Equal("bar", ci.Configuration.DeliverSubject);

                ci = jsm.GetConsumerInfo("foo", "dur");
                Assert.Equal("dur", ci.Configuration.Durable);
                Assert.Equal("bar", ci.Configuration.DeliverSubject);

                Assert.True(jsm.DeleteConsumer("foo", "dur"));
                Assert.False(jsm.DeleteConsumer("foo", "dur"));
                Assert.False(jsm.DeleteConsumer("garbage", "garbage"));
                Assert.Throws<NATSJetStreamException>(() => jsm.GetConsumerInfo("foo", "dur"));
            }

            // check for failure.
            using (var s = NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                // see if it fails.
                Assert.Throws<NATSJetStreamException>(() => c.CreateJetStreamManagementContext());
            }
        }
    }
}
