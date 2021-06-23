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
using System.Text;
using NATS.Client;
using NATS.Client.JetStream;
using Xunit;

namespace IntegrationTests
{
    public class TestJetStream : TestSuite<ConnectionSuiteContext>
    {
        public TestJetStream(ConnectionSuiteContext context) : base(context) { }

        internal void SetupTestStream(IConnection c, string streamName)
        {
            var jsm = c.CreateJetStreamManagementContext();
            var sc = StreamConfiguration.Builder().WithName(streamName).WithStorageType(StorageType.Memory).WithSubjects("foo").Build();
            jsm.AddStream(sc);
        }

        [Fact]
        public void TestJetStreamCreate()
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                // see if it succeeds.
                IJetStream js = c.CreateJetStreamContext();
            }

            // check for failure.
            using (var s = NATSServer.CreateFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                // see if it fails.
                Assert.Throws<NATSJetStreamException>(() => c.CreateJetStreamContext());
            }
        }

        [Fact]
        public void TestJetStreamSimplePublish()
        {
            using (var s = NATSServer.CreateJetStreamFastAndVerify(Context.Server1.Port))
            {
                using var c = Context.OpenConnection(Context.Server1.Port);
                SetupTestStream(c, "foo");

                IJetStream js = c.CreateJetStreamContext();

                PublishAck pa = js.Publish("foo", Encoding.ASCII.GetBytes("Hello World!"));
                Assert.True(pa.HasError == false);
                Assert.True(pa.Seq == 1);
                Assert.Equal("foo", pa.Stream);
            }
        }
    }
}
