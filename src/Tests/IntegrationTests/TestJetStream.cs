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
    public class TestJetStream : TestSuite<JetStreamSuiteContext>
    {
        public TestJetStream(JetStreamSuiteContext context) : base(context) { }

        [Fact]
        public void TestJetStreamCreate()
        {
            Context.RunInJsServer(c => c.CreateJetStreamContext());

            // check for failure.
            Context.RunInServer(c => 
                Assert.Throws<NATSJetStreamException>(() => 
                    c.CreateJetStreamContext()));
        }

        [Fact]
        public void TestJetStreamSimplePublish()
        {
            Context.RunInJsServer(c =>
                {
                    CreateMemoryStream(c, STREAM, SUBJECT);

                    IJetStream js = c.CreateJetStreamContext();

                    PublishAck pa = js.Publish(SUBJECT, DataBytes());
                    Assert.True(pa.HasError == false);
                    Assert.True(pa.Seq == 1);
                    Assert.Equal(STREAM, pa.Stream);
                });
        }
    }
}
