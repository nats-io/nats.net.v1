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
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

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
                CreateTestStream(c); // tries management functions
                c.CreateJetStreamManagementContext().GetAccountStatistics(); // another management
                c.CreateJetStreamContext().Publish(SUBJECT, DataBytes(1));
            });
        }

        [Fact]
        public void TestJetStreamNotEnabled()
        {
            Context.RunInServer(c =>
            {
                // TODO
                // Assert.Throws<NATSNoRespondersException>(() => c.CreateJetStreamContext().Subscribe(SUBJECT));

                Assert.Throws<NATSNoRespondersException>(() => 
                    c.CreateJetStreamManagementContext().GetAccountStatistics());
            });
        }
    }
}
