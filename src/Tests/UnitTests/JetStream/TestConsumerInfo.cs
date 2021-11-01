// Copyright 2020 The NATS Authors
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

using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestConsumerInfo : TestBase
    {
        [Fact]
        public void JsonIsReadProperly()
        {
            string json = ReadDataFile("ConsumerInfo.json");
            ConsumerInfo ci = new ConsumerInfo(json, false);
            Assert.Equal("foo-stream", ci.Stream);
            Assert.Equal("foo-consumer", ci.Name);

            Assert.Equal(1u, ci.Delivered.ConsumerSeq);
            Assert.Equal(2u, ci.Delivered.StreamSeq);
            Assert.Equal(3u, ci.AckFloor.ConsumerSeq);
            Assert.Equal(4u, ci.AckFloor.StreamSeq);

            Assert.Equal(24u, ci.NumPending);
            Assert.Equal(42, ci.NumAckPending);
            Assert.Equal(42, ci.NumRedelivered);

            ConsumerConfiguration c = ci.ConsumerConfiguration;
            Assert.Equal("foo-consumer", c.Durable);
            Assert.Equal("bar", c.DeliverSubject);
            Assert.Equal(DeliverPolicy.All, c.DeliverPolicy);
            Assert.Equal(AckPolicy.All, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(30), c.AckWait);
            Assert.Equal(10, c.MaxDeliver);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
        }
    }
}
