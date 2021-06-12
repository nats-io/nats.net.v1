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

using NATS.Client;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestJetStreamMsg : TestBase
    {
        [Fact]
        public void MetaDataTests()
        {
            string ReplyTo = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";
            IConnection conn = null;
            JetStreamMsg jsm = new JetStreamMsg(conn, "foo", ReplyTo, null);

            Assert.True(jsm.IsJetStream);

            MetaData jsmd = jsm.Metadata;
            Assert.NotNull(jsmd);
            Assert.Equal("test-stream", jsmd.Stream);
            Assert.Equal("test-consumer", jsmd.Consumer);
            Assert.True(1 == jsmd.NumDelivered);
            Assert.True(2 == jsmd.Sequence.StreamSequence);
            Assert.True(3 == jsmd.Sequence.ConsumerSequence);
            Assert.True(2020 == jsmd.Timestamp.Year);
            Assert.True(6 == jsmd.Timestamp.Minute);
            Assert.True(113 == jsmd.Timestamp.Millisecond);
            Assert.True(1605139610113260000 == jsmd.TimestampNanos);

            Assert.Null(jsm.Reply);
            Assert.Equal("foo", jsm.Subject);
        }
    }
}
