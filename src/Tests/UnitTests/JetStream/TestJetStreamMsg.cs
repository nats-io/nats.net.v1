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

using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestJetStreamMsg : TestBase
    {
        const string TestMetaV0 = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000";
        const string TestMetaV1 = "$JS.ACK.test-stream.test-consumer.1.2.3.1605139610113260000.4";
        const string TestMetaV2 = "$JS.ACK.v2Domain.v2Hash.test-stream.test-consumer.1.2.3.1605139610113260000.4.v2Token";

        [Fact]
        public void MetaDataTests()
        {
            ValidateMeta(false, false, new MetaData(TestMetaV0));
            ValidateMeta(true, false, new MetaData(TestMetaV1));
            ValidateMeta(true, true, new MetaData(TestMetaV2));
        }

        private static void ValidateMeta(bool hasPending, bool hasDomainHashToken, MetaData meta)
        {
            Assert.NotNull(meta);
            Assert.Equal("test-stream", meta.Stream);
            Assert.Equal("test-consumer", meta.Consumer);
            Assert.Equal(1U, meta.NumDelivered);
            Assert.Equal(2U, meta.StreamSequence);
            Assert.Equal(3U, meta.ConsumerSequence);
            Assert.Equal(2020, meta.Timestamp.Year);
            Assert.Equal(6, meta.Timestamp.Minute);
            Assert.Equal(113, meta.Timestamp.Millisecond);
            Assert.Equal(1605139610113260000U, meta.TimestampNanos);
            Assert.Equal(hasPending ? 4U : 0U, meta.NumPending);
            
            if (hasDomainHashToken) {
                Assert.Equal("v2Domain", meta.Domain);
                Assert.Equal("v2Hash", meta.AccountHash);
                Assert.Equal("v2Token", meta.Token);

            }
            else {
                Assert.Null(meta.Domain);
                Assert.Null(meta.AccountHash);
                Assert.Null(meta.Token);
            }
        }
    }
}
