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
    public class TestPublishOptions : TestBase
    {
        [Fact]
        public void TestBuilder()
        {
            PublishOptions.Builder builder = new PublishOptions.Builder();
            PublishOptions po = builder.Build();
            Assert.Equal(PublishOptions.UnsetStream, po.Stream);
            Assert.Equal(PublishOptions.DefaultTimeout, po.StreamTimeout);
            Assert.Equal(PublishOptions.UnsetLastSequence, po.ExpectedLastSeq);

            po = builder
                .Stream(STREAM)
                .StreamTimeout(Duration.OfSeconds(99))
                .ExpectedLastMsgId("1")
                .ExpectedStream("bar")
                .ExpectedLastSequence(42)
                .MessageId("msgId")
                .Build();

            Assert.Equal(STREAM, po.Stream);
            Assert.Equal(Duration.OfSeconds(99), po.StreamTimeout);
            Assert.Equal("1", po.ExpectedLastMsgId);
            Assert.Equal(42, po.ExpectedLastSeq);
            Assert.Equal("bar", po.ExpectedStream);
            Assert.Equal("msgId", po.MessageId);

            po = builder.ClearExpected().Build();
            Assert.Null(po.ExpectedLastMsgId);
            Assert.Equal(PublishOptions.UnsetLastSequence, po.ExpectedLastSeq);
            Assert.Equal("bar", po.ExpectedStream);
            Assert.Null(po.MessageId);

            po = builder.Stream(null).StreamTimeout(null).Build();
            Assert.Equal(PublishOptions.UnsetStream, po.Stream);
            Assert.Equal(PublishOptions.DefaultTimeout, po.StreamTimeout);

            po = builder.Stream(STREAM).Build();
            Assert.Equal(STREAM, po.Stream);

            po = builder.Stream("").Build();
            Assert.Equal(PublishOptions.UnsetStream, po.Stream);
        }
    }
}
