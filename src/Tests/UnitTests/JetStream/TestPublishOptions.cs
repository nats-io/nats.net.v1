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

using System;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestPublishOptions
    {
        [Fact]
        public void TestDefaultBuilder()
        {
            var po = PublishOptions.Builder().Build();
            Assert.Equal(PublishOptions.DefaultLastSequence, po.ExpectedLastSeq);
            Assert.Equal(PublishOptions.DefaultLastSequence, po.ExpectedLastSubjectSeq);
            Assert.Equal(PublishOptions.DefaultStream, po.ExpectedStream);
            Assert.Equal(PublishOptions.DefaultTimeout, po.StreamTimeout);
            Assert.Null(po.ExpectedLastMsgId);
            Assert.Null(po.MessageId);
            Assert.Null(po.Stream);
        }

        [Fact]
        public void TestValidBuilderArgs()
        {
            PublishOptions.PublishOptionsBuilder builder = PublishOptions.Builder()
                .WithExpectedStream("expectedstream")
                .WithExpectedLastMsgId("expectedmsgid")
                .WithExpectedLastSequence(42)
                .WithExpectedLastSubjectSequence(43)
                .WithMessageId("msgid")
                .WithStream("stream")
                .WithTimeout(5150);

            var po = builder.Build();
            Assert.Equal("expectedstream", po.ExpectedStream);
            Assert.Equal("expectedmsgid", po.ExpectedLastMsgId);
            Assert.Equal(42ul, po.ExpectedLastSeq);
            Assert.Equal(43ul, po.ExpectedLastSubjectSeq);
            Assert.Equal("msgid", po.MessageId);
            Assert.Equal("stream", po.Stream);
            Assert.Equal(5150, po.StreamTimeout.Millis);

            po = builder.ClearExpected().Build();
            Assert.Null(po.ExpectedLastMsgId);
            Assert.Equal(PublishOptions.DefaultLastSequence, po.ExpectedLastSeq);
            Assert.Equal(PublishOptions.DefaultLastSequence, po.ExpectedLastSubjectSeq);
            Assert.Null(po.MessageId);
            Assert.Equal("stream", po.Stream);
            
            po = PublishOptions.Builder().
                WithTimeout(Duration.OfMillis(5150)).
                Build();

            Assert.Equal(Duration.OfMillis(5150), po.StreamTimeout);

            // check to allow -
            PublishOptions.Builder().
                WithExpectedStream("test-stream").
                Build();
        }

        [Fact]
        public void TestInvalidBuilderArgs()
        {
            Assert.Throws<ArgumentException>(() => PublishOptions.Builder().
                WithMessageId("").
                Build());

            Assert.Throws<ArgumentException>(() => PublishOptions.Builder().
                WithStream("stream.*").
                Build());

            Assert.Throws<ArgumentException>(() => PublishOptions.Builder().
                WithStream("stream.>").
                Build());

            Assert.Throws<ArgumentException>(() => PublishOptions.Builder().
                WithStream("stream.one").
                Build());
        }
    }
}
