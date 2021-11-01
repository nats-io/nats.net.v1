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
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestConsumerConfiguration : TestBase
    {
        [Fact]
        public void BuilderWorks()
        {
            DateTime dt = DateTime.UtcNow;

            ConsumerConfiguration c = ConsumerConfiguration.Builder()
                .WithAckPolicy(AckPolicy.Explicit)
                .WithAckWait(Duration.OfSeconds(99))
                .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                .WithDurable("durable")
                .WithFilterSubject("fs")
                .WithMaxDeliver(5555)
                .WithMaxAckPending(6666)
                .WithRateLimit(4242)
                .WithReplayPolicy(ReplayPolicy.Original)
                .WithSampleFrequency("10s")
                .WithStartSequence(2001)
                .WithStartTime(dt)
                .WithDeliverSubject("deliver")
                .WithFlowControl(Duration.OfMillis(66))
                .WithHeadersOnly(true)
                .Build();

            Assert.Equal(AckPolicy.Explicit, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(99), c.AckWait);
            Assert.Equal(Duration.OfMillis(66), c.IdleHeartbeat);
            Assert.Equal(DeliverPolicy.ByStartSequence, c.DeliverPolicy);
            Assert.Equal("deliver", c.DeliverSubject);
            Assert.Equal("durable", c.Durable);
            Assert.Equal("fs", c.FilterSubject);
            Assert.Equal(5555, c.MaxDeliver);
            Assert.Equal(6666, c.MaxAckPending);
            Assert.Equal(4242, c.RateLimit);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
            Assert.Equal(2001ul, c.StartSeq);
            Assert.Equal(dt, c.StartTime);
            Assert.True(c.FlowControl);
            Assert.True(c.HeadersOnly);

            ConsumerCreateRequest ccr = new ConsumerCreateRequest("stream", c);
            Assert.Equal("stream", ccr.StreamName);
            Assert.NotNull(ccr.Config);

            JSONNode node = ccr.ToJsonNode();
            c = new ConsumerConfiguration(node[ApiConstants.Config]);
            Assert.Equal(AckPolicy.Explicit, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(99), c.AckWait);
            Assert.Equal(Duration.OfMillis(66), c.IdleHeartbeat);
            Assert.Equal(DeliverPolicy.ByStartSequence, c.DeliverPolicy);
            Assert.Equal("deliver", c.DeliverSubject);
            Assert.Equal("durable", c.Durable);
            Assert.Equal("fs", c.FilterSubject);
            Assert.Equal(5555, c.MaxDeliver);
            Assert.Equal(4242, c.RateLimit);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
            Assert.Equal(2001ul, c.StartSeq);
            Assert.Equal(dt, c.StartTime);
            Assert.True(c.FlowControl);

            // flow control idle heartbeat combo
            c = ConsumerConfiguration.Builder()
                .WithFlowControl(Duration.OfMillis(501)).Build();
            Assert.True(c.FlowControl);
            Assert.Equal(501, c.IdleHeartbeat.Millis);

            c = ConsumerConfiguration.Builder()
                .WithFlowControl(502).Build();
            Assert.True(c.FlowControl);
            Assert.Equal(502, c.IdleHeartbeat.Millis);

            // millis instead of duration coverage
            // supply null as deliverPolicy, ackPolicy , replayPolicy,
            c = ConsumerConfiguration.Builder()
                .WithDeliverPolicy(null)
                .WithAckPolicy(null)
                .WithReplayPolicy(null)
                .WithAckWait(9000) // millis
                .WithIdleHeartbeat(6000) // millis
                .Build();

            Assert.Equal(AckPolicy.Explicit, c.AckPolicy);
            Assert.Equal(DeliverPolicy.All, c.DeliverPolicy);
            Assert.Equal(ReplayPolicy.Instant, c.ReplayPolicy);
            Assert.Equal(Duration.OfSeconds(9), c.AckWait);
            Assert.Equal(Duration.OfSeconds(6), c.IdleHeartbeat);

            AssertDefaultCc(ConsumerConfiguration.Builder().Build());
        }

        [Fact]
        public void ParsingWorks()
        {
            string json = ReadDataFile("ConsumerConfiguration.json");
            ConsumerConfiguration c = new ConsumerConfiguration(json);
            Assert.Equal("foo-desc", c.Description);
            Assert.Equal(DeliverPolicy.All, c.DeliverPolicy);
            Assert.Equal(AckPolicy.All, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(30), c.AckWait);
            Assert.Equal(Duration.OfSeconds(20), c.IdleHeartbeat);
            Assert.Equal(10, c.MaxDeliver);
            Assert.Equal(73, c.RateLimit);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
            Assert.Equal(2020, c.StartTime.Year);
            Assert.Equal(21, c.StartTime.Second);
            Assert.Equal("foo-durable", c.Durable);
            Assert.Equal("grp", c.DeliverGroup);
            Assert.Equal("bar", c.DeliverSubject);
            Assert.Equal("foo-filter", c.FilterSubject);
            Assert.Equal(42, c.MaxAckPending);
            Assert.Equal("sample_freq-value", c.SampleFrequency);
            Assert.True(c.FlowControl);
            Assert.Equal(128, c.MaxPullWaiting);
            Assert.True(c.HeadersOnly);
            Assert.Equal(99U, c.StartSeq);

            AssertDefaultCc(new ConsumerConfiguration("{}"));
        }

        private static void AssertDefaultCc(ConsumerConfiguration c)
        {
            Assert.Equal(DeliverPolicy.All, c.DeliverPolicy);
            Assert.Equal(AckPolicy.Explicit, c.AckPolicy);
            Assert.Equal(ReplayPolicy.Instant, c.ReplayPolicy);
            Assert.True(string.IsNullOrWhiteSpace(c.Durable));
            Assert.True(string.IsNullOrWhiteSpace(c.DeliverGroup));
            Assert.True(string.IsNullOrWhiteSpace(c.DeliverSubject));
            Assert.True(string.IsNullOrWhiteSpace(c.FilterSubject));
            Assert.True(string.IsNullOrWhiteSpace(c.Description));
            Assert.True(string.IsNullOrWhiteSpace(c.SampleFrequency));

            Assert.Equal(ConsumerConfiguration.DefaultAckWait, c.AckWait);
            Assert.Equal(ConsumerConfiguration.MinDefaultIdleHeartbeat, c.IdleHeartbeat);

            Assert.Equal(DateTime.MinValue, c.StartTime);

            Assert.False(c.FlowControl);
            Assert.False(c.HeadersOnly);

            Assert.Equal(CcNumeric.StartSeq.InitialUlong(), c.StartSeq);
            Assert.Equal(CcNumeric.MaxDeliver.Initial(), c.MaxDeliver);
            Assert.Equal(CcNumeric.RateLimit.Initial(), c.RateLimit);
            Assert.Equal(CcNumeric.MaxAckPending.Initial(), c.MaxAckPending);
            Assert.Equal(CcNumeric.MaxPullWaiting.Initial(), c.MaxPullWaiting);
        }
    }
}
