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
using static NATS.Client.JetStream.CcChangeHelper;

namespace UnitTests.JetStream
{
    public class TestConsumerConfiguration : TestBase
    {
        [Fact]
        public void BuilderWorks()
        {
            AssertDefaultCc(ConsumerConfiguration.Builder().Build());

            DateTime dt = DateTime.UtcNow;

            ConsumerConfiguration c = ConsumerConfiguration.Builder()
                .WithAckPolicy(AckPolicy.Explicit)
                .WithAckWait(Duration.OfSeconds(99))
                .WithDeliverPolicy(DeliverPolicy.ByStartSequence)
                .WithDescription("blah")
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
                .WithFlowControl(Duration.OfMillis(166))
                .WithMaxPullWaiting(73)
                .WithMaxBatch(55)
                .WithMaxExpires(177)
                .WithInactiveThreshold(188)
                .WithHeadersOnly(true)
                .Build();

            AssertAsBuilt(c, dt);

            ConsumerCreateRequest ccr = new ConsumerCreateRequest("stream", c);
            Assert.Equal("stream", ccr.StreamName);
            Assert.NotNull(ccr.Config);

            JSONNode node = ccr.ToJsonNode();
            c = new ConsumerConfiguration(node[ApiConstants.Config]);
            AssertAsBuilt(c, dt);

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
        }

        private static void AssertAsBuilt(ConsumerConfiguration c, DateTime dt)
        {
            Assert.Equal(AckPolicy.Explicit, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(99), c.AckWait);
            Assert.Equal(Duration.OfMillis(166), c.IdleHeartbeat);
            Assert.Equal(Duration.OfMillis(177), c.MaxExpires);
            Assert.Equal(Duration.OfMillis(188), c.InactiveThreshold);
            Assert.Equal(DeliverPolicy.ByStartSequence, c.DeliverPolicy);
            Assert.Equal("10s", c.SampleFrequency);
            Assert.Equal("deliver", c.DeliverSubject);
            Assert.Equal("blah", c.Description);
            Assert.Equal("durable", c.Durable);
            Assert.Equal("fs", c.FilterSubject);
            Assert.Equal(5555, c.MaxDeliver);
            Assert.Equal(6666, c.MaxAckPending);
            Assert.Equal(4242, c.RateLimit);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
            Assert.Equal(2001ul, c.StartSeq);
            Assert.Equal(dt, c.StartTime);
            Assert.Equal(73, c.MaxPullWaiting);
            Assert.Equal(55, c.MaxBatch);
            Assert.True(c.FlowControl);
            Assert.True(c.HeadersOnly);
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
            Assert.Equal(Duration.OfSeconds(40), c.MaxExpires);
            Assert.Equal(Duration.OfSeconds(50), c.InactiveThreshold);
            Assert.Equal(55, c.MaxBatch);

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

            Assert.Null(c.AckWait);
            Assert.Null(c.IdleHeartbeat);

            Assert.Equal(DateTime.MinValue, c.StartTime);

            Assert.False(c.FlowControl);
            Assert.False(c.HeadersOnly);

            Assert.Equal(StartSeq.InitialUlong, c.StartSeq);
            Assert.Equal(MaxDeliver.Initial, c.MaxDeliver);
            Assert.Equal(RateLimit.Initial, c.RateLimit);
            Assert.Equal(MaxAckPending.Initial, c.MaxAckPending);
            Assert.Equal(MaxPullWaiting.Initial, c.MaxPullWaiting);
            Assert.Equal(MaxBatch.Initial, c.MaxBatch);
        }

        [Fact]
        public void ChangeHelperWorks()
        {
            // value
            Assert.False(StartSeq.WouldBeChange(2L, 2L));
            Assert.False(MaxDeliver.WouldBeChange(2L, 2L));
            Assert.False(RateLimit.WouldBeChange(2L, 2L));
            Assert.False(MaxAckPending.WouldBeChange(2L, 2L));
            Assert.False(MaxPullWaiting.WouldBeChange(2L, 2L));
            Assert.False(MaxBatch.WouldBeChange(2L, 2L));
            Assert.False(AckWait.WouldBeChange(Duration.OfSeconds(2), Duration.OfSeconds(2)));

            // null
            Assert.False(StartSeq.WouldBeChange(null, 2L));
            Assert.False(MaxDeliver.WouldBeChange(null, 2L));
            Assert.False(RateLimit.WouldBeChange(null, 2L));
            Assert.False(MaxAckPending.WouldBeChange(null, 2L));
            Assert.False(MaxPullWaiting.WouldBeChange(null, 2L));
            Assert.False(MaxBatch.WouldBeChange(null, 2L));
            Assert.False(AckWait.WouldBeChange(null, Duration.OfSeconds(2)));

            // < min vs initial
            Assert.False(StartSeq.WouldBeChange(-99L, StartSeq.Initial));
            Assert.False(MaxDeliver.WouldBeChange(-99L, MaxDeliver.Initial));
            Assert.False(RateLimit.WouldBeChange(-99L, RateLimit.Initial));
            Assert.False(MaxAckPending.WouldBeChange(-99L, MaxAckPending.Initial));
            Assert.False(MaxPullWaiting.WouldBeChange(-99L, MaxPullWaiting.Initial));
            Assert.False(MaxBatch.WouldBeChange(-99L, MaxBatch.Initial));
            Assert.False(AckWait.WouldBeChange(Duration.OfSeconds(-99), Duration.OfNanos(AckWait.Initial)));

            // server vs initial
            Assert.False(StartSeq.WouldBeChange(StartSeq.Server, StartSeq.Initial));
            Assert.False(MaxDeliver.WouldBeChange(MaxDeliver.Server, MaxDeliver.Initial));
            Assert.False(RateLimit.WouldBeChange(RateLimit.Server, RateLimit.Initial));
            Assert.False(MaxAckPending.WouldBeChange(MaxAckPending.Server, MaxAckPending.Initial));
            Assert.False(MaxPullWaiting.WouldBeChange(MaxPullWaiting.Server, MaxPullWaiting.Initial));
            Assert.False(MaxBatch.WouldBeChange(MaxBatch.Server, MaxBatch.Initial));
            Assert.False(AckWait.WouldBeChange(Duration.OfNanos(AckWait.Server), Duration.OfNanos(AckWait.Initial)));

            Assert.True(StartSeq.WouldBeChange(1L, 2L));
            Assert.True(MaxDeliver.WouldBeChange(1L, 2L));
            Assert.True(RateLimit.WouldBeChange(1L, 2L));
            Assert.True(MaxAckPending.WouldBeChange(1L, 2L));
            Assert.True(MaxPullWaiting.WouldBeChange(1L, 2L));
            Assert.True(MaxBatch.WouldBeChange(1L, 2L));
            Assert.True(AckWait.WouldBeChange(Duration.OfSeconds(1), Duration.OfSeconds(2)));
        }
    }
}