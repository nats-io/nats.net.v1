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
using static NATS.Client.JetStream.LongChangeHelper;
using static NATS.Client.JetStream.UlongChangeHelper;
using static NATS.Client.JetStream.DurationChangeHelper;

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
                .WithRateLimitBps(4242U)
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
                .WithBackoff(1000, 2000, 3000)
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
            Assert.Equal(4242U, c.RateLimitBps);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
            Assert.Equal(2001ul, c.StartSeq);
            Assert.Equal(dt, c.StartTime);
            Assert.Equal(73, c.MaxPullWaiting);
            Assert.Equal(55, c.MaxBatch);
            Assert.True(c.FlowControl);
            Assert.True(c.HeadersOnly);
            Assert.Equal(3, c.Backoff.Count);
            Assert.Equal(Duration.OfSeconds(1), c.Backoff[0]);
            Assert.Equal(Duration.OfSeconds(2), c.Backoff[1]);
            Assert.Equal(Duration.OfSeconds(3), c.Backoff[2]);
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
            Assert.Equal(73U, c.RateLimitBps);
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
            Assert.Equal(3, c.Backoff.Count);
            Assert.Equal(Duration.OfSeconds(1), c.Backoff[0]);
            Assert.Equal(Duration.OfSeconds(2), c.Backoff[1]);
            Assert.Equal(Duration.OfSeconds(3), c.Backoff[2]);

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

            Assert.Equal(-1, c.MaxDeliver);
            Assert.Equal(-1, c.MaxAckPending);
            Assert.Equal(-1, c.MaxPullWaiting);
            Assert.Equal(-1, c.MaxBatch);
            Assert.Equal(0U, c.StartSeq);
            Assert.Equal(0U, c.RateLimitBps);
            Assert.Equal(0, c.Backoff.Count);
        }

        [Fact]
        public void ChangeHelperWorks()
        {
            void AssertLongChangeHelper(LongChangeHelper h)
            {
                Assert.False(h.WouldBeChange(h.Min, h.Min)); // has value vs server has same value
                Assert.True(h.WouldBeChange(h.Min, h.Min + 1)); // has value vs server has different value

                Assert.False(h.WouldBeChange(null, h.Min)); // value not set vs server has value
                Assert.False(h.WouldBeChange(null, h.Unset)); // value not set vs server has unset value

                Assert.True(h.WouldBeChange(h.Min, null)); // has value vs server not set
                Assert.False(h.WouldBeChange(h.Unset, null)); // has unset value versus server not set
            }
            
            void AssertUlongChangeHelper(UlongChangeHelper h)
            {
                Assert.False(h.WouldBeChange(h.Min, h.Min)); // has value vs server has same value
                Assert.True(h.WouldBeChange(h.Min, h.Min + 1)); // has value vs server has different value

                Assert.False(h.WouldBeChange(null, h.Min)); // value not set vs server has value
                Assert.False(h.WouldBeChange(null, h.Unset)); // value not set vs server has unset value

                Assert.True(h.WouldBeChange(h.Min, null)); // has value vs server not set
                Assert.False(h.WouldBeChange(h.Unset, null)); // has unset value versus server not set
            }
            
            void AssertDurationChangeHelper(DurationChangeHelper h)
            {
                Assert.False(h.WouldBeChange(h.Min, h.Min)); // has value vs server has same value
                Assert.True(h.WouldBeChange(h.Min, Duration.OfNanos(h.MinNanos + 1))); // has value vs server has different value

                Assert.False(h.WouldBeChange(null, h.Min)); // value not set vs server has value
                Assert.False(h.WouldBeChange(null, h.Unset)); // value not set vs server has unset value

                Assert.True(h.WouldBeChange(h.Min, null)); // has value vs server not set
                Assert.False(h.WouldBeChange(h.Unset, null)); // has unset value versus server not set
            }

            AssertLongChangeHelper(MaxDeliver);
            AssertLongChangeHelper(MaxAckPending);
            AssertLongChangeHelper(MaxPullWaiting);
            AssertLongChangeHelper(MaxBatch);
            
            AssertUlongChangeHelper(StartSeq);
            AssertUlongChangeHelper(RateLimit);
            
            AssertDurationChangeHelper(AckWait);
        }
    }
}