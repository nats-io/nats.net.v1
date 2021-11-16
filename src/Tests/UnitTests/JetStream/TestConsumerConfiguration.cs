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

            ConsumerConfiguration original = ConsumerConfiguration.Builder().Build();
            ValidateDefault(original);

            ConsumerConfiguration ccTest = ConsumerConfiguration.Builder(null).Build();
            ValidateDefault(ccTest);

            ccTest = new ConsumerConfiguration.ConsumerConfigurationBuilder(null).Build();
            ValidateDefault(ccTest);

            ccTest = ConsumerConfiguration.Builder(original).Build();
            ValidateDefault(ccTest);        }
        
        private void ValidateDefault(ConsumerConfiguration cc) {
            AssertDefaultCc(cc);
            Assert.False(cc.DeliverPolicyWasSet);
            Assert.False(cc.AckPolicyWasSet);
            Assert.False(cc.ReplayPolicyWasSet);
            Assert.False(cc.StartSeqWasSet);
            Assert.False(cc.MaxDeliverWasSet);
            Assert.False(cc.RateLimitWasSet);
            Assert.False(cc.MaxAckPendingWasSet);
            Assert.False(cc.MaxPullWaitingWasSet);
            Assert.False(cc.FlowControlWasSet);
            Assert.False(cc.HeadersOnlyWasSet);
        }

        [Fact]
        public void TestChanges()
        {
            ConsumerConfiguration original = ConsumerConfiguration.Builder().Build();
            Assert.False(original.WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithDeliverPolicy(DeliverPolicy.All).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithDeliverPolicy(DeliverPolicy.New).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithAckPolicy(AckPolicy.Explicit).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithAckPolicy(AckPolicy.None).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithReplayPolicy(ReplayPolicy.Instant).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithReplayPolicy(ReplayPolicy.Original).Build()
                .WouldBeChangeTo(original));

            ConsumerConfiguration ccTest = ConsumerConfiguration.Builder(original).WithFlowControl(1000).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            ccTest = ConsumerConfiguration.Builder(original).WithIdleHeartbeat(1000).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            ccTest = ConsumerConfiguration.Builder(original).WithStartTime(DateTime.Now).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithHeadersOnly(false).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithHeadersOnly(true).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithStartSequence(CcNumeric.StartSeq.InitialUlong()).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithStartSequence(99).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithMaxDeliver(CcNumeric.MaxDeliver.Initial()).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithMaxDeliver(99).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithRateLimit(CcNumeric.RateLimit.Initial()).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithRateLimit(99).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithMaxAckPending(CcNumeric.MaxAckPending.Initial()).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithMaxAckPending(99).Build()
                .WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithMaxPullWaiting(CcNumeric.MaxPullWaiting.Initial()).Build()
                .WouldBeChangeTo(original));
            Assert.True(ConsumerConfiguration.Builder(original).WithMaxPullWaiting(99).Build()
                .WouldBeChangeTo(original));


            Assert.False(ConsumerConfiguration.Builder(original).WithFilterSubject(string.Empty).Build()
                .WouldBeChangeTo(original));
            ccTest = ConsumerConfiguration.Builder(original).WithFilterSubject(Plain).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithDescription(string.Empty).Build()
                .WouldBeChangeTo(original));
            ccTest = ConsumerConfiguration.Builder(original).WithDescription(Plain).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithSampleFrequency(string.Empty).Build()
                .WouldBeChangeTo(original));
            ccTest = ConsumerConfiguration.Builder(original).WithSampleFrequency(Plain).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithDeliverSubject(string.Empty).Build()
                .WouldBeChangeTo(original));
            ccTest = ConsumerConfiguration.Builder(original).WithDeliverSubject(Plain).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));

            Assert.False(ConsumerConfiguration.Builder(original).WithDeliverGroup(string.Empty).Build()
                .WouldBeChangeTo(original));
            ccTest = ConsumerConfiguration.Builder(original).WithDeliverGroup(Plain).Build();
            Assert.False(ccTest.WouldBeChangeTo(ccTest));
            Assert.True(ccTest.WouldBeChangeTo(original));        
        }
        
        private static void AssertAsBuilt(ConsumerConfiguration c, DateTime dt)
        {
            Assert.Equal(AckPolicy.Explicit, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(99), c.AckWait);
            Assert.Equal(Duration.OfMillis(166), c.IdleHeartbeat);
            Assert.Equal(DeliverPolicy.ByStartSequence, c.DeliverPolicy);
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
            Assert.True(c.FlowControl);
            Assert.True(c.HeadersOnly);
            Assert.True(c.DeliverPolicyWasSet);
            Assert.True(c.AckPolicyWasSet);
            Assert.True(c.ReplayPolicyWasSet);
            Assert.True(c.StartSeqWasSet);
            Assert.True(c.MaxDeliverWasSet);
            Assert.True(c.RateLimitWasSet);
            Assert.True(c.MaxAckPendingWasSet);
            Assert.True(c.MaxPullWaitingWasSet);
            Assert.True(c.FlowControlWasSet);
            Assert.True(c.HeadersOnlyWasSet);        }

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

            Assert.Null(c.AckWait);
            Assert.Null(c.IdleHeartbeat);

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
