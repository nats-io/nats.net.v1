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
using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using Xunit;
using static NATS.Client.JetStream.ConsumerConfiguration;

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
                .WithDurable(NAME)
                .WithName(NAME)
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
                .WithMaxBytes(56)
                .WithMaxExpires(177)
                .WithInactiveThreshold(188)
                .WithHeadersOnly(true)
                .WithBackoff(1000, 2000, 3000)
                .WithNumReplicas(5)
                .WithMemStorage(true)
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
            Assert.Equal(NAME, c.Durable);
            Assert.Equal(NAME, c.Durable);
            Assert.Equal("fs", c.FilterSubject);
            Assert.Equal(5555, c.MaxDeliver);
            Assert.Equal(6666, c.MaxAckPending);
            Assert.Equal(4242U, c.RateLimitBps);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);
            Assert.Equal(2001ul, c.StartSeq);
            Assert.Equal(dt, c.StartTime);
            Assert.Equal(73, c.MaxPullWaiting);
            Assert.Equal(55, c.MaxBatch);
            Assert.Equal(56, c.MaxBytes);
            Assert.Equal(5, c.NumReplicas);
            Assert.True(c.FlowControl);
            Assert.True(c.HeadersOnly);
            Assert.True(c.MemStorage);
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
            Assert.Equal("foo-name", c.Durable);
            Assert.Equal("foo-name", c.Name);
            Assert.Equal("grp", c.DeliverGroup);
            Assert.Equal("bar", c.DeliverSubject);
            Assert.Equal("foo-filter", c.FilterSubject);
            Assert.Equal(42, c.MaxAckPending);
            Assert.Equal("sample_freq-value", c.SampleFrequency);
            Assert.True(c.FlowControl);
            Assert.True(c.HeadersOnly);
            Assert.True(c.MemStorage);
            Assert.Equal(128, c.MaxPullWaiting);
            Assert.Equal(99U, c.StartSeq);
            Assert.Equal(Duration.OfSeconds(40), c.MaxExpires);
            Assert.Equal(Duration.OfSeconds(50), c.InactiveThreshold);
            Assert.Equal(55, c.MaxBatch);
            Assert.Equal(56, c.MaxBytes);
            Assert.Equal(5, c.NumReplicas);
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
            Assert.False(c.MemStorage);

            Assert.Equal(-1, c.MaxDeliver);
            Assert.Equal(-1, c.MaxAckPending);
            Assert.Equal(-1, c.MaxPullWaiting);
            Assert.Equal(-1, c.MaxBatch);
            Assert.Equal(-1, c.MaxBytes);
            Assert.Equal(-1, c.NumReplicas);
            Assert.Equal(0U, c.StartSeq);
            Assert.Equal(0U, c.RateLimitBps);
            Assert.Equal(0, c.Backoff.Count);
        }

        private void AssertNotChange(ConsumerConfiguration original, ConsumerConfiguration server) {
            Assert.Equal(0, original.GetChanges(server).Count);
        }

        private void AssertChange(ConsumerConfiguration original, ConsumerConfiguration server, params string[] changeFields) {
            IList<string> changes = original.GetChanges(server);
            Assert.Equal(changeFields.Length, changes.Count);
            foreach (string ch in changeFields)
            {
                Assert.Contains(ch, changes);
            }
        }

        private ConsumerConfigurationBuilder Builder(ConsumerConfiguration orig) {
            return ConsumerConfiguration.Builder(orig);
        }

        [Fact]
        public void ChangeFieldsIdentified()
        {
            ConsumerConfiguration orig = ConsumerConfiguration.Builder()
                .WithAckWait(DurationUnsetLong)
                .WithIdleHeartbeat(DurationUnsetLong)
                .WithMaxExpires(DurationUnsetLong)
                .WithInactiveThreshold(DurationUnsetLong)
                .Build();
            AssertNotChange(orig, orig);

            AssertNotChange(Builder(orig).WithDeliverPolicy(DeliverPolicy.All).Build(), orig);
            AssertChange(Builder(orig).WithDeliverPolicy(DeliverPolicy.New).Build(), orig, "DeliverPolicy");

            AssertNotChange(Builder(orig).WithAckPolicy(AckPolicy.Explicit).Build(), orig);
            AssertChange(Builder(orig).WithAckPolicy(AckPolicy.None).Build(), orig, "AckPolicy");

            AssertNotChange(Builder(orig).WithReplayPolicy(ReplayPolicy.Instant).Build(), orig);
            AssertChange(Builder(orig).WithReplayPolicy(ReplayPolicy.Original).Build(), orig, "ReplayPolicy");

            AssertNotChange(Builder(orig).WithAckWait(DurationUnsetLong).Build(), orig);
            AssertNotChange(Builder(orig).WithAckWait(null).Build(), orig);
            AssertChange(Builder(orig).WithAckWait(DurationMinLong).Build(), orig, "AckWait");

            AssertNotChange(Builder(orig).WithIdleHeartbeat(DurationUnsetLong).Build(), orig);
            AssertNotChange(Builder(orig).WithIdleHeartbeat(null).Build(), orig);
            AssertChange(Builder(orig).WithIdleHeartbeat(MinIdleHeartbeat).Build(), orig, "IdleHeartbeat");

            AssertNotChange(Builder(orig).WithMaxExpires(DurationUnsetLong).Build(), orig);
            AssertNotChange(Builder(orig).WithMaxExpires(null).Build(), orig);
            AssertChange(Builder(orig).WithMaxExpires(DurationMinLong).Build(), orig, "MaxExpires");

            AssertNotChange(Builder(orig).WithInactiveThreshold(DurationUnsetLong).Build(), orig);
            AssertNotChange(Builder(orig).WithInactiveThreshold(null).Build(), orig);
            AssertChange(Builder(orig).WithInactiveThreshold(DurationMinLong).Build(), orig, "InactiveThreshold");

            ConsumerConfiguration ccTest = Builder(orig).WithFlowControl(1000).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "FlowControl", "IdleHeartbeat");

            ccTest = Builder(orig).WithStartTime(DateTime.UtcNow).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "StartTime");

            AssertNotChange(Builder(orig).WithHeadersOnly(false).Build(), orig);
            AssertChange(Builder(orig).WithHeadersOnly(true).Build(), orig, "HeadersOnly");

            AssertNotChange(Builder(orig).WithMemStorage(false).Build(), orig);
            AssertChange(Builder(orig).WithMemStorage(true).Build(), orig, "MemStorage");

            AssertNotChange(Builder(orig).WithStartSequence(0U).Build(), orig);
            AssertNotChange(Builder(orig).WithStartSequence(null).Build(), orig);
            AssertChange(Builder(orig).WithStartSequence(1).Build(), orig, "StartSequence");

            AssertNotChange(Builder(orig).WithMaxDeliver(IntUnset).Build(), orig);
            AssertNotChange(Builder(orig).WithMaxDeliver(null).Build(), orig);
            AssertChange(Builder(orig).WithMaxDeliver(MaxDeliverMin).Build(), orig, "MaxDeliver");

            AssertNotChange(Builder(orig).WithRateLimitBps(0U).Build(), orig);
            AssertNotChange(Builder(orig).WithRateLimitBps(null).Build(), orig);
            AssertChange(Builder(orig).WithRateLimitBps(1U).Build(), orig, "RateLimitBps");

            AssertNotChange(Builder(orig).WithMaxAckPending(-1).Build(), orig);
            AssertNotChange(Builder(orig).WithMaxAckPending(null).Build(), orig);
            AssertChange(Builder(orig).WithMaxAckPending(1).Build(), orig, "MaxAckPending");

            AssertNotChange(Builder(orig).WithMaxPullWaiting(IntUnset).Build(), orig);
            AssertNotChange(Builder(orig).WithMaxPullWaiting(null).Build(), orig);
            AssertChange(Builder(orig).WithMaxPullWaiting(1).Build(), orig, "MaxPullWaiting");

            AssertNotChange(Builder(orig).WithMaxBatch(IntUnset).Build(), orig);
            AssertNotChange(Builder(orig).WithMaxBatch(null).Build(), orig);
            AssertChange(Builder(orig).WithMaxBatch(1).Build(), orig, "MaxBatch");

            AssertNotChange(Builder(orig).WithNumReplicas(null).Build(), orig);
            AssertChange(Builder(orig).WithNumReplicas(1).Build(), orig, "NumReplicas");

            AssertNotChange(Builder(orig).WithMaxBytes(-1).Build(), orig);
            AssertNotChange(Builder(orig).WithMaxBytes(null).Build(), orig);
            AssertChange(Builder(orig).WithMaxBytes(1).Build(), orig, "MaxBytes");

            AssertNotChange(Builder(orig).WithFilterSubject("").Build(), orig);
            ccTest = Builder(orig).WithFilterSubject(Plain).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "FilterSubject");

            AssertNotChange(Builder(orig).WithDescription("").Build(), orig);
            ccTest = Builder(orig).WithDescription(Plain).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "Description");

            AssertNotChange(Builder(orig).WithSampleFrequency("").Build(), orig);
            ccTest = Builder(orig).WithSampleFrequency(Plain).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "SampleFrequency");

            AssertNotChange(Builder(orig).WithDeliverSubject("").Build(), orig);
            ccTest = Builder(orig).WithDeliverSubject(Plain).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "DeliverSubject");

            AssertNotChange(Builder(orig).WithDeliverGroup("").Build(), orig);
            ccTest = Builder(orig).WithDeliverGroup(Plain).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "DeliverGroup");

            AssertNotChange(Builder(orig).WithBackoff((Duration[])null).Build(), orig);
            AssertNotChange(Builder(orig).WithBackoff((Duration)null).Build(), orig);
            AssertNotChange(Builder(orig).WithBackoff(Array.Empty<Duration>()).Build(), orig);
            ccTest = Builder(orig).WithBackoff(1000, 2000).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "Backoff");

            AssertNotChange(Builder(orig).WithBackoff((long[])null).Build(), orig);
            AssertNotChange(Builder(orig).WithBackoff(Array.Empty<long>()).Build(), orig);
            ccTest = Builder(orig).WithBackoff(1000, 2000).Build();
            AssertNotChange(ccTest, ccTest);
            AssertChange(ccTest, orig, "Backoff");
        }
    }
}
