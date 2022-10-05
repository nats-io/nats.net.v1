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
using NATS.Client;
using NATS.Client.JetStream;
using Xunit;
using static NATS.Client.ClientExDetail;
using static NATS.Client.JetStream.SubscribeOptions;

namespace UnitTests.JetStream
{
    public class TestSubscribeOptions : TestBase
    {
        private static readonly string[] BadNames = {HasDot, HasGt, HasStar, HasFwdSlash, HasBackSlash};
    
        [Fact]
        public void TestPushAffirmative()
        {
            PushSubscribeOptions so = PushSubscribeOptions.Builder().Build();

            // starts out all null which is fine
            Assert.Null(so.Stream);
            Assert.Null(so.Durable);
            Assert.Null(so.DeliverSubject);

            so = PushSubscribeOptions.Builder()
                .WithStream(STREAM).WithDurable(DURABLE).WithDeliverSubject(DELIVER).Build();

            Assert.Equal(STREAM, so.Stream);
            Assert.Equal(DURABLE, so.Durable);
            Assert.Equal(DELIVER, so.DeliverSubject);

            // demonstrate that you can clear the builder
            so = PushSubscribeOptions.Builder()
                .WithStream(null).WithDeliverSubject(null).WithDurable(null).Build();
            Assert.Null(so.Stream);
            Assert.Null(so.Durable);
            Assert.Null(so.DeliverSubject);
            Assert.False(so.Pull);
        }
    
        [Fact]
        public void TestPullAffirmative() 
        {
            PullSubscribeOptions so = PullSubscribeOptions.Builder()
                .WithStream(STREAM)
                .WithDurable(DURABLE)
                .Build();
            Assert.Equal(STREAM, so.Stream);
            Assert.Equal(DURABLE, so.Durable);
            Assert.True(so.Pull);
        }

        [Fact]
        public void TestPushFieldValidation() 
        {
            foreach (string bad in BadNames) {
                PushSubscribeOptions.PushSubscribeOptionsBuilder pushBuilder = PushSubscribeOptions.Builder();
                Assert.Throws<ArgumentException>(() => pushBuilder.WithStream(bad).Build());
                Assert.Throws<ArgumentException>(() => pushBuilder.WithDurable(bad).Build());
                Assert.Throws<ArgumentException>(() => pushBuilder.WithName(bad).Build());
                Assert.Throws<ArgumentException>(() => ConsumerConfiguration.Builder().WithDurable(bad).Build());
                Assert.Throws<ArgumentException>(() => ConsumerConfiguration.Builder().WithName(bad).Build());
            }

            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder().WithName(NAME).WithDurable(DURABLE).Build());
            Assert.Contains(JsConsumerNameDurableMismatch.Id, e.Message);

            // durable directly
            PushSubscribeOptions.Builder().WithDurable(DURABLE).Build();
            PushSubscribeOptions.Builder().WithName(NAME).Build();

            // in configuration
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
            PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
            cc = ConsumerConfiguration.Builder().WithName(NAME).Build();
            PushSubscribeOptions.Builder().WithConfiguration(cc).Build();

            // new helper
            ConsumerConfiguration.Builder().WithDurable(DURABLE).BuildPushSubscribeOptions();
            ConsumerConfiguration.Builder().WithName(NAME).BuildPushSubscribeOptions();
            
            // ordered rules
        }

        [Fact]
        public void TestPullFieldValidation() 
        {
            foreach (string bad in BadNames) {
                PullSubscribeOptions.PullSubscribeOptionsBuilder pullBuilder = PullSubscribeOptions.Builder();
                Assert.Throws<ArgumentException>(() => pullBuilder.WithStream(bad).Build());
                Assert.Throws<ArgumentException>(() => pullBuilder.WithDurable(bad).Build());
                Assert.Throws<ArgumentException>(() => pullBuilder.WithName(bad).Build());
                Assert.Throws<ArgumentException>(() => ConsumerConfiguration.Builder().WithDurable(bad).Build());
                Assert.Throws<ArgumentException>(() => ConsumerConfiguration.Builder().WithName(bad).Build());
            }

            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => PullSubscribeOptions.Builder().WithName(NAME).WithDurable(DURABLE).Build());
            Assert.Contains(JsConsumerNameDurableMismatch.Id, e.Message);

            // durable directly
            PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();

            // in configuration
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
            PullSubscribeOptions.Builder().WithConfiguration(cc).Build();

            // new helper
            ConsumerConfiguration.Builder().WithDurable(DURABLE).BuildPullSubscribeOptions();
        }

        [Fact]
        public void TestDurableValidation()
        {
            // push
            Assert.Null(PushSubscribeOptions.Builder()
                .WithDurable(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable(null).Build())
                .Build()
                .Durable);

            Assert.Equal("y", PushSubscribeOptions.Builder()
                .WithDurable(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("y").Build())
                .Build()
                .Durable);

            Assert.Equal("x", PushSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable(null).Build())
                .Build()
                .Durable);

            Assert.Equal("x", PushSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("x").Build())
                .Build()
                .Durable);

            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("y").Build())
                .Build());
            Assert.Contains(JsSoDurableMismatch.Id, e.Message);

            Assert.Null(PushSubscribeOptions.Builder().Build().Durable);

            // pull
            Assert.Equal("y", PullSubscribeOptions.Builder()
                .WithDurable(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("y").Build())
                .Build()
                .Durable);

            Assert.Equal("x", PullSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable(null).Build())
                .Build()
                .Durable);

            Assert.Equal("x", PullSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("x").Build())
                .Build()
                .Durable);

            e = Assert.Throws<NATSJetStreamClientException>(() => PullSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("y").Build())
                .Build());
            Assert.Contains(JsSoDurableMismatch.Id, e.Message);

        }

        [Fact]
        public void TestDeliverGroupValidation()
        {
            Assert.Null(PushSubscribeOptions.Builder()
                .WithDeliverGroup(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverGroup(null).Build())
                .Build()
                .DeliverGroup);

            Assert.Equal("y", PushSubscribeOptions.Builder()
                .WithDeliverGroup(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverGroup("y").Build())
                .Build()
                .DeliverGroup);

            Assert.Equal("x", PushSubscribeOptions.Builder()
                .WithDeliverGroup("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverGroup(null).Build())
                .Build()
                .DeliverGroup);

            Assert.Equal("x", PushSubscribeOptions.Builder()
                .WithDeliverGroup("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverGroup("x").Build())
                .Build()
                .DeliverGroup);

            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder()
                .WithDeliverGroup("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverGroup("y").Build())
                .Build());
            Assert.Contains(JsSoDeliverGroupMismatch.Id, e.Message);
        }

        [Fact]
        public void TestDeliverSubjectValidation()
        {
            Assert.Null(PushSubscribeOptions.Builder()
                .WithDeliverSubject(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverSubject(null).Build())
                .Build()
                .DeliverSubject);

            Assert.Equal("y", PushSubscribeOptions.Builder()
                .WithDeliverSubject(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverSubject("y").Build())
                .Build()
                .DeliverSubject);

            Assert.Equal("x", PushSubscribeOptions.Builder()
                .WithDeliverSubject("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverSubject(null).Build())
                .Build()
                .DeliverSubject);

            Assert.Equal("x", PushSubscribeOptions.Builder()
                .WithDeliverSubject("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverSubject("x").Build())
                .Build()
                .DeliverSubject);

            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder()
                .WithDeliverSubject("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverSubject("y").Build())
                .Build());
            Assert.Contains(JsSoDeliverSubjectMismatch.Id, e.Message);
        }

        [Fact]
        public void TestOrderedCreation() {
            ConsumerConfiguration ccEmpty = ConsumerConfiguration.Builder().Build();
            PushSubscribeOptions.Builder().WithConfiguration(ccEmpty).WithOrdered(true).Build();

            AssertClientError(JsSoOrderedNotAllowedWithBind,
                () => PushSubscribeOptions.Builder().WithStream(STREAM).WithBind(true).WithOrdered(true).Build());

            AssertClientError(JsSoOrderedNotAllowedWithDeliverGroup,
                () => PushSubscribeOptions.Builder().WithDeliverGroup(DELIVER).WithOrdered(true).Build());

            AssertClientError(JsSoOrderedNotAllowedWithDurable,
                () => PushSubscribeOptions.Builder().WithDurable(DURABLE).WithOrdered(true).Build());

            AssertClientError(JsSoOrderedNotAllowedWithDeliverSubject,
                () => PushSubscribeOptions.Builder().WithDeliverSubject(DELIVER).WithOrdered(true).Build());

            ConsumerConfiguration ccAckNotNone1 = ConsumerConfiguration.Builder().WithAckPolicy(AckPolicy.All).Build();
            AssertClientError(JsSoOrderedRequiresAckPolicyNone,
                () => PushSubscribeOptions.Builder().WithConfiguration(ccAckNotNone1).WithOrdered(true).Build());

            ConsumerConfiguration ccAckNotNone2 = ConsumerConfiguration.Builder().WithAckPolicy(AckPolicy.Explicit).Build();
            AssertClientError(JsSoOrderedRequiresAckPolicyNone,
                () => PushSubscribeOptions.Builder().WithConfiguration(ccAckNotNone2).WithOrdered(true).Build());

            ConsumerConfiguration ccAckNoneOk = ConsumerConfiguration.Builder().WithAckPolicy(AckPolicy.None).Build();
            PushSubscribeOptions.Builder().WithConfiguration(ccAckNoneOk).WithOrdered(true).Build();

            ConsumerConfiguration ccMax = ConsumerConfiguration.Builder().WithMaxDeliver(2).Build();
            AssertClientError(JsSoOrderedRequiresMaxDeliver,
                () => PushSubscribeOptions.Builder().WithConfiguration(ccMax).WithOrdered(true).Build());

            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithIdleHeartbeat(100).Build();
            PushSubscribeOptions pso = PushSubscribeOptions.Builder().WithConfiguration(cc).WithOrdered(true).Build();
            Assert.Equal(DefaultOrderedHeartbeat, (long)pso.ConsumerConfiguration.IdleHeartbeat.Millis);

            cc = ConsumerConfiguration.Builder().WithIdleHeartbeat(DefaultOrderedHeartbeat + 1).Build();
            pso = PushSubscribeOptions.Builder().WithConfiguration(cc).WithOrdered(true).Build();
            Assert.Equal(DefaultOrderedHeartbeat + 1, (long)pso.ConsumerConfiguration.IdleHeartbeat.Millis);

            // okay if you set it to true
            cc = ConsumerConfiguration.Builder().WithMemStorage(true).Build();
            PushSubscribeOptions.Builder().WithConfiguration(cc).WithOrdered(true).Build();

            // okay if you set it to 1
            cc = ConsumerConfiguration.Builder().WithNumReplicas(1).Build();
            PushSubscribeOptions.Builder().WithConfiguration(cc).WithOrdered(true).Build();
                
            // not okay if you set it to false
            ConsumerConfiguration ccMs = ConsumerConfiguration.Builder().WithMemStorage(false).Build();
            AssertClientError(JsSoOrderedMemStorageNotSuppliedOrTrue,
                () => PushSubscribeOptions.Builder().WithConfiguration(ccMs).WithOrdered(true).Build());

            // not okay if you set it to something other than 1
            ConsumerConfiguration ccR = ConsumerConfiguration.Builder().WithNumReplicas(3).Build();
            AssertClientError(JsSoOrderedReplicasNotSuppliedOrOne,
                () => PushSubscribeOptions.Builder().WithConfiguration(ccR).WithOrdered(true).Build());
        }
    }
}
