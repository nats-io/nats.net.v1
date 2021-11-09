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

namespace UnitTests.JetStream
{
    public class TestSubscribeOptions : TestBase
    {
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
            PushSubscribeOptions.PushSubscribeOptionsBuilder builder = PushSubscribeOptions.Builder();
            Assert.Throws<ArgumentException>(() => builder.WithStream(HasDot).Build());
            Assert.Throws<ArgumentException>(() => builder.WithDurable(HasDot).Build());

            ConsumerConfiguration ccBadDur = ConsumerConfiguration.Builder().WithDurable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => builder.WithConfiguration(ccBadDur).Build());

            // durable directly
            PushSubscribeOptions.Builder().WithDurable(DURABLE).Build();

            // in configuration
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(DURABLE).Build();
            PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
            
            // new helper
            ConsumerConfiguration.Builder().WithDurable(DURABLE).BuildPushSubscribeOptions();
        }

        [Fact]
        public void TestPullValidation() 
        {
            PullSubscribeOptions.PullSubscribeOptionsSubscribeOptionsBuilder builder1 = PullSubscribeOptions.Builder();
            Assert.Throws<ArgumentException>(() => builder1.WithStream(HasDot).Build());
            Assert.Throws<ArgumentException>(() => builder1.WithDurable(HasDot).Build());

            ConsumerConfiguration ccBadDur = ConsumerConfiguration.Builder().WithDurable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => builder1.WithConfiguration(ccBadDur).Build());

            // durable required direct or in configuration
            PullSubscribeOptions.PullSubscribeOptionsSubscribeOptionsBuilder builder2 = PullSubscribeOptions.Builder();

            Assert.Throws<ArgumentException>(() => builder2.Build());

            ConsumerConfiguration ccNoDur = ConsumerConfiguration.Builder().Build();
            Assert.Throws<ArgumentException>(() => builder2.WithConfiguration(ccNoDur).Build());

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

            Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("y").Build())
                .Build());

            Assert.Null(PushSubscribeOptions.Builder().Build().Durable);

            // pull
            Assert.Throws<ArgumentException>(() => PullSubscribeOptions.Builder()
                .WithDurable(null)
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable(null).Build())
                .Build()
                .Durable);

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

            Assert.Throws<NATSJetStreamClientException>(() => PullSubscribeOptions.Builder()
                .WithDurable("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDurable("y").Build())
                .Build());

            Assert.Throws<ArgumentException>(() => PullSubscribeOptions.Builder().Build());
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

            Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder()
                .WithDeliverGroup("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverGroup("y").Build())
                .Build());
            
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

            Assert.Throws<NATSJetStreamClientException>(() => PushSubscribeOptions.Builder()
                .WithDeliverSubject("x")
                .WithConfiguration(ConsumerConfiguration.Builder().WithDeliverSubject("y").Build())
                .Build());
            
        }
    }
}
