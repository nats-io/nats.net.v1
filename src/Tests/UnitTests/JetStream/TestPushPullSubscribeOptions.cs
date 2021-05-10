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
        }
    
        [Fact]
        public void TestPullAffirmative() 
        {
            PullSubscribeOptions so = PullSubscribeOptions.Builder()
                .WithStream(STREAM)
                .WithDurable(DURABLE)
                .Build();
            Console.WriteLine(so.Stream);
            Console.WriteLine(so.Durable);
            Assert.Equal(STREAM, so.Stream);
            Assert.Equal(DURABLE, so.Durable);
        }

        [Fact]
        public void TestPushFieldValidation() 
        {
            PushSubscribeOptions.PushSubscribeOptionsBuilder builder = PushSubscribeOptions.Builder();
            Assert.Throws<ArgumentException>(() => builder.WithStream(HasDot).Build());
            Assert.Throws<ArgumentException>(() => builder.WithDurable(HasDot).Build());

            ConsumerConfiguration ccBadDur = ConsumerConfiguration.Builder().Durable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => builder.WithConfiguration(ccBadDur).Build());

            // durable directly
            PushSubscribeOptions.Builder().WithDurable(DURABLE).Build();

            // in configuration
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().Durable(DURABLE).Build();
            PushSubscribeOptions.Builder().WithConfiguration(cc).Build();
        }

        [Fact]
        public void TestPullValidation() 
        {
            PullSubscribeOptions.PullSubscribeOptionsBuilder builder1 = PullSubscribeOptions.Builder();
            Assert.Throws<ArgumentException>(() => builder1.WithStream(HasDot).Build());
            Assert.Throws<ArgumentException>(() => builder1.WithDurable(HasDot).Build());

            ConsumerConfiguration ccBadDur = ConsumerConfiguration.Builder().Durable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => builder1.WithConfiguration(ccBadDur).Build());

            // durable required direct or in configuration
            PullSubscribeOptions.PullSubscribeOptionsBuilder builder2 = PullSubscribeOptions.Builder();

            Assert.Throws<ArgumentException>(() => builder2.Build());

            ConsumerConfiguration ccNoDur = ConsumerConfiguration.Builder().Build();
            Assert.Throws<ArgumentException>(() => builder2.WithConfiguration(ccNoDur).Build());

            // durable directly
            PullSubscribeOptions.Builder().WithDurable(DURABLE).Build();

            // in configuration
            ConsumerConfiguration cc = ConsumerConfiguration.Builder().Durable(DURABLE).Build();
            PullSubscribeOptions.Builder().WithConfiguration(cc).Build();
        }
    }
}
