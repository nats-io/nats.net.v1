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
            PushSubscribeOptions so = new PushSubscribeOptions.Builder().Build();

            // starts out all null which is fine
            Assert.Null(so.Stream);
            Assert.Null(so.Durable);
            Assert.Null(so.DeliverSubject);

            so = new PushSubscribeOptions.Builder()
                .Stream(STREAM).Durable(DURABLE).DeliverSubject(DELIVER).Build();

            Assert.Equal(STREAM, so.Stream);
            Assert.Equal(DURABLE, so.Durable);
            Assert.Equal(DELIVER, so.DeliverSubject);

            // demonstrate that you can clear the builder
            so = new PushSubscribeOptions.Builder()
                .Stream(null).DeliverSubject(null).Durable(null).Build();
            Assert.Null(so.Stream);
            Assert.Null(so.Durable);
            Assert.Null(so.DeliverSubject);
        }
    
        [Fact]
        public void TestPullAffirmative() 
        {
            PullSubscribeOptions.Builder builder = new PullSubscribeOptions.Builder()
                .Stream(STREAM)
                .Durable(DURABLE);

            PullSubscribeOptions so = builder.Build();
            Assert.Equal(STREAM, so.Stream);
            Assert.Equal(DURABLE, so.Durable);
        }

        [Fact]
        public void TestPushFieldValidation() 
        {
            PushSubscribeOptions.Builder builder = new PushSubscribeOptions.Builder();
            Assert.Throws<ArgumentException>(() => builder.Stream(HasDot).Build());
            Assert.Throws<ArgumentException>(() => builder.Durable(HasDot).Build());

            ConsumerConfiguration ccBadDur = new ConsumerConfiguration.Builder().Durable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => builder.Configuration(ccBadDur).Build());

            // durable directly
            new PushSubscribeOptions.Builder().Durable(DURABLE).Build();

            // in configuration
            ConsumerConfiguration cc = new ConsumerConfiguration.Builder().Durable(DURABLE).Build();
            new PushSubscribeOptions.Builder().Configuration(cc).Build();
        }

        [Fact]
        public void TestPullValidation() 
        {
            PullSubscribeOptions.Builder builder1 = new PullSubscribeOptions.Builder();
            Assert.Throws<ArgumentException>(() => builder1.Stream(HasDot).Build());
            Assert.Throws<ArgumentException>(() => builder1.Durable(HasDot).Build());

            ConsumerConfiguration ccBadDur = new ConsumerConfiguration.Builder().Durable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => builder1.Configuration(ccBadDur).Build());

            // durable required direct or in configuration
            PullSubscribeOptions.Builder builder2 = new PullSubscribeOptions.Builder();

            Assert.Throws<ArgumentException>(() => builder2.Build());

            ConsumerConfiguration ccNoDur = new ConsumerConfiguration.Builder().Build();
            Assert.Throws<ArgumentException>(() => builder2.Configuration(ccNoDur).Build());

            // durable directly
            new PullSubscribeOptions.Builder().Durable(DURABLE).Build();

            // in configuration
            ConsumerConfiguration cc = new ConsumerConfiguration.Builder().Durable(DURABLE).Build();
            new PullSubscribeOptions.Builder().Configuration(cc).Build();
        }
    }
}
