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
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestJetStreamOptions : TestBase
    {
        [Fact]
        public void TestPushAffirmative()
        {
            JetStreamOptions jso = new JetStreamOptions.Builder().Build();
            Assert.Equal(NatsJetStreamConstants.JsapiPrefix, jso.Prefix);
            Assert.Equal(Duration.OfMillis(Defaults.Timeout), jso.RequestTimeout);

            jso = new JetStreamOptions.Builder()
                .Prefix("pre")
                .RequestTimeout(Duration.OfSeconds(42))
                .Build();
            Assert.Equal("pre.", jso.Prefix);
            Assert.Equal(Duration.OfSeconds(42), jso.RequestTimeout);
            Assert.False(jso.PublishNoAck);

            jso = new JetStreamOptions.Builder()
                .Prefix("pre.")
                .PublishNoAck(true)
                .Build();
            Assert.Equal("pre.", jso.Prefix);
            Assert.True(jso.PublishNoAck);
        }
    
        [Fact]
        public void TestInvalidPrefix() 
        {
            Assert.Throws<ArgumentException>(() => new JetStreamOptions.Builder().Prefix(HasStar).Build());
            Assert.Throws<ArgumentException>(() => new JetStreamOptions.Builder().Prefix(HasGt).Build());
            Assert.Throws<ArgumentException>(() => new JetStreamOptions.Builder().Prefix(HasDollar).Build());
        }
    }
}
