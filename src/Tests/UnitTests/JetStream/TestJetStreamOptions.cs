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
            JetStreamOptions jso = JetStreamOptions.Builder().Build();
            Assert.Equal(NatsJetStreamConstants.JsapiPrefix, jso.Prefix);
            Assert.Equal(Duration.OfMillis(Defaults.Timeout), jso.RequestTimeout);

            jso = JetStreamOptions.Builder(jso).Build();
            Assert.Equal(NatsJetStreamConstants.JsapiPrefix, jso.Prefix);
            Assert.Equal(Duration.OfMillis(Defaults.Timeout), jso.RequestTimeout);

            jso = JetStreamOptions.Builder()
                .WithPrefix("pre")
                .WithRequestTimeout(Duration.OfSeconds(42))
                .Build();
            Assert.Equal("pre.", jso.Prefix);
            Assert.Equal(Duration.OfSeconds(42), jso.RequestTimeout);
            Assert.False(jso.IsPublishNoAck);

            jso = JetStreamOptions.Builder(jso).Build();
            Assert.Equal("pre.", jso.Prefix);
            Assert.Equal(Duration.OfSeconds(42), jso.RequestTimeout);
            Assert.False(jso.IsPublishNoAck);
            
            jso = JetStreamOptions.Builder()
                .WithPrefix("pre.")
                .WithPublishNoAck(true)
                .WithRequestTimeout(42000)
                .Build();
            Assert.Equal("pre.", jso.Prefix);
            Assert.Equal(Duration.OfSeconds(42), jso.RequestTimeout);
            Assert.True(jso.IsPublishNoAck);

            jso = JetStreamOptions.Builder(jso).Build();
            Assert.Equal("pre.", jso.Prefix);
            Assert.Equal(Duration.OfSeconds(42), jso.RequestTimeout);
            Assert.True(jso.IsPublishNoAck);
        }
    
        [Fact]
        public void TestInvalidPrefix() 
        {
            Assert.Throws<ArgumentException>(() => JetStreamOptions.Builder().WithPrefix(HasStar).Build());
            Assert.Throws<ArgumentException>(() => JetStreamOptions.Builder().WithPrefix(HasGt).Build());
            Assert.Throws<ArgumentException>(() => JetStreamOptions.Builder().WithPrefix(HasDollar).Build());
        }
    }
}
