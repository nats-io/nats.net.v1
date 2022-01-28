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
using static NATS.Client.Internals.JetStreamConstants;

namespace UnitTests.JetStream
{
    public class TestJetStreamOptions : TestBase
    {
        [Fact]
        public void TestPushAffirmative()
        {
            JetStreamOptions jso = JetStreamOptions.Builder().Build();
            Assert.Equal(DefaultApiPrefix, jso.Prefix);
            Assert.Equal(Duration.OfMillis(Defaults.Timeout), jso.RequestTimeout);

            jso = JetStreamOptions.Builder(jso).Build();
            Assert.Equal(DefaultApiPrefix, jso.Prefix);
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
        public void TestPrefixValidation()
        {
            AssertDefaultPrefix(null);
            AssertDefaultPrefix("");
            AssertDefaultPrefix(" ");

            AssertValidPrefix(Plain);
            AssertValidPrefix(HasPrintable);
            AssertValidPrefix(HasDot);
            AssertValidPrefix(HasDash);
            AssertValidPrefix(HasUnder);
            AssertValidPrefix(HasDollar);
            AssertValidPrefix(HasFwdSlash);
            AssertValidPrefix(HasEquals);
            AssertValidPrefix(HasTic);

            AssertInvalidPrefix(HasSpace);
            AssertInvalidPrefix(HasStar);
            AssertInvalidPrefix(HasGt);
            AssertInvalidPrefix(HasLow);
            AssertInvalidPrefix(Has127);

            AssertInvalidPrefix(".");
            AssertInvalidPrefix("." + Plain);
        }

        private void AssertValidPrefix(string prefix) {
            JetStreamOptions jso = JetStreamOptions.Builder().WithPrefix(prefix).Build();
            string prefixWithDot = prefix.EndsWith(".") ? prefix : prefix + ".";
            Assert.Equal(prefixWithDot, jso.Prefix);
        }

        private void AssertDefaultPrefix(string prefix) {
            JetStreamOptions jso = JetStreamOptions.Builder().WithPrefix(prefix).Build();
            Assert.Equal(DefaultApiPrefix, jso.Prefix);
        }

        private void AssertInvalidPrefix(string prefix) {
            Assert.Throws<ArgumentException>(() => JetStreamOptions.Builder().WithPrefix(prefix).Build());
        }

        [Fact]
        public void TestDomainValidation()
        {
            AssertDefaultDomain(null);
            AssertDefaultDomain("");
            AssertDefaultDomain(" ");

            AssertValidDomain(Plain);
            AssertValidDomain(HasPrintable);
            AssertValidDomain(HasDot);
            AssertValidDomain(HasDash);
            AssertValidDomain(HasUnder);
            AssertValidDomain(HasDollar);
            AssertValidDomain(HasFwdSlash);
            AssertValidDomain(HasEquals);
            AssertValidDomain(HasTic);

            AssertInvalidDomain(HasSpace);
            AssertInvalidDomain(HasStar);
            AssertInvalidDomain(HasGt);
            AssertInvalidDomain(HasLow);
            AssertInvalidDomain(Has127);

            AssertInvalidDomain(".");
            AssertInvalidDomain("." + Plain);
        }

        private void AssertValidDomain(string domain) {
            JetStreamOptions jso = JetStreamOptions.Builder().WithDomain(domain).Build();
            if (domain.StartsWith(".")) {
                domain = domain.Substring(1);
            }
            string prefixWithDot = domain.EndsWith(".") ? domain : domain + ".";
            string expected = PrefixDollarJsDot + prefixWithDot + PrefixApiDot;
            Assert.Equal(expected, jso.Prefix);
        }

        private void AssertDefaultDomain(string domain) {
            JetStreamOptions jso = JetStreamOptions.Builder().WithDomain(domain).Build();
            Assert.Equal(DefaultApiPrefix, jso.Prefix);
        }

        private void AssertInvalidDomain(string domain) {
            Assert.Throws<ArgumentException>(() => JetStreamOptions.Builder().WithDomain(domain).Build());
        }
    }
}
