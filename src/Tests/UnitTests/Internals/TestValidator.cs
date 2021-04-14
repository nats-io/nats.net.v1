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
using NATS.Client.Api;
using NATS.Client.Internals;
using Xunit;
using static NATS.Client.Internals.NatsJetStreamConstants;

namespace UnitTests.Internals
{
    public class TestValidator : TestBase
    {
        [Fact]
        public void TestValidateMessageSubjectRequired()
        {
            Allowed(Validator.ValidateMessageSubjectRequired, Plain, HasSpace, HasDash, HasDot, HasStar, HasGt);
            NotAllowed(Validator.ValidateMessageSubjectRequired, null, Empty);
        }

        [Fact]
        public void TestValidateJsSubscribeSubjectRequired()
        {
            Allowed(Validator.ValidateJsSubscribeSubjectRequired, Plain, HasDash, HasDot, HasStar, HasGt);
            NotAllowed(Validator.ValidateJsSubscribeSubjectRequired, null, Empty, HasSpace);
        }

        [Fact]
        public void TestValidateQueueNameRequired()
        {
            Allowed(Validator.ValidateQueueNameRequired, Plain, HasDash, HasDot, HasStar, HasGt);
            NotAllowed(Validator.ValidateQueueNameRequired, null, Empty, HasSpace);
        }

        [Fact]
        public void TestValidateReplyTo()
        {
            Allowed(Validator.ValidateReplyToNullButNotEmpty, null, Plain, HasSpace, HasDash, HasDot, HasStar, HasGt);
            NotAllowed(Validator.ValidateReplyToNullButNotEmpty, Empty);
        }

        [Fact]
        public void TestValidateStreamName()
        {
            Allowed(Validator.ValidateStreamName, null, Empty, Plain, HasSpace, HasDash);
            NotAllowed(Validator.ValidateStreamName, HasDot, HasStar, HasGt);
        }

        [Fact]
        public void TestValidateStreamNameRequired()
        {
            Allowed(Validator.ValidateStreamNameRequired, Plain, HasSpace, HasDash);
            NotAllowed(Validator.ValidateStreamNameRequired, null, Empty, HasDot, HasStar, HasGt);
        }

        [Fact]
        public void TestValidateStreamNameOrEmptyAsNull()
        {
            Allowed(Validator.ValidateStreamNameOrEmptyAsNull, Plain, HasSpace, HasDash);
            AllowedEmptyAsNull(Validator.ValidateStreamNameOrEmptyAsNull, null, Empty);
            NotAllowed(Validator.ValidateStreamNameOrEmptyAsNull, HasDot, HasStar, HasGt);
        }

        [Fact]
        public void TestValidateDurableOrEmptyAsNull()
        {
            Allowed(Validator.ValidateDurableOrEmptyAsNull, Plain, HasSpace, HasDash);
            AllowedEmptyAsNull(Validator.ValidateDurableOrEmptyAsNull, null, Empty);
            NotAllowed(Validator.ValidateDurableOrEmptyAsNull, HasDot, HasStar, HasGt);
        }

        [Fact]
        public void TestValidateDurableRequired()
        {
            Allowed(Validator.ValidateDurableRequired, Plain, HasSpace, HasDash);
            NotAllowed(Validator.ValidateDurableRequired, null, Empty, HasDot, HasStar, HasGt);
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurableRequired(null, null));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurableRequired(HasDot, null));
            ConsumerConfiguration cc1 = new ConsumerConfiguration.Builder().Build();
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurableRequired(null, cc1));
            ConsumerConfiguration cc2 = new ConsumerConfiguration.Builder().Durable(HasDot).Build();
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurableRequired(null, cc2));
        }

        [Fact]
        public void TestValidatePullBatchSize()
        {
            Assert.Equal(1, Validator.ValidatePullBatchSize(1));
            Assert.Equal(MaxPullSize, Validator.ValidatePullBatchSize(MaxPullSize));
            Assert.Throws<ArgumentException>(() => Validator.ValidatePullBatchSize(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidatePullBatchSize(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidatePullBatchSize(MaxPullSize + 1));
        }

        [Fact]
        public void TestValidateMaxConsumers()
        {
            Assert.Equal(1, Validator.ValidateMaxConsumers(1));
            Assert.Equal(-1, Validator.ValidateMaxConsumers(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxConsumers(0));
        }

        [Fact]
        public void TestValidateMaxMessages()
        {
            Assert.Equal(1, Validator.ValidateMaxMessages(1));
            Assert.Equal(-1, Validator.ValidateMaxMessages(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessages(0));
        }

        [Fact]
        public void TestValidateMaxBytes()
        {
            Assert.Equal(1, Validator.ValidateMaxBytes(1));
            Assert.Equal(-1, Validator.ValidateMaxBytes(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBytes(0));
        }

        [Fact]
        public void TestValidateMaxMessageSize()
        {
            Assert.Equal(1, Validator.ValidateMaxMessageSize(1));
            Assert.Equal(-1, Validator.ValidateMaxMessageSize(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessageSize(0));
        }

        [Fact]
        public void TestValidateNumberOfReplicas()
        {
            Assert.Equal(1, Validator.ValidateNumberOfReplicas(1));
            Assert.Equal(5, Validator.ValidateNumberOfReplicas(5));
            Assert.Throws<ArgumentException>(() => Validator.ValidateNumberOfReplicas(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateNumberOfReplicas(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateNumberOfReplicas(7));
        }

        [Fact]
        public void TestValidateDurationRequired()
        {
            Assert.Equal(Duration.OfNanos(1), Validator.ValidateDurationRequired(Duration.OfNanos(1)));
            Assert.Equal(Duration.OfSeconds(1), Validator.ValidateDurationRequired(Duration.OfSeconds(1)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationRequired(null));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationRequired(Duration.OfNanos(0)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationRequired(Duration.OfSeconds(0)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationRequired(Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationRequired(Duration.OfSeconds(-1)));
        }

        [Fact]
        public void TestValidateDurationNotRequiredGtOrEqZero()
        {
            Assert.Equal(Duration.ZERO, Validator.ValidateDurationNotRequiredGtOrEqZero(null));
            Assert.Equal(Duration.ZERO, Validator.ValidateDurationNotRequiredGtOrEqZero(null));
            Assert.Equal(Duration.ZERO, Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.ZERO));
            Assert.Equal(Duration.ZERO, Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.ZERO));
            Assert.Equal(Duration.OfNanos(1), Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.OfNanos(1)));
            Assert.Equal(Duration.OfSeconds(1), Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.OfSeconds(1)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationNotRequiredGtOrEqZero(
                Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationNotRequiredGtOrEqZero(
                Duration.OfSeconds(-1)));
        }

        [Fact]
        public void TestValidateJetStreamPrefix()
        {
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasStar));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasGt));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasDollar));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasSpace));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasTab));
        }

        [Fact]
        public void TestNotNull()
        {
            object o1 = null;
            string s1 = null;
            Assert.Throws<ArgumentException>(() => Validator.ValidateNotNull(o1, "fieldName"));
            Assert.Throws<ArgumentException>(() => Validator.ValidateNotNull(s1, "fieldName"));
            object o2 = new object();
            string s2 = "";
            Assert.Equal(o2, Validator.ValidateNotNull(o2, "fieldName"));
            Assert.Equal(s2, Validator.ValidateNotNull(s2, "fieldName"));
        }

        [Fact]
        public void TestZeroOrLtMinus1()
        {
            Assert.True(Validator.ZeroOrLtMinus1(0));
            Assert.True(Validator.ZeroOrLtMinus1(-2));
            Assert.False(Validator.ZeroOrLtMinus1(1));
            Assert.False(Validator.ZeroOrLtMinus1(-1));
        }

        [Fact]
        public void TestContainsWhitespace()
        {
            Assert.True(Validator.ContainsWhitespace(HasSpace));
            Assert.False(Validator.ContainsWhitespace(Plain));
            Assert.False(Validator.ContainsWhitespace(null));
        }

        [Fact]
        public void TestContainsWildGtDollarSpaceTab()
        {
            Assert.True(Validator.ContainsWildGtDollarSpaceTab(HasStar));
            Assert.True(Validator.ContainsWildGtDollarSpaceTab(HasGt));
            Assert.True(Validator.ContainsWildGtDollarSpaceTab(HasDollar));
            Assert.True(Validator.ContainsWildGtDollarSpaceTab(HasTab));
            Assert.False(Validator.ContainsWildGtDollarSpaceTab(Plain));
            Assert.False(Validator.ContainsWildGtDollarSpaceTab(null));
        }

        private void Allowed(Func<string, string> test, params String[] strings)
        {
            foreach (string s in strings) {
                Assert.Equal(s, test.Invoke(s));
            }
        }

        private void AllowedEmptyAsNull(Func<string, string> test, params String[] strings)
        {
            foreach (string s in strings) {
                Assert.Null(test.Invoke(s));
            }
        }

        private void NotAllowed(Func<string, string> test, params String[] strings)
        {
            foreach (string s in strings)
            {
                Assert.Throws<ArgumentException>(() => test.Invoke(s));
            }
        }
    }
}


