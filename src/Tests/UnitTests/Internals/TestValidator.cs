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

namespace UnitTests.Internals
{
    public class TestValidator : TestBase
    {
        private readonly string[] _utfOnlyStrings;
        
        public TestValidator()
        {
            _utfOnlyStrings = ReadDataFileLines("utf8-only-no-ws-test-strings.txt");
        }

        [Fact]
        public void TestValidateMessageSubjectRequired()
        {
            AllowedRequired(Validator.ValidateSubject, Plain, HasPrintable, HasDot, HasStar, HasGt, HasDollar);
            NotAllowedRequired(Validator.ValidateSubject, null, String.Empty, HasSpace, HasLow, Has127);
            NotAllowedRequired(Validator.ValidateSubject, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(Validator.ValidateSubject, null, String.Empty);

            NotAllowedRequired(Validator.ValidateSubject, null, String.Empty, HasSpace, HasLow, Has127);
            AllowedNotRequiredEmptyAsNull(Validator.ValidateSubject, null, String.Empty);
        }

        [Fact]
        public void TestValidateReplyTo()
        {
            AllowedRequired(Validator.ValidateReplyTo, Plain, HasPrintable, HasDot, HasDollar);
            NotAllowedRequired(Validator.ValidateReplyTo, null, String.Empty, HasSpace, HasStar, HasGt, HasLow, Has127);
            NotAllowedRequired(Validator.ValidateReplyTo, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(Validator.ValidateReplyTo, null, String.Empty);
        }

        [Fact]
        public void TestValidateQueueNameRequired()
        {
            AllowedRequired(Validator.ValidateQueueName, Plain, HasPrintable, HasDollar);
            NotAllowedRequired(Validator.ValidateQueueName, null, String.Empty, HasSpace, HasDot, HasStar, HasGt, HasLow, Has127);
            NotAllowedRequired(Validator.ValidateQueueName, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(Validator.ValidateQueueName, null, String.Empty);
        }

        [Fact]
        public void TestValidateStreamName()
        {
            AllowedRequired(Validator.ValidateStreamName, Plain, HasPrintable, HasDollar);
            NotAllowedRequired(Validator.ValidateStreamName, null, String.Empty, HasSpace, HasDot, HasStar, HasGt, HasLow, Has127);
            NotAllowedRequired(Validator.ValidateStreamName, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(Validator.ValidateStreamName, null, String.Empty);
        }

        [Fact]
        public void TestValidateDurable()
        {
            AllowedRequired(Validator.ValidateDurable, Plain, HasPrintable, HasDollar);
            NotAllowedRequired(Validator.ValidateDurable, null, String.Empty, HasSpace, HasDot, HasStar, HasGt, HasLow, Has127);
            NotAllowedRequired(Validator.ValidateDurable, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(Validator.ValidateDurable, null, String.Empty);
        }

        [Fact]
        public void TestValidateDurableRequired()
        {
            AllowedRequired((s, r) => Validator.ValidateDurableRequired(s, null), Plain, HasPrintable);
            NotAllowedRequired((s, r) => Validator.ValidateDurableRequired(s, null), null, string.Empty, HasSpace, HasDot, HasStar, HasGt, HasLow, Has127);
            NotAllowedRequired((s, r) => Validator.ValidateDurableRequired(s, null), _utfOnlyStrings);

            foreach (var data in new []{Plain, HasPrintable, HasDollar})
            {
                ConsumerConfiguration ccAllowed = ConsumerConfiguration.Builder().WithDurable(data).Build();
                Assert.Equal(data, Validator.ValidateDurableRequired(null, ccAllowed));
            }

            foreach (var data in new []{null, string.Empty, HasSpace, HasDot, HasStar, HasGt, HasLow, Has127})
            {
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(data).Build();
                NotAllowedRequired((s, r) => Validator.ValidateDurableRequired(null, cc));
            }

            foreach (var data in _utfOnlyStrings)
            {
                ConsumerConfiguration cc = ConsumerConfiguration.Builder().WithDurable(data).Build();
                NotAllowedRequired((s, r) => Validator.ValidateDurableRequired(null, cc));
            }
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
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxConsumers(-2));
        }

        [Fact]
        public void TestValidateMaxMessages()
        {
            Assert.Equal(1, Validator.ValidateMaxMessages(1));
            Assert.Equal(-1, Validator.ValidateMaxMessages(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessages(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessages(-2));
        }

        [Fact]
        public void TestValidateMaxBucketValues()
        {
            Assert.Equal(1, Validator.ValidateMaxBucketValues(1));
            Assert.Equal(-1, Validator.ValidateMaxBucketValues(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBucketValues(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBucketValues(-2));
        }

        [Fact]
        public void TestValidateMaxMessagesPerSubject()
        {
            Assert.Equal(1, Validator.ValidateMaxMessagesPerSubject(1));
            Assert.Equal(-1, Validator.ValidateMaxMessagesPerSubject(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessagesPerSubject(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessagesPerSubject(-2));
        }

        [Fact]
        public void TestValidateMaxHistoryPerKey()
        {
            Assert.Equal(1, Validator.ValidateMaxHistoryPerKey(1));
            Assert.Equal(-1, Validator.ValidateMaxHistoryPerKey(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxHistoryPerKey(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxHistoryPerKey(-2));
        }

        [Fact]
        public void TestValidateMaxBytes()
        {
            Assert.Equal(1, Validator.ValidateMaxBytes(1));
            Assert.Equal(-1, Validator.ValidateMaxBytes(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBytes(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBytes(-2));
        }

        [Fact]
        public void TestValidateMaxBucketBytes()
        {
            Assert.Equal(1, Validator.ValidateMaxBucketBytes(1));
            Assert.Equal(-1, Validator.ValidateMaxBucketBytes(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBucketBytes(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxBucketBytes(-2));
        }

        [Fact]
        public void TestValidateMaxMessageSize()
        {
            Assert.Equal(1, Validator.ValidateMaxMessageSize(1));
            Assert.Equal(-1, Validator.ValidateMaxMessageSize(-1));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessageSize(0));
            Assert.Throws<ArgumentException>(() => Validator.ValidateMaxMessageSize(-2));
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
            Assert.Equal(Duration.Zero, Validator.ValidateDurationNotRequiredGtOrEqZero(null));
            Assert.Equal(Duration.Zero, Validator.ValidateDurationNotRequiredGtOrEqZero(null));
            Assert.Equal(Duration.Zero, Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.Zero));
            Assert.Equal(Duration.Zero, Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.Zero));
            Assert.Equal(Duration.OfNanos(1), Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.OfNanos(1)));
            Assert.Equal(Duration.OfSeconds(1), Validator.ValidateDurationNotRequiredGtOrEqZero(Duration.OfSeconds(1)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationNotRequiredGtOrEqZero(
                Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationNotRequiredGtOrEqZero(
                Duration.OfSeconds(-1)));
        }

        [Fact]
        public void TestValidateDurationNotRequiredNotLessThanMin() {
            Duration min = Duration.OfMillis(99);
            Duration less = Duration.OfMillis(9);
            Duration more = Duration.OfMillis(9999);

            Assert.Null(Validator.ValidateDurationNotRequiredNotLessThanMin(null, min));
            Assert.Equal(more, Validator.ValidateDurationNotRequiredNotLessThanMin(more, min));

            Assert.Throws<ArgumentException>(() => Validator.ValidateDurationNotRequiredNotLessThanMin(less, min));
        }

        [Fact]
        public void TestValidateJetStreamPrefix()
        {
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasStar));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasGt));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasDollar));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasSpace));
            Assert.Throws<ArgumentException>(() => Validator.ValidateJetStreamPrefix(HasLow));
        }
        
        [Fact]
        public void TestValidateNotSupplied() {
            ClientExDetail err = new ClientExDetail("TEST", 999999, "desc");

            // string version
            Validator.ValidateNotSupplied((string)null, err);
            Validator.ValidateNotSupplied("", err);
            Assert.Throws<NATSJetStreamClientException>(() => Validator.ValidateNotSupplied("notempty", err));

            Validator.ValidateNotSupplied(0, 0, err);
            Assert.Throws<NATSJetStreamClientException>(() => Validator.ValidateNotSupplied(1, 0, err));
        }

        [Fact]
        public void TestValidateMustMatchIfBothSupplied()
        {
            ClientExDetail detail = new ClientExDetail("TEST", 999999, "desc");
            Assert.Null(Validator.ValidateMustMatchIfBothSupplied(null, null, detail));
            Assert.Equal("y", Validator.ValidateMustMatchIfBothSupplied(null, "y", detail));
            Assert.Equal("y", Validator.ValidateMustMatchIfBothSupplied("", "y", detail));
            Assert.Equal("x", Validator.ValidateMustMatchIfBothSupplied("x", null, detail));
            Assert.Equal("x", Validator.ValidateMustMatchIfBothSupplied("x", " ", detail));
            Assert.Equal("x", Validator.ValidateMustMatchIfBothSupplied("x", "x", detail));
            Assert.Throws<NATSJetStreamClientException>(() => Validator.ValidateMustMatchIfBothSupplied("x", "y", detail));
        }

        [Fact]
        public void TestNotNull()
        {
            object o1 = null;
            string s1 = null;
            Assert.Throws<ArgumentNullException>(() => Validator.ValidateNotNull(o1, "fieldName"));
            Assert.Throws<ArgumentNullException>(() => Validator.ValidateNotNull(s1, "fieldName"));
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

        private void AllowedRequired(Func<string, bool, string> test, params String[] strings)
        {
            foreach (string s in strings) {
                Assert.Equal(s, test.Invoke(s, true));
            }
        }

        private void AllowedNotRequiredEmptyAsNull(Func<string, bool, string> test, params String[] strings)
        {
            foreach (string s in strings) {
                Assert.Null(test.Invoke(s, false));
            }
        }

        private void NotAllowedRequired(Func<string, bool, string> test, params String[] strings)
        {
            foreach (string s in strings)
            {
                Assert.Throws<ArgumentException>(() => test.Invoke(s, true));
            }
        }

        [Fact]
        public void TestNatsJetStreamClientError() 
        {
            ClientExDetail err = new ClientExDetail("TEST", 999999, "desc");
            Assert.Equal("[TEST-999999] desc", err.Message);
        }
    }
}


