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
using NATS.Client;
using NATS.Client.Internals;
using Xunit;
using static NATS.Client.Internals.Validator;

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
        public void TestValidateSubject()
        {
            // subject is required
            AllowedRequired(ValidateSubject, Plain, HasPrintable, HasDot, HasDollar, HasLow, Has127);
            AllowedRequired(ValidateSubject, _utfOnlyStrings);
            AllowedRequired(ValidateSubject, StarSegment, GtLastSegment);
            NotAllowedRequired(ValidateSubject, null, string.Empty, HasSpace, HasCr, HasLf);
            NotAllowedRequired(ValidateSubject, StartsWithDot, StarNotSegment, GtNotSegment, EmptySegment, GtNotLastSegment);
            NotAllowedRequired(ValidateSubject, EndsWithDot, EndsWithDotSpace, EndsWithCr, EndsWithLf, EndsWithTab);


            // subject not required, null and empty both mean not supplied
            AllowedNotRequiredEmptyAsNull(ValidateSubject, null, string.Empty);
            NotAllowedNotRequired(ValidateSubject, HasSpace, HasCr, HasLf);
            NotAllowedNotRequired(ValidateSubject, StartsWithDot, StarNotSegment, GtNotSegment, EmptySegment, GtNotLastSegment);
            NotAllowedNotRequired(ValidateSubject, EndsWithDot, EndsWithDotSpace, EndsWithCr, EndsWithLf, EndsWithTab);
        }

        [Fact]
        public void TestValidateReplyTo()
        {
            AllowedRequired(ValidateReplyTo, Plain, HasPrintable, HasDot, HasDollar);
            NotAllowedRequired(ValidateReplyTo, null, string.Empty, HasSpace, HasStar, HasGt, Has127);
            NotAllowedRequired(ValidateReplyTo, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(ValidateReplyTo, null, string.Empty);
        }

        [Fact]
        public void TestValidateQueueNameRequired()
        {
            AllowedRequired(ValidateQueueName, Plain, HasPrintable, HasDollar);
            NotAllowedRequired(ValidateQueueName, null, string.Empty, HasSpace, HasDot, HasStar, HasGt, Has127);
            NotAllowedRequired(ValidateQueueName, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(ValidateQueueName, null, string.Empty);
        }

        [Fact]
        public void TestValidateStreamName()
        {
            AllowedRequired(ValidateStreamName, Plain, HasPrintable, HasDollar);
            NotAllowedRequired(ValidateStreamName, null, string.Empty, HasSpace, HasDot, HasStar, HasGt, Has127);
            NotAllowedRequired(ValidateStreamName, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(ValidateStreamName, null, string.Empty);
        }

        [Fact]
        public void TestValidateDurable()
        {
            AllowedRequired(ValidateDurable, Plain, HasPrintable, HasDollar);
            NotAllowedRequired(ValidateDurable, null, string.Empty, HasSpace, HasDot, HasStar, HasGt, Has127);
            NotAllowedRequired(ValidateDurable, _utfOnlyStrings);
            AllowedNotRequiredEmptyAsNull(ValidateDurable, null, string.Empty);
        }

        [Fact]
        public void TestValidateValidateGtZero()
        {
            Assert.Equal(1, ValidateGtZero(1, "test"));
            Assert.Throws<ArgumentException>(() => ValidateGtZero(0, "test"));
            Assert.Throws<ArgumentException>(() => ValidateGtZero(-1, "test"));
        }

        [Fact]
        public void TestValidateMaxConsumers()
        {
            Assert.Equal(1, ValidateMaxConsumers(1));
            Assert.Equal(-1, ValidateMaxConsumers(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxConsumers(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxConsumers(-2));
        }

        [Fact]
        public void TestValidateMaxMessages()
        {
            Assert.Equal(1, ValidateMaxMessages(1));
            Assert.Equal(-1, ValidateMaxMessages(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxMessages(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxMessages(-2));
        }

        [Fact]
        public void TestValidateMaxBucketValues()
        {
            Assert.Equal(1, ValidateMaxBucketValues(1));
            Assert.Equal(-1, ValidateMaxBucketValues(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxBucketValues(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxBucketValues(-2));
        }

        [Fact]
        public void TestValidateMaxMessagesPerSubject()
        {
            Assert.Equal(1, ValidateMaxMessagesPerSubject(1));
            Assert.Equal(-1, ValidateMaxMessagesPerSubject(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxMessagesPerSubject(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxMessagesPerSubject(-2));
        }

        [Fact]
        public void TestValidateMaxHistoryPerKey()
        {
            Assert.Equal(1, ValidateMaxHistoryPerKey(1));
            Assert.Equal(-1, ValidateMaxHistoryPerKey(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxHistoryPerKey(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxHistoryPerKey(-2));
        }

        [Fact]
        public void TestValidateMaxBytes()
        {
            Assert.Equal(1, ValidateMaxBytes(1));
            Assert.Equal(-1, ValidateMaxBytes(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxBytes(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxBytes(-2));
        }

        [Fact]
        public void TestValidateMaxBucketBytes()
        {
            Assert.Equal(1, ValidateMaxBucketBytes(1));
            Assert.Equal(-1, ValidateMaxBucketBytes(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxBucketBytes(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxBucketBytes(-2));
        }

        [Fact]
        public void TestValidateMaxMessageSize()
        {
            Assert.Equal(1, ValidateMaxMessageSize(1));
            Assert.Equal(-1, ValidateMaxMessageSize(-1));
            Assert.Throws<ArgumentException>(() => ValidateMaxMessageSize(0));
            Assert.Throws<ArgumentException>(() => ValidateMaxMessageSize(-2));
        }

        [Fact]
        public void TestValidateNumberOfReplicas()
        {
            Assert.Equal(1, ValidateNumberOfReplicas(1));
            Assert.Equal(5, ValidateNumberOfReplicas(5));
            Assert.Throws<ArgumentException>(() => ValidateNumberOfReplicas(-1));
            Assert.Throws<ArgumentException>(() => ValidateNumberOfReplicas(0));
            Assert.Throws<ArgumentException>(() => ValidateNumberOfReplicas(7));
        }

        [Fact]
        public void TestValidateDurationRequired()
        {
            Assert.Equal(Duration.OfNanos(1), ValidateDurationRequired(Duration.OfNanos(1)));
            Assert.Equal(Duration.OfSeconds(1), ValidateDurationRequired(Duration.OfSeconds(1)));
            Assert.Throws<ArgumentException>(() => ValidateDurationRequired(null));
            Assert.Throws<ArgumentException>(() => ValidateDurationRequired(Duration.OfNanos(0)));
            Assert.Throws<ArgumentException>(() => ValidateDurationRequired(Duration.OfSeconds(0)));
            Assert.Throws<ArgumentException>(() => ValidateDurationRequired(Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => ValidateDurationRequired(Duration.OfSeconds(-1)));
        }

        [Fact]
        public void TestValidateDurationNotRequiredGtOrEqZero()
        {
            Assert.Equal(Duration.Zero, ValidateDurationNotRequiredGtOrEqZero(null));
            Assert.Equal(Duration.Zero, ValidateDurationNotRequiredGtOrEqZero(null));
            Assert.Equal(Duration.Zero, ValidateDurationNotRequiredGtOrEqZero(Duration.Zero));
            Assert.Equal(Duration.Zero, ValidateDurationNotRequiredGtOrEqZero(Duration.Zero));
            Assert.Equal(Duration.OfNanos(1), ValidateDurationNotRequiredGtOrEqZero(Duration.OfNanos(1)));
            Assert.Equal(Duration.OfSeconds(1), ValidateDurationNotRequiredGtOrEqZero(Duration.OfSeconds(1)));
            Assert.Throws<ArgumentException>(() => ValidateDurationNotRequiredGtOrEqZero(
                Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => ValidateDurationNotRequiredGtOrEqZero(
                Duration.OfSeconds(-1)));
        }

        [Fact]
        public void TestValidateDurationNotRequiredNotLessThanMin() {
            Duration min = Duration.OfMillis(99);
            Duration less = Duration.OfMillis(9);
            Duration more = Duration.OfMillis(9999);

            Assert.Null(ValidateDurationNotRequiredNotLessThanMin(null, min));
            Assert.Equal(more, ValidateDurationNotRequiredNotLessThanMin(more, min));

            Assert.Throws<ArgumentException>(() => ValidateDurationNotRequiredNotLessThanMin(less, min));
        }

        [Fact]
        public void TestValidateJetStreamPrefix()
        {
            Assert.Throws<ArgumentException>(() => ValidateJetStreamPrefix(HasStar));
            Assert.Throws<ArgumentException>(() => ValidateJetStreamPrefix(HasGt));
            Assert.Throws<ArgumentException>(() => ValidateJetStreamPrefix(HasDollar));
            Assert.Throws<ArgumentException>(() => ValidateJetStreamPrefix(HasSpace));
        }
        
        [Fact]
        public void TestValidateMustMatchIfBothSupplied()
        {
            ClientExDetail detail = new ClientExDetail("TEST", 999999, "desc");
            Assert.Null(ValidateMustMatchIfBothSupplied(null, null, detail));
            Assert.Equal("y", ValidateMustMatchIfBothSupplied(null, "y", detail));
            Assert.Equal("y", ValidateMustMatchIfBothSupplied("", "y", detail));
            Assert.Equal("x", ValidateMustMatchIfBothSupplied("x", null, detail));
            Assert.Equal("x", ValidateMustMatchIfBothSupplied("x", " ", detail));
            Assert.Equal("x", ValidateMustMatchIfBothSupplied("x", "x", detail));
            Assert.Throws<NATSJetStreamClientException>(() => ValidateMustMatchIfBothSupplied("x", "y", detail));
        }

        [Fact]
        public void TestNotNull()
        {
            object o1 = null;
            string s1 = null;
            Assert.Throws<ArgumentNullException>(() => ValidateNotNull(o1, "fieldName"));
            Assert.Throws<ArgumentNullException>(() => ValidateNotNull(s1, "fieldName"));
            object o2 = new object();
            string s2 = "";
            Assert.Equal(o2, ValidateNotNull(o2, "fieldName"));
            Assert.Equal(s2, ValidateNotNull(s2, "fieldName"));
        }

        [Fact]
        public void TestZeroOrLtMinus1()
        {
            Assert.True(ZeroOrLtMinus1(0));
            Assert.True(ZeroOrLtMinus1(-2));
            Assert.False(ZeroOrLtMinus1(1));
            Assert.False(ZeroOrLtMinus1(-1));
        }

        private void AllowedRequired(Func<string, bool, string> test, params string[] strings)
        {
            foreach (string s in strings) {
                Assert.Equal(s, test.Invoke(s, true));
            }
        }

        private void NotAllowedRequired(Func<string, bool, string> test, params string[] strings)
        {
            foreach (string s in strings)
            {
                Assert.Throws<ArgumentException>(() => test.Invoke(s, true));
            }
        }

        private void AllowedNotRequiredEmptyAsNull(Func<string, bool, string> test, params string[] strings)
        {
            foreach (string s in strings) {
                Assert.Null(test.Invoke(s, false));
            }
        }

        private void NotAllowedNotRequired(Func<string, bool, string> test, params string[] strings)
        {
            foreach (string s in strings)
            {
                Assert.Throws<ArgumentException>(() => test.Invoke(s, false));
            }
        }

        [Fact]
        public void TestNatsJetStreamClientError() 
        {
            ClientExDetail err = new ClientExDetail("TEST", 999999, "desc");
            Assert.Equal("[TEST-999999] desc", err.Message);
        }

        [Fact]
        public void TestSemVer()
        {
            string label = "Version";
            ValidateSemVer("0.0.4", label, true);
            ValidateSemVer("1.2.3", label, true);
            ValidateSemVer("10.20.30", label, true);
            ValidateSemVer("1.1.2-prerelease+meta", label, true);
            ValidateSemVer("1.1.2+meta", label, true);
            ValidateSemVer("1.1.2+meta-valid", label, true);
            ValidateSemVer("1.0.0-alpha", label, true);
            ValidateSemVer("1.0.0-beta", label, true);
            ValidateSemVer("1.0.0-alpha.beta", label, true);
            ValidateSemVer("1.0.0-alpha.beta.1", label, true);
            ValidateSemVer("1.0.0-alpha.1", label, true);
            ValidateSemVer("1.0.0-alpha0.valid", label, true);
            ValidateSemVer("1.0.0-alpha.0valid", label, true);
            ValidateSemVer("1.0.0-alpha-a.b-c-somethinglong+build.1-aef.1-its-okay", label, true);
            ValidateSemVer("1.0.0-rc.1+build.1", label, true);
            ValidateSemVer("2.0.0-rc.1+build.123", label, true);
            ValidateSemVer("1.2.3-beta", label, true);
            ValidateSemVer("10.2.3-DEV-SNAPSHOT", label, true);
            ValidateSemVer("1.2.3-SNAPSHOT-123", label, true);
            ValidateSemVer("1.0.0", label, true);
            ValidateSemVer("2.0.0", label, true);
            ValidateSemVer("1.1.7", label, true);
            ValidateSemVer("2.0.0+build.1848", label, true);
            ValidateSemVer("2.0.1-alpha.1227", label, true);
            ValidateSemVer("1.0.0-alpha+beta", label, true);
            ValidateSemVer("1.2.3----RC-SNAPSHOT.12.9.1--.12+788", label, true);
            ValidateSemVer("1.2.3----R-S.12.9.1--.12+meta", label, true);
            ValidateSemVer("1.2.3----RC-SNAPSHOT.12.9.1--.12", label, true);
            ValidateSemVer("1.0.0+0.build.1-rc.10000aaa-kk-0.1", label, true);
            ValidateSemVer("99999999999999999999999.999999999999999999.99999999999999999", label, true);
            ValidateSemVer("1.0.0-0A.is.legal", label, true);

            Assert.Null(ValidateSemVer(null, label, false));
            Assert.Null(ValidateSemVer("", label, false));

            Assert.Throws<ArgumentException>(() => ValidateSemVer(null, label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("", label, true));

            Assert.Throws<ArgumentException>(() => ValidateSemVer("1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2.3-0123", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2.3-0123.0123", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.1.2+.123", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("+invalid", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("-invalid", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("-invalid+invalid", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("-invalid.01", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha.beta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha.beta.1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha.1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha+beta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha_beta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha.", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("alpha..", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("beta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha_beta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("-alpha.", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha..", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha..1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha...1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha....1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha.....1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha......1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.0.0-alpha.......1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("01.1.1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.01.1", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.1.01", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2.3.DEV", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2-SNAPSHOT", label, true));
            Assert.Throws<ArgumentException>(() =>
                ValidateSemVer("1.2.31.2.3----RC-SNAPSHOT.12.09.1--..12+788", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("1.2-RC-SNAPSHOT", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("-1.0.3-gamma+b7718", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("+justmeta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("9.8.7+meta+meta", label, true));
            Assert.Throws<ArgumentException>(() => ValidateSemVer("9.8.7-whatever+meta+meta", label, true));
            Assert.Throws<ArgumentException>(() =>
                ValidateSemVer(
                    "99999999999999999999999.999999999999999999.99999999999999999----RC-SNAPSHOT.12.09.1--------------------------------..12",
                    label, true));
        }

        [Fact]
        public void TestDictionariesEqual()
        {
            Dictionary<string, string> dN = null; 
            Dictionary<string, string> d1 = new Dictionary<string, string>(); 
            d1["a"] = "A";
            Dictionary<string, string> d2 = new Dictionary<string, string>(); 
            d2["b"] = "B";
            Dictionary<string, string> dA = new Dictionary<string, string>(); 
            dA["a"] = "A"; dA["b"] = "B";
            Dictionary<string, string> dB = new Dictionary<string, string>(); 
            dB["b"] = "B"; dB["a"] = "A";
            
            Assert.True(Validator.DictionariesEqual(dN, dN));
            Assert.True(Validator.DictionariesEqual(d1, d1));
            Assert.True(Validator.DictionariesEqual(dA, dB));
            Assert.False(Validator.DictionariesEqual(d1, dN));
            Assert.False(Validator.DictionariesEqual(dN, d1));
            Assert.False(Validator.DictionariesEqual(d1, d2));
            Assert.False(Validator.DictionariesEqual(d1, dA));
        }
    }
}
