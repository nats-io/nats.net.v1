// Copyright 2015-2018 The NATS Authors
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

using NATS.Client;
using Xunit;

namespace UnitTests
{
    public class TestSubscriptions
    {
        static readonly string[] invalidSubjects = { "foo bar", "foo..bar", ".foo", "bar.baz.", "baz\t.foo" };
        static readonly string[] invalidQNames = { "foo group", "group\t1", "g1\r\n2" };

        [Fact]
        public void TestSubscriptionValidationAPI()
        {
            Assert.True(Subscription.IsValidSubject("foo"));
            Assert.True(Subscription.IsValidSubject("foo.bar"));

            foreach (string s in invalidSubjects)
            {
                Assert.False(Subscription.IsValidSubject(s));
            }

            foreach (string s in invalidQNames)
            {
                Assert.False(Subscription.IsValidQueueGroupName(s));
            }
        }
    }
}