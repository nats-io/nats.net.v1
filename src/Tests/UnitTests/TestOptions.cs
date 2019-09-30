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

using System;
using NATS.Client;
using Xunit;

namespace UnitTests
{
    public class TestOptions
    {
        private Options GetDefaultOptions() => ConnectionFactory.GetDefaultOptions();

        [Fact]
        public void TestBadOptionTimeoutConnect()
        {
            var opts = GetDefaultOptions();

            Assert.ThrowsAny<Exception>(() => opts.Timeout = -1);
        }

        [Fact]
        public void TestBadOptionSubscriptionBatchSize()
        {
            var opts = GetDefaultOptions();

            Assert.ThrowsAny<ArgumentException>(() => opts.SubscriptionBatchSize = -1);

            Assert.ThrowsAny<ArgumentException>(() => opts.SubscriptionBatchSize = 0);
        }
    }
}