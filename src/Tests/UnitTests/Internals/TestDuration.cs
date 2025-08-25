// Copyright 2022 The NATS Authors
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
using NATS.Client.Internals;
using Xunit;

namespace UnitTests.Internals
{
    public class TestDuration : TestBase
    {
        [Fact]
        public void TestDurationWorks()
        {
            long hours = 2;
            long minutes = hours * 60;
            long seconds = minutes * 60;
            long millis = seconds * 1000;
            long nanos = millis * 1_000_000;
            Assert.Equal(nanos, Duration.OfHours(hours).Nanos);
            Assert.Equal(nanos, Duration.OfMinutes(minutes).Nanos);
            Assert.Equal(nanos, Duration.OfSeconds(seconds).Nanos);
            Assert.Equal(nanos, Duration.OfMillis(millis).Nanos);
            Assert.Equal(nanos, Duration.OfNanos(nanos).Nanos);
        }
    }
}
