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

using System.Collections.Generic;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestAccountStatistics : TestBase
    {
        [Fact]
        public void JsonIsReadProperly()
        {
            string json = ReadDataFile("AccountStatistics.json");
            AccountStatistics acctStats = new AccountStatistics(json, false);
            Assert.Equal(101, acctStats.Memory);
            Assert.Equal(102, acctStats.Storage);
            Assert.Equal(103, acctStats.Streams);
            Assert.Equal(104, acctStats.Consumers);
            validateAccountLimits(acctStats.Limits, 200);

            Assert.Equal("ngs", acctStats.Domain);

            ApiStats api = acctStats.Api;
            Assert.Equal(301, api.Total);
            Assert.Equal(302, api.Errors);

            IDictionary<string, AccountTier> tiers = acctStats.Tiers;
            AccountTier tier = tiers["R1"];
            Assert.NotNull(tier);
            Assert.Equal(401, tier.Memory);
            Assert.Equal(402, tier.Storage);
            Assert.Equal(403, tier.Streams);
            Assert.Equal(404, tier.Consumers);
            validateAccountLimits(tier.Limits, 500);

            tier = tiers["R3"];
            Assert.NotNull(tier);
            Assert.Equal(601, tier.Memory);
            Assert.Equal(602, tier.Storage);
            Assert.Equal(603, tier.Streams);
            Assert.Equal(604, tier.Consumers);
            validateAccountLimits(tier.Limits, 700);
            
            acctStats = new AccountStatistics("{}", false);
            Assert.Equal(0, acctStats.Memory);
            Assert.Equal(0, acctStats.Storage);
            Assert.Equal(0, acctStats.Streams);
            Assert.Equal(0, acctStats.Consumers);
            
            AccountLimits al = acctStats.Limits;
            Assert.NotNull(al);
            Assert.Equal(0, al.MaxMemory);
            Assert.Equal(0, al.MaxStorage);
            Assert.Equal(0, al.MaxStreams);
            Assert.Equal(0, al.MaxConsumers);
            Assert.Equal(0, al.MaxAckPending);
            Assert.Equal(0, al.MemoryMaxStreamBytes);
            Assert.Equal(0, al.StorageMaxStreamBytes);
            Assert.False(al.MaxBytesRequired);

            api = acctStats.Api;
            Assert.NotNull(api);
            Assert.Equal(0, api.Total);
            Assert.Equal(0, api.Errors);
        }
        
        private void validateAccountLimits(AccountLimits al, int id) {
            Assert.Equal(id + 1, al.MaxMemory);
            Assert.Equal(id + 2, al.MaxStorage);
            Assert.Equal(id + 3, al.MaxStreams);
            Assert.Equal(id + 4, al.MaxConsumers);
            Assert.Equal(id + 5, al.MaxAckPending);
            Assert.Equal(id + 6, al.MemoryMaxStreamBytes);
            Assert.Equal(id + 7, al.StorageMaxStreamBytes);
            Assert.True(al.MaxBytesRequired);
        }
    }
}
