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

using NATS.Client.Internals;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestKeyValueConfiguration
    {
        [Fact]
        public void TestConstructionWorks() {
            // builder
            KeyValueConfiguration kvc = new KeyValueConfiguration.KeyValueConfigurationBuilder()
                .WithName("bucketName")
                .WithMaxHistoryPerKey(44)
                .WithMaxBucketSize(555)
                .WithMaxValueSize(666)
                .WithTtl(Duration.OfMillis(777))
                .WithStorageType(StorageType.Memory)
                .WithReplicas(2)
                .Build();
            Validate(kvc);

            Validate(KeyValueConfiguration.Builder(kvc).Build());

            Validate(KeyValueConfiguration.Instance(kvc.BackingConfig.ToJsonNode().ToString()));

            kvc = KeyValueConfiguration.Builder()
                .WithName("bucketName")
                .Build();

            Assert.Equal(1, kvc.MaxHistoryPerKey);
        }

        private void Validate(KeyValueConfiguration bc) {
            Assert.Equal("bucketName", bc.BucketName);
            Assert.Equal(44, bc.MaxHistoryPerKey);
            Assert.Equal(555, bc.MaxBucketSize);
            Assert.Equal(666, bc.MaxValueSize);
            Assert.Equal(777, bc.Ttl);
            Assert.Equal(StorageType.Memory, bc.StorageType);
            Assert.Equal(2, bc.Replicas);
        }
    }
}