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
            Placement p = Placement.Builder().WithCluster("cluster").WithTags("a", "b").Build();
            Republish r = Republish.Builder().WithSource("src").WithDestination("dest").WithHeadersOnly(true).Build();

            // builder
            KeyValueConfiguration kvc = new KeyValueConfiguration.KeyValueConfigurationBuilder()
                .WithName("bucketName")
                .WithMaxHistoryPerKey(44)
                .WithMaxBucketSize(555)
                .WithMaxValueSize(666)
                .WithTtl(Duration.OfMillis(777))
                .WithStorageType(StorageType.Memory)
                .WithReplicas(2)
                .WithPlacement(p)
                .WithRepublish(r)
                .WithAllowDirect(true)
                .Build();
            Validate(kvc);

            Validate(KeyValueConfiguration.Builder(kvc).Build());

            Validate(KeyValueConfiguration.Instance(kvc.BackingConfig.ToJsonNode().ToString()));

            kvc = KeyValueConfiguration.Builder()
                .WithName("bucketName")
                .Build();

            Assert.Equal(1, kvc.MaxHistoryPerKey);
        }

        private void Validate(KeyValueConfiguration kvc) {
            Assert.Equal("bucketName", kvc.BucketName);
            Assert.Equal(44, kvc.MaxHistoryPerKey);
            Assert.Equal(555, kvc.MaxBucketSize);
            Assert.Equal(666, kvc.MaxValueSize);
            Assert.Equal(Duration.OfMillis(777), kvc.Ttl);
            Assert.Equal(StorageType.Memory, kvc.StorageType);
            Assert.True(kvc.AllowDirect);
            Assert.Equal(2, kvc.Replicas);
            Assert.NotNull(kvc.Placement);
            Assert.Equal("cluster", kvc.Placement.Cluster);
            Assert.Equal(2, kvc.Placement.Tags.Count);
            Assert.NotNull(kvc.Republish);
            Assert.Equal("src", kvc.Republish.Source);
            Assert.Equal("dest", kvc.Republish.Destination);
            Assert.True(kvc.Republish.HeadersOnly);
        }
    }
}
