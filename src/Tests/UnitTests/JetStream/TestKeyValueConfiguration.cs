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
using NATS.Client.Internals;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;
using Xunit;
using static NATS.Client.Internals.NatsConstants;

namespace UnitTests.JetStream
{
    public class TestKeyValueConfiguration : TestBase
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
                .WithMaximumValueSize(666)
                .WithTtl(Duration.OfMillis(777))
                .WithStorageType(StorageType.Memory)
                .WithReplicas(2)
                .WithPlacement(p)
                .WithRepublish(r)
                .WithAllowDirect(true)
                .WithCompression(true)
                .WithLimitMarker(8888)
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
            Assert.Equal(666, kvc.MaximumValueSize);
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
            Assert.True(kvc.IsCompressed);
            Assert.Equal(8888, kvc.LimitMarkerTtl.Millis);
        }

        [Fact]
        public void TestCoverBucketAndKey() {
            BucketAndKey bak1 = new BucketAndKey(Dot + BUCKET + Dot + KEY);
            BucketAndKey bak2 = new BucketAndKey(Dot + BUCKET + Dot + KEY);
            BucketAndKey bak3 = new BucketAndKey(Dot + Bucket(1) + Dot + KEY);
            BucketAndKey bak4 = new BucketAndKey(Dot + BUCKET + Dot + Key(1));
            Assert.Equal(BUCKET, bak1.Bucket);
            Assert.Equal(KEY, bak1.Key);
            Assert.Equal(bak1, bak1);
            Assert.Equal(bak1, bak2);
            Assert.Equal(bak2, bak1);
            Assert.NotEqual(bak1, bak3);
            Assert.NotEqual(bak1, bak4);
            Assert.NotEqual(bak3, bak1);
            Assert.NotEqual(bak4, bak1);
            Assert.False(bak4.Equals(new object()));        
        }

        [Fact]
        public void TestCoverPrefix() {
            Assert.True(KeyValueUtil.HasPrefix("KV_has"));
            Assert.False(KeyValueUtil.HasPrefix("doesn't"));
            Assert.Equal("has", KeyValueUtil.TrimPrefix("KV_has"));
            Assert.Equal("doesn't", KeyValueUtil.TrimPrefix("doesn't"));
        }

        [Fact]
        public void TestMirrorSourceBuilderPrefixConversion()
        {
            KeyValueConfiguration kvc = KeyValueConfiguration.Builder()
                .WithName(BUCKET)
                .WithMirror(Mirror.Builder().WithName("name").Build())
                .Build();
            Assert.Equal("KV_name", kvc.BackingConfig.Mirror.Name);

            kvc = KeyValueConfiguration.Builder()
                .WithName(BUCKET)
                .WithMirror(Mirror.Builder().WithName("KV_name").Build())
                .Build();
            Assert.Equal("KV_name", kvc.BackingConfig.Mirror.Name);

            Source s1 = Source.Builder().WithName("s1").Build();
            Source s2 = Source.Builder().WithName("s2").Build();
            Source s3 = Source.Builder().WithName("s3").Build();
            Source s4 = Source.Builder().WithName("s4").Build();
            Source s5 = Source.Builder().WithName("KV_s5").Build();
            Source s6 = Source.Builder().WithName("KV_s6").Build();

            kvc = KeyValueConfiguration.Builder()
                .WithName(BUCKET)
                .WithSources(s3, s4)
                .WithSources(new List<Source>(new []{s1, s2}))
                .AddSources(s1, s2)
                .AddSources(new List<Source>(new []{s1, s2, null}))
                .AddSources(s3, s4)
                .AddSource(null)
                .AddSource(s5)
                .AddSource(s5)
                .AddSources(s6)
                .AddSources((Source[])null)
                .AddSources((List<Source>)null)
                .Build();

            Assert.Equal(6, kvc.BackingConfig.Sources.Count);
            List<string> names = new List<string>();
            foreach (Source source in kvc.BackingConfig.Sources) {
                names.Add(source.Name);
            }
            Assert.Contains("KV_s1", names);
            Assert.Contains("KV_s2", names);
            Assert.Contains("KV_s3", names);
            Assert.Contains("KV_s4", names);
            Assert.Contains("KV_s5", names);
            Assert.Contains("KV_s6", names);        
        }
        
        [Fact]
        public void TestConstructionInvalidsCoverage() {
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().Build());
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(null));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(string.Empty));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(HasSpace));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(HasPrintable));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(HasDot));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(StarNotSegment));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(GtNotSegment));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithName(HasDollar));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder(Has127));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder(HasFwdSlash));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder(HasBackSlash));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder(HasEquals));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder(HasTic));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaxHistoryPerKey(0));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaxHistoryPerKey(-1));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaxHistoryPerKey(65));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaxBucketSize(0));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaxBucketSize(-2));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaximumValueSize(0));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithMaximumValueSize(-2));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithTtl(Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithReplicas(0));
            Assert.Throws<ArgumentException>(() => KeyValueConfiguration.Builder().WithReplicas(6));
        }
    }
}
