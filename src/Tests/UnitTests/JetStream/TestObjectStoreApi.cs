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
using System.Collections.Generic;
using System.Text;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using Xunit;
using static UnitTests.TestBase;

namespace UnitTests.JetStream
{
    public class TestObjectStoreApi
    {
        [Fact]
        public void TestConfigurationConstruction()
        {
            Placement p = Placement.Builder().WithCluster("cluster").WithTags("a", "b").Build();

            // builder
            ObjectStoreConfiguration bc = ObjectStoreConfiguration.Builder("bucketName")
                .WithDescription("bucketDesc")
                .WithMaxBucketSize(555)
                .WithTtl(Duration.OfMillis(777))
                .WithStorageType(StorageType.Memory)
                .WithReplicas(2)
                .WithPlacement(p)
                .Build();
            Validate(bc);

            Validate(ObjectStoreConfiguration.Builder(bc).Build());

            Validate(ObjectStoreConfiguration.Instance(Encoding.UTF8.GetString(bc.BackingConfig.Serialize())));
        }

        private void Validate(ObjectStoreConfiguration osc)
        {
            Assert.Equal("bucketName", osc.BucketName);
            Assert.Equal("bucketDesc", osc.Description);
            Assert.Equal(555, osc.MaxBucketSize);
            Assert.Equal(Duration.OfMillis(777), osc.Ttl);
            Assert.Equal(StorageType.Memory, osc.StorageType);
            Assert.Equal(2, osc.Replicas);
            Assert.NotNull(osc.Placement);
            Assert.Equal("cluster", osc.Placement.Cluster);
            Assert.Equal(2, osc.Placement.Tags.Count);
        }

        [Fact]
        public void TestObjectInfoConstruction()
        {
            string json = ReadDataFile("ObjectInfo.json");
            DateTime now = DateTime.UtcNow;
            ObjectInfo oi = new ObjectInfo(json, now);
            ValidateObjectInfo(oi, now);

            now = DateTime.UtcNow;
            oi = new ObjectInfo(Encoding.UTF8.GetString(oi.Serialize()), now);
            ValidateObjectInfo(oi, now);
        }

        void ValidateObjectInfo(ObjectInfo oi, DateTime modified)
        {
            Assert.Equal(BUCKET, oi.Bucket);
            Assert.Equal("object-name", oi.ObjectName);
            Assert.Equal("object-desc", oi.Description);
            Assert.Equal(344000, oi.Size);
            Assert.Equal(42, oi.Chunks);
            Assert.Equal(8196, oi.ObjectMeta.ObjectMetaOptions.ChunkSize);
            Assert.Equal("nuidnuidnuid", oi.Nuid);
            Assert.Equal("SHA-256=abcdefghijklmnopqrstuvwxyz=", oi.Digest);
            Assert.True(oi.IsDeleted);
            Assert.Equal(modified, oi.Modified);
            Assert.Equal(2, oi.Headers.Count);
            IList<string> list = oi.Headers.GetValues(Key(1));
            Assert.NotNull(list);
            Assert.Equal(1, list.Count);
            Assert.Equal(Data(1), oi.Headers[Key(1)]);
            list = oi.Headers.GetValues(Key(2));
            Assert.NotNull(list);
            Assert.Equal(2, list.Count);
            Assert.True(list.Contains(Data(21)));
            Assert.True(list.Contains(Data(22)));
            
            IDictionary<string, string> meta = oi.Metadata;
            Assert.NotNull(meta);
            Assert.Equal(2, meta.Count);
            Assert.Equal("meta-data-1", meta["meta-key-1"]);
            Assert.Equal("meta-data-2", meta["meta-key-2"]);
        }

        [Fact]
        public void TestObjectInfoCoverage()
        {
            ObjectLink link1A = ObjectLink.ForObject("bucket", "name");
            ObjectLink link1B = ObjectLink.ForObject("bucket", "name");
            ObjectLink link2 = ObjectLink.ForObject("bucket", "name2");
            ObjectLink blink1A = ObjectLink.ForBucket("bucket");
            ObjectLink blink1B = ObjectLink.ForBucket("bucket");
            ObjectLink blink2 = ObjectLink.ForBucket("bucket2");

            ObjectMetaOptions metaOptions = ObjectMetaOptions.Builder().WithLink(link1A).WithChunkSize(1024).Build();
            ObjectMetaOptions metaOptions2 = ObjectMetaOptions.Builder().WithLink(link1A).WithChunkSize(2048).Build();
            ObjectMetaOptions metaOptionsL = ObjectMetaOptions.Builder().WithLink(link1A).Build();
            ObjectMetaOptions metaOptionsL2 = ObjectMetaOptions.Builder().WithLink(link2).Build();
            ObjectMetaOptions metaOptionsC = ObjectMetaOptions.Builder().WithChunkSize(1024).Build();

            LinkCoverage(link1A, link1B, link2, blink1A, blink1B, blink2);
            MetaOptionsCoverage(metaOptions, metaOptions2, metaOptionsL, metaOptionsL2, metaOptionsC);
            MetaCoverage(link1A, link2);
            InfoCoverage(link1A, link2);
        }

        private void LinkCoverage(ObjectLink link1A, ObjectLink link1B, ObjectLink link2, ObjectLink blink1A,
            ObjectLink blink1B, ObjectLink blink2)
        {
            Assert.True(link1A.IsObjectLink);
            Assert.False(link1A.IsBucketLink);
            Assert.False(blink1A.IsObjectLink);
            Assert.True(blink1A.IsBucketLink);

            Assert.Equal(link1A, link1A);
            Assert.Equal(link1A, link1B);
            Assert.Equal(link1A, ObjectLink.ForObject(link1A.Bucket, link1A.ObjectName));
            Assert.Equal(blink1A, blink1A);
            Assert.Equal(blink1A, blink1B);
            Assert.NotEqual(link1A, link2);
            Assert.NotEqual(link1A, blink1A);
            Assert.NotEqual(blink1A, link1A);
            Assert.NotEqual(blink1A, link2);
            Assert.NotEqual(blink1A, blink2);

            Assert.True(link1A.GetHashCode() != 0);
            Assert.True(link2.GetHashCode() != 0);
            Assert.True(blink1A.GetHashCode() != 0);
            Assert.True(blink2.GetHashCode() != 0);
        }

        private void MetaOptionsCoverage(ObjectMetaOptions metaOptions, ObjectMetaOptions metaOptions2,
            ObjectMetaOptions metaOptionsL, ObjectMetaOptions metaOptionsL2, ObjectMetaOptions metaOptionsC)
        {
            Assert.True(metaOptions.HasData);
            Assert.True(metaOptionsL.HasData);
            Assert.True(metaOptionsC.HasData);
            Assert.False(ObjectMetaOptions.Builder().Build().HasData);

            Assert.Equal(metaOptions, metaOptions);
            Assert.NotEqual(metaOptions, metaOptions2);
            Assert.NotEqual(metaOptions, metaOptionsL);
            Assert.NotEqual(metaOptions, metaOptionsC);
            Assert.NotEqual(metaOptionsC, metaOptionsL);
            Assert.NotEqual(metaOptionsL, metaOptionsC);
            Assert.NotEqual(metaOptionsL, metaOptionsL2);
            Assert.NotEqual(metaOptionsL2, metaOptionsL);

            Assert.True(metaOptions.GetHashCode() != 0); // coverage
            Assert.True(metaOptions2.GetHashCode() != 0); // coverage
            Assert.True(metaOptionsL.GetHashCode() != 0); // coverage
            Assert.True(metaOptionsC.GetHashCode() != 0); // coverage
        }

        private void MetaCoverage(ObjectLink link, ObjectLink link2)
        {
            ObjectMeta meta1A = ObjectMeta.ForObjectName("meta");
            ObjectMeta meta1B = ObjectMeta.ForObjectName("meta");
            ObjectMeta meta1C = ObjectMeta.ForObjectName("diff");
            ObjectMeta meta2A = ObjectMeta.Builder("meta").WithDescription("desc").Build();
            ObjectMeta meta2B = ObjectMeta.Builder("meta").WithDescription("desc").Build();
            ObjectMeta meta2C = ObjectMeta.Builder("meta").WithDescription("diff").Build();
            ObjectMeta meta3A = ObjectMeta.Builder("meta").WithHeaders(new MsgHeader { ["key"] = "data" }).Build();
            ObjectMeta meta3B = ObjectMeta.Builder("meta").WithHeaders(new MsgHeader { ["key"] = "data" }).Build();
            ObjectMeta meta3C = ObjectMeta.Builder("meta").WithHeaders(new MsgHeader { ["key"] = "diff" }).Build();
            ObjectMeta meta4A = ObjectMeta.Builder("meta").WithLink(link).Build();
            ObjectMeta meta4B = ObjectMeta.Builder("meta").WithLink(link).Build();
            ObjectMeta meta4C = ObjectMeta.Builder("meta").WithLink(link2).Build();

            ObjectMeta metaH = ObjectMeta.Builder("meta").WithHeaders(new MsgHeader { ["key"] = "data" }).WithHeaders(null).Build();
            Assert.Equal(0, metaH.Headers.Count);

            Assert.Equal(meta1A, meta1A);
            Assert.Equal(meta1A, meta1B);
            Assert.NotEqual(meta1A, meta1C);
            Assert.NotEqual(meta1A, meta2A);
            Assert.NotEqual(meta1A, meta3A);
            Assert.NotEqual(meta1A, meta4A);

            Assert.Equal(meta2A, meta2A);
            Assert.Equal(meta2A, meta2B);
            Assert.NotEqual(meta2A, meta2C);
            Assert.NotEqual(meta2A, meta1A);

            Assert.Equal(meta3A, meta3A);
            Assert.Equal(meta3A, meta3B);
            Assert.NotEqual(meta3A, meta3C);
            Assert.NotEqual(meta3A, meta1A);

            Assert.Equal(meta4A, meta4A);
            Assert.Equal(meta4A, meta4B);
            Assert.NotEqual(meta4A, meta4C);
            Assert.NotEqual(meta4A, meta1A);
        }

        private void InfoCoverage(ObjectLink link1, ObjectLink link2)
        {
            ObjectInfo info1A = ObjectInfo.Builder("buck", "name").Build();
            ObjectInfo info1B = ObjectInfo.Builder("buck", "name").Build();
            ObjectInfo info1C = ObjectInfo.Builder("buck2", "name").Build();

            ObjectInfo info2A = ObjectInfo.Builder("buck", "name").WithSize(1).Build();
            ObjectInfo info2B = ObjectInfo.Builder("buck", "name").WithSize(1).Build();
            ObjectInfo info2C = ObjectInfo.Builder("buck", "name").WithSize(2).Build();

            ObjectInfo info3A = ObjectInfo.Builder("buck", "name").WithChunks(1).Build();
            ObjectInfo info3B = ObjectInfo.Builder("buck", "name").WithChunks(1).Build();
            ObjectInfo info3C = ObjectInfo.Builder("buck", "name").WithChunks(2).Build();

            ObjectInfo info4A = ObjectInfo.Builder("buck", "name").WithDeleted(true).Build();
            ObjectInfo info4B = ObjectInfo.Builder("buck", "name").WithDeleted(true).Build();
            ObjectInfo info4C = ObjectInfo.Builder("buck", "name").WithDeleted(false).Build();

            ObjectInfo info5A = ObjectInfo.Builder("buck", "name").WithNuid("1").Build();
            ObjectInfo info5B = ObjectInfo.Builder("buck", "name").WithNuid("1").Build();
            ObjectInfo info5C = ObjectInfo.Builder("buck", "name").WithNuid("2").Build();

            DateTime mod1 = DateTime.UtcNow;
            DateTime mod2 = mod1.AddDays(-1);
            ObjectInfo info6A = ObjectInfo.Builder("buck", "name").WithModified(mod1).Build();
            ObjectInfo info6B = ObjectInfo.Builder("buck", "name").WithModified(mod1).Build();
            ObjectInfo info6C = ObjectInfo.Builder("buck", "name").WithModified(mod2).Build();

            ObjectInfo info7A = ObjectInfo.Builder("buck", "name").WithDigest("1").Build();
            ObjectInfo info7B = ObjectInfo.Builder("buck", "name").WithDigest("1").Build();
            ObjectInfo info7C = ObjectInfo.Builder("buck", "name").WithDigest("2").Build();

            ObjectInfo info8A = ObjectInfo.Builder("buck", "name").WithLink(link1).Build();
            ObjectInfo info8B = ObjectInfo.Builder("buck", "name").WithLink(link1).Build();
            ObjectInfo info8C = ObjectInfo.Builder("buck", "name").WithLink(link2).Build();

            ObjectInfo.Builder("buck", "name").WithOptions(ObjectMetaOptions.Builder().Build()).Build(); // coverage

            Assert.Equal(info1A, info1B);
            Assert.NotEqual(info1A, info1C);
            Assert.Equal(info1A, info1A);
            Assert.NotEqual(info1A, info2A);
            Assert.NotEqual(info1A, info3A);
            Assert.NotEqual(info1A, info4A);
            Assert.NotEqual(info1A, info5A);
            Assert.NotEqual(info1A, info6A);
            Assert.NotEqual(info1A, info7A);
            Assert.NotEqual(info1A, info8A);

            Assert.Equal(info2A, info2A);
            Assert.Equal(info2A, info2B);
            Assert.NotEqual(info2A, info2C);
            Assert.NotEqual(info2A, info1A);

            Assert.Equal(info3A, info3A);
            Assert.Equal(info3A, info3B);
            Assert.NotEqual(info3A, info3C);
            Assert.NotEqual(info3A, info1A);

            Assert.Equal(info4A, info4A);
            Assert.Equal(info4A, info4B);
            Assert.NotEqual(info4A, info4C);
            Assert.NotEqual(info4A, info1A);

            Assert.Equal(info5A, info5A);
            Assert.Equal(info5A, info5B);
            Assert.NotEqual(info5A, info5C);
            Assert.NotEqual(info5A, info1A);

            Assert.Equal(info6A, info6A);
            Assert.Equal(info6A, info6B);
            Assert.NotEqual(info6A, info6C);
            Assert.NotEqual(info6A, info1A);

            Assert.Equal(info7A, info7A);
            Assert.Equal(info7A, info7B);
            Assert.NotEqual(info7A, info7C);
            Assert.NotEqual(info7A, info1A);

            Assert.Equal(info8A, info8A);
            Assert.Equal(info8A, info8B);
            Assert.NotEqual(info8A, info8C);
            Assert.NotEqual(info8A, info1A);
        }
        
        [Fact]
        public void TestConstructionInvalidsCoverage() {
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().Build());
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(null));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(string.Empty));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(HasSpace));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(HasPrintable));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(HasDot));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(StarNotSegment));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(GtNotSegment));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithName(HasDollar));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder(Has127));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder(HasFwdSlash));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder(HasBackSlash));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder(HasEquals));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder(HasTic));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithMaxBucketSize(0));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithMaxBucketSize(-2));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithTtl(Duration.OfNanos(-1)));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithReplicas(0));
            Assert.Throws<ArgumentException>(() => ObjectStoreConfiguration.Builder().WithReplicas(6));
        }
    }
}
