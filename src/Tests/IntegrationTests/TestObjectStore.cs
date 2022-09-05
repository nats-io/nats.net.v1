// Copyright 2021 The NATS Authors
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
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using NATS.Client.ObjectStore;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.ClientExDetail;
using static NATS.Client.JetStream.JetStreamOptions;
using static NATS.Client.ObjectStore.ObjectStoreWatchOption;

namespace IntegrationTests
{
    [SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
    public class TestObjectStore : TestSuite<ObjectStoreSuiteContext>
    {
        public TestObjectStore(ObjectStoreSuiteContext context) : base(context) { }

        [Fact]
        public void TestWorkFlow()
        {
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();

                // create the bucket
                ObjectStoreConfiguration osc = ObjectStoreConfiguration.Builder(BUCKET)
                    .WithDescription(Plain)
                    .WithTtl(Duration.OfHours(24))
                    .WithStorageType(StorageType.Memory)
                    .Build();

                ObjectStoreStatus status = osm.Create(osc);
                Assert.Equal(BUCKET, status.BucketName);
                Assert.Equal(Plain, status.Description);
                Assert.False(status.Sealed);
                Assert.Equal(0U, status.Size);
                Assert.Equal(Duration.OfHours(24), status.Ttl);
                Assert.Equal(StorageType.Memory, status.StorageType);
                Assert.Equal(1, status.Replicas);
                Assert.Null(status.Placement);
                Assert.NotNull(status.Config); // coverage
                Assert.NotNull(status.BackingStreamInfo); // coverage
                Assert.Equal("JetStream", status.BackingStore);

                IJetStreamManagement jsm = nc.CreateJetStreamManagementContext();
                Assert.NotNull(jsm.GetStreamInfo("OBJ_" + BUCKET));

                IList<string> names = osm.GetBucketNames();
                Assert.Equal(1, names.Count);
                Assert.True(names.Contains(BUCKET));

                // put some objects into the stores
                IObjectStore os = nc.CreateObjectStoreContext(BUCKET);

                // object not found errors
                AssertClientError(OsObjectNotFound, () => os.Get("notFound", new MemoryStream()));
                AssertClientError(OsObjectNotFound, () => os.UpdateMeta("notFound", ObjectMeta.ForObjectName("notFound")));
                AssertClientError(OsObjectNotFound, () => os.Delete("notFound"));

                MsgHeader h = new MsgHeader
                {
                    [Key(1)] = Data(1),
                    [Key(2)] = Data(21), 
                };
                h.Add(Key(2), Data(22));

                ObjectMeta meta = ObjectMeta.Builder("object-name")
                    .WithDescription("object-desc")
                    .WithHeaders(h)
                    .WithChunkSize(4096)
                    .Build();

                Object[] input = GetInput(4096 * 10, ".", long.MaxValue, null);
                long len = (long)input[0];
                long expectedChunks = len / 4096;
                if (expectedChunks * 4096 < len) {
                    expectedChunks++;
                }
                ObjectInfo oi1 = ValidateObjectInfo(os.Put(meta, ((FileInfo)input[1]).OpenRead()), len, expectedChunks, 4096);

                MemoryStream baos = ValidateGet(os, len, expectedChunks, 4096);
                byte[] bytes = baos.ToArray();
                byte[] bytes4K = new byte[4096];
                for (int x = 0; x < 4096; x++)
                {
                    bytes4K[x] = bytes[x];
                }

                ObjectInfo oi2 = ValidateObjectInfo(os.Put(meta, new MemoryStream(bytes4K)), 4096, 1, 4096);
                ValidateGet(os, 4096, 1, 4096);

                Assert.NotEqual(oi1.Nuid, oi2.Nuid);

                // update meta
                h = new MsgHeader
                {
                    ["newkey"] = "newval"
                };

                ObjectInfo oi3 = os.UpdateMeta(oi2.ObjectName,
                    ObjectMeta.Builder("new object name")
                        .WithDescription("new object description")
                        .WithHeaders(h).Build());
                Assert.Equal("new object name", oi3.ObjectName);
                Assert.Equal("new object description", oi3.Description);
                Assert.NotNull(oi3.Headers);
                Assert.Equal(1, oi3.Headers.Count);
                Assert.Equal("newval", oi3.Headers["newkey"]);

                // check the old object is not found at all
                AssertClientError(OsObjectNotFound, () => os.Get("object-name", new MemoryStream()));
                AssertClientError(OsObjectNotFound, () => os.UpdateMeta("object-name", ObjectMeta.ForObjectName("notFound")));

                // delete object, then try to update meta
                os.Delete(oi3.ObjectName);
                AssertClientError(OsObjectIsDeleted, () => os.UpdateMeta(oi3.ObjectName, ObjectMeta.ForObjectName("notFound")));

                // can't update meta to an existing object
                os.Put("another1", Encoding.UTF8.GetBytes("another1"));
                os.Put("another2", Encoding.UTF8.GetBytes("another2"));
                AssertClientError(OsObjectAlreadyExists, () => os.UpdateMeta("another1", ObjectMeta.ForObjectName("another2")));

            });
        }

        private void AssertClientError(ClientExDetail ced, Action testCode)
        {
            NATSJetStreamClientException e = Assert.Throws<NATSJetStreamClientException>(testCode);
            Assert.Contains(ced.Id, e.Message);
        }

        private MemoryStream ValidateGet(IObjectStore os, long len, long chunks, int chunkSize) {
            MemoryStream ms = new MemoryStream();
            ObjectInfo oi = os.Get("object-name", ms);
            byte[] bytes = ms.ToArray();
            Assert.Equal(len, bytes.Length);
            ValidateObjectInfo(oi, len, chunks, chunkSize);
            return ms;
        }

        private ObjectInfo ValidateObjectInfo(ObjectInfo oi, long size, long chunks, int chunkSize) {
            Assert.Equal(BUCKET, oi.Bucket);
            Assert.Equal("object-name", oi.ObjectName);
            Assert.Equal("object-desc", oi.Description);
            Assert.Equal(size, oi.Size);
            Assert.Equal(chunks, oi.Chunks);
            Assert.NotNull(oi.Nuid);
            Assert.False(oi.IsDeleted);
            Assert.True(oi.Modified > DateTime.MinValue);
            if (chunkSize > 0) {
                Assert.Equal(chunkSize, oi.ObjectMeta.ObjectMetaOptions.ChunkSize);
            }
            Assert.NotNull(oi.Headers);
            Assert.Equal(2, oi.Headers.Count);
            IList<string> list = oi.Headers.GetValues(Key(1));
            Assert.Equal(1, list.Count);
            Assert.Equal(Data(1), oi.Headers[Key(1)]);
            list = oi.Headers.GetValues(Key(2));
            Assert.Equal(2, list.Count);
            Assert.True(list.Contains(Data(21)));
            Assert.True(list.Contains(Data(22)));
            return oi;
        }

        private static object[] GetInput(int size, string path, long foundLen, FileInfo found)
        {
            foreach (string sf in Directory.GetFiles(path))
            {
                FileInfo f = new FileInfo(sf); 
                if (f.Length == size)
                {
                    return new object[] {f.Length, f};
                }

                if (f.Length >= size && f.Length < foundLen)
                {
                    foundLen = f.Length;
                    found = f;
                }
            }

            foreach (string sd in Directory.GetDirectories(path))
            {
                object[] recur = GetInput(size, sd, foundLen, found);
                long flen = (long)recur[0];
                if (flen == size)
                {
                    return recur;
                }
                foundLen = flen;
                found = (FileInfo)recur[1];
            }

            return new object[] {foundLen, found};
        }

        [Fact]
        public void TestManageGetBucketNames() {
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();

                // create bucket 1
                osm.Create(ObjectStoreConfiguration.Builder(Bucket(1))
                    .WithStorageType(StorageType.Memory)
                    .Build());

                // create bucket 2
                osm.Create(ObjectStoreConfiguration.Builder(Bucket(2))
                    .WithStorageType(StorageType.Memory)
                    .Build());

                CreateMemoryStream(nc, Stream(1));
                CreateMemoryStream(nc, Stream(2));

                IList<string> buckets = osm.GetBucketNames();
                Assert.Equal(2, buckets.Count);
                Assert.True(buckets.Contains(Bucket(1)));
                Assert.True(buckets.Contains(Bucket(2)));
            });
        }

        [Fact]
        public void TestObjectStoreOptionsBuilderCoverage() {
            AssertOso(ObjectStoreOptions.Builder().Build());
            AssertOso(ObjectStoreOptions.Builder().WithJetStreamOptions(DefaultJsOptions).Build());
            AssertOso(ObjectStoreOptions.Builder((ObjectStoreOptions) null).Build());
            AssertOso(ObjectStoreOptions.Builder(ObjectStoreOptions.Builder().Build()).Build());
            AssertOso(ObjectStoreOptions.Builder(DefaultJsOptions).Build());

            ObjectStoreOptions oso = ObjectStoreOptions.Builder().WithJsPrefix("prefix").Build();
            Assert.Equal("prefix.", oso.JSOptions.Prefix);
            Assert.False(oso.JSOptions.IsDefaultPrefix);

            oso = ObjectStoreOptions.Builder().WithJsDomain("domain").Build();
            Assert.Equal("$JS.domain.API.", oso.JSOptions.Prefix);
            Assert.False(oso.JSOptions.IsDefaultPrefix);

            oso = ObjectStoreOptions.Builder().WithRequestTimeout(Duration.OfSeconds(10)).Build();
            Assert.Equal(Duration.OfSeconds(10), oso.JSOptions.RequestTimeout);
        }

        private void AssertOso(ObjectStoreOptions oso) {
            JetStreamOptions jso = oso.JSOptions;
            Assert.Equal(DefaultJsOptions.RequestTimeout, jso.RequestTimeout);
            Assert.Equal(DefaultJsOptions.Prefix, jso.Prefix);
            Assert.Equal(DefaultJsOptions.IsDefaultPrefix, jso.IsDefaultPrefix);
            Assert.Equal(DefaultJsOptions.IsPublishNoAck, jso.IsPublishNoAck);
        }

        [Fact]
        public void TestObjectLinks() {
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();

                osm.Create(ObjectStoreConfiguration.Builder(Bucket(1)).WithStorageType(StorageType.Memory).Build());
                IObjectStore os1 = nc.CreateObjectStoreContext(Bucket(1));

                osm.Create(ObjectStoreConfiguration.Builder(Bucket(2)).WithStorageType(StorageType.Memory).Build());
                IObjectStore os2 = nc.CreateObjectStoreContext(Bucket(2));

                os1.Put(Key(11), Encoding.UTF8.GetBytes("11"));
                os1.Put(Key(12), Encoding.UTF8.GetBytes("12"));

                ObjectInfo info11 = os1.GetInfo(Key(11));
                ObjectInfo info12 = os1.GetInfo(Key(12));

                // can't overwrite object with a link
                AssertClientError(OsObjectAlreadyExists, () => os1.AddLink(info11.ObjectName, info11)); // can't overwrite object with a link

                // can't overwrite object with a bucket link
                AssertClientError(OsObjectAlreadyExists, () => os1.AddBucketLink(info11.ObjectName, os1));

                // Link to individual object.
                ObjectInfo linkTo11 = os1.AddLink("linkTo11", info11);
                ValidateLink(linkTo11, "linkTo11", info11, null);

                // link to a link
                AssertClientError(OsCantLinkToLink, () => os1.AddLink("linkToLinkIsErr", linkTo11));

                os2.Put(Key(21), Encoding.UTF8.GetBytes("21"));
                ObjectInfo info21 = os2.GetInfo(Key(21));

                ObjectInfo crossLink = os2.AddLink("crossLink", info11);
                ValidateLink(crossLink, "crossLink", info11, null);

                ObjectInfo bucketLink = os2.AddBucketLink("bucketLink", os1);
                ValidateLink(bucketLink, "bucketLink", null, os1);

                // getInfo targetWillBeDeleted still gets info b/c the link is there
                ObjectInfo targetWillBeDeleted = os2.AddLink("targetWillBeDeleted", info21);
                ValidateLink(targetWillBeDeleted, "targetWillBeDeleted", info21, null);
                os2.Delete(info21.ObjectName);
                ObjectInfo oiDeleted = os2.GetInfo(info21.ObjectName);
                Assert.True(oiDeleted.IsDeleted); // object is deleted
                AssertClientError(OsObjectIsDeleted, () => os2.AddLink("willException", oiDeleted));
                ObjectInfo oiLink = os2.GetInfo("targetWillBeDeleted");
                Assert.NotNull(oiLink); // link is still there but link target is deleted
                AssertClientError(OsObjectNotFound, () => os2.Get("targetWillBeDeleted", new MemoryStream()));
                AssertClientError(OsCantLinkToLink, () => os2.AddLink("willException", oiLink)); // can't link to link

                os2.AddLink("cross", info12);
                MemoryStream ms = new MemoryStream();
                ObjectInfo crossInfo12 = os2.Get("cross", ms);
                Assert.Equal(info12, crossInfo12);
                Assert.Equal(2, ms.Length);
                Assert.Equal("12", Encoding.UTF8.GetString(ms.ToArray()));
            });
        }

        private void ValidateLink(ObjectInfo oiLink, string linkName, ObjectInfo targetStore, IObjectStore targetBucket) {
            Assert.Equal(linkName, oiLink.ObjectName);
            Assert.NotNull(oiLink.Nuid);
            Assert.NotNull(oiLink.Modified);

            Assert.NotNull(oiLink.Link);
            if (targetStore == null) { // link to bucket
                Assert.Null(oiLink.Link.ObjectName);
                Assert.Equal(oiLink.Link.Bucket, targetBucket.BucketName);
            }
            else {
                Assert.Equal(oiLink.Link.ObjectName, targetStore.ObjectName);
                Assert.Equal(oiLink.Link.Bucket, targetStore.Bucket);
            }
        }

        [Fact]
        public void TestObjectList() {
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();
                osm.Create(ObjectStoreConfiguration.Builder(BUCKET).WithStorageType(StorageType.Memory).Build());

                IObjectStore os = nc.CreateObjectStoreContext(BUCKET);

                os.Put(Key(1), Encoding.UTF8.GetBytes("11"));
                os.Put(Key(2), Encoding.UTF8.GetBytes("21"));
                os.Put(Key(3), Encoding.UTF8.GetBytes("31"));
                ObjectInfo info = os.Put(Key(2), Encoding.UTF8.GetBytes("22"));

                os.AddLink(Key(4), info);

                os.Put(Key(9), Encoding.UTF8.GetBytes("91"));
                os.Delete(Key(9));

                IList<ObjectInfo> list = os.GetList();
                Assert.Equal(4, list.Count);

                IList<string> names = new List<string>();
                for (int x = 1; x <= 4; x++) {
                    names.Add(list[x-1].ObjectName);
                }

                for (int x = 1; x <= 4; x++) {
                    Assert.True(names.Contains(Key(x)));
                }
            });
        }

        [Fact]
        public void TestSeal() {
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();

                osm.Create(ObjectStoreConfiguration.Builder(BUCKET)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IObjectStore os = nc.CreateObjectStoreContext(BUCKET);
                os.Put("name", Encoding.UTF8.GetBytes("data"));

                os.Seal();

                Assert.Throws<NATSJetStreamException>(() => os.Put("another", Encoding.UTF8.GetBytes("data")));

                ObjectMeta meta = new ObjectMeta.ObjectMetaBuilder("change").Build();
                Assert.Throws<NATSJetStreamException>(() => os.UpdateMeta("name", meta));
            });
        }

        [Fact]
        public void TestWatch() {
            TestObjectStoreWatcher fullB4Watcher = new TestObjectStoreWatcher("fullB4Watcher", true);
            TestObjectStoreWatcher delB4Watcher = new TestObjectStoreWatcher("delB4Watcher", true, IgnoreDelete);

            TestObjectStoreWatcher fullAfterWatcher = new TestObjectStoreWatcher("fullAfterWatcher", false);
            TestObjectStoreWatcher delAfterWatcher = new TestObjectStoreWatcher("delAfterWatcher", false, IgnoreDelete);

            Context.RunInJsServer(nc =>
            {
                _testWatch(nc, fullB4Watcher, new Object[]{"A", "B", null}, os => os.Watch(fullB4Watcher, fullB4Watcher.WatchOptions));
                _testWatch(nc, delB4Watcher, new Object[]{"A", "B"}, os => os.Watch(delB4Watcher, delB4Watcher.WatchOptions));
                _testWatch(nc, fullAfterWatcher, new Object[]{"B", null}, os => os.Watch(fullAfterWatcher, fullAfterWatcher.WatchOptions));
                _testWatch(nc, delAfterWatcher, new Object[]{"B"}, os => os.Watch(delAfterWatcher, delAfterWatcher.WatchOptions));
            });
        }

        delegate ObjectStoreWatchSubscription GetOsWatchSub(IObjectStore os);

        private void _testWatch(IConnection nc, TestObjectStoreWatcher watcher, object[] expecteds, GetOsWatchSub supplier)
        {
            IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();

            String bucket = watcher.Name + "Bucket";
            osm.Create(ObjectStoreConfiguration.Builder(bucket).WithStorageType(StorageType.Memory).Build());

            IObjectStore os = nc.CreateObjectStoreContext(bucket);

            ObjectStoreWatchSubscription sub = null;

            if (watcher.BeforeWatcher)
            {
                sub = supplier.Invoke(os);
            }

            byte[] a = Encoding.UTF8.GetBytes("A23456789012345");
            byte[] b = Encoding.UTF8.GetBytes("B23456789012345");

            os.Put(ObjectMeta.Builder("A").WithChunkSize(ChunkSize).Build(), new MemoryStream(a));
            os.Put(ObjectMeta.Builder("B").WithChunkSize(ChunkSize).Build(), new MemoryStream(b));
            os.Delete("A");

            if (!watcher.BeforeWatcher) {
                sub = supplier.Invoke(os);
            }

            Thread.Sleep(1500); // give time for the watches to get messages

            ValidateWatcher(expecteds, watcher);

            // ReSharper disable once PossibleNullReferenceException
            sub.Unsubscribe();
            
            osm.Delete(bucket);
        }

        const int Size = 15;
        const int Chunks = 2;
        const int ChunkSize = 10;

        private void ValidateWatcher(object[] expecteds, TestObjectStoreWatcher watcher) {
            Assert.Equal(expecteds.Length, watcher.Entries.Count);
            Assert.Equal(1, watcher.EndOfDataReceived);

            Assert.Equal(watcher.BeforeWatcher, watcher.EndBeforeEntries);

            int aix = 0;
            DateTime lastMod = DateTime.MinValue;

            foreach (ObjectInfo oi in watcher.Entries) {

                Assert.True(oi.Modified >= lastMod);
                lastMod = oi.Modified;

                Assert.NotNull(oi.Nuid);

                Object expected = expecteds[aix++];
                if (expected == null) {
                    Assert.True(oi.IsDeleted);
                }
                else {
                    Assert.Equal(ChunkSize, oi.ObjectMeta.ObjectMetaOptions.ChunkSize);
                    Assert.Equal(Chunks, oi.Chunks);
                    Assert.Equal(Size, oi.Size);
                }
            }
        }
    }
    
    class TestObjectStoreWatcher : IObjectStoreWatcher
    {
        public string Name;
        public IList<ObjectInfo> Entries = new List<ObjectInfo>();
        public ObjectStoreWatchOption[] WatchOptions;
        public bool BeforeWatcher;
        public int EndOfDataReceived;
        public bool EndBeforeEntries;

        public TestObjectStoreWatcher(string name, bool beforeWatcher, params ObjectStoreWatchOption[] watchOptions)
        {
            Name = name;
            BeforeWatcher = beforeWatcher;
            WatchOptions = watchOptions;
        }

        public void Watch(ObjectInfo oi) {
            Entries.Add(oi);
        }

        public void EndOfData() {
            if (++EndOfDataReceived == 1 && Entries.Count == 0) {
                EndBeforeEntries = true;
            }
        }
    }
}
