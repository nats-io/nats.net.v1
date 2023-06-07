// Copyright 2021-2023 The NATS Authors
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
using static NATS.Client.ObjectStore.ObjectStoreUtil;
using static NATS.Client.ObjectStore.ObjectStoreWatchOption;

namespace IntegrationTests
{
    [SuppressMessage("ReSharper", "ParameterOnlyUsedForPreconditionCheck.Local")]
    public class TestObjectStore : TestSuite<OneServerSuiteContext>
    {
        public TestObjectStore(OneServerSuiteContext context) : base(context) { }

        [Fact]
        public void TestWorkFlow()
        {
            string bucketName = Bucket(Nuid.NextGlobal());
            
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();
                nc.CreateObjectStoreManagementContext(ObjectStoreOptions.Builder(DefaultJsOptions).Build()); // coverage

                // create the bucket
                ObjectStoreConfiguration osc = ObjectStoreConfiguration.Builder(bucketName)
                    .WithDescription(Plain)
                    .WithTtl(Duration.OfHours(24))
                    .WithStorageType(StorageType.Memory)
                    .Build();

                ObjectStoreStatus status = osm.Create(osc);
                ValidateStatus(bucketName, status);
                ValidateStatus(bucketName, osm.GetStatus(bucketName));

                IJetStreamManagement jsm = nc.CreateJetStreamManagementContext();
                Assert.NotNull(jsm.GetStreamInfo("OBJ_" + bucketName));

                IList<string> names = osm.GetBucketNames();
                Assert.Equal(1, names.Count);
                Assert.True(names.Contains(bucketName));

                // put some objects into the stores
                IObjectStore os = nc.CreateObjectStoreContext(bucketName);
                ValidateStatus(bucketName, os.GetStatus());

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

                FileInfo fileInfo = (FileInfo)input[1];
                ObjectInfo oi1 = ValidateObjectInfo(bucketName, os.Put(meta, fileInfo.OpenRead()), len, expectedChunks, 4096);

                MemoryStream baos = ValidateGet(bucketName, os, len, expectedChunks, 4096);
                byte[] bytes = baos.ToArray();
                byte[] bytes4K = new byte[4096];
                for (int x = 0; x < 4096; x++)
                {
                    bytes4K[x] = bytes[x];
                }

                ObjectInfo oi2 = ValidateObjectInfo(bucketName, os.Put(meta, new MemoryStream(bytes4K)), 4096, 1, 4096);
                ValidateGet(bucketName, os, 4096, 1, 4096);

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

                // delete object
                ObjectInfo delInfo = os.Delete(oi3.ObjectName);
                Assert.Equal(oi3.ObjectName, delInfo.ObjectName);
                Assert.True(delInfo.IsDeleted);

                // delete an object a second time is fine
                delInfo = os.Delete(oi3.ObjectName); // the second covers a path where the object is already deleted
                Assert.Equal(oi3.ObjectName, delInfo.ObjectName);
                Assert.True(delInfo.IsDeleted);

                // try to update meta for deleted
                ObjectInfo oiWillError = oi3;
                AssertClientError(OsObjectIsDeleted, () => os.UpdateMeta(oiWillError.ObjectName, ObjectMeta.ForObjectName("notFound")));

                // can't update meta to an existing object
                ObjectInfo oi = os.Put("another1", Encoding.UTF8.GetBytes("another1"));
                os.Put("another2", Encoding.UTF8.GetBytes("another2"));
                AssertClientError(OsObjectAlreadyExists, () => os.UpdateMeta("another1", ObjectMeta.ForObjectName("another2")));

                // but you can update a name to a deleted object's name
                os.UpdateMeta(oi.ObjectName, ObjectMeta.ForObjectName(oi3.ObjectName));

                // alternate puts
                String name = "put-name-input-coverage";
                expectedChunks = len / DefaultChunkSize;
                if (expectedChunks * DefaultChunkSize < len) {
                    expectedChunks++;
                }
                ValidateObjectInfo(bucketName, os.Put(name, fileInfo.OpenRead()), name, null, false, len, expectedChunks, DefaultChunkSize);

                name = fileInfo.Name;
                ValidateObjectInfo(bucketName, os.Put(fileInfo), name, null, false, len, expectedChunks, DefaultChunkSize);
            });
        }

        private static void ValidateStatus(string bucketName, ObjectStoreStatus status)
        {
            Assert.Equal(bucketName, status.BucketName);
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
        }

        private MemoryStream ValidateGet(string bucketName, IObjectStore os, long len, long chunks, int chunkSize) {
            MemoryStream ms = new MemoryStream();
            ObjectInfo oi = os.Get("object-name", ms);
            byte[] bytes = ms.ToArray();
            Assert.Equal(len, bytes.Length);
            ValidateObjectInfo(bucketName, oi, len, chunks, chunkSize);
            return ms;
        }

        private ObjectInfo ValidateObjectInfo(string bucketName, ObjectInfo oi, long size, long chunks, int chunkSize)
        {
            return ValidateObjectInfo(bucketName, oi, "object-name", "object-desc", true, size, chunks, chunkSize);
        }

        private ObjectInfo ValidateObjectInfo(string bucketName, ObjectInfo oi, string objectName, string objectDesc,
            bool headers, long size, long chunks, int chunkSize) {
            Assert.Equal(bucketName, oi.Bucket);
            Assert.Equal(objectName, oi.ObjectName);
            if (objectDesc == null) {
                Assert.Null(oi.Description);
            }
            else {
                Assert.Equal(objectDesc, oi.Description);
            }

            Assert.Equal(size, oi.Size);
            Assert.Equal(chunks, oi.Chunks);
            Assert.NotNull(oi.Nuid);
            Assert.False(oi.IsDeleted);
            Assert.True(oi.Modified > DateTime.MinValue);
            if (chunkSize > 0) {
                Assert.Equal(chunkSize, oi.ObjectMeta.ObjectMetaOptions.ChunkSize);
            }

            if (headers)
            {
                Assert.NotNull(oi.Headers);
                Assert.Equal(2, oi.Headers.Count);
                IList<string> list = oi.Headers.GetValues(Key(1));
                Assert.Equal(1, list.Count);
                Assert.Equal(Data(1), oi.Headers[Key(1)]);
                list = oi.Headers.GetValues(Key(2));
                Assert.Equal(2, list.Count);
                Assert.True(list.Contains(Data(21)));
                Assert.True(list.Contains(Data(22)));
            }

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
        public void TestManageGetBucketNamesStatuses() {
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

                IList<ObjectStoreStatus> infos = osm.GetStatuses();
                Assert.Equal(2, infos.Count);
                IList<string> buckets = new List<string>();
                foreach (ObjectStoreStatus status in infos) {
                    buckets.Add(status.BucketName);
                }
                Assert.Equal(2, buckets.Count);
                Assert.True(buckets.Contains(Bucket(1)));
                Assert.True(buckets.Contains(Bucket(2)));

                buckets = osm.GetBucketNames();
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
                
                // can overwrite a deleted link or another link
                os1.Put("overwrite", Encoding.UTF8.GetBytes("over"));
                os1.Delete("overwrite");
                os1.AddLink("overwrite", info11); // over deleted
                os1.AddLink("overwrite", info11); // over another link

                // can't overwrite object with a bucket link
                AssertClientError(OsObjectAlreadyExists, () => os1.AddBucketLink(info11.ObjectName, os1));
                
                // can overwrite a deleted link or another link
                os1.Delete("overwrite");
                os1.AddBucketLink("overwrite", os1);
                os1.AddBucketLink("overwrite", os1);

                // Link to individual object.
                ObjectInfo linkTo11 = os1.AddLink("linkTo11", info11);
                ValidateLink(linkTo11, "linkTo11", info11, null);

                // link to a link
                AssertClientError(OsCantLinkToLink, () => os1.AddLink("linkToLinkIsErr", linkTo11));

                os2.Put(Key(21), Encoding.UTF8.GetBytes("21"));
                ObjectInfo info21 = os2.GetInfo(Key(21));

                ObjectInfo crossLink = os2.AddLink("crossLink", info11);
                ValidateLink(crossLink, "crossLink", info11, null);
                MemoryStream mem = new MemoryStream();
                ObjectInfo crossGet = os2.Get(crossLink.ObjectName, mem);
                Assert.Equal(info11, crossGet);
                Assert.Equal(2, mem.Length);

                ObjectInfo bucketLink = os2.AddBucketLink("bucketLink", os1);
                ValidateLink(bucketLink, "bucketLink", null, os1);

                // can't get a bucket
                AssertClientError(OsGetLinkToBucket, () => os2.Get(bucketLink.ObjectName, new MemoryStream()));

                // getInfo targetWillBeDeleted still gets info b/c the link is there
                ObjectInfo targetWillBeDeleted = os2.AddLink("targetWillBeDeleted", info21);
                ValidateLink(targetWillBeDeleted, "targetWillBeDeleted", info21, null);
                os2.Delete(info21.ObjectName);
                ObjectInfo oiDeleted = os2.GetInfo(info21.ObjectName, true);
                Assert.True(oiDeleted.IsDeleted); // object is deleted but includingDeleted = true
                Assert.Null(os2.GetInfo(info21.ObjectName)); // does includingDeleted = false
                Assert.Null(os2.GetInfo(info21.ObjectName, false)); // explicit includingDeleted = false
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
                
                ObjectMeta linkInPut = ObjectMeta.Builder("linkInPut").WithLink(ObjectLink.ForObject("na", "na")).Build();
                AssertClientError(OsLinkNotAllowOnPut, () => os2.Put(linkInPut, new MemoryStream()));
            });
        }

        private void ValidateLink(ObjectInfo oiLink, string linkName, ObjectInfo targetStore, IObjectStore targetBucket) {
            Assert.Equal(linkName, oiLink.ObjectName);
            Assert.NotNull(oiLink.Nuid);
            Assert.True(oiLink.Modified > DateTime.MinValue);

            Assert.NotNull(oiLink.Link);
            if (targetStore == null) { // link to bucket
                Assert.True(oiLink.Link.IsBucketLink);
                Assert.False(oiLink.Link.IsObjectLink);
                Assert.Null(oiLink.Link.ObjectName);
                Assert.Equal(oiLink.Link.Bucket, targetBucket.BucketName);
            }
            else {
                Assert.False(oiLink.Link.IsBucketLink);
                Assert.True(oiLink.Link.IsObjectLink);
                Assert.Equal(oiLink.Link.ObjectName, targetStore.ObjectName);
                Assert.Equal(oiLink.Link.Bucket, targetStore.Bucket);
            }
        }

        [Fact]
        public void TestList()
        {
            string bucketName = Bucket(Nuid.NextGlobal());
            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();
                osm.Create(ObjectStoreConfiguration.Builder(bucketName).WithStorageType(StorageType.Memory).Build());

                IObjectStore os = nc.CreateObjectStoreContext(bucketName);

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
            string bucketName = Bucket(Nuid.NextGlobal());

            Context.RunInJsServer(nc =>
            {
                IObjectStoreManagement osm = nc.CreateObjectStoreManagementContext();

                osm.Create(ObjectStoreConfiguration.Builder(bucketName)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IObjectStore os = nc.CreateObjectStoreContext(bucketName);
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
