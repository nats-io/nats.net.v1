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
using System.Linq;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestKeyValue : TestSuite<KeyValueSuiteContext>
    {
        public TestKeyValue(KeyValueSuiteContext context) : base(context)
        {
        }

        [Fact]
        public void TestWorkFLow()
        {
            DateTime now = DateTime.Now;

            string byteKey = "byteKey";
            string stringKey = "stringKey";
            string longKey = "longKey";
            string notFoundKey = "notFound";
            string byteValue1 = "Byte Value 1";
            string byteValue2 = "Byte Value 2";
            string stringValue1 = "string Value 1";
            string stringValue2 = "string Value 2";

            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create the bucket
                KeyValueConfiguration kvc = KeyValueConfiguration.Builder()
                    .WithName(BUCKET)
                    .WithMaxHistoryPerKey(3)
                    .WithStorageType(StorageType.Memory)
                    .Build();

                KeyValueStatus status = kvm.Create(kvc);

                kvc = status.Config;
                Assert.Equal(BUCKET, status.BucketName);
                Assert.Equal(BUCKET, kvc.BucketName);
                Assert.Equal(3, status.MaxHistoryPerKey);
                Assert.Equal(3, kvc.MaxHistoryPerKey);
                Assert.Equal(-1, status.MaxBucketSize);
                Assert.Equal(-1, kvc.MaxBucketSize);
                Assert.Equal(-1, status.MaxValueSize);
                Assert.Equal(-1, kvc.MaxValueSize);
                Assert.Equal(0, status.Ttl);
                Assert.Equal(0, kvc.Ttl);
                Assert.Equal(StorageType.Memory, status.StorageType);
                Assert.Equal(StorageType.Memory, kvc.StorageType);
                Assert.Equal(1, status.Replicas);
                Assert.Equal(1, kvc.Replicas);
                Assert.Equal(0U, status.EntryCount);
                Assert.Equal("JetStream", status.BackingStore);

                // get the kv context for the specific bucket
                IKeyValue kv = c.CreateKeyValueContext(BUCKET);

                // Put some keys. Each key is put in a subject in the bucket (stream)
                // The put returns the sequence number in the bucket (stream)
                Assert.Equal(1U, kv.Put(byteKey, Encoding.UTF8.GetBytes(byteValue1)));
                Assert.Equal(2U, kv.Put(stringKey, stringValue1));
                Assert.Equal(3U, kv.Put(longKey, 1));

                // retrieve the values. all types are stored as bytes
                // so you can always get the bytes directly
                Assert.Equal(byteValue1, Encoding.UTF8.GetString(kv.Get(byteKey).Value));
                Assert.Equal(stringValue1, Encoding.UTF8.GetString(kv.Get(stringKey).Value));
                Assert.Equal("1", Encoding.UTF8.GetString(kv.Get(longKey).Value));

                // if you know the value is not binary and can safely be read
                // as a UTF-8 string, the getStringValue method is ok to use
                Assert.Equal(byteValue1, kv.Get(byteKey).ValueAsString());
                Assert.Equal(stringValue1, kv.Get(stringKey).ValueAsString());
                Assert.Equal("1", kv.Get(longKey).ValueAsString());

                // if you know the value is a long, you can use the TryGetLongValue method
                long lvalue;
                Assert.True(kv.Get(longKey).TryGetLongValue(out lvalue));
                Assert.Equal(1, lvalue);
                Assert.False(kv.Get(stringKey).TryGetLongValue(out lvalue));
                Assert.Equal(0, lvalue);

                // going to manually track history for verification later
                IList<KeyValueEntry> byteHistory = new List<KeyValueEntry>();
                IList<KeyValueEntry> stringHistory = new List<KeyValueEntry>();
                IList<KeyValueEntry> longHistory = new List<KeyValueEntry>();

                // entry gives detail about latest entry of the key
                byteHistory.Add(
                    AssertEntry(BUCKET, byteKey, KeyValueOperation.Put, 1, byteValue1, now, kv.Get(byteKey)));

                stringHistory.Add(
                    AssertEntry(BUCKET, stringKey, KeyValueOperation.Put, 2, stringValue1, now, kv.Get(stringKey)));

                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 3, "1", now, kv.Get(longKey)));

                // history gives detail about the key
                AssertHistory(byteHistory, kv.History(byteKey));
                AssertHistory(stringHistory, kv.History(stringKey));
                AssertHistory(longHistory, kv.History(longKey));

                // let's check the bucket info
                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(3U, status.EntryCount);
                Assert.Equal(3U, status.BackingStreamInfo.State.LastSeq);

                // delete a key. Its entry will still exist, but it's value is null
                kv.Delete(byteKey);

                byteHistory.Add(
                    AssertEntry(BUCKET, byteKey, KeyValueOperation.Delete, 4, null, now, kv.Get(byteKey)));
                AssertHistory(byteHistory, kv.History(byteKey));

                // let's check the bucket info
                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(4U, status.EntryCount);
                Assert.Equal(4U, status.BackingStreamInfo.State.LastSeq);

                // if the key has been deleted
                // all varieties of get will return null
                Assert.Null(kv.Get(byteKey).Value);
                Assert.Null(kv.Get(byteKey).ValueAsString());
                Assert.False(kv.Get(byteKey).TryGetLongValue(out lvalue));
                Assert.Equal(0, lvalue);

                // if the key does not exist (no history) there is no entry
                Assert.Null(kv.Get(notFoundKey));

                // Update values. You can even update a deleted key
                Assert.Equal(5U, kv.Put(byteKey, Encoding.UTF8.GetBytes(byteValue2)));
                Assert.Equal(6U, kv.Put(stringKey, stringValue2));
                Assert.Equal(7U, kv.Put(longKey, 2));

                // values after updates
                Assert.Equal(byteValue2, Encoding.UTF8.GetString(kv.Get(byteKey).Value));
                Assert.Equal(stringValue2, kv.Get(stringKey).ValueAsString());
                Assert.True(kv.Get(longKey).TryGetLongValue(out lvalue));
                Assert.Equal(2, lvalue);

                // entry and history after update
                byteHistory.Add(
                    AssertEntry(BUCKET, byteKey, KeyValueOperation.Put, 5, byteValue2, now, kv.Get(byteKey)));
                AssertHistory(byteHistory, kv.History(byteKey));

                stringHistory.Add(
                    AssertEntry(BUCKET, stringKey, KeyValueOperation.Put, 6, stringValue2, now, kv.Get(stringKey)));
                AssertHistory(stringHistory, kv.History(stringKey));

                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 7, "2", now, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                // let's check the bucket info
                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(7U, status.EntryCount);
                Assert.Equal(7U, status.BackingStreamInfo.State.LastSeq);

                // make sure it only keeps the correct amount of history
                Assert.Equal(8U, kv.Put(longKey, 3));
                Assert.True(kv.Get(longKey).TryGetLongValue(out lvalue));
                Assert.Equal(3, lvalue);

                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 8, "3", now, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(8U, status.EntryCount);
                Assert.Equal(8U, status.BackingStreamInfo.State.LastSeq);

                // this would be the 4th entry for the longKey
                // sp the total records will stay the same
                Assert.Equal(9U, kv.Put(longKey, 4));
                Assert.True(kv.Get(longKey).TryGetLongValue(out lvalue));
                Assert.Equal(4, lvalue);

                // history only retains 3 records
                longHistory.RemoveAt(0);
                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 9, "4", now, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                // record count does not increase
                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(8U, status.EntryCount);
                Assert.Equal(9U, status.BackingStreamInfo.State.LastSeq);

                // should have exactly these 3 keys
                AssertKeys(kv.Keys(), byteKey, stringKey, longKey);

                // purge
                kv.Purge(longKey);
                longHistory.Clear();
                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Purge, 10, null, now, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(6U, status.EntryCount); // includes 1 purge
                Assert.Equal(10U, status.BackingStreamInfo.State.LastSeq);

                // only 2 keys now
                AssertKeys(kv.Keys(), byteKey, stringKey);

                kv.Purge(byteKey);
                byteHistory.Clear();
                byteHistory.Add(
                    AssertEntry(BUCKET, byteKey, KeyValueOperation.Purge, 11, null, now, kv.Get(byteKey)));
                AssertHistory(byteHistory, kv.History(byteKey));

                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(4U, status.EntryCount); // includes 2 purges
                Assert.Equal(11U, status.BackingStreamInfo.State.LastSeq);

                // only 1 key now
                AssertKeys(kv.Keys(), stringKey);

                kv.Purge(stringKey);
                stringHistory.Clear();
                stringHistory.Add(
                    AssertEntry(BUCKET, stringKey, KeyValueOperation.Purge, 12, null, now, kv.Get(stringKey)));
                AssertHistory(stringHistory, kv.History(stringKey));

                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(3U, status.EntryCount); // 3 purges
                Assert.Equal(12U, status.BackingStreamInfo.State.LastSeq);

                // no more keys left
                AssertKeys(kv.Keys());

                // clear things
                kv.PurgeDeletes();
                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(0U, status.EntryCount); // purges are all gone
                Assert.Equal(12U, status.BackingStreamInfo.State.LastSeq);

                longHistory.Clear();
                AssertHistory(longHistory, kv.History(longKey));

                stringHistory.Clear();
                AssertHistory(stringHistory, kv.History(stringKey));

                // put some more
                Assert.Equal(13U, kv.Put(longKey, 110));
                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 13, "110", now, kv.Get(longKey)));

                Assert.Equal(14U, kv.Put(longKey, 111));
                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 14, "111", now, kv.Get(longKey)));

                Assert.Equal(15U, kv.Put(longKey, 112));
                longHistory.Add(
                    AssertEntry(BUCKET, longKey, KeyValueOperation.Put, 15, "112", now, kv.Get(longKey)));

                Assert.Equal(16U, kv.Put(stringKey, stringValue1));
                stringHistory.Add(
                    AssertEntry(BUCKET, stringKey, KeyValueOperation.Put, 16, stringValue1, now, kv.Get(stringKey)));

                Assert.Equal(17U, kv.Put(stringKey, stringValue2));
                stringHistory.Add(
                    AssertEntry(BUCKET, stringKey, KeyValueOperation.Put, 17, stringValue2, now, kv.Get(stringKey)));

                AssertHistory(longHistory, kv.History(longKey));
                AssertHistory(stringHistory, kv.History(stringKey));

                status = kvm.GetBucketInfo(BUCKET);
                Assert.Equal(5U, status.EntryCount);
                Assert.Equal(17U, status.BackingStreamInfo.State.LastSeq);

                // delete the bucket
                kvm.Delete(BUCKET);
                Assert.Throws<NATSJetStreamException>(() => kvm.Delete(BUCKET));
                Assert.Throws<NATSJetStreamException>(() => kvm.GetBucketInfo(BUCKET));

                Assert.Equal(0, kvm.GetBucketNames().Count);
            });
        }
        
        [Fact]
        public void TestKeys() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(BUCKET)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(BUCKET);

                for (int x = 1; x <= 10; x++)
                {
                    kv.Put("k" + x, x);
                }

                IList<string> keys = kv.Keys();
                Assert.Equal(10, keys.Count);
                
                kv.Delete("k1");
                kv.Delete("k3");
                kv.Delete("k5");
                kv.Purge("k7");
                kv.Purge("k9");
                
                keys = kv.Keys();
                Assert.Equal(5, keys.Count);

                for (int x = 2; x <= 10; x += 2)
                {
                    Assert.True(keys.Contains("k" + x));
                }
            });
        }
        
        [Fact]
        public void TestMaxHistoryPerKey() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // default maxHistoryPerKey is 1
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(Bucket(1))
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(Bucket(1));
                kv.Put(KEY, 1);
                kv.Put(KEY, 2);

                IList<KeyValueEntry> history = kv.History(KEY);
                Assert.Equal(1, history.Count);
                long l = 0;
                history[0].TryGetLongValue(out l);
                Assert.Equal(2, l);

                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(Bucket(2))
                    .WithMaxHistoryPerKey(2)
                    .WithStorageType(StorageType.Memory)
                    .Build());
                
                kv = c.CreateKeyValueContext(Bucket(2));
                kv.Put(KEY, 1);
                kv.Put(KEY, 2);
                kv.Put(KEY, 3);
                
                history = kv.History(KEY);
                Assert.Equal(2, history.Count);
                l = 0;
                history[0].TryGetLongValue(out l);
                Assert.Equal(2, l);
                history[1].TryGetLongValue(out l);
                Assert.Equal(3, l);
            });
        }
        
        [Fact]
        public void TestHistoryDeletePurge() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(BUCKET)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(BUCKET);
                kv.Put(KEY, "a");
                kv.Put(KEY, "b");
                kv.Put(KEY, "c");

                IList<KeyValueEntry> list = kv.History(KEY);
                Assert.Equal(3, list.Count);
                
                kv.Delete(KEY);
                list = kv.History(KEY);
                Assert.Equal(4, list.Count);
                
                kv.Purge(KEY);
                list = kv.History(KEY);
                Assert.Equal(1, list.Count);
            });
        }

        [Fact]
        public void TestPurgeDeletes() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(BUCKET)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(BUCKET);
                kv.Put(Key(1), "a");
                kv.Delete(Key(1));
                kv.Put(Key(2), "b");
                kv.Put(Key(3), "c");
                kv.Put(Key(4), "d");
                kv.Purge(Key(4));

                IJetStream js = c.CreateJetStreamContext();
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(KeyValueUtil.ToStreamSubject(BUCKET));

                Msg m = sub.NextMessage(1000);
                Assert.Equal("a", Encoding.UTF8.GetString(m.Data));

                m = sub.NextMessage(1000);
                Assert.Empty(m.Data);

                m = sub.NextMessage(1000);
                Assert.Equal("b", Encoding.UTF8.GetString(m.Data));

                m = sub.NextMessage(1000);
                Assert.Equal("c", Encoding.UTF8.GetString(m.Data));

                m = sub.NextMessage(1000);
                Assert.Empty(m.Data);

                sub.Unsubscribe();

                kv.PurgeDeletes();
                sub = js.PushSubscribeSync(KeyValueUtil.ToStreamSubject(BUCKET));

                m = sub.NextMessage(1000);
                Assert.Equal("b", Encoding.UTF8.GetString(m.Data));

                m = sub.NextMessage(1000);
                Assert.Equal("c", Encoding.UTF8.GetString(m.Data));

                sub.Unsubscribe();
            });
        }

        [Fact]
        public void TestCreateAndUpdate() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(BUCKET)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(BUCKET);

                // 1. allowed to create something that does not exist
                ulong rev1 = kv.Create(KEY, Encoding.UTF8.GetBytes("a"));

                // 2. allowed to update with proper revision
                kv.Update(KEY, Encoding.UTF8.GetBytes("ab"), rev1);

                // 3. not allowed to update with wrong revision
                Assert.Throws<NATSJetStreamException>(() => kv.Update(KEY, Encoding.UTF8.GetBytes("zzz"), rev1));

                // 4. not allowed to create a key that exists
                Assert.Throws<NATSJetStreamException>(() => kv.Create(KEY, Encoding.UTF8.GetBytes("zzz")));

                // 5. not allowed to update a key that does not exist
                Assert.Throws<NATSJetStreamException>(() => kv.Update(KEY, Encoding.UTF8.GetBytes("zzz"), 1));

                // 6. allowed to create a key that is deleted
                kv.Delete(KEY);
                kv.Create(KEY, Encoding.UTF8.GetBytes("abc"));

                // 7. allowed to update a key that is deleted, as long as you have it's revision
                kv.Delete(KEY);
                IList<KeyValueEntry> hist = kv.History(KEY);
                kv.Update(KEY, Encoding.UTF8.GetBytes("abcd"), hist[hist.Count-1].Revision);

                // 8. allowed to create a key that is purged
                kv.Purge(KEY);
                kv.Create(KEY, Encoding.UTF8.GetBytes("abcde"));

                // 9. allowed to update a key that is deleted, as long as you have it's revision
                kv.Purge(KEY);
                hist = kv.History(KEY);
                kv.Update(KEY, Encoding.UTF8.GetBytes("abcdef"), hist[hist.Count-1].Revision);
            });
        }

        [Fact]
        public void TestManageGetBucketNames() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create bucket 1
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(Bucket(1))
                    .WithStorageType(StorageType.Memory)
                    .Build());

                // create bucket 2
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(Bucket(2))
                    .WithStorageType(StorageType.Memory)
                    .Build());

                CreateMemoryStream(c, Stream(1));
                CreateMemoryStream(c, Stream(2));

                IList<string> buckets = kvm.GetBucketNames();
                Assert.Equal(2, buckets.Count);
                Assert.Contains(Bucket(1), buckets);
                Assert.Contains(Bucket(2), buckets);
            });
        }

        private void AssertKeys(IList<string> apiKeys, params string[] manualKeys) {
            Assert.Equal(manualKeys.Length, apiKeys.Count);
            foreach (string k in manualKeys)
            {
                Assert.Contains(k, apiKeys);
            }
        }

        private void AssertHistory(IList<KeyValueEntry> manualHistory, IList<KeyValueEntry> apiHistory) {
            Assert.Equal(apiHistory.Count, manualHistory.Count);
            for (int x = 0; x < apiHistory.Count; x++) {
                AssertKvEquals(apiHistory[x], manualHistory[x]);
            }
        }

        private KeyValueEntry AssertEntry(string bucket, string key, KeyValueOperation op, ulong seq, string value, DateTime now, KeyValueEntry entry) {
            Assert.Equal(bucket, entry.Bucket);
            Assert.Equal(key, entry.Key);
            Assert.Equal(op, entry.Operation);
            Assert.Equal(seq, entry.Revision);
            if (op.Equals(KeyValueOperation.Put)) {
                Assert.Equal(value, Encoding.UTF8.GetString(entry.Value));
            }
            else {
                Assert.Null(entry.Value);
            }
            Assert.True(now.CompareTo(entry.Created) < 0);
            return entry;
        }

        private void AssertKvEquals(KeyValueEntry kv1, KeyValueEntry kv2) {
            Assert.Equal(kv1.Operation, kv2.Operation);
            Assert.Equal(kv1.Revision, kv2.Revision);
            Assert.Equal(kv1.Bucket, kv2.Bucket);
            Assert.Equal(kv1.Key, kv2.Key);

            if (kv1.Value == null)
            {
                Assert.Null(kv2.Value);
            }
            else
            {
                Assert.True(kv1.Value.SequenceEqual(kv2.Value));
            }
            Assert.Equal(kv1.Created, kv2.Created);
        }

        [Fact]
        public void TestWatch()
        {
            string keyNull = "key.nl";
            string key1 = "key.1";
            string key2 = "key.2";
            
            Object[] key1AllExpecteds = new Object[] {
                "a", "aa", KeyValueOperation.Delete, "aaa", KeyValueOperation.Delete, KeyValueOperation.Purge
            };

            Object[] noExpecteds = new Object[0];
            Object[] purgeOnlyExpecteds = { KeyValueOperation.Purge };

            Object[] key2AllExpecteds = {
                "z", "zz", KeyValueOperation.Delete, "zzz"
            };

            Object[] key2AfterExpecteds = { "zzz" };

            Object[] allExpecteds = new object[] {
                "a", "aa", "z", "zz",
                KeyValueOperation.Delete, KeyValueOperation.Delete,
                "aaa", "zzz",
                KeyValueOperation.Delete, KeyValueOperation.Purge,
                null
            };

            Object[] allPutsExpecteds = {
                "a", "aa", "z", "zz", "aaa", "zzz", null
            };
            
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(BUCKET)
                    .WithMaxHistoryPerKey(10)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(BUCKET);

                TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher(true);
                TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key1StartNewWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key1StartAllWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher(true);
                TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher allAllFullWatcher = new TestKeyValueWatcher(true);
                TestKeyValueWatcher allAllMetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher allIgDelFullWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.IgnoreDelete);
                TestKeyValueWatcher allIgDelMetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IgnoreDelete);
                TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher(true);
                TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher(true);
                TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);

                IList<KeyValueWatchSubscription> subs = new List<KeyValueWatchSubscription>();
                subs.Add(kv.Watch(key1, key1FullWatcher, key1FullWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1MetaWatcher, key1MetaWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1StartNewWatcher, key1StartNewWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1StartAllWatcher, key1StartAllWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2FullWatcher, key2FullWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2MetaWatcher, key2MetaWatcher.WatchOptions));
                subs.Add(kv.WatchAll(allAllFullWatcher, allAllFullWatcher.WatchOptions));
                subs.Add(kv.WatchAll(allAllMetaWatcher, allAllMetaWatcher.WatchOptions));
                subs.Add(kv.WatchAll(allIgDelFullWatcher, allIgDelFullWatcher.WatchOptions));
                subs.Add(kv.WatchAll(allIgDelMetaWatcher, allIgDelMetaWatcher.WatchOptions));
                subs.Add(kv.Watch("key.*", starFullWatcher, starFullWatcher.WatchOptions));
                subs.Add(kv.Watch("key.*", starMetaWatcher, starMetaWatcher.WatchOptions));
                subs.Add(kv.Watch("key.>", gtFullWatcher, gtFullWatcher.WatchOptions));
                subs.Add(kv.Watch("key.>", gtMetaWatcher, gtMetaWatcher.WatchOptions));

                kv.Put(key1, "a");
                kv.Put(key1, "aa");
                kv.Put(key2, "z");
                kv.Put(key2, "zz");
                kv.Delete(key1);
                kv.Delete(key2);
                kv.Put(key1, "aaa");
                kv.Put(key2, "zzz");
                kv.Delete(key1);
                kv.Purge(key1);
                kv.Put(keyNull, (byte[])null);
                
                Thread.Sleep(100); // give time for all the data to be setup

                TestKeyValueWatcher key1AfterWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key1AfterIgDelWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IgnoreDelete);
                TestKeyValueWatcher key1AfterStartNewWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.UpdatesOnly);
                TestKeyValueWatcher key1AfterStartFirstWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IncludeHistory);
                TestKeyValueWatcher key2AfterWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key2AfterStartNewWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.UpdatesOnly);
                TestKeyValueWatcher key2AfterStartFirstWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IncludeHistory);

                subs.Add(kv.Watch(key1, key1AfterWatcher, key1AfterWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1AfterIgDelWatcher, key1AfterIgDelWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1AfterStartNewWatcher, key1AfterStartNewWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1AfterStartFirstWatcher, key1AfterStartFirstWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2AfterWatcher, key2AfterWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2AfterStartNewWatcher, key2AfterStartNewWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2AfterStartFirstWatcher, key2AfterStartFirstWatcher.WatchOptions));

                Thread.Sleep(2000); // give time for the watches to get messages

                // unsubscribe so the watchers don't get any more messages
                foreach (KeyValueWatchSubscription sub in subs)
                {
                    sub.Unsubscribe();
                }

                // put some more data which should not be seen by watches
                kv.Put(key1, "aaaa");
                kv.Put(key2, "zzzz");

                ValidateWatcher(key1AllExpecteds, key1FullWatcher);
                ValidateWatcher(key1AllExpecteds, key1MetaWatcher);
                ValidateWatcher(key1AllExpecteds, key1StartNewWatcher);
                ValidateWatcher(key1AllExpecteds, key1StartAllWatcher);

                ValidateWatcher(key2AllExpecteds, key2FullWatcher);
                ValidateWatcher(key2AllExpecteds, key2MetaWatcher);

                ValidateWatcher(allExpecteds, allAllFullWatcher);
                ValidateWatcher(allExpecteds, allAllMetaWatcher);
                ValidateWatcher(allPutsExpecteds, allIgDelFullWatcher);
                ValidateWatcher(allPutsExpecteds, allIgDelMetaWatcher);

                ValidateWatcher(allExpecteds, starFullWatcher);
                ValidateWatcher(allExpecteds, starMetaWatcher);
                ValidateWatcher(allExpecteds, gtFullWatcher);
                ValidateWatcher(allExpecteds, gtMetaWatcher);

                ValidateWatcher(purgeOnlyExpecteds, key1AfterWatcher);
                ValidateWatcher(noExpecteds, key1AfterIgDelWatcher);
                ValidateWatcher(noExpecteds, key1AfterStartNewWatcher);
                ValidateWatcher(purgeOnlyExpecteds, key1AfterStartFirstWatcher);

                ValidateWatcher(key2AfterExpecteds, key2AfterWatcher);
                ValidateWatcher(noExpecteds, key2AfterStartNewWatcher);
                ValidateWatcher(key2AllExpecteds, key2AfterStartFirstWatcher);
            });
        }

        private void ValidateWatcher(object[] expectedKves, TestKeyValueWatcher watcher) {
            Assert.Equal(expectedKves.Length, watcher.Entries.Count);
            Assert.Equal(1, watcher.EndOfDataReceived);
            
            if (expectedKves.Length > 0) {
                Assert.Equal(watcher.BeforeWatcher, watcher.EndBeforeEntries);
            }

            int aix = 0;
            DateTime lastCreated = DateTime.MinValue;
            ulong lastRevision = 0;
            
            foreach (KeyValueEntry kve in watcher.Entries) {

                Assert.True(kve.Created.CompareTo(lastCreated) >= 0);
                lastCreated = kve.Created;

                Assert.True(lastRevision < kve.Revision);
                lastRevision = kve.Revision;

                Object expected = expectedKves[aix++];
                if (expected == null) {
                    Assert.Equal(KeyValueOperation.Put, kve.Operation);
                    Assert.True(kve.Value == null || kve.Value.Length == 0);
                    Assert.Equal(0, kve.DataLength);
                }
                else if (expected is string s) {
                    Assert.Equal(KeyValueOperation.Put, kve.Operation);
                    if (watcher.MetaOnly) {
                        Assert.True(kve.Value == null || kve.Value.Length == 0);
                        Assert.Equal(s.Length, kve.DataLength);
                    }
                    else {
                        Assert.NotNull(kve.Value);
                        Assert.Equal(s.Length, kve.DataLength);
                        Assert.Equal(s, kve.ValueAsString());
                    }
                }
                else {
                    Assert.True(kve.Value == null || kve.Value.Length == 0);
                    Assert.Equal(0, kve.DataLength);
                    Assert.Equal(expected, kve.Operation);
                }
            }
        }

        const string BucketCreatedByUserA = "bucketA";
        const string BucketCreatedByUserI = "bucketI";
        
        [Fact]
        public void TestWithAccount()
        {
            using (NATSServer.CreateFast(Context.Server1.Port, "-js -config kv_account.conf"))
            {
                Options acctA = Context.GetTestOptions(Context.Server1.Port);
                acctA.User = "a";
                acctA.Password = "a";

                Options acctI = Context.GetTestOptions(Context.Server1.Port);
                acctI.User = "i";
                acctI.Password = "i";
                acctI.CustomInboxPrefix = "forI.";

                using (IConnection connUserA = Context.ConnectionFactory.CreateConnection(acctA))
                using( IConnection connUserI = Context.ConnectionFactory.CreateConnection(acctI))
                {
                    // some prep
                    KeyValueOptions jsOpt_UserA_NoPrefix = KeyValueOptions.Builder().Build();
                    
                    KeyValueOptions jsOpt_UserI_BucketA_WithPrefix = KeyValueOptions.Builder()
                        .WithFeaturePrefix("iBucketA")
                        .WithJetStreamOptions(JetStreamOptions.Builder().WithPrefix("jsFromA").Build())
                        .Build();
                    
                    KeyValueOptions jsOpt_UserI_BucketI_WithPrefix = KeyValueOptions.Builder()
                        .WithFeaturePrefix("iBucketI")
                        .WithJetStreamOptions(JetStreamOptions.Builder().WithPrefix("jsFromA").Build())
                        .Build();

                    IKeyValueManagement kvmUserA = connUserA.CreateKeyValueManagementContext(jsOpt_UserA_NoPrefix);
                    IKeyValueManagement kvmUserIBcktA = connUserI.CreateKeyValueManagementContext(jsOpt_UserI_BucketA_WithPrefix);
                    IKeyValueManagement kvmUserIBcktI = connUserI.CreateKeyValueManagementContext(jsOpt_UserI_BucketI_WithPrefix);

                    KeyValueConfiguration kvcA = KeyValueConfiguration.Builder()
                        .WithName(BucketCreatedByUserA)
                        .WithStorageType(StorageType.Memory)
                        .WithMaxHistoryPerKey(64)
                        .Build();

                    KeyValueConfiguration kvcI = KeyValueConfiguration.Builder()
                        .WithName(BucketCreatedByUserI)
                        .WithStorageType(StorageType.Memory)
                        .WithMaxHistoryPerKey(64)
                        .Build();

                    // testing KVM API
                    Assert.Equal(BucketCreatedByUserA, kvmUserA.Create(kvcA).BucketName);
                    Assert.Equal(BucketCreatedByUserI, kvmUserIBcktI.Create(kvcI).BucketName);

                    AssertKvAccountBucketNames(kvmUserA.GetBucketNames());
                    AssertKvAccountBucketNames(kvmUserIBcktI.GetBucketNames());

                    Assert.Equal(BucketCreatedByUserA, kvmUserA.GetBucketInfo(BucketCreatedByUserA).BucketName);
                    Assert.Equal(BucketCreatedByUserA, kvmUserIBcktA.GetBucketInfo(BucketCreatedByUserA).BucketName);
                    Assert.Equal(BucketCreatedByUserI, kvmUserA.GetBucketInfo(BucketCreatedByUserI).BucketName);
                    Assert.Equal(BucketCreatedByUserI, kvmUserIBcktI.GetBucketInfo(BucketCreatedByUserI).BucketName);

                    // some more prep
                    IKeyValue kv_connA_bucketA = connUserA.CreateKeyValueContext(BucketCreatedByUserA, jsOpt_UserA_NoPrefix);
                    IKeyValue kv_connA_bucketI = connUserA.CreateKeyValueContext(BucketCreatedByUserI, jsOpt_UserA_NoPrefix);
                    IKeyValue kv_connI_bucketA = connUserI.CreateKeyValueContext(BucketCreatedByUserA, jsOpt_UserI_BucketA_WithPrefix);
                    IKeyValue kv_connI_bucketI = connUserI.CreateKeyValueContext(BucketCreatedByUserI, jsOpt_UserI_BucketI_WithPrefix);

                    // check the names
                    Assert.Equal(BucketCreatedByUserA, kv_connA_bucketA.BucketName);
                    Assert.Equal(BucketCreatedByUserA, kv_connI_bucketA.BucketName);
                    Assert.Equal(BucketCreatedByUserI, kv_connA_bucketI.BucketName);
                    Assert.Equal(BucketCreatedByUserI, kv_connI_bucketI.BucketName);

                    TestKeyValueWatcher watcher_connA_BucketA = new TestKeyValueWatcher(true);
                    TestKeyValueWatcher watcher_connA_BucketI = new TestKeyValueWatcher(true);
                    TestKeyValueWatcher watcher_connI_BucketA = new TestKeyValueWatcher(true);
                    TestKeyValueWatcher watcher_connI_BucketI = new TestKeyValueWatcher(true);

                    kv_connA_bucketA.WatchAll(watcher_connA_BucketA);
                    kv_connA_bucketI.WatchAll(watcher_connA_BucketI);
                    kv_connI_bucketA.WatchAll(watcher_connI_BucketA);
                    kv_connI_bucketI.WatchAll(watcher_connI_BucketI);

                    // bucket a from user a: AA, check AA, IA
                    AssertKveAccount(kv_connA_bucketA, Key(11), kv_connA_bucketA, kv_connI_bucketA);

                    // bucket a from user i: IA, check AA, IA
                    AssertKveAccount(kv_connI_bucketA, Key(12), kv_connA_bucketA, kv_connI_bucketA);

                    // bucket i from user a: AI, check AI, II
                    AssertKveAccount(kv_connA_bucketI, Key(21), kv_connA_bucketI, kv_connI_bucketI);

                    // bucket i from user i: II, check AI, II
                    AssertKveAccount(kv_connI_bucketI, Key(22), kv_connA_bucketI, kv_connI_bucketI);

                    // check keys from each kv
                    AssertKvAccountKeys(kv_connA_bucketA.Keys(), Key(11), Key(12));
                    AssertKvAccountKeys(kv_connI_bucketA.Keys(), Key(11), Key(12));
                    AssertKvAccountKeys(kv_connA_bucketI.Keys(), Key(21), Key(22));
                    AssertKvAccountKeys(kv_connI_bucketI.Keys(), Key(21), Key(22));
                
                    Object[] expecteds = {
                        Data(0), Data(1), KeyValueOperation.Delete, KeyValueOperation.Purge, Data(2),
                        Data(0), Data(1), KeyValueOperation.Delete, KeyValueOperation.Purge, Data(2)
                    };

                    ValidateWatcher(expecteds, watcher_connA_BucketA);
                    ValidateWatcher(expecteds, watcher_connA_BucketI);
                    ValidateWatcher(expecteds, watcher_connI_BucketA);
                    ValidateWatcher(expecteds, watcher_connI_BucketI);
                }
            }
        }

        private void AssertKvAccountBucketNames(IList<string> bnames) {
            Assert.Equal(2, bnames.Count);
            Assert.Contains(BucketCreatedByUserA, bnames);
            Assert.Contains(BucketCreatedByUserI, bnames);
        }

        private void AssertKvAccountKeys(IList<string> keys, string key1, string key2) {
            Assert.Equal(2, keys.Count);
            Assert.Contains(key1, keys);
            Assert.Contains(key2, keys);
        }

        private void AssertKveAccount(IKeyValue kvWorker, string key, IKeyValue kvUserA, IKeyValue kvUserI) {
            kvWorker.Create(key, DataBytes(0));
            AssertKveAccountGet(kvUserA, kvUserI, key, Data(0));

            kvWorker.Put(key, DataBytes(1));
            AssertKveAccountGet(kvUserA, kvUserI, key, Data(1));

            kvWorker.Delete(key);
            KeyValueEntry kveUserA = kvUserA.Get(key);
            KeyValueEntry kveUserI = kvUserI.Get(key);
            Assert.NotNull(kveUserA);
            Assert.NotNull(kveUserI);
            Assert.Equal(kveUserA, kveUserI);
            Assert.Equal(KeyValueOperation.Delete, kveUserA.Operation);

            AssertKveAccountHistory(kvUserA.History(key), Data(0), Data(1), KeyValueOperation.Delete);
            AssertKveAccountHistory(kvUserI.History(key), Data(0), Data(1), KeyValueOperation.Delete);

            kvWorker.Purge(key);
            AssertKveAccountHistory(kvUserA.History(key), KeyValueOperation.Purge);
            AssertKveAccountHistory(kvUserI.History(key), KeyValueOperation.Purge);

            // leave data for keys checking
            kvWorker.Put(key, DataBytes(2));
            AssertKveAccountGet(kvUserA, kvUserI, key, Data(2));
        }

        private void AssertKveAccountHistory(IList<KeyValueEntry> history, params Object[] expecteds) {
            Assert.Equal(expecteds.Length, history.Count);
            for (int x = 0; x < expecteds.Length; x++) {
                if (expecteds[x] is string expected) {
                    Assert.Equal(expected, history[x].ValueAsString());
                }
                else {
                    Assert.Equal((KeyValueOperation)expecteds[x], history[x].Operation);
                }
            }
        }

        private void AssertKveAccountGet(IKeyValue kvUserA, IKeyValue kvUserI, string key, string data) {
            KeyValueEntry kveUserA = kvUserA.Get(key);
            KeyValueEntry kveUserI = kvUserI.Get(key);
            Assert.NotNull(kveUserA);
            Assert.NotNull(kveUserI);
            Assert.Equal(kveUserA, kveUserI);
            Assert.Equal(data, kveUserA.ValueAsString());
            Assert.Equal(KeyValueOperation.Put, kveUserA.Operation);
        }
    }

    class TestKeyValueWatcher : IKeyValueWatcher 
    {
        public IList<KeyValueEntry> Entries = new List<KeyValueEntry>();
        public KeyValueWatchOption[] WatchOptions;
        public bool BeforeWatcher;
        public bool MetaOnly;
        public int EndOfDataReceived;
        public bool EndBeforeEntries;

        public TestKeyValueWatcher(bool beforeWatcher, params KeyValueWatchOption[] watchOptions)
        {
            BeforeWatcher = beforeWatcher;
            WatchOptions = watchOptions;
            foreach (KeyValueWatchOption wo in watchOptions)
            {
                if (wo == KeyValueWatchOption.MetaOnly)
                {
                    MetaOnly = true;
                    break;
                }
            }
        }

        public void Watch(KeyValueEntry kve)
        {
            Entries.Add(kve);
        }

        public void EndOfData()
        {
            if (++EndOfDataReceived == 1 && Entries.Count == 0) {
                EndBeforeEntries = true;
            }
        }
    }
}