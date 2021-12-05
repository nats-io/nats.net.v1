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
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using NATS.Client;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;
using Xunit;
using Xunit.Abstractions;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;

namespace IntegrationTests
{
    public class TestKeyValue : TestSuite<KeyValueSuiteContext>
    {
        private readonly ITestOutputHelper output;

        public TestKeyValue(ITestOutputHelper output, KeyValueSuiteContext context) : base(context)
        {
            this.output = output;
        }

        public class ConsoleWriter : StringWriter
        {
            private ITestOutputHelper output;
            public ConsoleWriter(ITestOutputHelper output)
            {
                this.output = output;
            }

            public override void WriteLine(string m)
            {
                output.WriteLine(m);
            }
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
                Assert.Equal(KeyValueUtil.StreamName(BUCKET), kvc.BackingConfig.Name);
                Assert.Equal(-1, kvc.MaxValues);
                Assert.Equal(3, status.MaxHistoryPerKey);
                Assert.Equal(3, kvc.MaxHistoryPerKey);
                Assert.Equal(-1, kvc.MaxBucketSize);
                Assert.Equal(-1, kvc.MaxValueBytes);
                Assert.Equal(0, status.Ttl);
                Assert.Equal(0, kvc.Ttl);
                Assert.Equal(StorageType.Memory, kvc.StorageType);
                Assert.Equal(1, kvc.Replicas);
                Assert.Equal(0U, status.EntryCount);
                Assert.Equal("JetStream", status.BackingStore);

                // get the kv context for the specific bucket
                IKeyValue kv = c.CreateKeyValueContext(BUCKET, JetStreamOptions.DefaultJsOptions);  // use options here for coverage

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
                Assert.Equal(byteValue1, kv.Get(byteKey).StringValue);
                Assert.Equal(stringValue1, kv.Get(stringKey).StringValue);
                Assert.Equal("1", kv.Get(longKey).StringValue);

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
                Assert.Null(kv.Get(byteKey).StringValue);
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
                Assert.Equal(stringValue2, kv.Get(stringKey).StringValue);
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

                Assert.Equal(0, kvm.GetBucketsNames().Count);
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
                IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(KeyValueUtil.StreamSubject(BUCKET));

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
                sub = js.PushSubscribeSync(KeyValueUtil.StreamSubject(BUCKET));

                m = sub.NextMessage(1000);
                Assert.Equal("b", Encoding.UTF8.GetString(m.Data));

                m = sub.NextMessage(1000);
                Assert.Equal("c", Encoding.UTF8.GetString(m.Data));

                sub.Unsubscribe();
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

                IList<string> buckets = kvm.GetBucketsNames();
                Assert.Equal(2, buckets.Count);
                Assert.Contains(Bucket(1), buckets);
                Assert.Contains(Bucket(2), buckets);
            });
        }

        private void AssertKeys(IList<String> apiKeys, params String[] manualKeys) {
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
            Console.SetOut(new ConsoleWriter(output));

            string keyNull = "key.nl";
            string key1 = "key.1";
            string key2 = "key.2";
            
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
                
                TestKeyValueWatcher key1FullWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher key1MetaWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher key2FullWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher key2MetaWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher allPutWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher allDelWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher allPurgeWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher allDelPurgeWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher allFullWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher allMetaWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher starFullWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher starMetaWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher gtFullWatcher = new TestKeyValueWatcher();
                TestKeyValueWatcher gtMetaWatcher = new TestKeyValueWatcher();

                IList<KeyValueWatchSubscription> subs = new List<KeyValueWatchSubscription>();
                subs.Add(kv.Watch(key1, key1FullWatcher.Watcher, false));
                subs.Add(kv.Watch(key1, key1MetaWatcher.Watcher, true));
                subs.Add(kv.Watch(key2, key2FullWatcher.Watcher, false));
                subs.Add(kv.Watch(key2, key2MetaWatcher.Watcher, true));
                subs.Add(kv.WatchAll(allPutWatcher.Watcher, false, KeyValueOperation.Put));
                subs.Add(kv.WatchAll(allDelWatcher.Watcher, true, KeyValueOperation.Delete));
                subs.Add(kv.WatchAll(allPurgeWatcher.Watcher, true, KeyValueOperation.Purge));
                subs.Add(kv.WatchAll(allDelPurgeWatcher.Watcher, true, KeyValueOperation.Delete, KeyValueOperation.Purge));
                subs.Add(kv.WatchAll(allFullWatcher.Watcher, false));
                subs.Add(kv.WatchAll(allMetaWatcher.Watcher, true));
                subs.Add(kv.Watch("key.*", starFullWatcher.Watcher, false));
                subs.Add(kv.Watch("key.*", starMetaWatcher.Watcher, true));
                subs.Add(kv.Watch("key.>", gtFullWatcher.Watcher, false));
                subs.Add(kv.Watch("key.>", gtMetaWatcher.Watcher, true));

                kv.Put(key1, "a");
                kv.Put(key1, "aa");
                kv.Put(key2, "z");
                kv.Put(key2, "zz");
                kv.Delete(key1);
                kv.Delete(key2);
                kv.Put(key1, "aaa");
                kv.Put(key2, "zzz");
                kv.Delete(key1);
                kv.Delete(key2);
                kv.Purge(key1);
                kv.Purge(key2);
                kv.Put(keyNull, (byte[])null);
                
                Thread.Sleep(2000);

                object[] key1Expecteds = new object[] {
                    "a", "aa", KeyValueOperation.Delete, "aaa", KeyValueOperation.Delete, KeyValueOperation.Purge
                };

                object[] key2Expecteds = new object[] {
                    "z", "zz", KeyValueOperation.Delete, "zzz", KeyValueOperation.Delete, KeyValueOperation.Purge
                };

                object[] allExpecteds = new object[] {
                    "a", "aa", "z", "zz",
                    KeyValueOperation.Delete, KeyValueOperation.Delete,
                    "aaa", "zzz",
                    KeyValueOperation.Delete, KeyValueOperation.Delete,
                    KeyValueOperation.Purge, KeyValueOperation.Purge,
                    null
                };

                object[] allPuts = new object[] {
                    "a", "aa", "z", "zz", "aaa", "zzz", null
                };

                object[] allDels = new object[] {
                    KeyValueOperation.Delete, KeyValueOperation.Delete,
                    KeyValueOperation.Delete, KeyValueOperation.Delete
                };

                object[] allPurges = new object[] {
                    KeyValueOperation.Purge, KeyValueOperation.Purge
                };

                object[] allDelsPurges = new object[] {
                    KeyValueOperation.Delete, KeyValueOperation.Delete,
                    KeyValueOperation.Delete, KeyValueOperation.Delete,
                    KeyValueOperation.Purge, KeyValueOperation.Purge
                };

                // unsubscribe so the watchers don't get any more messages
                foreach (KeyValueWatchSubscription sub in subs)
                {
                    sub.Unsubscribe();
                }

                // put some more data which should not be seen by watches
                kv.Put(key1, "aaaa");
                kv.Put(key2, "zzzz");

                ValidateWatcher(key1Expecteds, key1FullWatcher, false);
                ValidateWatcher(key1Expecteds, key1MetaWatcher, true);
                ValidateWatcher(key2Expecteds, key2FullWatcher, false);
                ValidateWatcher(key2Expecteds, key2MetaWatcher, true);
                ValidateWatcher(allPuts, allPutWatcher, false);
                ValidateWatcher(allDels, allDelWatcher, true);
                ValidateWatcher(allPurges, allPurgeWatcher, true);
                ValidateWatcher(allDelsPurges, allDelPurgeWatcher, true);
                ValidateWatcher(allExpecteds, allFullWatcher, false);
                ValidateWatcher(allExpecteds, allMetaWatcher, true);
                ValidateWatcher(allExpecteds, starFullWatcher, false);
                ValidateWatcher(allExpecteds, starMetaWatcher, true);
                ValidateWatcher(allExpecteds, gtFullWatcher, false);
                ValidateWatcher(allExpecteds, gtMetaWatcher, true);
            });
        }

        private void ValidateWatcher(object[] expecteds, TestKeyValueWatcher watcher, bool metaOnly) {
            int aix = 0;
            DateTime lastCreated = DateTime.MinValue;
            ulong lastRevision = 0;
            foreach (KeyValueEntry kve in watcher.entries) {

                Assert.True(kve.Created.CompareTo(lastCreated) >= 0);
                lastCreated = kve.Created;

                Assert.True(lastRevision < kve.Revision);
                lastRevision = kve.Revision;

                Object expected = expecteds[aix++];
                if (expected == null) {
                    Assert.Equal(KeyValueOperation.Put, kve.Operation);
                    Assert.True(kve.Value == null || kve.Value.Length == 0);
                    Assert.Equal(0, kve.DataLength);
                }
                else if (expected is string) {
                    Assert.Equal(KeyValueOperation.Put, kve.Operation);
                    String s = (String) expected;
                    if (metaOnly) {
                        Assert.True(kve.Value == null || kve.Value.Length == 0);
                        Assert.Equal(s.Length, kve.DataLength);
                    }
                    else {
                        Assert.NotNull(kve.Value);
                        Assert.Equal(s.Length, kve.DataLength);
                        Assert.Equal(s, kve.StringValue);
                    }
                }
                else {
                    Assert.True(kve.Value == null || kve.Value.Length == 0);
                    Assert.Equal(0, kve.DataLength);
                    Assert.Equal(expected, kve.Operation);
                }
            }
        }
    }

    class TestKeyValueWatcher
    {
        public IList<KeyValueEntry> entries = new List<KeyValueEntry>();

        public Action<KeyValueEntry> Watcher => kve =>
        {
            entries.Add(kve);
        };
    }
}