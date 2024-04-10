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
using NATS.Client.Internals;
using NATS.Client.JetStream;
using NATS.Client.KeyValue;
using Xunit;
using static UnitTests.TestBase;
using static IntegrationTests.JetStreamTestBase;
using static NATS.Client.JetStream.JetStreamOptions;
using static NATS.Client.KeyValue.KeyValuePurgeOptions;

namespace IntegrationTests
{
    public class TestKeyValue : TestSuite<KeyValueSuiteContext>
    {
        public TestKeyValue(KeyValueSuiteContext context) : base(context) {}

        [Fact]
        public void TestWorkFlow()
        {
            DateTime utcNow = DateTime.UtcNow;

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
                c.CreateKeyValueManagementContext(KeyValueOptions.Builder(DefaultJsOptions).Build()); // coverage

                IDictionary<string, string> metadata = new Dictionary<string, string>();
                metadata["foo"] = "bar";

                string bucket = Bucket();
                
                // create the bucket
                KeyValueConfiguration kvc = KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithMaxHistoryPerKey(3)
                    .WithStorageType(StorageType.Memory)
                    .WithMetadata(metadata)
                    .Build();

                KeyValueStatus status = kvm.Create(kvc);

                kvc = status.Config;
                Assert.Equal(bucket, status.BucketName);
                Assert.Equal(bucket, kvc.BucketName);
                Assert.Equal(3, status.MaxHistoryPerKey);
                Assert.Equal(3, kvc.MaxHistoryPerKey);
                Assert.Equal(-1, status.MaxBucketSize);
                Assert.Equal(-1, kvc.MaxBucketSize);
                Assert.Equal(-1, status.MaxValueSize);
                Assert.Equal(-1, kvc.MaxValueSize);
                Assert.Equal(Duration.Zero, status.Ttl);
                Assert.Equal(Duration.Zero, kvc.Ttl);
                Assert.Equal(StorageType.Memory, status.StorageType);
                Assert.Equal(StorageType.Memory, kvc.StorageType);
                Assert.Equal(1, status.Replicas);
                Assert.Equal(1, kvc.Replicas);
                Assert.Equal(0U, status.EntryCount);
                Assert.Equal("JetStream", status.BackingStore);
                Assert.False(status.IsCompressed);
                Assert.Equal(1, kvc.Metadata.Count);
                Assert.Equal("bar", kvc.Metadata["foo"]);
                Assert.Equal(1, status.Metadata.Count);
                Assert.Equal("bar", status.Metadata["foo"]);

                // get the kv context for the specific bucket
                IKeyValue kv = c.CreateKeyValueContext(bucket);

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
                IList<object> byteHistory = new List<object>();
                IList<object> stringHistory = new List<object>();
                IList<object> longHistory = new List<object>();

                // entry gives detail about latest entry of the key
                byteHistory.Add(
                    AssertEntry(bucket, byteKey, KeyValueOperation.Put, 1, byteValue1, utcNow, kv.Get(byteKey)));

                stringHistory.Add(
                    AssertEntry(bucket, stringKey, KeyValueOperation.Put, 2, stringValue1, utcNow, kv.Get(stringKey)));

                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 3, "1", utcNow, kv.Get(longKey)));

                // history gives detail about the key
                AssertHistory(byteHistory, kv.History(byteKey));
                AssertHistory(stringHistory, kv.History(stringKey));
                AssertHistory(longHistory, kv.History(longKey));

                // let's check the bucket info
                status = kvm.GetStatus(bucket);
                AssertState(status, 3, 3); 
                    
                // delete a key. Its entry will still exist, but it's value is null
                kv.Delete(byteKey);
                Assert.Null(kv.Get(byteKey));
                byteHistory.Add(KeyValueOperation.Delete);
                AssertHistory(byteHistory, kv.History(byteKey));

                // let's check the bucket info
                status = kvm.GetStatus(bucket);
                AssertState(status, 4, 4);
                
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
                    AssertEntry(bucket, byteKey, KeyValueOperation.Put, 5, byteValue2, utcNow, kv.Get(byteKey)));
                AssertHistory(byteHistory, kv.History(byteKey));

                stringHistory.Add(
                    AssertEntry(bucket, stringKey, KeyValueOperation.Put, 6, stringValue2, utcNow, kv.Get(stringKey)));
                AssertHistory(stringHistory, kv.History(stringKey));

                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 7, "2", utcNow, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                // let's check the bucket info
                status = kvm.GetStatus(bucket);
                AssertState(status, 7, 7);
                
                // make sure it only keeps the correct amount of history
                Assert.Equal(8U, kv.Put(longKey, 3));
                Assert.True(kv.Get(longKey).TryGetLongValue(out lvalue));
                Assert.Equal(3, lvalue);

                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 8, "3", utcNow, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                status = kvm.GetStatus(bucket);
                AssertState(status, 8, 8);
                
                // this would be the 4th entry for the longKey
                // sp the total records will stay the same
                Assert.Equal(9U, kv.Put(longKey, 4));
                Assert.True(kv.Get(longKey).TryGetLongValue(out lvalue));
                Assert.Equal(4, lvalue);

                // history only retains 3 records
                longHistory.RemoveAt(0);
                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 9, "4", utcNow, kv.Get(longKey)));
                AssertHistory(longHistory, kv.History(longKey));

                // record count does not increase
                status = kvm.GetStatus(bucket);
                AssertState(status, 8, 9);
                
                // should have exactly these 3 keys
                AssertKeys(kv.Keys(), byteKey, stringKey, longKey);

                // purge
                kv.Purge(longKey);
                longHistory.Clear();
                Assert.Null(kv.Get(longKey));
                longHistory.Add(KeyValueOperation.Purge);
                AssertHistory(longHistory, kv.History(longKey));

                status = kvm.GetStatus(bucket);
                AssertState(status, 6, 10); 

                // only 2 keys now
                AssertKeys(kv.Keys(), byteKey, stringKey);

                kv.Purge(byteKey);
                byteHistory.Clear();
                Assert.Null(kv.Get(byteKey));
                byteHistory.Add(KeyValueOperation.Purge);
                AssertHistory(byteHistory, kv.History(byteKey));

                status = kvm.GetStatus(bucket);
                AssertState(status, 4, 11);
                
                // only 1 key now
                AssertKeys(kv.Keys(), stringKey);

                kv.Purge(stringKey);
                stringHistory.Clear();
                Assert.Null(kv.Get(stringKey));
                stringHistory.Add(KeyValueOperation.Purge);
                AssertHistory(stringHistory, kv.History(stringKey));

                status = kvm.GetStatus(bucket);
                AssertState(status, 3, 12);
                
                // no more keys left
                AssertKeys(kv.Keys());

                // clear things
                KeyValuePurgeOptions kvpo = KeyValuePurgeOptions.Builder().WithDeleteMarkersNoThreshold().Build();
                kv.PurgeDeletes(kvpo);
                status = kvm.GetStatus(bucket);
                AssertState(status, 0, 12);
                
                longHistory.Clear();
                AssertHistory(longHistory, kv.History(longKey));

                stringHistory.Clear();
                AssertHistory(stringHistory, kv.History(stringKey));

                // put some more
                Assert.Equal(13U, kv.Put(longKey, 110));
                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 13, "110", utcNow, kv.Get(longKey)));

                Assert.Equal(14U, kv.Put(longKey, 111));
                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 14, "111", utcNow, kv.Get(longKey)));

                Assert.Equal(15U, kv.Put(longKey, 112));
                longHistory.Add(
                    AssertEntry(bucket, longKey, KeyValueOperation.Put, 15, "112", utcNow, kv.Get(longKey)));

                Assert.Equal(16U, kv.Put(stringKey, stringValue1));
                stringHistory.Add(
                    AssertEntry(bucket, stringKey, KeyValueOperation.Put, 16, stringValue1, utcNow, kv.Get(stringKey)));

                Assert.Equal(17U, kv.Put(stringKey, stringValue2));
                stringHistory.Add(
                    AssertEntry(bucket, stringKey, KeyValueOperation.Put, 17, stringValue2, utcNow, kv.Get(stringKey)));

                AssertHistory(longHistory, kv.History(longKey));
                AssertHistory(stringHistory, kv.History(stringKey));

                status = kvm.GetStatus(bucket);
                AssertState(status, 5, 17);
                
                // delete the bucket
                kvm.Delete(bucket);
                Assert.Throws<NATSJetStreamException>(() => kvm.Delete(bucket));
                Assert.Throws<NATSJetStreamException>(() => kvm.GetStatus(bucket));

                Assert.Equal(0, kvm.GetBucketNames().Count);
            });
        }

        private static void AssertState(KeyValueStatus status, ulong entryCount, ulong lastSeq) {
            Assert.Equal(entryCount, status.EntryCount);
            Assert.Equal(lastSeq, status.BackingStreamInfo.State.LastSeq);
            Assert.Equal(status.Bytes, status.BackingStreamInfo.State.Bytes);
        }
        
        [Fact]
        public void TestGetRevisions() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket = Bucket();

                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithMaxHistoryPerKey(2)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                string key = Key();
                
                IKeyValue kv = c.CreateKeyValueContext(bucket);
                ulong seq1 = kv.Put(key, "A");
                ulong seq2 = kv.Put(key, "B");
                ulong seq3 = kv.Put(key, "C");

                KeyValueEntry kve = kv.Get(key);
                Assert.NotNull(kve);
                Assert.Equal("C", kve.ValueAsString());

                kve = kv.Get(key, seq3);
                Assert.NotNull(kve);
                Assert.Equal("C", kve.ValueAsString());

                kve = kv.Get(key, seq2);
                Assert.NotNull(kve);
                Assert.Equal("B", kve.ValueAsString());

                kve = kv.Get(key, seq1);
                Assert.Null(kve);

                kve = kv.Get("notkey", seq3);
                Assert.Null(kve);
            });
        }
        
        [Fact]
        public void TestKeys() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket = Bucket();

                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(bucket);

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

                string keyWithDot = "part1.part2.part3";
                kv.Put(keyWithDot, "key has dot");
                KeyValueEntry kve = kv.Get(keyWithDot);
                Assert.Equal(keyWithDot, kve.Key);
            });
        }
        
        [Fact]
        public void TestMaxHistoryPerKey() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket1 = Bucket();
                string bucket2 = Bucket();
                
                // default maxHistoryPerKey is 1
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket1)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                string key = Key();

                IKeyValue kv = c.CreateKeyValueContext(bucket1);
                kv.Put(key, 1);
                kv.Put(key, 2);

                IList<KeyValueEntry> history = kv.History(key);
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
                kv.Put(key, 1);
                kv.Put(key, 2);
                kv.Put(key, 3);
                
                history = kv.History(key);
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

                string bucket = Bucket();
                string key = Key();
                
                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(bucket);
                kv.Put(key, "a");
                kv.Put(key, "b");
                kv.Put(key, "c");

                IList<KeyValueEntry> list = kv.History(key);
                Assert.Equal(3, list.Count);
                
                kv.Delete(key);
                list = kv.History(key);
                Assert.Equal(4, list.Count);
                
                kv.Purge(key);
                list = kv.History(key);
                Assert.Equal(1, list.Count);
            });
        }
        
        [Fact]
        public void TestAtomicDeletePurge() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket = Bucket();
                
                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                string key = Key();

                IKeyValue kv = c.CreateKeyValueContext(bucket);
                kv.Put(key, "a");
                kv.Put(key, "b");
                kv.Put(key, "c");

                Assert.Equal(3U, kv.Get(key).Revision);
                
                // Delete wrong revision rejected
                Assert.Throws<NATSJetStreamException>(() => kv.Delete(key, 1));

                // Correct revision writes tombstone and bumps revision
                kv.Delete(key, 3);
                
                IList<object> history = new List<object>();
                history.Add(kv.Get(key, 1U));
                history.Add(kv.Get(key, 2U));
                history.Add(kv.Get(key, 3U));
                history.Add(KeyValueOperation.Delete);
                AssertHistory(history, kv.History(key));
                
                // Wrong revision rejected again
                Assert.Throws<NATSJetStreamException>(() => kv.Delete(key, 3));
                
                // Delete is idempotent: two consecutive tombstones
                kv.Delete(key, 4);
                
                history.Add(KeyValueOperation.Delete);
                AssertHistory(history, kv.History(key));

                // Purge wrong revision rejected
                Assert.Throws<NATSJetStreamException>(() => kv.Purge(key, 1));
                
                kv.Purge(key, 5);

                history.Clear();
                history.Add(KeyValueOperation.Purge);
                AssertHistory(history, kv.History(key));

            });
        }
        
        [Fact]
        public void TestCreateUpdate() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket = Bucket();

                Assert.Throws<NATSJetStreamException>(() => kvm.GetStatus(bucket));
                
                KeyValueStatus kvs = kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                Assert.Equal(bucket, kvs.BucketName);
                Assert.Empty(kvs.Description);
                Assert.Equal(1, kvs.MaxHistoryPerKey);
                Assert.Equal(-1, kvs.MaxBucketSize);
                Assert.Equal(-1, kvs.MaxValueSize);
                Assert.Equal(Duration.Zero, kvs.Ttl);
                Assert.Equal(StorageType.Memory, kvs.StorageType);
                Assert.Equal(1, kvs.Replicas);
                Assert.Equal(0U, kvs.EntryCount);
                Assert.Equal("JetStream", kvs.BackingStore);

                string key = Key();
                
                IKeyValue kv = c.CreateKeyValueContext(bucket);
                kv.Put(key, 1);
                kv.Put(key, 2);

                IList<KeyValueEntry> history = kv.History(key);
                Assert.Equal(1, history.Count);
                long lvalue = 0;
                Assert.True(history[0].TryGetLongValue(out lvalue));
                Assert.Equal(2, lvalue);

                KeyValueConfiguration kvc = KeyValueConfiguration.Builder(kvs.Config)
                    .WithDescription(Plain)
                    .WithMaxHistoryPerKey(3)
                    .WithMaxBucketSize(10_000)
                    .WithMaxValueSize(100)
                    .WithTtl(Duration.OfHours(1))
                    .Build();

                kvs = kvm.Update(kvc);

                Assert.Equal(bucket, kvs.BucketName);
                Assert.Equal(Plain, kvs.Description);
                Assert.Equal(3, kvs.MaxHistoryPerKey);
                Assert.Equal(10_000, kvs.MaxBucketSize);
                Assert.Equal(100, kvs.MaxValueSize);
                Assert.Equal(Duration.OfHours(1), kvs.Ttl);
                Assert.Equal(StorageType.Memory, kvs.StorageType);
                Assert.Equal(1, kvs.Replicas);
                Assert.Equal(1U, kvs.EntryCount);
                Assert.Equal("JetStream", kvs.BackingStore);

                history = kv.History(key);
                Assert.Equal(1, history.Count);
                lvalue = 0;
                Assert.True(history[0].TryGetLongValue(out lvalue));
                Assert.Equal(2, lvalue);

                KeyValueConfiguration kvcStor = KeyValueConfiguration.Builder(kvs.Config)
                    .WithStorageType(StorageType.File)
                    .Build();
                Assert.Throws<NATSJetStreamException>(() => kvm.Update(kvcStor));
            });
        }

        [Fact]
        public void TestPurgeDeletes() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket = Bucket();
                
                // create the bucket
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(bucket);
                kv.Put(Key(1), "a");
                kv.Delete(Key(1));
                kv.Put(Key(2), "b");
                kv.Put(Key(3), "c");
                kv.Put(Key(4), "d");
                kv.Purge(Key(4));

                IJetStream js = c.CreateJetStreamContext();
                assertPurgeDeleteEntries(js, new []{"a", null, "b", "c", null}, bucket);

                // default purge deletes uses the default threshold of 30 minutes
                // so no markers will be deleted
                kv.PurgeDeletes();
                assertPurgeDeleteEntries(js, new []{null, "b", "c", null}, bucket);

                // no threshold causes all to be removed
                kv.PurgeDeletes(KeyValuePurgeOptions.Builder().WithDeleteMarkersNoThreshold().Build());
                assertPurgeDeleteEntries(js, new[]{"b", "c"}, bucket);
            });
        }

        private void assertPurgeDeleteEntries(IJetStream js, string[] expected, string bucket) {
            IJetStreamPushSyncSubscription sub = js.PushSubscribeSync(KeyValueUtil.ToStreamSubject(bucket));

            foreach (string s in expected) {
                Msg m = sub.NextMessage(1000);
                KeyValueEntry kve = new KeyValueEntry(m);
                if (s == null) {
                    Assert.NotEqual(KeyValueOperation.Put, kve.Operation);
                    Assert.Equal(0, kve.DataLength);
                }
                else {
                    Assert.Equal(KeyValueOperation.Put, kve.Operation);
                    Assert.Equal(s, Encoding.UTF8.GetString(m.Data));
                }
            }

            sub.Unsubscribe();
        }

        [Fact]
        public void TestCreateAndUpdate() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket = Bucket();

                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket)
                    .WithStorageType(StorageType.Memory)
                    .WithMaxHistoryPerKey(64)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(bucket);

                string key = Key();
                
                // 1. allowed to create something that does not exist
                ulong rev1 = kv.Create(key, Encoding.UTF8.GetBytes("a"));

                // 2. allowed to update with proper revision
                kv.Update(key, Encoding.UTF8.GetBytes("ab"), rev1);

                // 3. not allowed to update with wrong revision
                Assert.Throws<NATSJetStreamException>(() => kv.Update(key, Encoding.UTF8.GetBytes("zzz"), rev1));

                // 4. not allowed to create a key that exists
                Assert.Throws<NATSJetStreamException>(() => kv.Create(key, Encoding.UTF8.GetBytes("zzz")));

                // 5. not allowed to update a key that does not exist
                Assert.Throws<NATSJetStreamException>(() => kv.Update(key, Encoding.UTF8.GetBytes("zzz"), 1));

                // 6. allowed to create a key that is deleted
                kv.Delete(key);
                kv.Create(key, Encoding.UTF8.GetBytes("abc"));

                // 7. allowed to update a key that is deleted, as long as you have it's revision
                kv.Delete(key);
                IList<KeyValueEntry> hist = kv.History(key);
                kv.Update(key, Encoding.UTF8.GetBytes("abcd"), hist[hist.Count-1].Revision);

                // 8. allowed to create a key that is purged
                kv.Purge(key);
                kv.Create(key, Encoding.UTF8.GetBytes("abcde"));

                // 9. allowed to update a key that is deleted, as long as you have it's revision
                kv.Purge(key);
                hist = kv.History(key);
                kv.Update(key, Encoding.UTF8.GetBytes("abcdef"), hist[hist.Count-1].Revision);
            });
        }

        [Fact]
        public void TestManageGetBucketNamesStatuses() {
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket1 = Bucket();
                string bucket2 = Bucket();
                
                // create bucket 1
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket1)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                // create bucket 2
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket2)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                CreateMemoryStream(c, Stream(1));
                CreateMemoryStream(c, Stream(2));
                
                IList<KeyValueStatus> infos = kvm.GetStatuses();
                Assert.Equal(2, infos.Count);
                IList<string> buckets = new List<string>();
                foreach (KeyValueStatus status in infos) {
                    buckets.Add(status.BucketName);
                }
                Assert.Equal(2, buckets.Count);
                Assert.True(buckets.Contains(bucket1));
                Assert.True(buckets.Contains(bucket2));

                buckets = kvm.GetBucketNames();
                Assert.Equal(2, buckets.Count);
                Assert.True(buckets.Contains(bucket1));
                Assert.True(buckets.Contains(bucket2));
            });
        }

        private void AssertKeys(IList<string> apiKeys, params string[] manualKeys) {
            Assert.Equal(manualKeys.Length, apiKeys.Count);
            foreach (string k in manualKeys)
            {
                Assert.Contains(k, apiKeys);
            }
        }

        private void AssertHistory(IList<object> manualHistory, IList<KeyValueEntry> apiHistory) {
            Assert.Equal(manualHistory.Count, apiHistory.Count);
            for (int x = 0; x < apiHistory.Count; x++)
            {
                object o = manualHistory[x];
                if (o is KeyValueOperation kvOp)
                {
                    Assert.Equal(kvOp, apiHistory[x].Operation);
                }
                else
                {
                    AssertKvEquals((KeyValueEntry)o, apiHistory[x]);
                }
            }
        }

        private KeyValueEntry AssertEntry(string bucket, string key, KeyValueOperation op, ulong seq, string value, DateTime utcNow, KeyValueEntry entry) {
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
            Assert.True(utcNow.CompareTo(entry.Created.ToUniversalTime()) < 0);
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
            
            object[] key1AllExpecteds = {
                "a", "aa", KeyValueOperation.Delete, "aaa", KeyValueOperation.Delete, KeyValueOperation.Purge
            };
            
            object[] key1FromRevisionExpecteds  = {
                "aa", KeyValueOperation.Delete, "aaa"
            };

            object[] noExpecteds = Array.Empty<object>();
            object[] purgeOnlyExpecteds = { KeyValueOperation.Purge };

            object[] key2AllExpecteds = {
                "z", "zz", KeyValueOperation.Delete, "zzz"
            };

            object[] key2AfterExpecteds = { "zzz" };

            object[] allExpecteds = {
                "a", "aa", "z", "zz",
                KeyValueOperation.Delete, KeyValueOperation.Delete,
                "aaa", "zzz",
                KeyValueOperation.Delete, KeyValueOperation.Purge,
                null
            };

            object[] allFromRevisionExpecteds  = {
                "aa", "z", "zz",
                KeyValueOperation.Delete, KeyValueOperation.Delete,
                "aaa", "zzz"
            };

            object[] allPutsExpecteds = {
                "a", "aa", "z", "zz", "aaa", "zzz", null
            };

            IList<string> allKeys = new List<string> {key1, key2, keyNull};
            
            Context.RunInJsServer(c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string bucket1 = Bucket(); 
                string bucket2 = Bucket();
                
                // create the buckets
                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket1)
                    .WithMaxHistoryPerKey(10)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(bucket2)
                    .WithMaxHistoryPerKey(10)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IKeyValue kv = c.CreateKeyValueContext(bucket1);
                IKeyValue kv2 = c.CreateKeyValueContext(bucket2);

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
                TestKeyValueWatcher multipleFullWatcher = new TestKeyValueWatcher(true);
                TestKeyValueWatcher multipleMetaWatcher = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher multipleFullWatcher2 = new TestKeyValueWatcher(true);
                TestKeyValueWatcher multipleMetaWatcher2 = new TestKeyValueWatcher(true, KeyValueWatchOption.MetaOnly);

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
                subs.Add(kv.Watch(allKeys, multipleFullWatcher, multipleFullWatcher.WatchOptions));
                subs.Add(kv.Watch(allKeys, multipleMetaWatcher, multipleMetaWatcher.WatchOptions));
                subs.Add(kv.Watch(string.Join(",", allKeys), multipleFullWatcher2, multipleFullWatcher2.WatchOptions));
                subs.Add(kv.Watch(string.Join(",", allKeys), multipleMetaWatcher2, multipleMetaWatcher2.WatchOptions));

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

                kv2.Put(key1, "a");
                kv2.Put(key1, "aa");
                kv2.Put(key2, "z");
                kv2.Put(key2, "zz");
                kv2.Delete(key1);
                kv2.Delete(key2);
                kv2.Put(key1, "aaa");
                kv2.Put(key2, "zzz");

                Thread.Sleep(100); // give time for all the data to be setup

                TestKeyValueWatcher key1AfterWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key1AfterIgDelWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IgnoreDelete);
                TestKeyValueWatcher key1AfterStartNewWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.UpdatesOnly);
                TestKeyValueWatcher key1AfterStartFirstWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IncludeHistory);
                TestKeyValueWatcher key2AfterWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly);
                TestKeyValueWatcher key2AfterStartNewWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.UpdatesOnly);
                TestKeyValueWatcher key2AfterStartFirstWatcher = new TestKeyValueWatcher(false, KeyValueWatchOption.MetaOnly, KeyValueWatchOption.IncludeHistory);

                TestKeyValueWatcher key1FromRevisionAfterWatcher = new TestKeyValueWatcher(false);
                TestKeyValueWatcher allFromRevisionAfterWatcher = new TestKeyValueWatcher(false);

                subs.Add(kv.Watch(key1, key1AfterWatcher, key1AfterWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1AfterIgDelWatcher, key1AfterIgDelWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1AfterStartNewWatcher, key1AfterStartNewWatcher.WatchOptions));
                subs.Add(kv.Watch(key1, key1AfterStartFirstWatcher, key1AfterStartFirstWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2AfterWatcher, key2AfterWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2AfterStartNewWatcher, key2AfterStartNewWatcher.WatchOptions));
                subs.Add(kv.Watch(key2, key2AfterStartFirstWatcher, key2AfterStartFirstWatcher.WatchOptions));

                subs.Add(kv2.Watch(key1, key1FromRevisionAfterWatcher, 2, key1FromRevisionAfterWatcher.WatchOptions));
                subs.Add(kv2.WatchAll(allFromRevisionAfterWatcher, 2, allFromRevisionAfterWatcher.WatchOptions));

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

                ValidateWatcher(key1FromRevisionExpecteds, key1FromRevisionAfterWatcher);
                ValidateWatcher(allFromRevisionExpecteds, allFromRevisionAfterWatcher);
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

                object expected = expectedKves[aix++];
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
                acctI.CustomInboxPrefix = "ForI.";

                using (IConnection connUserA = Context.ConnectionFactory.CreateConnection(acctA))
                using( IConnection connUserI = Context.ConnectionFactory.CreateConnection(acctI))
                {
                    // some prep
                    KeyValueOptions jsOpt_UserA_NoPrefix = KeyValueOptions.Builder().Build();
                    
                    KeyValueOptions jsOpt_UserI_BucketA_WithPrefix = KeyValueOptions.Builder()
                        .WithJetStreamOptions(JetStreamOptions.Builder().WithPrefix("FromA").Build())
                        .Build();
                    
                    KeyValueOptions jsOpt_UserI_BucketI_WithPrefix = KeyValueOptions.Builder()
                        .WithJetStreamOptions(JetStreamOptions.Builder().WithPrefix("FromA").Build())
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

                    Assert.Equal(BucketCreatedByUserA, kvmUserA.GetStatus(BucketCreatedByUserA).BucketName);
                    Assert.Equal(BucketCreatedByUserA, kvmUserIBcktA.GetStatus(BucketCreatedByUserA).BucketName);
                    Assert.Equal(BucketCreatedByUserI, kvmUserA.GetStatus(BucketCreatedByUserI).BucketName);
                    Assert.Equal(BucketCreatedByUserI, kvmUserIBcktI.GetStatus(BucketCreatedByUserI).BucketName);

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
                
                    object[] expecteds = {
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
            Assert.Null(kveUserA);
            Assert.Null(kveUserI);

            AssertKveAccountHistory(kvUserA.History(key), Data(0), Data(1), KeyValueOperation.Delete);
            AssertKveAccountHistory(kvUserI.History(key), Data(0), Data(1), KeyValueOperation.Delete);

            kvWorker.Purge(key);
            AssertKveAccountHistory(kvUserA.History(key), KeyValueOperation.Purge);
            AssertKveAccountHistory(kvUserI.History(key), KeyValueOperation.Purge);

            // leave data for keys checking
            kvWorker.Put(key, DataBytes(2));
            AssertKveAccountGet(kvUserA, kvUserI, key, Data(2));
        }

        private void AssertKveAccountHistory(IList<KeyValueEntry> history, params object[] expecteds) {
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

        [Fact]
        public void TestKeyValueEntryEqualsImpl() {
            Assert.Equal(DefaultThresholdMillis,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThreshold(null).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(DefaultThresholdMillis,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThreshold(Duration.Zero).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(1,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThreshold(Duration.OfMillis(1)).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(-1,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThreshold(Duration.OfMillis(-1)).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(DefaultThresholdMillis,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThresholdMillis(0).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(1,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThresholdMillis(1).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(-1,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersThresholdMillis(-1).Build()
                    .DeleteMarkersThresholdMillis);

            Assert.Equal(-1,
                KeyValuePurgeOptions.Builder().WithDeleteMarkersNoThreshold().Build()
                    .DeleteMarkersThresholdMillis);
        }

        [Fact]
        public void TestCreateDiscardPolicy()
        {
            Context.RunInJsServer(c =>
            {
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                // create bucket
                KeyValueStatus status = kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(Bucket())
                    .WithStorageType(StorageType.Memory)
                    .Build());

                DiscardPolicy dp = status.BackingStreamInfo.Config.DiscardPolicy;
                if (c.ServerInfo.IsSameOrNewerThanVersion("2.7.2")) {
                    Assert.Equal(DiscardPolicy.New, dp);
                }
                else {
                    Assert.True(dp == DiscardPolicy.New || dp == DiscardPolicy.Old);
                }
            });
        }

        [Fact]
        public void TestCompression()
        {
            Context.RunInJsServer(AtLeast2_10, c =>
            {
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();
                
                // create bucket
                KeyValueStatus status = kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(Bucket())
                    .WithCompression(true)
                    .WithStorageType(StorageType.Memory)
                    .Build());
                Assert.True(status.IsCompressed);
            });
        }

        [Fact]
        public void TestKeyValueMirrorCrossDomains()
        {
            Context.RunInJsHubLeaf((hub, leaf) =>
            {
                IKeyValueManagement hubKvm = hub.CreateKeyValueManagementContext();
                IKeyValueManagement leafKvm = leaf.CreateKeyValueManagementContext();

                // Create main KV on HUB
                KeyValueStatus hubStatus = hubKvm.Create(KeyValueConfiguration.Builder()
                    .WithName("TEST")
                    .WithStorageType(StorageType.Memory)
                    .Build());            
              
                IKeyValue hubKv = hub.CreateKeyValueContext("TEST");
                hubKv.Put("key1", "aaa0");
                hubKv.Put("key2", "bb0");
                hubKv.Put("key3", "c0");
                hubKv.Delete("key3");

                leafKvm.Create(KeyValueConfiguration.Builder()
                    .WithName("MIRROR")
                    .WithMirror(Mirror.Builder()
                        .WithName("TEST")
                        .WithDomain(null)  // just for coverage!
                        .WithDomain("HUB") // it will take this since it comes last
                        .Build())
                    .Build());
                
                Thread.Sleep(1000); // make sure things get a chance to propagate
                StreamInfo si = leaf.CreateJetStreamManagementContext().GetStreamInfo("KV_MIRROR");
                if (hub.ServerInfo.IsSameOrNewerThanVersion("2.9.0"))
                {
                    Assert.True(si.Config.MirrorDirect);
                }
                Assert.Equal(3U, si.State.Messages);

                IKeyValue leafKv = leaf.CreateKeyValueContext("MIRROR");
                _testMirror(hubKv, leafKv, 1);

                // Bind through leafnode connection but to origin KV.
                IKeyValue hubViaLeafKv =
                    leaf.CreateKeyValueContext("TEST", KeyValueOptions.Builder().WithJsDomain("HUB").Build());
                _testMirror(hubKv, hubViaLeafKv, 2);

                hub.Close();
                _testMirror(null, leafKv, 3);
                _testMirror(null, hubViaLeafKv, 4);
            });
        }
        
        private void _testMirror(IKeyValue okv, IKeyValue mkv, int num) {
            mkv.Put("key1", "aaa" + num);
            mkv.Put("key3", "c" + num);

            Thread.Sleep(200); // make sure things get a chance to propagate
            KeyValueEntry kve = mkv.Get("key3");
            Assert.Equal("c" + num, kve.ValueAsString());

            mkv.Delete("key3");
            Thread.Sleep(200); // make sure things get a chance to propagate
            Assert.Null(mkv.Get("key3"));

            kve = mkv.Get("key1");
            Assert.Equal("aaa" + num, kve.ValueAsString());

            // Make sure we can create a watcher on the mirror KV.
            TestKeyValueWatcher mWatcher = new TestKeyValueWatcher(false);
            using (KeyValueWatchSubscription mWatchSub = mkv.WatchAll(mWatcher)) 
            {
                Thread.Sleep(200); // give the messages time to propagate
            }
            ValidateWatcher(new object[]{"bb0", "aaa" + num, KeyValueOperation.Delete}, mWatcher);

            // Does the origin data match?
            if (okv != null) {
                TestKeyValueWatcher oWatcher = new TestKeyValueWatcher(false);
                using (KeyValueWatchSubscription oWatchSub = mkv.WatchAll(oWatcher)) 
                {
                    Thread.Sleep(200); // give the messages time to propagate
                }
                ValidateWatcher(new object[]{"bb0", "aaa" + num, KeyValueOperation.Delete}, oWatcher);
            }
        }
        
        [Fact]
        public void TestDontGetNoResponders()
        {
            const int NUM_MESSAGES = 1000;
    
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
        
                IKeyValue kvContext = c.CreateKeyValueContext(BUCKET);

                for (int i = 0; i < NUM_MESSAGES; i++)
                {
                    kvContext.Put(i.ToString(), i.ToString());
                }
        
                TestKeyValueWatcher watcher = new TestKeyValueWatcher(true);

                var watch = kvContext.Watch(">", watcher, watcher.WatchOptions);

                int count = 0;
                while (watcher.EndOfDataReceived == 0 && count < 100)
                {
                    Thread.Sleep(10);
                    count++;
                }
        
                Assert.True(watcher.EndOfDataReceived == 1);
                Assert.True(watcher.Entries.Count == 1000);
        
                watch.Unsubscribe();
            });
        }

        [Fact]
        public void TestKeyValueTransform() {
            Context.RunInJsServer(AtLeast2_10_3,c =>
            {
                // get the kv management context
                IKeyValueManagement kvm = c.CreateKeyValueManagementContext();

                string kvName1 = Variant();
                string kvName2 = kvName1 + "-mir";
                string mirrorSegment = "MirrorMe";
                string dontMirrorSegment = "DontMirrorMe";
                string generic = "foo";
                
                KeyValueStatus status = kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(kvName1)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                SubjectTransform transform = SubjectTransform.Builder()
                    .WithSource("$KV." + kvName1 + "." + mirrorSegment + ".*")
                    .WithDestination("$KV." + kvName2 + "." + mirrorSegment + ".{{wildcard(1)}}")
                    .Build();

                Mirror mirr = Mirror.Builder()
                    .WithName(kvName1)
                    .WithSubjectTransforms(transform)
                    .Build();

                status = kvm.Create(KeyValueConfiguration.Builder()
                    .WithName(kvName2)
                    .WithMirror(mirr)
                    .WithStorageType(StorageType.Memory)
                    .Build());

                IKeyValue kv1 = c.CreateKeyValueContext(kvName1);

                String key1 = mirrorSegment + "." + generic;
                String key2 = dontMirrorSegment + "." + generic;
                kv1.Put(key1, Encoding.UTF8.GetBytes(mirrorSegment));
                kv1.Put(key2, Encoding.UTF8.GetBytes(dontMirrorSegment));

                Thread.Sleep(5000); // transforming takes some amount of time, otherwise the kv2.getKeys() fails
                
                IList<string> keys = kv1.Keys();
                Assert.True(keys.Contains(key1));
                Assert.True(keys.Contains(key2));
                // TODO COME BACK ONCE SERVER IS FIXED
                // Assert.NotNull(kv1.Get(key1));
                // Assert.NotNull(kv1.Get(key2));

                IKeyValue kv2 = c.CreateKeyValueContext(kvName2);
                keys = kv2.Keys();
                Assert.True(keys.Contains(key1));
                Assert.False(keys.Contains(key2));
                // TODO COME BACK ONCE SERVER IS FIXED
                // Assert.NotNull(kv2.Get(key1));
                // Assert.Null(kv2.Get(key2));
            });
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