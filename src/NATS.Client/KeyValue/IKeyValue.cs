// Copyright 2021 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;

namespace NATS.Client.KeyValue
{
    public interface IKeyValue
    {
        /// <summary>
        /// The name of the bucket
        /// </summary>
        string BucketName { get; }

        /// <summary>
        /// Get the entry for a key
        /// when the key exists and is live (not deleted and not purged)
        /// </summary>
        /// <param name="key">the key</param>
        /// <returns>The entry</returns>
        KeyValueEntry Get(string key);

        /// <summary>
        /// Get the specific revision of an entry for a key
        /// when the key exists and is live (not deleted and not purged)
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="revision">the specific revision</param>
        /// <returns>The entry</returns>
        KeyValueEntry Get(string key, ulong revision);

        /// <summary>
        /// Put a byte[] as the value for a key
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="value">the bytes of the value</param>
        /// <returns>the revision number for the key</returns>
        ulong Put(string key, byte[] value);

        /// <summary>
        /// Put a string as the value for a key
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="value">the UTF-8 string</param>
        /// <returns>the revision number for the key</returns>
        ulong Put(string key, string value);

        /// <summary>
        ///Put a long as the value for a key
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="value">the number</param>
        /// <returns>the revision number for the key</returns>
        ulong Put(string key, long value);

        /// <summary>
        /// Put as the value for a key iff the key does not exist (there is no history)
        /// or is deleted (history shows the key is deleted)
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="value">the bytes of the value</param>
        /// <returns>the revision number for the key</returns>
        ulong Create(string key, byte[] value);

        /// <summary>
        /// Put as the value for a key iff the key exists and its last revision matches the expected
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="value">the bytes of the value</param>
        /// <param name="expectedRevision"></param>
        /// <returns>the revision number for the key</returns>
        ulong Update(string key, byte[] value, ulong expectedRevision);

        /// <summary>
        /// Soft deletes the key by placing a delete marker. 
        /// </summary>
        /// <param name="key">the key</param>
        void Delete(string key);

        /// <summary>
        /// Purge all values/history from the specific key. 
        /// </summary>
        /// <param name="key">the key</param>
        void Purge(string key);

        /// <summary>
        /// Watch updates for a specific key
        /// </summary>
        /// <param name="key">the key</param>
        /// <param name="watcher">the watcher</param>
        /// <param name="watchOptions">the watch options to apply. If multiple conflicting options are supplied, the last options wins.</param>
        /// <returns></returns>
        KeyValueWatchSubscription Watch(string key, IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions);

        /// <summary>
        /// Watch updates for all keys
        /// </summary>
        /// <param name="watcher">the watcher</param>
        /// <param name="watchOptions">the watch options to apply. If multiple conflicting options are supplied, the last options wins.</param>
        /// <returns>The KeyValueWatchSubscription</returns>
        KeyValueWatchSubscription WatchAll(IKeyValueWatcher watcher, params KeyValueWatchOption[] watchOptions);

        /// <summary>
        /// Get a list of the keys in a bucket.
        /// </summary>
        /// <returns>The list of keys</returns>
        IList<string> Keys();

        /// <summary>
        /// Get the history (list of KeyValueEntry) for a key
        /// </summary>
        /// <param name="key">the key</param>
        /// <returns>The list of KeyValueEntry</returns>
        IList<KeyValueEntry> History(string key);

        /// <summary>
        /// Remove history from all keys that currently are deleted or purged,
        /// using a default KeyValuePurgeOptions
        /// </summary>
        void PurgeDeletes();

        /// <summary>
        /// Remove history from all keys that currently are deleted or purged
        /// </summary>
        void PurgeDeletes(KeyValuePurgeOptions options);

        /// <summary>
        /// Get the KeyValueStatus object
        /// </summary>
        /// <returns>the status object</returns>
        KeyValueStatus Status();
    }
}