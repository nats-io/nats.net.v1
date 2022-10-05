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

using System;
using System.Collections.Generic;

namespace NATS.Client.KeyValue
{
    public interface IKeyValueManagement
    {
        /// <summary>
        /// Create a key value store.
        /// </summary>
        /// <param name="config">the key value configuration</param>
        /// <returns>The status</returns>
        KeyValueStatus Create(KeyValueConfiguration config);

        /// <summary>
        /// Update a key value store configuration. Storage type cannot change.
        /// </summary>
        /// <param name="config">the key value configuration</param>
        /// <returns>The status</returns>
        KeyValueStatus Update(KeyValueConfiguration config);

        /// <summary>
        /// Get the list of bucket names.
        /// </summary>
        /// <returns>list of bucket names</returns>
        IList<string> GetBucketNames();

        /// <summary>
        /// Gets the info for an existing bucket.
        /// </summary>
        /// <param name="bucketName">the bucket name to use</param>
        /// <returns>the bucket status object</returns>
        [Obsolete("This method will soon be deprecated. Use GetStatus instead.")]
        KeyValueStatus GetBucketInfo(string bucketName);

        /// <summary>
        /// Gets the status for an existing bucket.
        /// </summary>
        /// <param name="bucketName">the bucket name to use</param>
        /// <returns>the bucket status object</returns>
        KeyValueStatus GetStatus(string bucketName);

        /// <summary>
        /// Gets the status for all buckets.
        /// </summary>
        /// <returns>the list of statuses</returns>
        IList<KeyValueStatus> GetStatuses();

        /// <summary>
        /// Deletes an existing bucket. Will throw a NATSJetStreamException if the delete fails.
        /// </summary>
        /// <param name="bucketName"></param>
        void Delete(string bucketName);
    }
}