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
using System.Threading;
using System.Threading.Tasks;

namespace NATS.Client.KeyValue
{
    public interface IKeyValueManagementAsync
    {
        /// <summary>
        /// Create a key value store.
        /// </summary>
        /// <param name="config">the key value configuration</param>
        /// <param name="cancellationToken">the cancellation token</param>
        /// <returns>The status</returns>
        Task<KeyValueStatus> CreateAsync(KeyValueConfiguration config, CancellationToken cancellationToken = default);

        /// <summary>
        /// Update a key value store configuration. Storage type cannot change.
        /// </summary>
        /// <param name="config">the key value configuration</param>
        /// <param name="cancellationToken">the cancellation token</param>
        /// <returns>The status</returns>
        Task<KeyValueStatus> UpdateAsync(KeyValueConfiguration config, CancellationToken cancellationToken = default);

        /// <summary>
        /// Get the list of bucket names.
        /// </summary>
        /// <param name="cancellationToken">the cancellation token</param>
        /// <returns>list of bucket names</returns>
        Task<IList<string>> GetBucketNamesAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the status for an existing bucket.
        /// </summary>
        /// <param name="bucketName">the bucket name to use</param>
        /// <param name="cancellationToken">the cancellation token</param>
        /// <returns>the bucket status object</returns>
        Task<KeyValueStatus> GetStatusAsync(string bucketName, CancellationToken cancellationToken = default);

        /// <summary>
        /// Gets the status for all buckets.
        /// </summary>
        /// <param name="cancellationToken">the cancellation token</param>
        /// <returns>the list of statuses</returns>
        Task<IList<KeyValueStatus>> GetStatusesAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Deletes an existing bucket. Will throw a NATSJetStreamException if the delete fails.
        /// </summary>
        /// <param name="bucketName">the bucket name to use</param>
        /// <param name="cancellationToken">the cancellation token</param>
        Task DeleteAsync(string bucketName, CancellationToken cancellationToken = default);
    }
}