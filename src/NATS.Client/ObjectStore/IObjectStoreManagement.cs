// Copyright 2022 The NATS Authors
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

namespace NATS.Client.ObjectStore
{
    public interface IObjectStoreManagement
    {
        /// <summary>
        /// Create an object store.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="config">the object store configuration</param>
        /// <returns>bucket info</returns>
        ObjectStoreStatus Create(ObjectStoreConfiguration config);

        /// <summary>
        /// Get the list of object stores bucket names
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <returns>list of object stores bucket names</returns>
        IList<string> GetBucketNames();

        /// <summary>
        /// Gets the status for an existing object store.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="bucketName">the object store bucket name to get info for</param>
        ObjectStoreStatus GetStatus(string bucketName);

        /// <summary>
        /// Gets the status for all object store buckets.
        /// </summary>
        /// <returns>the bucket status object</returns>
        IList<ObjectStoreStatus> GetStatuses();

        /// <summary>
        /// Deletes an existing object store. Will throw a JetStreamApiException if the delete fails.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        void Delete(string bucketName);
    }
}
