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
using System.IO;

namespace NATS.Client.ObjectStore
{
    public interface IObjectStore
    {
        /// <summary>
        /// Get the name of the object store's bucket.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <returns>the name</returns>
        string BucketName { get; }

        /// <summary>
        /// Place the contents of the input stream into a new object.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="meta">the metadata for the object</param>
        /// <param name="inputStream">the source input stream</param>
        /// <returns>the ObjectInfo for the saved object</returns>
        ObjectInfo Put(ObjectMeta meta, Stream inputStream);

        /// <summary>
        /// Place the contents of the input stream into a new object.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">the name of the object</param>
        /// <param name="inputStream">the source input stream</param>
        /// <returns>the ObjectInfo for the saved object</returns>
        ObjectInfo Put(string objectName, Stream inputStream);

        /// <summary>
        /// Place the bytes into a new object.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">the name of the object</param>
        /// <param name="input">the bytes to store</param>
        /// <returns>the ObjectInfo for the saved object</returns>
        ObjectInfo Put(string objectName, byte[] input);

        /// <summary>
        /// Place the contents of the file into a new object using the file name as the object name.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="fileInfo">the file to read</param>
        /// <returns>the ObjectInfo for the saved object</returns>
        ObjectInfo Put(FileInfo fileInfo);

        /// <summary>
        /// Get an object by name from the store, reading it into the output stream, if the object exists.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <param name="outputStream">the destination stream.</param>
        /// <returns>the ObjectInfo for the object name or throw an exception if it does not exist or is deleted.</returns>
        ObjectInfo Get(string objectName, Stream outputStream);

        /// <summary>
        /// Get the info for an object if the object exists exists / is not deleted.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <returns>the ObjectInfo for the object name or throw an exception if it does not exist.</returns>
        ObjectInfo GetInfo(string objectName);

        /// <summary>
        /// Get the info for an object if the object exists, optionally including deleted.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <param name="includingDeleted">Whether to return info for deleted objects</param>
        /// <returns>the ObjectInfo for the object name or throw an exception if it does not exist.</returns>
        ObjectInfo GetInfo(string objectName, bool includingDeleted);

        /// <summary>
        /// Update the metadata of name, description or headers. All other changes are ignored.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <param name="meta">the metadata with the new or unchanged name, description and headers.</param>
        /// <returns>the ObjectInfo after update</returns>
        ObjectInfo UpdateMeta(string objectName, ObjectMeta meta);

        /// <summary>
        /// Delete the object by name. A No-op if the object is already deleted.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <returns>the ObjectInfo after delete or throw an exception if it does not exist.</returns>
        ObjectInfo Delete(string objectName);

        /// <summary>
        /// Add a link to another object. A link cannot be for another link.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <param name="toInfo">the info object of the object to link to</param>
        /// <returns>the ObjectInfo for the link as saved or throws an exception</returns>
        ObjectInfo AddLink(string objectName, ObjectInfo toInfo);

        /// <summary>
        /// Add a link to another object store (bucket).
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="objectName">The name of the object</param>
        /// <param name="toStore">the store object to link to</param>
        /// <returns>the ObjectInfo for the link as saved or throws an exception</returns>
        ObjectInfo AddBucketLink(string objectName, IObjectStore toStore);

        /// <summary>
        /// Close (seal) the bucket to changes. The store (bucket) will be read only.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <returns>the status object</returns>
        ObjectStoreStatus Seal();

        /// <summary>
        /// Get a list of all object [infos] in the store.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <returns>the list of objects</returns>
        IList<ObjectInfo> GetList();

        /// <summary>
        /// Create a watch on the store (bucket).
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <param name="watcher">the implementation to receive changes.</param>
        /// <param name="watchOptions">the watch options to apply. If multiple conflicting options are supplied, the last options wins.</param>
        /// <returns>the NatsObjectStoreWatchSubscription</returns>
        ObjectStoreWatchSubscription Watch(IObjectStoreWatcher watcher, params ObjectStoreWatchOption[] watchOptions);

        /// <summary>
        /// Get the ObjectStoreStatus object.
        /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
        /// </summary>
        /// <returns>the status object</returns>
        ObjectStoreStatus GetStatus();
    }
}
