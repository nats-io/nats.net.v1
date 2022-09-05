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

using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client.ObjectStore
{
    public class ObjectStoreStatus
    {
        /// <summary>
        /// The info for the stream which backs the bucket. Valid for BackingStore "JetStream"
        /// </summary>
        public StreamInfo BackingStreamInfo { get; }

        /// <summary>
        /// The configuration object directly
        /// </summary>
        public ObjectStoreConfiguration Config { get; }

        public ObjectStoreStatus(StreamInfo si) {
            BackingStreamInfo = si;
            Config = new ObjectStoreConfiguration(si.Config);
        }

        /// <summary>
        /// The name of ths object store
        /// </summary>
        public string BucketName => Config.BucketName;

        /// <summary>
        /// The description of this bucket
        /// </summary>
        public string Description => Config.Description;

        /// <summary>
        /// The combined size of all data in the bucket including metadata, in bytes
        /// </summary>
        public ulong Size => BackingStreamInfo.State.Bytes;

        /// <summary>
        /// If true, indicates the stream is sealed and cannot be modified in any way
        /// </summary>
        public bool Sealed => Config.BackingConfig.Sealed;

        /// <summary>
        /// The maximum age for a value in this bucket
        /// </summary>
        public Duration Ttl => Config.Ttl;

        /// <summary>
        /// The storage type for this bucket
        /// </summary>
        public StorageType StorageType => Config.StorageType;
        
        /// <summary>
        /// The number of replicas for this bucket
        /// </summary>
        public int Replicas => Config.Replicas;

        /// <summary>
        /// Placement directives to consider when placing replicas of this stream
        /// </summary>
        public Placement Placement => Config.Placement;

        /// <summary>
        /// The name of the type of backing store, currently only "JetStream"
        /// </summary>
        public string BackingStore => "JetStream";

        public override string ToString()
        {
            return $"BucketName: {BucketName}, Description: {Description}, Size: {Size}, Sealed: {Sealed}, Ttl: {Ttl}, StorageType: {StorageType}, Replicas: {Replicas}, BackingStore: {BackingStore}";
        }
    }
}
