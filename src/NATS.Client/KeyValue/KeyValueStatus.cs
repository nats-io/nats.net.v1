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

namespace NATS.Client.KeyValue
{
    public class KeyValueStatus
    {
        /// <summary>
        /// The info for the stream which backs the bucket. Valid for BackingStore "JetStream"
        /// </summary>
        public StreamInfo BackingStreamInfo { get; }

        /// <summary>
        /// The configuration object directly
        /// </summary>
        public KeyValueConfiguration Config { get; }

        public KeyValueStatus(StreamInfo si) {
            BackingStreamInfo = si;
            Config = new KeyValueConfiguration(si.Config);
        }

        /// <summary>
        /// The name of the bucket
        /// </summary>
        public string BucketName => Config.BucketName;

        /// <summary>
        /// The description of this bucket
        /// </summary>
        public string Description => Config.Description;

        /// <summary>
        /// The number of total entries in the bucket, including historical entries
        /// </summary>
        public ulong EntryCount => BackingStreamInfo.State.Messages;

        /// <summary>
        /// The size of the bucket in bytes
        /// </summary>
        public ulong Bytes => BackingStreamInfo.State.Bytes;

        /// <summary>
        /// The maximum number of history for any one key. Includes the current value
        /// </summary>
        public long MaxHistoryPerKey => Config.MaxHistoryPerKey;

        /// <summary>
        /// The maximum number of bytes for this bucket
        /// </summary>
        public long MaxBucketSize => Config.MaxBucketSize;

        /// <summary>
        /// The maximum size for an individual value in the bucket
        /// </summary>
        public long MaxValueSize => Config.MaxValueSize;

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
        /// Republish options
        /// </summary>
        public Republish Republish => Config.Republish;

        /// <summary>
        /// The name of the type of backing store, currently only "JetStream"
        /// </summary>
        public string BackingStore => "JetStream";

        public override string ToString()
        {
            return $"BucketName: {BucketName}, Description: {Description}, EntryCount: {EntryCount}, MaxHistoryPerKey: {MaxHistoryPerKey}, MaxBucketSize: {MaxBucketSize}, MaxValueSize: {MaxValueSize}, Ttl: {Ttl}, StorageType: {StorageType}, Replicas: {Replicas}, BackingStore: {BackingStore}";
        }
    }
}
