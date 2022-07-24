// Copyright 2021-2022 The NATS Authors
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
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class AccountLimits
    {
        /// <summary>
        /// The maximum amount of Memory storage Stream Messages may consume.
        /// </summary>
        public long MaxMemory { get; }
        
        /// <summary>
        /// The maximum amount of File storage Stream Messages may consume.
        /// </summary>
        public long MaxStorage { get; }
        
        /// <summary>
        /// The maximum number of Streams an account can create.
        /// </summary>
        public long MaxStreams { get; }
        
        /// <summary>
        /// The maximum number of Consumers an account can create.
        /// </summary>
        public long MaxConsumers { get; }
        
        /// <summary>
        /// The maximum number of outstanding ACKs any consumer may configure.
        /// </summary>
        public long MaxAckPending { get; }
        
        /// <summary>
        /// The maximum size any single memory stream may be.
        /// </summary>
        public long MemoryMaxStreamBytes { get; }
        
        /// <summary>
        /// The maximum size any single storage based stream may be.
        /// </summary>
        public long StorageMaxStreamBytes { get; }
        
        /// <summary>
        /// Indicates if streams created in this account requires the max_bytes property set.
        /// </summary>
        public bool MaxBytesRequired { get; }

        internal AccountLimits(JSONNode node)
        {
            MaxMemory = JsonUtils.AsLongOrZero(node, ApiConstants.MaxMemory);
            MaxStorage = JsonUtils.AsLongOrZero(node, ApiConstants.MaxStorage);
            MaxStreams = JsonUtils.AsLongOrZero(node, ApiConstants.MaxStreams);
            MaxConsumers = JsonUtils.AsLongOrZero(node, ApiConstants.MaxConsumers);
            MaxAckPending = JsonUtils.AsLongOrZero(node, ApiConstants.MaxAckPending);
            MemoryMaxStreamBytes = JsonUtils.AsLongOrZero(node, ApiConstants.MemoryMaxStreamBytes);
            StorageMaxStreamBytes = JsonUtils.AsLongOrZero(node, ApiConstants.StorageMaxStreamBytes);
            MaxBytesRequired = node[ApiConstants.MaxBytesRequired].AsBool;
        }
    }
}
