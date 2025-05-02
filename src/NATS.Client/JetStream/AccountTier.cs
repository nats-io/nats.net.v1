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

using System;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class AccountTier
    {
        /// <summary>
        /// Memory Storage being used for Stream Message storage in this tier.
        /// </summary>
        public ulong MemoryBytes { get; }
        
        /// <summary>
        /// File Storage being used for Stream Message storage in this tier.
        /// </summary>
        public ulong StorageBytes { get; }

        /// <summary>
        /// Bytes that is reserved for memory usage by this account on the server
        /// </summary>
        public ulong ReservedMemoryBytes { get; }
        
        /// <summary>
        /// Bytes that is reserved for disk usage by this account on the server
        /// </summary>
        public ulong ReservedStorageBytes { get; }

        /// <summary>
        /// Number of active streams in this tier.
        /// </summary>
        public long Streams { get; }
        
        /// <summary>
        /// Number of active consumers in this tier.
        /// </summary>
        public long Consumers { get; }
        
        /// <summary>
        /// The limits of this tier.
        /// </summary>
        public AccountLimits Limits { get; }

        internal AccountTier(JSONNode jsonNode) 
        {
            MemoryBytes = jsonNode[ApiConstants.Memory].AsUlong;
            StorageBytes = jsonNode[ApiConstants.Storage].AsUlong;
            ReservedMemoryBytes = jsonNode[ApiConstants.ReservedMemory].AsUlong;
            ReservedStorageBytes = jsonNode[ApiConstants.ReservedStorage].AsUlong;
            Streams = jsonNode[ApiConstants.Streams].AsLong;
            Consumers = jsonNode[ApiConstants.Consumers].AsLong;
            Limits = new AccountLimits(jsonNode[ApiConstants.Limits]);
        }
        
        [Obsolete("This property is obsolete in favor of MemoryBytes which has the proper type to match the server.", false)]
        public long Memory => (long)MemoryBytes;
        
        [Obsolete("This property is obsolete in favor of StorageBytes which has the proper type to match the server.", false)]
        public long Storage => (long)MemoryBytes;
    }
}
