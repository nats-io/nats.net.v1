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

using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class AccountStatistics : ApiResponse
    {
        // rollup contains the memory, storage, streams, consumers and limits from the top level
        private AccountTier _rollupTier;
        
        /// <summary>
        /// Gets the amount of memory storage used by the JetStream deployment.
        /// </summary>
        public ulong MemoryBytes => _rollupTier.MemoryBytes;
        
        /// <summary>
        /// Gets the amount of file storage used by the JetStream deployment.
        /// </summary>
        public ulong StorageBytes => _rollupTier.StorageBytes;
        
        /// <summary>
        /// Bytes that is reserved for memory usage by this account on the server.
        /// </summary>
        public ulong ReservedMemoryBytes => _rollupTier.ReservedMemoryBytes;
        
        /// <summary>
        /// Bytes that is reserved for disk usage by this account on the server.
        /// </summary>
        public ulong ReservedStorageBytes => _rollupTier.ReservedStorageBytes;
        
        /// <summary>
        /// Gets the number of streams used by the JetStream deployment.
        /// </summary>
        public long Streams => _rollupTier.Streams;
        
        /// <summary>
        /// Gets the number of consumers used by the JetStream deployment.
        /// </summary>
        public long Consumers => _rollupTier.Consumers;

        /// <summary>
        /// Gets the Account Limits object. If the account has tiers,
        /// the object will be present but all values will be zero.
        /// See the Account Limits for the specific tier.
        /// </summary>
        public AccountLimits Limits => _rollupTier.Limits;
        
        /// <summary>
        /// Gets the account domain
        /// </summary>
        public string Domain { get; private set;  }
        
        /// <summary>
        /// Gets the account api stats
        /// </summary>
        public ApiStats Api { get; private set;  }
        public IDictionary<string, AccountTier> Tiers;

        public AccountStatistics(Msg msg, bool throwOnError) : base(msg, throwOnError)
        {
            Init();
        }

        public AccountStatistics(string json, bool throwOnError) : base(json, throwOnError)
        {
            Init();
        }

        private void Init()
        {
            _rollupTier = new AccountTier(JsonNode);
            Domain = JsonNode[ApiConstants.Domain].Value;
            Api = new ApiStats(JsonNode[ApiConstants.Api]);
            IDictionary<string, AccountTier> temp = new Dictionary<string, AccountTier>();
            JSONNode tnode = JsonNode[ApiConstants.Tiers];
            foreach (string key in tnode.Keys)
            {
                temp[key] = new AccountTier(tnode[key]);
            }
            Tiers = new ReadOnlyDictionary<string, AccountTier>(temp);
        }

        [Obsolete("This property is obsolete in favor of MemoryBytes which has the proper type to match the server.", false)]
        public long Memory => (long)MemoryBytes;
        
        [Obsolete("This property is obsolete in favor of StorageBytes which has the proper type to match the server.", false)]
        public long Storage => (long)MemoryBytes;
    }
}
