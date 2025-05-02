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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class ApiStats
    {
        /// <summary>
        /// The api level. For future use.
        /// </summary>
        public int Level { get; }
        
        /// <summary>
        /// Total number of API requests received for this account.
        /// </summary>
        public ulong TotalRequests { get; }
        
        /// <summary>
        /// API requests that resulted in an error response.
        /// </summary>
        public ulong ErrorResponses { get; }
        
        /// <summary>
        /// Total number of current in-flight requests.
        /// </summary>
        public ulong InFlight { get; }

        internal ApiStats(JSONNode jsonNode) 
        {
            Level = jsonNode[ApiConstants.Level].AsInt;
            TotalRequests = JsonUtils.AsUlongOrZero(jsonNode, ApiConstants.Total);
            ErrorResponses = JsonUtils.AsUlongOrZero(jsonNode, ApiConstants.Errors);
            InFlight = JsonUtils.AsUlongOrZero(jsonNode, ApiConstants.Inflight);
        }

        [Obsolete("This property is obsolete in favor of TotalRequests which has the proper type to match the server.", false)]
        public long Total => (long)TotalRequests;
        
        [Obsolete("This property is obsolete in favor of ErrorResponses which has the proper type to match the server.", false)]
        public long Errors => (long)TotalRequests;
    }
}
