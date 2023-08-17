// Copyright 2023 The NATS Authors
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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// Stats response class forms the stats json payload, for example:
    /// <code>
    /// {
    /// "id": "ZP1oVevzLGu4CBORMXKKke",
    /// "name": "Service1",
    /// "version": "0.0.1",
    /// "endpoints": [{
    ///     "name": "SortEndpointAscending",
    ///     "subject": "sort.ascending",
    ///     "num_requests": 1,
    ///     "processing_time": 538900,
    ///     "average_processing_time": 538900,
    ///     "started": "2023-08-15T13:51:41.318000000Z"
    /// }, {
    ///     "name": "SortEndpointDescending",
    ///     "subject": "sort.descending",
    ///     "num_requests": 1,
    ///     "processing_time": 88400,
    ///     "average_processing_time": 88400,
    ///     "started": "2023-08-15T13:51:41.318000000Z"
    /// }, {
    ///     "name": "EchoEndpoint",
    ///     "subject": "echo",
    ///     "num_requests": 5,
    ///     "processing_time": 1931600,
    ///     "average_processing_time": 386320,
    ///     "data": {
    ///          "idata": 2,
    ///          "sdata": "s-996409223"
    ///     },
    ///     "started": "2023-08-15T13:51:41.318000000Z"
    /// }],
    /// "started": "2023-08-15T13:51:41.319000000Z",
    /// "type": "io.nats.micro.v1.stats_response"
    /// }
    /// </code>
    /// </summary>
    public class StatsResponse : ServiceResponse
    {
        public const string ResponseType = "io.nats.micro.v1.stats_response";

        /// <value>The time the endpoint was started (or restarted)</value>
        public DateTime Started { get; }
        
        /// <value>Get the list of <see cref="EndpointStats"/></value>
        public IList<EndpointStats> EndpointStatsList { get; }
        
        internal StatsResponse(ServiceResponse template, DateTime started, IList<EndpointStats> endpointStatsList) 
            :base(ResponseType, template)
        {
            Started = started;
            EndpointStatsList = endpointStatsList;
        }

        internal StatsResponse(string json) : this(JSON.Parse(json)) {}

        internal StatsResponse(JSONNode node) : base(ResponseType, node)
        {
            Started = JsonUtils.AsDate(node[ApiConstants.Started]);
            EndpointStatsList = EndpointStats.ListOf(node[ApiConstants.Endpoints]);
        }
        
        public override JSONNode ToJsonNode()
        {
            JSONObject jso = BaseJsonObject();
            jso[ApiConstants.Started] = JsonUtils.UnsafeToString(Started);
            JSONArray arr = new JSONArray();
            foreach (var ess in EndpointStatsList)
            {
                arr.Add(null, ess.ToJsonNode());
            }

            jso[ApiConstants.Endpoints] = arr;
            return jso;
        }

        protected bool Equals(StatsResponse other)
        {
            return base.Equals(other) && Started.Equals(other.Started) && Equals(EndpointStatsList, other.EndpointStatsList);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((StatsResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ Started.GetHashCode();
                hashCode = (hashCode * 397) ^ (EndpointStatsList != null ? EndpointStatsList.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
