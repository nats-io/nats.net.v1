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

using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// Info response class forms the info json payload, for example:
    /// <code>{"id":"JlkwZvmHAXCQGwwxiPwaBJ","name":"MyService","version":"0.0.1","endpoints":[{"name":"MyEndpoint","subject":"myend"}],"type":"io.nats.micro.v1.info_response"}</code>
    /// </summary>
    public class InfoResponse : ServiceResponse
    {
        public const string ResponseType = "io.nats.micro.v1.info_response";

        /// <value>Description for the service</value>
        public string Description { get; }
        
        /// <value>List of endpoints</value>
        public IList<Endpoint> Endpoints { get; }

        public InfoResponse(string id, string name, string version, IDictionary<string, string> metadata, string description, IList<Endpoint> endpoints)
            : base(ResponseType, id, name, version, metadata)
        {
            Description = description;
            Endpoints = endpoints;
        }

        internal InfoResponse(string json) : this(JSON.Parse(json)) {}

        internal InfoResponse(JSONNode node) : base(ResponseType, node)
        {
            Description = node[ApiConstants.Description];
            Endpoints = JsonUtils.ListOf<Endpoint>(
                node, ApiConstants.Endpoints, jsonNode => new Endpoint(jsonNode));
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = BaseJsonObject();
            JsonUtils.AddField(jso, ApiConstants.Description, Description);
            JsonUtils.AddField(jso, ApiConstants.Endpoints, Endpoints);
            return jso;
        }

        protected bool Equals(InfoResponse other)
        {
            return base.Equals(other) && Description == other.Description && Validator.SequenceEqual(Endpoints, other.Endpoints);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((InfoResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (Description != null ? Description.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Endpoints != null ? Endpoints.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
