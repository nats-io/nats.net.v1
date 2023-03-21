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
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class SchemaResponse : ServiceResponse
    {
        public const string ResponseType = "io.nats.micro.v1.schema_response";

        public string ApiUrl { get; }
        public IList<EndpointResponse> Endpoints { get; }

        public SchemaResponse(string id, string name, string version, Dictionary<string, string> metadata, string apiUrl, IList<EndpointResponse> endpoints)
        : base(ResponseType, id, name, version, metadata)
        {
            ApiUrl = apiUrl;
            Endpoints = endpoints;
        }

        internal SchemaResponse(string json) : this(JSON.Parse(json)) {}

        internal SchemaResponse(JSONNode node) : base(ResponseType, node)
        {
            ApiUrl = node[ApiConstants.ApiUrl];
            Endpoints = EndpointResponse.ListOf(node[ApiConstants.Endpoints]);
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = BaseJsonObject();
            JsonUtils.AddField(jso, ApiConstants.ApiUrl, ApiUrl);
            JSONArray arr = new JSONArray();
            foreach (var endpoint in Endpoints)
            {
                arr.Add(null, endpoint.ToJsonNode());
            }
            jso[ApiConstants.Endpoints] = arr;
            return jso;
        }

        protected bool Equals(SchemaResponse other)
        {
            return base.Equals(other) && ApiUrl == other.ApiUrl && Equals(Endpoints, other.Endpoints);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((SchemaResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = base.GetHashCode();
                hashCode = (hashCode * 397) ^ (ApiUrl != null ? ApiUrl.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Endpoints != null ? Endpoints.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
