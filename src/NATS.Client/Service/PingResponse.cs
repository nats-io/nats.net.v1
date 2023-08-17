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
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.Service
{
    /// <summary>
    /// Ping response class forms the ping json payload, for example:
    /// <code>{"id":"JlkwZvmHAXCQGwwxiPwaBJ","name":"MyService","version":"0.0.1","type":"io.nats.micro.v1.ping_response"}</code>
    /// </summary>
    public class PingResponse : ServiceResponse
    {
        public const string ResponseType = "io.nats.micro.v1.ping_response";

        internal PingResponse(string id, string name, string version, IDictionary<string, string> metadata) 
            : base(ResponseType, id, name, version, metadata) {}

        internal PingResponse(string json) : this(JSON.Parse(json)) {}

        internal PingResponse(JSONNode node) : base(ResponseType, node) {}
        
        public override JSONNode ToJsonNode()
        {
            return BaseJsonObject();
        }
    }
}
