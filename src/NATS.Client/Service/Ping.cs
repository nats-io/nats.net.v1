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

using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class Ping : JsonSerializable
    {
        public string ServiceId { get; }
        public string Name { get; }

        internal Ping(string serviceId, string name)
        {
            ServiceId = serviceId;
            Name = name;
        }

        internal Ping(string json) : this(JSON.Parse(json)) {}

        internal Ping(JSONNode node)
        {
            ServiceId = node[ApiConstants.Id];
            Name = node[ApiConstants.Name];
        }
        
        internal override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, ServiceId);
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            return jso;
        }
    }
}
