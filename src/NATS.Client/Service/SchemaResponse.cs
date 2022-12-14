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
    public class SchemaResponse : JsonSerializable
    {
        public string ServiceId { get; }
        public string Name { get; }
        public string Version { get; }
        public Schema Schema { get; }

        internal SchemaResponse(string serviceId, ServiceDescriptor descriptor)
        {
            ServiceId = serviceId;
            Name = descriptor.Name;
            Version = descriptor.Version;
            if (string.IsNullOrEmpty(descriptor.SchemaRequest) && string.IsNullOrEmpty(descriptor.SchemaResponse))
            {
                Schema = null;
            }
            else
            {
                Schema = new Schema(descriptor.SchemaRequest, descriptor.SchemaResponse);
            }
        }

        internal SchemaResponse(string json)
        {
            JSONNode node = JSON.Parse(json);
            ServiceId = node[ApiConstants.Id];
            Name = node[ApiConstants.Name];
            Version = node[ApiConstants.Version];
            Schema = Schema.OptionalInstance(node[ApiConstants.Schema]);
        }

        internal override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, ServiceId);
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Version, Version);
            jso[ApiConstants.Schema] = Schema?.ToJsonNode();
            return jso;
        }

        public override string ToString()
        {
            return $"ServiceId: {ServiceId}, Name: {Name}, Version: {Version}, Schema: {Schema}";
        }
    }
}
