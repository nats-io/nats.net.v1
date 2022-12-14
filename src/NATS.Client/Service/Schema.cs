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
    public class Schema : JsonSerializable
    {
        public string Request { get; }
        public string Response { get; }

        internal Schema(string request, string response)
        {
            Request = request;
            Response = response;
        }

        internal static Schema OptionalInstance(JSONNode schemaNode)
        {
            return schemaNode == null ? null : new Schema(schemaNode);
        }

        internal Schema(JSONNode schemaNode)
        {
            Request = schemaNode[ApiConstants.Request];
            Response = schemaNode[ApiConstants.Response];
        }

        internal override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Request, Request);
            JsonUtils.AddField(jso, ApiConstants.Response, Response);
            return jso;
        }

        public override string ToString()
        {
            return $"Request: {Request}, Response: {Response}";
        }
    }
}
