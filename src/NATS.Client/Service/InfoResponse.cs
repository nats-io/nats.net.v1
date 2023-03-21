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
    public class InfoResponse : ServiceResponse
    {
        public const string ResponseType = "io.nats.micro.v1.info_response";

        public string Description { get; }
        public IList<string> Subjects { get; }

        public InfoResponse(string id, string name, string version, Dictionary<string, string> metadata, string description, IList<string> subjects)
            : base(ResponseType, id, name, version, metadata)
        {
            Description = description;
            Subjects = subjects;
        }

        internal InfoResponse(string json) : this(JSON.Parse(json)) {}

        internal InfoResponse(JSONNode node) : base(ResponseType, node)
        {
            Description = node[ApiConstants.Description];
            Subjects = JsonUtils.StringList(node, ApiConstants.Subjects);
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = BaseJsonObject();
            JsonUtils.AddField(jso, ApiConstants.Description, Description);
            JsonUtils.AddField(jso, ApiConstants.Subjects, Subjects);
            return jso;
        }

        protected bool Equals(InfoResponse other)
        {
            return base.Equals(other) && Description == other.Description && Equals(Subjects, other.Subjects);
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
                hashCode = (hashCode * 397) ^ (Subjects != null ? Subjects.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
