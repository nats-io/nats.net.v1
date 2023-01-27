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

namespace NATS.Client.ObjectStore
{
    /// <summary>
    /// 
    /// </summary>
    public class ObjectMeta : JsonSerializable
    {
        public string ObjectName { get; }
        public string Description { get; }
        public MsgHeader Headers { get; }
        public ObjectMetaOptions ObjectMetaOptions { get; }

        internal ObjectMeta(JSONNode node)
        {
            ObjectName = node[ApiConstants.Name];
            Description = node[ApiConstants.Description];
            Headers = JsonUtils.AsHeaders(node, ApiConstants.Headers);
            JSONNode optNode = node[ApiConstants.Options];
            if (optNode == null)
            {
                ObjectMetaOptions = new ObjectMetaOptions();
            }
            else
            {
                ObjectMetaOptions = new ObjectMetaOptions(optNode);
            }
        }

        private ObjectMeta(ObjectMetaBuilder b)
        {
            ObjectName = b._objectName;
            Description = b._description;
            Headers = b._headers;
            ObjectMetaOptions = b._metaOptionsBuilder.Build();
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            EmbedJson(jso);
            return jso;
        }

        internal void EmbedJson(JSONObject jsonObject)
        {
            jsonObject[ApiConstants.Name] = ObjectName;
            jsonObject[ApiConstants.Description] = Description;
            JsonUtils.AddField(jsonObject, ApiConstants.Headers, Headers);
            if (ObjectMetaOptions.HasData)
            {
                jsonObject[ApiConstants.Options] = ObjectMetaOptions.ToJsonNode();
            }
        }

        protected bool Equals(ObjectMeta other)
        {
            return ObjectName == other.ObjectName 
                   && Description == other.Description 
                   && Equals(Headers, other.Headers) 
                   && Equals(ObjectMetaOptions, other.ObjectMetaOptions);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ObjectMeta)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (ObjectName != null ? ObjectName.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Description != null ? Description.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Headers != null ? Headers.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (ObjectMetaOptions != null ? ObjectMetaOptions.GetHashCode() : 0);
                return hashCode;
            }
        }

        public static ObjectMetaBuilder Builder(string objectName) {
            return new ObjectMetaBuilder(objectName);
        }

        internal static ObjectMetaBuilder Builder(ObjectMeta om) {
            return new ObjectMetaBuilder(om);
        }

        public static ObjectMeta ForObjectName(string objectName) {
            return new ObjectMetaBuilder(objectName).Build();
        }

        public sealed class ObjectMetaBuilder {
            internal string _objectName;
            internal string _description;
            internal MsgHeader _headers;
            internal ObjectMetaOptions.ObjectMetaOptionsBuilder _metaOptionsBuilder;

            public ObjectMetaBuilder(string objectName)
            {
                _headers = new MsgHeader();
                _metaOptionsBuilder = ObjectMetaOptions.Builder();
                WithObjectName(objectName);
            }

            public ObjectMetaBuilder(ObjectMeta om) {
                _objectName = om.ObjectName;
                _description = om.Description;
                _headers = om.Headers;
                _metaOptionsBuilder = ObjectMetaOptions.Builder(om.ObjectMetaOptions);
            }

            public ObjectMetaBuilder WithObjectName(string objectName) {
                _objectName = Validator.ValidateNotNull(objectName, "Object Name");
                return this;
            }

            public ObjectMetaBuilder WithDescription(string description) {
                _description = description;
                return this;
            }

            public ObjectMetaBuilder WithHeaders(MsgHeader headers) {
                if (headers == null)
                {
                    _headers.Clear();
                }
                else
                {
                    _headers = headers;
                }
                return this;
            }

            public ObjectMetaBuilder WithOptions(ObjectMetaOptions options) {
                _metaOptionsBuilder = ObjectMetaOptions.Builder(options);
                return this;
            }

            public ObjectMetaBuilder WithChunkSize(int chunkSize) {
                _metaOptionsBuilder.WithChunkSize(chunkSize);
                return this;
            }

            public ObjectMetaBuilder WithLink(ObjectLink link) {
                _metaOptionsBuilder.WithLink(link);
                return this;
            }

            public ObjectMeta Build() {
                return new ObjectMeta(this);
            }
        }
    }
}
