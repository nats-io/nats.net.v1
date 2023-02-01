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
    /// The ObjectMeta is Object Meta is high level information about an object.
    /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
    /// </summary>
    public class ObjectMetaOptions : JsonSerializable
    {
        public ObjectLink Link { get; }
        public int ChunkSize { get; }
        public bool HasData => Link != null || ChunkSize > 0;

        internal ObjectMetaOptions() { }

        internal ObjectMetaOptions(JSONNode node)
        {
            Link = ObjectLink.OptionalInstance(node[ApiConstants.Link]);
            ChunkSize = JsonUtils.AsIntOrMinus1(node, ApiConstants.MaxChunkSize);
        }

        private ObjectMetaOptions(ObjectMetaOptionsBuilder b)
        {
            Link = b._link;
            ChunkSize = b._chunkSize;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            if (Link != null)
            {
                jso[ApiConstants.Link] = Link.ToJsonNode();
            }

            if (ChunkSize > 0)
            {
                jso[ApiConstants.MaxChunkSize] = ChunkSize;
            }

            return jso;
        }

        protected bool Equals(ObjectMetaOptions other)
        {
            return Equals(Link, other.Link) && ChunkSize == other.ChunkSize;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ObjectMetaOptions)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Link != null ? Link.GetHashCode() : 0) * 397) ^ ChunkSize;
            }
        }
 
        internal static ObjectMetaOptionsBuilder Builder() {
            return new ObjectMetaOptionsBuilder();
        }

        internal static ObjectMetaOptionsBuilder Builder(ObjectMetaOptions om) {
            return new ObjectMetaOptionsBuilder(om);
        }

        internal sealed class ObjectMetaOptionsBuilder {
            internal ObjectLink _link;
            internal int _chunkSize;

            internal ObjectMetaOptionsBuilder() {}

            internal ObjectMetaOptionsBuilder(ObjectMetaOptions om) {
                _link = om.Link;
                _chunkSize = om.ChunkSize;
            }

            internal ObjectMetaOptionsBuilder WithLink(ObjectLink link) {
                _link = link;
                return this;
            }

            internal ObjectMetaOptionsBuilder WithChunkSize(int chunkSize) {
                _chunkSize = chunkSize;
                return this;
            }

            internal ObjectMetaOptions Build() {
                return new ObjectMetaOptions(this);
            }
        }
    }
}
