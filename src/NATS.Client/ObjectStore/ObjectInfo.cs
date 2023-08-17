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

using System;
using System.Text;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.ObjectStore
{
    /// <summary>
    /// 
    /// </summary>
    public class ObjectInfo : JsonSerializable
    {
        public string Bucket { get; }
        public string Nuid { get; }
        public long Size { get; }
        public DateTime Modified { get; }
        public long Chunks { get; }
        public string Digest { get; }
        public bool IsDeleted { get; }
        public ObjectMeta ObjectMeta { get; }

        public string ObjectName => ObjectMeta.ObjectName;
        public string Description => ObjectMeta.Description;
        public MsgHeader Headers => ObjectMeta.Headers;
        public bool IsLink => ObjectMeta.ObjectMetaOptions.Link != null;
        public ObjectLink Link => ObjectMeta.ObjectMetaOptions.Link; 

        internal ObjectInfo(ObjectInfoBuilder b)
        {
            Bucket = b._bucket;
            Nuid = b._nuid;
            Size = b._size;
            Modified = b._modified;
            Chunks = b._chunks;
            Digest = b._digest;
            IsDeleted = b._deleted;
            ObjectMeta = b._metaBuilder.Build();
        }

        public ObjectInfo(MessageInfo mi) : this(Encoding.UTF8.GetString(mi.Data), mi.Time) {}

        public ObjectInfo(Msg m) : this(Encoding.UTF8.GetString(m.Data), m.MetaData.Timestamp) {}

        internal ObjectInfo(string json, DateTime messageTime) {
            JSONNode node = JSON.Parse(json);
            Bucket = node[ApiConstants.Bucket];
            Nuid = node[ApiConstants.Nuid];
            Size = JsonUtils.AsIntOrMinus1(node, ApiConstants.Size);
            Modified = messageTime.ToUniversalTime();
            Chunks = JsonUtils.AsIntOrMinus1(node, ApiConstants.Chunks);
            Digest = node[ApiConstants.Digest];
            IsDeleted = node[ApiConstants.Deleted].AsBool;
            ObjectMeta = new ObjectMeta(node);
        }

        public override JSONNode ToJsonNode()
        {
            // never write MTIME (Modified)
            JSONObject jso = new JSONObject();
            ObjectMeta.EmbedJson(jso); // the go code embeds the objectMeta's fields instead of as a child object.
            jso[ApiConstants.Bucket] = Bucket;
            jso[ApiConstants.Nuid] = Nuid;
            JsonUtils.AddField(jso, ApiConstants.Size, Size);
            JsonUtils.AddField(jso, ApiConstants.Chunks, Chunks);
            JsonUtils.AddField(jso, ApiConstants.Digest, Digest);
            JsonUtils.AddField(jso, ApiConstants.Deleted, IsDeleted);
            return jso;
        }

        protected bool Equals(ObjectInfo other)
        {
            return Bucket == other.Bucket && Nuid == other.Nuid && Size == other.Size 
                   && Modified.Equals(other.Modified) && Chunks == other.Chunks 
                   && Digest == other.Digest && IsDeleted == other.IsDeleted 
                   && Equals(ObjectMeta, other.ObjectMeta);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ObjectInfo)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Bucket != null ? Bucket.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Nuid != null ? Nuid.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Size.GetHashCode();
                hashCode = (hashCode * 397) ^ Modified.GetHashCode();
                hashCode = (hashCode * 397) ^ Chunks.GetHashCode();
                hashCode = (hashCode * 397) ^ (Digest != null ? Digest.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ IsDeleted.GetHashCode();
                hashCode = (hashCode * 397) ^ (ObjectMeta != null ? ObjectMeta.GetHashCode() : 0);
                return hashCode;
            }
        }

        internal static ObjectInfoBuilder Builder(string bucket, string objectName) {
            return new ObjectInfoBuilder(bucket, objectName);
        }

        internal static ObjectInfoBuilder Builder(string bucket, ObjectMeta meta) {
            return new ObjectInfoBuilder(bucket, meta);
        }

        internal static ObjectInfoBuilder Builder(ObjectInfo info) {
            return new ObjectInfoBuilder(info);
        }

        public sealed class ObjectInfoBuilder {
            internal string _bucket;
            internal string _nuid;
            internal long _size;
            internal DateTime _modified;
            internal long _chunks;
            internal string _digest;
            internal bool _deleted;
            internal ObjectMeta.ObjectMetaBuilder _metaBuilder;

            internal ObjectInfoBuilder(string bucket, string objectName) {
                _metaBuilder = ObjectMeta.Builder(objectName);
                WithBucket(bucket);
            }

            public ObjectInfoBuilder(string bucket, ObjectMeta meta) {
                _metaBuilder = ObjectMeta.Builder(meta);
                WithBucket(bucket);
            }

            public ObjectInfoBuilder(ObjectInfo info) {
                _bucket = info.Bucket;
                _nuid = info.Nuid;
                _size = info.Size;
                _modified = info.Modified;
                _chunks = info.Chunks;
                _digest = info.Digest;
                _deleted = info.IsDeleted;
                _metaBuilder = ObjectMeta.Builder(info.ObjectMeta);
            }

            public ObjectInfoBuilder WithObjectName(string name) {
                _metaBuilder.WithObjectName(name);
                return this;
            }

            public ObjectInfoBuilder WithBucket(string bucket) {
                this._bucket = Validator.ValidateBucketName(bucket, true);
                return this;
            }

            public ObjectInfoBuilder WithNuid(string nuid) {
                this._nuid = nuid;
                return this;
            }

            public ObjectInfoBuilder WithSize(long size) {
                this._size = size;
                return this;
            }

            public ObjectInfoBuilder WithModified(DateTime modified) {
                this._modified = modified;
                return this;
            }

            public ObjectInfoBuilder WithChunks(long chunks) {
                this._chunks = chunks;
                return this;
            }

            public ObjectInfoBuilder WithDigest(string digest) {
                this._digest = digest;
                return this;
            }

            public ObjectInfoBuilder WithDeleted(bool deleted) {
                this._deleted = deleted;
                return this;
            }

            public ObjectInfoBuilder WithDescription(string description) {
                _metaBuilder.WithDescription(description);
                return this;
            }

            public ObjectInfoBuilder WithHeaders(MsgHeader headers) {
                _metaBuilder.WithHeaders(headers);
                return this;
            }

            public ObjectInfoBuilder WithOptions(ObjectMetaOptions objectMetaOptions) {
                _metaBuilder.WithOptions(objectMetaOptions);
                return this;
            }

            public ObjectInfoBuilder WithChunkSize(int chunkSize) {
                _metaBuilder.WithChunkSize(chunkSize);
                return this;
            }

            public ObjectInfoBuilder WithLink(ObjectLink link) {
                _metaBuilder.WithLink(link);
                return this;
            }

            public ObjectInfoBuilder WithBucketLink(string bucket) {
                _metaBuilder.WithLink(new ObjectLink(bucket, null));
                return this;
            }

            public ObjectInfoBuilder WithObjectLink(string bucket, string objectName) {
                _metaBuilder.WithLink(new ObjectLink(bucket, objectName));
                return this;
            }

            public ObjectInfo Build() {
                return new ObjectInfo(this);
            }
        }
    }
}
