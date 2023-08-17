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
    /// The ObjectLink is used to embed links to other objects.
    /// OBJECT STORE IMPLEMENTATION IS EXPERIMENTAL AND SUBJECT TO CHANGE.
    /// </summary>
    public sealed class ObjectLink : JsonSerializable
    {
        public string Bucket { get; }
        public string ObjectName { get; }
        public bool IsObjectLink => !string.IsNullOrEmpty(ObjectName);
        public bool IsBucketLink => string.IsNullOrEmpty(ObjectName);

        internal static ObjectLink OptionalInstance(JSONNode objectLinkNode)
        {
            return objectLinkNode == null || objectLinkNode.Count == 0 ? null : new ObjectLink(objectLinkNode);
        }

        private ObjectLink(JSONNode objectLinkNode)
        {
            Bucket = objectLinkNode[ApiConstants.Bucket].Value;
            ObjectName = objectLinkNode[ApiConstants.Name].Value;
        }

        internal ObjectLink(string bucket, string objectName)
        {
            Bucket = Validator.ValidateBucketName(bucket, true);
            ObjectName = objectName;
        }

        public static ObjectLink ForBucket(string bucket) {
            return new ObjectLink(bucket, null);
        }

        public static ObjectLink ForObject(string bucket, string objectName) {
            return new ObjectLink(bucket, objectName);
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            if (Bucket != null)
            {
                jso[ApiConstants.Bucket] = Bucket;
            }

            if (ObjectName != null)
            {
                jso[ApiConstants.Name] = ObjectName;
            }

            return jso;
        }

        private bool Equals(ObjectLink other)
        {
            return Bucket == other.Bucket && ObjectName == other.ObjectName;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is ObjectLink other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Bucket != null ? Bucket.GetHashCode() : 0) * 397) ^ (ObjectName != null ? ObjectName.GetHashCode() : 0);
            }
        }
    }
}
