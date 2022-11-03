// Copyright 2021 The NATS Authors
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

namespace NATS.Client.KeyValue
{
    public static class KeyValueUtil
    {
        internal const string KvStreamPrefix = "KV_";
        internal static readonly int KvStreamPrefixLen = KvStreamPrefix.Length;
        internal const string KvSubjectPrefix = "$KV.";
        internal const string KvSubjectSuffix = ".>";
        internal const string KvOperationHeaderKey = "KV-Operation";
        
        public static MsgHeader DeleteHeaders => new MsgHeader
        {
            { KvOperationHeaderKey, KeyValueOperation.Delete.HeaderValue }
        };

        public static readonly MsgHeader PurgeHeaders = new MsgHeader
        {
            { KvOperationHeaderKey, KeyValueOperation.Purge.HeaderValue },
            { JetStreamConstants.RollupHeader, JetStreamConstants.RollupHeaderSubject }
        };

        public static string ExtractBucketName(string streamName) {
            return streamName.Substring(KvStreamPrefixLen);
        }

        public static string ToStreamName(string bucketName) {
            return KvStreamPrefix + bucketName;
        }

        public static string ToStreamSubject(string bucketName) {
            return KvSubjectPrefix + bucketName + KvSubjectSuffix;
        }

        public static string ToKeyPrefix(string bucketName) {
            return KvSubjectPrefix + bucketName + ".";
        }

        public static bool HasPrefix(string bucketName) {
            return bucketName.StartsWith(KvStreamPrefix);
        }

        public static string TrimPrefix(string bucketName) {
            if (bucketName.StartsWith(KvStreamPrefix)) {
                return bucketName.Substring(KvStreamPrefix.Length);
            }
            return bucketName;
        }

        public static string GetOperationHeader(MsgHeader h) {
            return h?[KvOperationHeaderKey];
        }

        public static KeyValueOperation GetOperation(MsgHeader h, KeyValueOperation dflt) {
            return KeyValueOperation.GetOrDefault(GetOperationHeader(h), dflt);
        }
    }

    internal class BucketAndKey {
        public string Bucket { get; }
        public string Key { get; }

        public BucketAndKey(Msg m) : this(m.Subject) {}

        public BucketAndKey(string subject)
        {
            string[] split = subject.Split(new[] { '.' }, 3);
            Bucket = split[1];
            Key = split[2];
        }

        public bool Equals(BucketAndKey other)
        {
            return Bucket == other.Bucket && Key == other.Key;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((BucketAndKey)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Bucket != null ? Bucket.GetHashCode() : 0) * 397) ^ (Key != null ? Key.GetHashCode() : 0);
            }
        }
    }
}