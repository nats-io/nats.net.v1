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

namespace NATS.Client.ObjectStore
{
    public static class ObjectStoreUtil
    {
        public const int DefaultChunkSize = 128 * 1024; // 128k
        internal const string ObjStreamPrefix = "OBJ_";
        internal static readonly int ObjStreamPrefixLen = ObjStreamPrefix.Length;
        internal const string ObjSubjectPrefix = "$O.";
        internal const string ObjSubjectSuffix = ".>";
        internal const string ObjMetaPart = ".M";
        internal const string ObjChunkPart = ".C";

        public static MsgHeader MetaHeaders => new MsgHeader
        {
            { JetStreamConstants.RollupHeader, JetStreamConstants.RollupHeaderSubject }
        };

        public static string ExtractBucketName(string streamName)
        {
            return streamName.Substring(ObjStreamPrefixLen);
        }

        public static string ToStreamName(string bucketName)
        {
            return ObjStreamPrefix + bucketName;
        }

        public static string ToMetaStreamSubject(string bucketName)
        {
            return ObjSubjectPrefix + bucketName + ObjMetaPart + ObjSubjectSuffix;
        }

        public static string ToChunkStreamSubject(string bucketName)
        {
            return ObjSubjectPrefix + bucketName + ObjChunkPart + ObjSubjectSuffix;
        }

        public static string ToMetaPrefix(string bucketName)
        {
            return ObjSubjectPrefix + bucketName + ObjMetaPart + ".";
        }

        public static string ToChunkPrefix(string bucketName)
        {
            return ObjSubjectPrefix + bucketName + ObjChunkPart + ".";
        }

        public static string EncodeForSubject(string name)
        {
            var plainTextBytes = System.Text.Encoding.UTF8.GetBytes(name);
            return System.Convert.ToBase64String(plainTextBytes);
        }
    }
}
