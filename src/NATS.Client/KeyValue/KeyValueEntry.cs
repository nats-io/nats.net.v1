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

using System;
using System.Text;
using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client.KeyValue
{
    public class KeyValueEntry
    {
        private readonly BucketAndKey bucketAndKey;
        public byte[] Value { get; }
        public DateTime Created { get; }
        public ulong Revision { get; }
        public ulong Delta { get; }
        public KeyValueOperation Operation { get; }
        public long DataLength { get; }

        public KeyValueEntry(MessageInfo mi) {
            bucketAndKey = new BucketAndKey(mi.Subject);
            Value = ExtractValue(mi.Data);
            DataLength = CalculateLength(Value, mi.Headers);
            Created = mi.Time;
            Revision = mi.Sequence;
            Delta = 0;
            Operation = KeyValueUtil.GetOperation(mi.Headers, KeyValueOperation.Put);
        }

        public KeyValueEntry(Msg m) {
            bucketAndKey = new BucketAndKey(m.Subject);
            Value = ExtractValue(m.Data);
            DataLength = CalculateLength(Value, m.Header);
            Created = m.MetaData.Timestamp;
            Revision = m.MetaData.StreamSequence;
            Delta = m.MetaData.NumPending;
            Operation = KeyValueUtil.GetOperation(m.Header, KeyValueOperation.Put);
        }

        public string Bucket => bucketAndKey.Bucket;
        public string Key => bucketAndKey.Key;

        public string StringValue => Value == null ? null : Encoding.UTF8.GetString(Value);

        public bool TryGetLongValue(out long lvalue)
        {
            return long.TryParse(StringValue, out lvalue);
        }

        private static byte[] ExtractValue(byte[] data) {
            return data == null || data.Length == 0 ? null : data;
        }

        private static long CalculateLength(byte[] value, MsgHeader h) {
            if (value == null)
            {
                long.TryParse(h?[JetStreamConstants.MsgSizeHeader], out var len);
                return len;
            }
            return value.Length;
        }
    }
}