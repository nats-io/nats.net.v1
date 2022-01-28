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

        public string ValueAsString() => Value == null ? null : Encoding.UTF8.GetString(Value);

        public bool TryGetLongValue(out long lvalue)
        {
            return long.TryParse(ValueAsString(), out lvalue);
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
 
        public override string ToString()
        {
            return $"Bucket: {Bucket}, Key: {Key}, Operation: {Operation}, Revision: {Revision}, Delta: {Delta}, DataLength: {DataLength}, Created: {Created}";
        }

        public bool Equals(KeyValueEntry other)
        {
            return bucketAndKey.Equals(other.bucketAndKey)
                   && Created.Equals(other.Created)
                   && Revision == other.Revision
                   && Delta == other.Delta
                   && Operation.Equals(other.Operation)
                   && DataLength == other.DataLength
                   && Validator.Equal(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((KeyValueEntry)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (bucketAndKey != null ? bucketAndKey.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Value != null ? Value.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ Created.GetHashCode();
                hashCode = (hashCode * 397) ^ Revision.GetHashCode();
                hashCode = (hashCode * 397) ^ Delta.GetHashCode();
                hashCode = (hashCode * 397) ^ (Operation != null ? Operation.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ DataLength.GetHashCode();
                return hashCode;
            }
        }
    }
}