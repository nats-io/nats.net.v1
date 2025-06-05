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

using System.Collections.Generic;

namespace NATS.Client.KeyValue
{
    public class KeyValueOperation
    {
        private readonly byte _id;
        public string HeaderValue { get; }
        public string[] MarkerReasons { get; }

        private KeyValueOperation(byte id, string headerValue, string[] markerReasons)
        {
            _id = id;
            HeaderValue = headerValue;
            MarkerReasons = markerReasons;
        }

        public static readonly KeyValueOperation Put = new KeyValueOperation(1, "PUT", new string[0]);
        public static readonly KeyValueOperation Delete = new KeyValueOperation(2, "DEL", new []{"Remove"});
        public static readonly KeyValueOperation Purge = new KeyValueOperation(3, "PURGE", new []{"MaxAge", "Purge"});
 
        public static KeyValueOperation GetOrDefault(string s, KeyValueOperation dflt)
        {
            if (!string.IsNullOrWhiteSpace(s))
            {
                if (s.Equals(Put.HeaderValue)) return Put;
                if (s.Equals(Delete.HeaderValue)) return Delete;
                if (s.Equals(Purge.HeaderValue)) return Purge;
            }
            return dflt;
        }

        public static KeyValueOperation GetByMarkerReason(string markerReason)
        {
            if ("Remove".Equals(markerReason))
            {
                return Delete;
                
            }
            if ("MaxAge".Equals(markerReason))
            {
                return Purge;
                
            }
            if ("Purge".Equals(markerReason))
            {
                return Purge;
                
            }
            return null;
        }

        public override bool Equals(object obj) => obj is KeyValueOperation other && Equals(other);

        public override int GetHashCode() => _id.GetHashCode();

        public bool Equals(KeyValueOperation other) {
            return _id.Equals(other?._id);
        }

        public override string ToString()
        {
            return HeaderValue;
        }
    }
}