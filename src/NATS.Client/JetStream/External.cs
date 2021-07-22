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

using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class External : JsonSerializable
    {
        public string Api { get; }
        public string Deliver { get; }

        internal static External OptionalInstance(JSONNode externalNode)
        {
            return externalNode == null || externalNode.Count == 0 ? null : new External(externalNode);
        }

        private External(JSONNode externalNode)
        {
            Api = externalNode[ApiConstants.Api].Value;
            Deliver = externalNode[ApiConstants.Deliver].Value;
        }

        internal override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Api] = Api,
                [ApiConstants.Deliver] = Deliver
            };
        }

        private bool Equals(External other)
        {
            return Api == other.Api && Deliver == other.Deliver;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is External other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Api != null ? Api.GetHashCode() : 0) * 397) ^ (Deliver != null ? Deliver.GetHashCode() : 0);
            }
        }
    }
}
