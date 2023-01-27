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
    /// <summary>
    /// Configuration referencing a stream source in another account or JetStream domain
    /// </summary>
    public sealed class External : JsonSerializable
    {
        /// <summary>
        /// The subject prefix that imports the other account/domain $JS.API.CONSUMER.> subjects
        /// </summary>
        public string Api { get; }
        
        /// <summary>
        /// The delivery subject to use for the push consumer
        /// </summary>
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

        public override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Api] = Api,
                [ApiConstants.Deliver] = Deliver
            };
        }

        /// <summary>
        /// Construct the External configuration
        /// </summary>
        /// <param name="api">The api prefix</param>
        /// <param name="deliver">The deliver subject</param>
        public External(string api, string deliver)
        {
            Api = api;
            Deliver = deliver;
        }

        /// <summary>
        /// Creates a builder for an External object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static ExternalBuilder Builder() {
            return new ExternalBuilder();
        }

        /// <summary>
        /// External can be created using a ExternalBuilder. 
        /// </summary>
        public sealed class ExternalBuilder
        {
            private string _api;
            private string _deliver;
            
            /// <summary>
            /// Set the api string.
            /// </summary>
            /// <param name="api">the api</param>
            /// <returns></returns>
            public ExternalBuilder WithApi(string api) {
                _api = api;
                return this;
            }
            
            /// <summary>
            /// Set the deliver string.
            /// </summary>
            /// <param name="deliver">the deliver</param>
            /// <returns></returns>
            public ExternalBuilder WithDeliver(string deliver) {
                _deliver = deliver;
                return this;
            }

            /// <summary>
            /// Build a External object
            /// </summary>
            /// <returns>The External</returns>
            public External Build() {
                return new External(_api, _deliver);
            }
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
