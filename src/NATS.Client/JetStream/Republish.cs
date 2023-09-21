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
using static NATS.Client.Internals.JsonUtils;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Republish options for a stream
    /// </summary>
    public sealed class Republish : JsonSerializable
    {
        /// <summary>
        /// The Published Subject-matching filter
        /// </summary>
        public string Source { get; }

        /// <summary>
        /// The RePublish Subject template
        /// </summary>
        public string Destination { get; }
        
        /// <summary>
        /// Whether to RePublish only headers (no body)
        /// </summary>
        public bool HeadersOnly { get; }

        internal static Republish OptionalInstance(JSONNode republishNode)
        {
            return republishNode.Count == 0 ? null : new Republish(republishNode);
        }

        private Republish(JSONNode republishNode)
        {
            Source = republishNode[ApiConstants.Src].Value;
            Destination = republishNode[ApiConstants.Dest].Value;
            HeadersOnly = republishNode[ApiConstants.HeadersOnly].AsBool;
        }

        /// <summary>
        /// Construct the Republish object
        /// </summary>
        /// <param name="source">the Published Subject-matching filter</param>
        /// <param name="destination">the RePublish Subject template</param>
        /// <param name="headersOnly">Whether to RePublish only headers (no body)</param>
        public Republish(string source, string destination, bool headersOnly)
        {
            Validator.Required(source, "Source");
            Validator.Required(destination, "Destination");
            Source = source;
            Destination = destination;
            HeadersOnly = headersOnly;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();
            AddField(o, ApiConstants.Src, Source);
            AddField(o, ApiConstants.Dest, Destination);
            AddField(o, ApiConstants.HeadersOnly, HeadersOnly);
            return o;
        }

        /// <summary>
        /// Creates a builder for a republish object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static RepublishBuilder Builder() {
            return new RepublishBuilder();
        }

        /// <summary>
        /// Republish can be created using a RepublishBuilder. 
        /// </summary>
        public sealed class RepublishBuilder {
            private string _source;
            private string _destination;
            private bool _headersOnly;

            /// <summary>
            /// Set the Published Subject-matching filter.
            /// </summary>
            /// <param name="source">the source</param>
            /// <returns></returns>
            public RepublishBuilder WithSource(string source) {
                _source = source;
                return this;
            }

            /// <summary>
            /// Set the RePublish Subject template
            /// </summary>
            /// <param name="destination">the destination</param>
            /// <returns></returns>
            public RepublishBuilder WithDestination(string destination) {
                _destination = destination;
                return this;
            }
            
            /// <summary>
            /// Set Whether to RePublish only headers (no body)
            /// </summary>
            /// <param name="headersOnly">the headers only flag</param>
            /// <returns></returns>
            public RepublishBuilder WithHeadersOnly(bool headersOnly) {
                _headersOnly = headersOnly;
                return this;
            }

            /// <summary>
            /// Build a Republish object
            /// </summary>
            /// <returns>The Republish</returns>
            public Republish Build() {
                return new Republish(_source, _destination, _headersOnly);
            }
        }
    }
}
