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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Placement requirements for a stream
    /// </summary>
    public sealed class Placement : JsonSerializable
    {
        /// <summary>
        /// The desired cluster name to place the stream
        /// </summary>
        public string Cluster { get; }
        
        /// <summary>
        /// Tags required on servers hosting this stream
        /// </summary>
        public List<string> Tags { get; }

        internal static Placement OptionalInstance(JSONNode placementNode)
        {
            return placementNode.Count == 0 ? null : new Placement(placementNode);
        }

        private Placement(JSONNode placementNode)
        {
            Cluster = placementNode[ApiConstants.Cluster].Value;
            Tags = JsonUtils.OptionalStringList(placementNode, ApiConstants.Tags);
        }

        /// <summary>
        /// Construct the Placement object
        /// </summary>
        /// <param name="cluster">The cluster name</param>
        public Placement(string cluster)
        {
            Cluster = cluster;
        }

        /// <summary>
        /// Construct the Placement object
        /// </summary>
        /// <param name="cluster">The cluster name</param>
        /// <param name="tags">The list of tags. May be null or empty</param>
        public Placement(string cluster, List<string> tags) : this(cluster)
        {
            Tags = tags == null || tags.Count == 0 ? null : tags;
        }

        public override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Cluster] = Cluster,
                [ApiConstants.Tags] = JsonUtils.ToArray(Tags),
            };
        }

        /// <summary>
        /// Creates a builder for a placements object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static PlacementBuilder Builder() {
            return new PlacementBuilder();
        }

        /// <summary>
        /// Placement can be created using a PlacementBuilder. 
        /// </summary>
        public sealed class PlacementBuilder {
            private string _cluster;
            private List<string> _tags;

            /// <summary>
            /// Set the cluster string.
            /// </summary>
            /// <param name="cluster">the cluster</param>
            /// <returns></returns>
            public PlacementBuilder WithCluster(string cluster) {
                _cluster = cluster;
                return this;
            }

            /// <summary>
            /// Set the tags 
            /// </summary>
            /// <param name="tags">tags the list of tags</param>
            /// <returns></returns>
            public PlacementBuilder WithTags(IList<string> tags) {
                _tags = new List<string>(tags);
                return this;
            }

            /// <summary>
            /// Set the tags 
            /// </summary>
            /// <param name="tags">tags the list of tags</param>
            /// <returns></returns>
            public PlacementBuilder WithTags(params string[] tags) {
                _tags = new List<string>(tags);
                return this;
            }

            /// <summary>
            /// Build a Placement object
            /// </summary>
            /// <returns>The Placement</returns>
            public Placement Build() {
                Validator.Required(_cluster, "Cluster");
                return new Placement(_cluster, _tags);
            }
        }
    }
}
