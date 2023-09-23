// Copyright 2023 The NATS Authors
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
using NATS.Client.Internals.SimpleJSON;
using static NATS.Client.Internals.JsonUtils;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// SubjectTransform options for a stream
    /// </summary>
    public sealed class SubjectTransform : JsonSerializable
    {
        /// <summary>
        /// The Published subject matching filter
        /// </summary>
        public string Source { get; }

        /// <summary>
        /// The SubjectTransform Subject template
        /// </summary>
        public string Destination { get; }

        internal static SubjectTransform OptionalInstance(JSONNode subjectTransformNode)
        {
            return subjectTransformNode.Count == 0 ? null : new SubjectTransform(subjectTransformNode);
        }
        
        internal static IList<SubjectTransform> OptionalListOf(JSONNode subjectTransformListNode)
        {
            if (subjectTransformListNode == null)
            {
                return null;
            }
            
            IList<SubjectTransform> list = new List<SubjectTransform>();
            foreach (var subjectTransformNode in subjectTransformListNode.Children)
            {
                list.Add(new SubjectTransform(subjectTransformNode));
            }
            return list.Count == 0 ? null : list;
        }

        private SubjectTransform(JSONNode subjectTransformNode)
        {
            Source = subjectTransformNode[ApiConstants.Src].Value;
            Destination = subjectTransformNode[ApiConstants.Dest].Value;
        }

        /// <summary>
        /// Construct the SubjectTransform object
        /// </summary>
        /// <param name="source">the Published subject matching filter</param>
        /// <param name="destination">the SubjectTransform Subject template</param>
        public SubjectTransform(string source, string destination)
        {
            Source = source;
            Destination = destination;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();
            AddField(o, ApiConstants.Src, Source);
            AddField(o, ApiConstants.Dest, Destination);
            return o;
        }

        /// <summary>
        /// Creates a builder for a SubjectTransform object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static SubjectTransformBuilder Builder() {
            return new SubjectTransformBuilder();
        }

        /// <summary>
        /// SubjectTransform can be created using a SubjectTransformBuilder. 
        /// </summary>
        public sealed class SubjectTransformBuilder {
            private string _source;
            private string _destination;

            /// <summary>
            /// Set the Published subject matching filter.
            /// </summary>
            /// <param name="source">the source</param>
            /// <returns>The SubjectTransformBuilder</returns>
            public SubjectTransformBuilder WithSource(string source) {
                _source = source;
                return this;
            }

            /// <summary>
            /// Set the SubjectTransform Subject template
            /// </summary>
            /// <param name="destination">the destination</param>
            /// <returns>The SubjectTransformBuilder</returns>
            public SubjectTransformBuilder WithDestination(string destination) {
                _destination = destination;
                return this;
            }
            
            /// <summary>
            /// Build a SubjectTransform object
            /// </summary>
            /// <returns>The SubjectTransform object</returns>
            public SubjectTransform Build() {
                return new SubjectTransform(_source, _destination);
            }
        }

        private bool Equals(SubjectTransform other)
        {
            return Source == other.Source && Destination == other.Destination;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(this, obj) || obj is SubjectTransform other && Equals(other);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Source != null ? Source.GetHashCode() : 0) * 397) ^ (Destination != null ? Destination.GetHashCode() : 0);
            }
        }
    }
}
