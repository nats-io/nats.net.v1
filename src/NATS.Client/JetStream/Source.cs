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
using System.Collections.Generic;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Information about an upstream stream source in a mirror
    /// </summary>
    public sealed class Source : SourceBase
    {
        internal static List<Source> OptionalListOf(JSONNode sourceListNode)
        {
            if (sourceListNode == null)
            {
                return null;
            }

            List<Source> list = new List<Source>();
            foreach (var sourceNode in sourceListNode.Children)
            {
                list.Add(new Source(sourceNode));
            }
            return list.Count == 0 ? null : list;
        }

        /// <summary>
        /// Construct a Source object
        /// </summary>
        /// <param name="name">the source stream name</param>
        /// <param name="startSeq">the start sequence</param>
        /// <param name="startTime">the start time</param>
        /// <param name="filterSubject">the filter subject</param>
        /// <param name="external">the external reference</param>
        /// <param name="subjectTransforms">the subject transforms, defaults to none</param>
        public Source(string name, ulong startSeq, DateTime startTime, string filterSubject, External external,
            IList<SubjectTransform> subjectTransforms = null)
            : base(name, startSeq, startTime, filterSubject, external, subjectTransforms) {}

        internal Source(JSONNode sourceBaseNode) : base(sourceBaseNode) {}

        internal Source(SourceBuilder sb) : base(sb) {}

        /// <summary>
        /// Creates a builder for a source object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static SourceBuilder Builder() {
            return new SourceBuilder();
        }
        
        /// <summary>
        /// Creates a builder for a source object based on an existing source object.
        /// </summary>
        /// <returns>The Builder</returns>
        public static SourceBuilder Builder(Source source) {
            return new SourceBuilder(source);
        }

        /// <summary>
        /// Source can be created using a SourceBuilder. 
        /// </summary>
        public sealed class SourceBuilder : SourceBaseBuilder<SourceBuilder, Source>
        {
            public SourceBuilder() {}
            public SourceBuilder(Source source) : base(source) {}

            protected override SourceBuilder GetThis()
            {
                return this;
            }

            public override Source Build()
            {
                return new Source(this);
            }
        }
    }
}
