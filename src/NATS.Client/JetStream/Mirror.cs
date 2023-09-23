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
    /// Information about a mirror.
    /// </summary>
    public sealed class Mirror : SourceBase
    {
        internal static Mirror OptionalInstance(JSONNode mirrorNode)
        {
            return mirrorNode == null || mirrorNode.Count == 0 ? null : new Mirror(mirrorNode);
        }

        /// <summary>
        /// Construct a Mirror object
        /// </summary>
        /// <param name="name">the mirror stream name</param>
        /// <param name="startSeq">the start sequence</param>
        /// <param name="startTime">the start time</param>
        /// <param name="filterSubject">the filter subject</param>
        /// <param name="external">the external reference</param>
        /// <param name="subjectTransforms">the subject transforms, defaults to none</param>
        public Mirror(string name, ulong startSeq, DateTime startTime, string filterSubject, External external,
            IList<SubjectTransform> subjectTransforms = null)
            : base(name, startSeq, startTime, filterSubject, external, subjectTransforms) {}

        internal Mirror(JSONNode mirrorNode) : base(mirrorNode) {}

        internal Mirror(MirrorBuilder mb) : base(mb) {}

        /// <summary>
        /// Creates a builder for a mirror object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static MirrorBuilder Builder() {
            return new MirrorBuilder();
        }

        /// <summary>
        /// Creates a builder for a mirror object based on an existing mirror object.
        /// </summary>
        /// <returns>The Builder</returns>
        public static MirrorBuilder Builder(Mirror mirror) {
            return new MirrorBuilder(mirror);
        }
        
        /// <summary>
        /// Mirror can be created using a MirrorBuilder. 
        /// </summary>
        public sealed class MirrorBuilder : SourceBaseBuilder<MirrorBuilder, Mirror>
        {
            public MirrorBuilder() {}
            public MirrorBuilder(Mirror mirror) : base(mirror) {}

            protected override MirrorBuilder GetThis()
            {
                return this;
            }

            public override Mirror Build()
            {
                return new Mirror(this);
            }
        }
    }
}
