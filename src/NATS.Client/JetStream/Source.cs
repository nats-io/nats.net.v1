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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Information about an upstream stream source in a mirror
    /// </summary>
    public sealed class Source : JsonSerializable
    {
        /// <summary>
        /// Source stream name.
        /// </summary>
        public string Name { get; }
        
        /// <summary>
        /// The sequence to start replicating from.
        /// </summary>
        public ulong StartSeq { get; }
        
        /// <summary>
        /// The time stamp to start replicating from.
        /// </summary>
        public DateTime StartTime { get; }
        
        /// <summary>
        /// The subject filter to replicate
        /// </summary>
        public string FilterSubject { get; }

        /// <summary>
        /// External stream reference
        /// </summary>
        public External External { get; }

        internal Source(JSONNode sourceBaseNode)
        {
            Name = sourceBaseNode[ApiConstants.Name].Value;
            StartSeq = sourceBaseNode[ApiConstants.OptStartSeq].AsUlong;
            StartTime = JsonUtils.AsDate(sourceBaseNode[ApiConstants.OptStartTime]);
            FilterSubject = sourceBaseNode[ApiConstants.FilterSubject].Value;
            External = External.OptionalInstance(sourceBaseNode[ApiConstants.External]);
        }

        internal override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Name] = Name,
                [ApiConstants.OptStartSeq] = StartSeq,
                [ApiConstants.OptStartTime] = JsonUtils.ToString(StartTime),
                [ApiConstants.FilterSubject] = FilterSubject,
                [ApiConstants.External] = External.ToJsonNode()
            };
        }

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
        public Source(string name, ulong startSeq, DateTime startTime, string filterSubject, External external)
        {
            Name = name;
            StartSeq = startSeq;
            StartTime = startTime;
            FilterSubject = filterSubject;
            External = external;
        }

        /// <summary>
        /// Creates a builder for a source object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static SourceBuilder Builder() {
            return new SourceBuilder();
        }
        
        /// <summary>
        /// Source can be created using a SourceBuilder. 
        /// </summary>
        public sealed class SourceBuilder
        {
            private string _name;
            private ulong _startSeq;
            private DateTime _startTime;
            private string _filterSubject;
            private External _external;

            /// <summary>
            /// Set the source name.
            /// </summary>
            /// <param name="name">the name</param>
            /// <returns>The Builder</returns>
            public SourceBuilder WithName(string name)
            {
                _name = name;
                return this;
            }

            /// <summary>
            /// Set the start sequence.
            /// </summary>
            /// <param name="startSeq">the start sequence</param>
            /// <returns>The Builder</returns>
            public SourceBuilder WithStartSeq(ulong startSeq)
            {
                _startSeq = startSeq;
                return this;
            }

            /// <summary>
            /// Set the start time.
            /// </summary>
            /// <param name="startTime">the start time</param>
            /// <returns>The Builder</returns>
            public SourceBuilder WithStartTime(DateTime startTime)
            {
                _startTime = startTime;
                return this;
            }

            /// <summary>
            /// Set the filter subject.
            /// </summary>
            /// <param name="filterSubject">the filterSubject</param>
            /// <returns>The Builder</returns>
            public SourceBuilder WithFilterSubject(string filterSubject)
            {
                _filterSubject = filterSubject;
                return this;
            }

            /// <summary>
            /// Set the external reference.
            /// </summary>
            /// <param name="external">the external</param>
            /// <returns>The Builder</returns>
            public SourceBuilder WithExternal(External external)
            {
                _external = external;
                return this;
            }

            /// <summary>
            /// Build a Source object
            /// </summary>
            /// <returns>The Source</returns>
            public Source Build()
            {
                return new Source(_name, _startSeq, _startTime, _filterSubject, _external);
            }
        }
    }
}
