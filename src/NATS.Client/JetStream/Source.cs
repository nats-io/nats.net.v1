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

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.OptStartSeq, StartSeq);
            JsonUtils.AddField(jso, ApiConstants.OptStartTime, JsonUtils.ToString(StartTime));
            JsonUtils.AddField(jso, ApiConstants.FilterSubject, FilterSubject);
            if (External != null)
            {
                jso[ApiConstants.External] = External.ToJsonNode();
            }
            return jso;
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
        /// Creates a builder for a source object based on an existing source object.
        /// </summary>
        /// <returns>The Builder</returns>
        public static SourceBuilder Builder(Source source) {
            return new SourceBuilder(source);
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

            public SourceBuilder() { }

            public SourceBuilder(Source source)
            {
                _name = source.Name;
                _startSeq = source.StartSeq;
                _startTime = source.StartTime;
                _filterSubject = source.FilterSubject;
                _external = source.External;
            }

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
            /// Set the external reference by using a domain based prefix.
            /// </summary>
            /// <param name="domain">the domain</param>
            /// <returns>The Builder</returns>
            public SourceBuilder WithDomain(string domain)
            {
                string prefix = JetStreamOptions.ConvertDomainToPrefix(domain);
                _external = prefix == null ? null : External.Builder().WithApi(prefix).Build();
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

        public bool Equals(Source other)
        {
            return Name == other.Name && StartSeq == other.StartSeq && StartTime.Equals(other.StartTime) && FilterSubject == other.FilterSubject && Equals(External, other.External);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Source) obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ StartSeq.GetHashCode();
                hashCode = (hashCode * 397) ^ StartTime.GetHashCode();
                hashCode = (hashCode * 397) ^ (FilterSubject != null ? FilterSubject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (External != null ? External.GetHashCode() : 0);
                return hashCode;
            }
        }    
    }
}
