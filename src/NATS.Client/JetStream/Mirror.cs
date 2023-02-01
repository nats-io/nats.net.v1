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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// Information about a mirror.
    /// </summary>
    public sealed class Mirror : JsonSerializable
    {
        /// <summary>
        /// Mirror stream name.
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

        internal Mirror(JSONNode mirrorBaseNode)
        {
            Name = mirrorBaseNode[ApiConstants.Name].Value;
            StartSeq = mirrorBaseNode[ApiConstants.OptStartSeq].AsUlong;
            StartTime = JsonUtils.AsDate(mirrorBaseNode[ApiConstants.OptStartTime]);
            FilterSubject = mirrorBaseNode[ApiConstants.FilterSubject].Value;
            External = External.OptionalInstance(mirrorBaseNode[ApiConstants.External]);
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();
            JsonUtils.AddField(o, ApiConstants.Name, Name);
            JsonUtils.AddField(o, ApiConstants.OptStartSeq, StartSeq);
            JsonUtils.AddField(o, ApiConstants.OptStartTime, JsonUtils.ToString(StartTime));
            JsonUtils.AddField(o, ApiConstants.FilterSubject, FilterSubject);
            if (External != null)
            {
                o[ApiConstants.External] = External.ToJsonNode();
            }
            return o;
        }

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
        public Mirror(string name, ulong startSeq, DateTime startTime, string filterSubject, External external)
        {
            Name = name;
            StartSeq = startSeq;
            StartTime = startTime;
            FilterSubject = filterSubject;
            External = external;
        }

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
        public sealed class MirrorBuilder
        {
            private string _name;
            private ulong _startSeq;
            private DateTime _startTime;
            private string _filterSubject;
            private External _external;

            public MirrorBuilder() { }

            public MirrorBuilder(Mirror mirror)
            {
                _name = mirror.Name;
                _startSeq = mirror.StartSeq;
                _startTime = mirror.StartTime;
                _filterSubject = mirror.FilterSubject;
                _external = mirror.External;
            }

            /// <summary>
            /// Set the mirror name.
            /// </summary>
            /// <param name="name">the name</param>
            /// <returns>The Builder</returns>
            public MirrorBuilder WithName(string name)
            {
                _name = name;
                return this;
            }

            /// <summary>
            /// Set the start sequence.
            /// </summary>
            /// <param name="startSeq">the start sequence</param>
            /// <returns>The Builder</returns>
            public MirrorBuilder WithStartSeq(ulong startSeq)
            {
                _startSeq = startSeq;
                return this;
            }

            /// <summary>
            /// Set the start time.
            /// </summary>
            /// <param name="startTime">the start time</param>
            /// <returns>The Builder</returns>
            public MirrorBuilder WithStartTime(DateTime startTime)
            {
                _startTime = startTime;
                return this;
            }

            /// <summary>
            /// Set the filter subject.
            /// </summary>
            /// <param name="filterSubject">the filterSubject</param>
            /// <returns>The Builder</returns>
            public MirrorBuilder WithFilterSubject(string filterSubject)
            {
                _filterSubject = filterSubject;
                return this;
            }

            /// <summary>
            /// Set the external reference.
            /// </summary>
            /// <param name="external">the external</param>
            /// <returns>The Builder</returns>
            public MirrorBuilder WithExternal(External external)
            {
                _external = external;
                return this;
            }

            /// <summary>
            /// Set the external reference by using a domain based prefix.
            /// </summary>
            /// <param name="domain">the domain</param>
            /// <returns>The Builder</returns>
            public MirrorBuilder WithDomain(string domain)
            {
                string prefix = JetStreamOptions.ConvertDomainToPrefix(domain);
                _external = prefix == null ? null : External.Builder().WithApi(prefix).Build();
                return this;
            }

            /// <summary>
            /// Build a Mirror object
            /// </summary>
            /// <returns>The Mirror</returns>
            public Mirror Build()
            {
                return new Mirror(_name, _startSeq, _startTime, _filterSubject, _external);
            }
        }

        public bool Equals(Mirror other)
        {
            return Name == other.Name && StartSeq == other.StartSeq && StartTime.Equals(other.StartTime) && FilterSubject == other.FilterSubject && Equals(External, other.External);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((Mirror) obj);
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
