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
    public abstract class SourceBase : JsonSerializable
    {
        public string Name { get; }
        public ulong StartSeq { get; }
        public DateTime StartTime { get; }
        public string FilterSubject { get; }
        public External External { get; }

        internal SourceBase(JSONNode sourceBaseNode)
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

        protected bool Equals(SourceBase other)
        {
            return Name == other.Name && StartSeq == other.StartSeq && StartTime.Equals(other.StartTime) && FilterSubject == other.FilterSubject && Equals(External, other.External);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != GetType()) return false;
            return Equals((SourceBase) obj);
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
