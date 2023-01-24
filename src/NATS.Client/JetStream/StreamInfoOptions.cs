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

namespace NATS.Client.JetStream
{
    public sealed class StreamInfoOptions : JsonSerializable
    {
        public string SubjectsFilter { get; }
        public bool DeletedDetails { get; }

        internal StreamInfoOptions(string subjectsFilter, bool deletedDetails)
        {
            SubjectsFilter = subjectsFilter;
            DeletedDetails = deletedDetails;
        }

        public override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.SubjectsFilter] = SubjectsFilter,
                [ApiConstants.DeletedDetails] = DeletedDetails
            };
        }

        /// <summary>
        /// Gets the stream info options builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static StreamInfoOptionsBuilder Builder()
        {
            return new StreamInfoOptionsBuilder();
        }

        public sealed class StreamInfoOptionsBuilder
        {
            public string _subjectsFilter;
            public bool _deletedDetails;

            public StreamInfoOptionsBuilder WithFilterSubjects(string subjectsFilter)
            {
                _subjectsFilter = Validator.EmptyAsNull(subjectsFilter);
                return this;
            }

            public StreamInfoOptionsBuilder WithAllSubjects() {
                _subjectsFilter = ">";
                return this;
            }

            public StreamInfoOptionsBuilder WithDeletedDetails()
            {
                _deletedDetails = true;
                return this;
            }

            public StreamInfoOptions Build()
            {
                return new StreamInfoOptions(_subjectsFilter, _deletedDetails);
            }
        }
    }
}
