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

        internal override JSONNode ToJsonNode()
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
                _subjectsFilter = subjectsFilter;
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