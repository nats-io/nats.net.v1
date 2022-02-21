using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class StreamInfoRequest : JsonSerializable
    {
        public string SubjectsFilter { get; }

        internal static byte[] FilterSubjects(string subjectsFilter) {
            return new StreamInfoRequest(subjectsFilter).Serialize();
        }

        internal static byte[] AllSubjects() {
            return new StreamInfoRequest(">").Serialize();
        }

        public StreamInfoRequest(string subjectsFilter)
        {
            SubjectsFilter = subjectsFilter;
        }

        internal override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.SubjectsFilter] = SubjectsFilter
            };
        }
    }
}