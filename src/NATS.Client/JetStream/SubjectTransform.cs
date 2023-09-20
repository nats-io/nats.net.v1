using NATS.Client.Internals.SimpleJSON;

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
            JSONObject o = new JSONObject
            {
                [ApiConstants.Src] = Source,
                [ApiConstants.Dest] = Destination
            };
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
            /// <returns></returns>
            public SubjectTransformBuilder WithSource(string source) {
                _source = source;
                return this;
            }

            /// <summary>
            /// Set the SubjectTransform Subject template
            /// </summary>
            /// <param name="destination">the destination</param>
            /// <returns></returns>
            public SubjectTransformBuilder WithDestination(string destination) {
                _destination = destination;
                return this;
            }
            
            /// <summary>
            /// Build a SubjectTransform object
            /// </summary>
            /// <returns>The SubjectTransform</returns>
            public SubjectTransform Build() {
                return new SubjectTransform(_source, _destination);
            }
        }
    }
}