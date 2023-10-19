using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// Endpoint encapsulates the name, subject and metadata for a <see cref="ServiceEndpoint"/>.
    /// <para>Endpoints can be used directly or as part of a group. <see cref="ServiceEndpoint"/> and <see cref="Group"/></para>
    /// <para>Endpoint names and subjects are considered 'Restricted Terms' and must only contain A-Z, a-z, 0-9, '-' or '_'</para>
    /// <para>To create an Endpoint, either use a direct constructor or use the Endpoint builder
    /// via the static method <code>builder()</code> or <c>new Endpoint.Builder() to get an instance.</c>
    /// </para>
    /// </summary>
    public class Endpoint : JsonSerializable
    {
        const string DefaultQueueGroup = "q";

        private IDictionary<string, string> _metadata;

        /// <value>The name of the Endpoint</value>
        public string Name { get; }
        
        /// <value>The subject of the Endpoint</value>
        public string Subject { get; }
        
        /// <value>The queue group of the Endpoint</value>
        public string QueueGroup { get; }

        /// <value>A copy of the metadata of the Endpoint</value>
        public IDictionary<string, string> Metadata =>
            _metadata == null ? null : new Dictionary<string, string>(_metadata);

        /// <summary>
        /// Directly construct an Endpoint with a name, which becomes the subject
        /// </summary>
        /// <param name="name">the name</param>
        public Endpoint(string name) : this(name, null, null, null, true) {}

        /// <summary>
        /// Directly construct an Endpoint with a name, which becomes the subject, and metadata
        /// </summary>
        /// <param name="name">the name</param>
        /// <param name="metadata">the metadata</param>
        public Endpoint(string name, IDictionary<string, string> metadata) : this(name, null, null, metadata, true) {}

        /// <summary>
        /// Directly construct an Endpoint with a name and a subject
        /// </summary>
        /// <param name="name">the name</param>
        /// <param name="subject">the subject</param>
        public Endpoint(string name, string subject) : this(name, subject, null, null, true) {}

        /// <summary>
        /// Directly construct an Endpoint with a name, the subject, and metadata
        /// </summary>
        /// <param name="name">the name</param>
        /// <param name="subject">the subject</param>
        /// <param name="metadata">the metadata</param>
        public Endpoint(string name, string subject, IDictionary<string, string> metadata) : this(name, subject, null, metadata, true) {}

        /// <summary>
        /// Directly construct an Endpoint with a name, the subject, and metadata
        /// </summary>
        /// <param name="name">the name</param>
        /// <param name="subject">the subject</param>
        /// <param name="queueGroup">the queue group</param>
        /// <param name="metadata">the metadata</param>
        public Endpoint(string name, string subject, string queueGroup, IDictionary<string, string> metadata) : this(name, subject, queueGroup, metadata, true) {}

        // internal use constructors
        internal Endpoint(string name, string subject, string queueGroup, IDictionary<string, string> metadata, bool validate) {
            if (validate) {
                Name = Validator.ValidateIsRestrictedTerm(name, "Endpoint Name", true);
                if (subject == null) {
                    Subject = Name;
                }
                else {
                    Subject = Validator.ValidateSubjectTerm(subject, "Endpoint Subject", false);
                }
                if (queueGroup == null) {
                    QueueGroup = DefaultQueueGroup;
                }
                else {
                    QueueGroup = Validator.ValidateSubjectTerm(queueGroup, "Endpoint Queue Group", true);
                }
            }
            else {
                Name = name;
                Subject = subject;
            }

            if (metadata == null || metadata.Count == 0)
            {
                _metadata = null;
            }
            else
            {
                _metadata = new Dictionary<string, string>(metadata);
            }
        }

        internal Endpoint(JSONNode node)
        {
            Name = node[ApiConstants.Name];
            Subject = node[ApiConstants.Subject];
            QueueGroup = node[ApiConstants.QueueGroup];
            _metadata = JsonUtils.StringStringDictionary(node, ApiConstants.Metadata, true);
        }

        private Endpoint(EndpointBuilder b) 
            : this(b._name, b._subject, b._queueGroup, b._metadata, true) {}

        /// <summary>
        /// Build a service using a fluent builder. Use Service.Builder() to get an instance or <c>new ServiceBuilder()</c>
        /// </summary>
        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Subject, Subject);
            JsonUtils.AddField(jso, ApiConstants.QueueGroup, QueueGroup);
            JsonUtils.AddField(jso, ApiConstants.Metadata, _metadata);
            return jso;
        }

        /// <summary>
        /// Get an instance of an Endpoint builder.
        /// </summary>
        /// <returns>the instance</returns>
        public static EndpointBuilder Builder() => new EndpointBuilder();
        
        /// <summary>
        /// Build an Endpoint using a fluent builder.
        /// </summary>
        public sealed class EndpointBuilder
        {
            internal string _name;
            internal string _subject;
            internal string _queueGroup = DefaultQueueGroup;
            internal IDictionary<string, string> _metadata;

            /// <summary>
            /// Copy the Endpoint, replacing all existing endpoint information.
            /// </summary>
            /// <param name="endpoint">the endpoint to copy</param>
            /// <returns></returns>
            public EndpointBuilder WithEndpoint(Endpoint endpoint)
            {
                return WithName(endpoint.Name).WithSubject(endpoint.Subject).WithMetadata(endpoint.Metadata);
            }

            /// <summary>
            /// Set the name for the Endpoint, replacing any name already set.
            /// </summary>
            /// <param name="name">the endpoint name</param>
            /// <returns></returns>
            public EndpointBuilder WithName(string name) {
                _name = name;
                return this;
            }

            /// <summary>
            /// Set the subject for the Endpoint, replacing any subject already set.
            /// </summary>
            /// <param name="subject">the subject</param>
            /// <returns></returns>
            public EndpointBuilder WithSubject(string subject) {
                _subject = subject;
                return this;
            }

            /// <summary>
            /// Set the queue group for the Endpoint, replacing any queue group already set.
            /// </summary>
            /// <param name="queueGroup">the queue group</param>
            /// <returns></returns>
            public EndpointBuilder WithQueueGroup(string queueGroup) {
                _queueGroup = queueGroup ?? DefaultQueueGroup;
                return this;
            }

            /// <summary>
            /// Set the metadata for the Endpoint, replacing any metadata already set.
            /// </summary>
            /// <param name="metadata">the metadata</param>
            /// <returns></returns>
            public EndpointBuilder WithMetadata(IDictionary<string, string> metadata)
            {
                if (metadata == null || metadata.Count == 0)
                {
                    _metadata = null;
                }
                else
                {
                    _metadata = new Dictionary<string, string>(metadata);
                }
                return this;
            }

            /// <summary>
            /// Build the Endpoint instance.
            /// </summary>
            /// <returns>the Endpoint instance</returns>
            public Endpoint Build() {
                return new Endpoint(this);
            }
        }

        public override string ToString()
        {
            return JsonUtils.ToKey(GetType()) + ToJsonString();
        }

        protected bool Equals(Endpoint other)
        {
            return Name == other.Name && Subject == other.Subject && QueueGroup == other.QueueGroup && Equals(_metadata, other._metadata);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((Endpoint)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (_metadata != null ? _metadata.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Subject != null ? Subject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (QueueGroup != null ? QueueGroup.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}