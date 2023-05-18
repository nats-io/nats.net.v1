using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class Endpoint : JsonSerializable
    {
        public string Name { get; }
        public string Subject { get; }
        public Dictionary<string, string> Metadata { get; }

        public Endpoint(string name) : this(name, null, null, true) {}

        public Endpoint(string name, string subject) : this(name, subject, null, true) {}

        // internal use constructors
        internal Endpoint(string name, string subject, Dictionary<string, string> metadata, bool validate) {
            if (validate) {
                Name = Validator.ValidateIsRestrictedTerm(name, "Endpoint Name", true);
                if (subject == null) {
                    Subject = Name;
                }
                else {
                    Subject = Validator.ValidateSubject(subject, "Endpoint Subject", false, false);
                }
            }
            else {
                Name = name;
                Subject = subject;
            }
            Metadata = metadata == null || metadata.Count == 0 ? null : metadata;
        }

        internal Endpoint(JSONNode node)
        {
            Name = node[ApiConstants.Name];
            Subject = node[ApiConstants.Subject];
            Metadata = JsonUtils.StringStringDictionary(node, ApiConstants.Metadata);
        }

        private Endpoint(EndpointBuilder b) 
            : this(b.Name, b.Subject, b.Metadata, true) {}

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Subject, Subject);
            JsonUtils.AddField(jso, ApiConstants.Metadata, Metadata);
            return jso;
        }

        public static EndpointBuilder Builder() => new EndpointBuilder();
        
        public sealed class EndpointBuilder
        {
            internal string Name;
            internal string Subject;
            internal Dictionary<string, string> Metadata;

            public EndpointBuilder WithEndpoint(Endpoint endpoint) {
                Name = endpoint.Name;
                Subject = endpoint.Subject;
                return this;
            }

            public EndpointBuilder WithName(string name) {
                Name = name;
                return this;
            }

            public EndpointBuilder WithSubject(string subject) {
                Subject = subject;
                return this;
            }

            public EndpointBuilder WithMetadata(Dictionary<string, string> metadata)
            {
                Metadata = metadata;
                return this;
            }

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
            return Name == other.Name && Subject == other.Subject && Validator.DictionariesEqual(Metadata, other.Metadata);
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
                var hashCode = (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Subject != null ? Subject.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Metadata != null ? Metadata.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}