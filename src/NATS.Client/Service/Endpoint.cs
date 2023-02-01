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
        public Schema Schema { get; }

        public Endpoint(string name, string subject, Schema schema) : this(name, subject, schema, true) {}

        public Endpoint(string name) : this(name, null, null, true) {}

        public Endpoint(string name, string subject) : this(name, subject, null, true) {}

        public Endpoint(string name, string subject, string schemaRequest, string schemaResponse)
            : this(name, subject, Schema.OptionalInstance(schemaRequest, schemaResponse), true) {}

        // internal use constructors
        internal Endpoint(string name, string subject, Schema schema, bool validate) {
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
            Schema = schema;
        }

        internal Endpoint(JSONNode node)
        {
            Name = node[ApiConstants.Name];
            Subject = node[ApiConstants.Subject];
            Schema = Schema.OptionalInstance(node[ApiConstants.Schema]);
        }

        private Endpoint(EndpointBuilder b) 
            : this(b.Name, b.Subject, Schema.OptionalInstance(b.SchemaRequest, b.SchemaResponse)) {}

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Subject, Subject);
            JsonUtils.AddField(jso, ApiConstants.Schema, Schema);
            return jso;
        }

        public static EndpointBuilder Builder() => new EndpointBuilder();
        
        public sealed class EndpointBuilder
        {
            internal string Name;
            internal string Subject;
            internal string SchemaRequest;
            internal string SchemaResponse;

            public EndpointBuilder WithEndpoint(Endpoint endpoint) {
                Name = endpoint.Name;
                Subject = endpoint.Subject;
                if (endpoint.Schema == null) {
                    SchemaRequest = null;
                    SchemaResponse = null;
                }
                else {
                    SchemaRequest = endpoint.Schema.Request;
                    SchemaResponse = endpoint.Schema.Response;
                }
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

            public EndpointBuilder WithSchemaRequest(string schemaRequest) {
                SchemaRequest = schemaRequest;
                return this;
            }

            public EndpointBuilder WithSchemaResponse(string schemaResponse) {
                SchemaResponse = schemaResponse;
                return this;
            }

            public EndpointBuilder WithSchema(Schema schema) {
                if (schema == null) {
                    SchemaRequest = null;
                    SchemaResponse = null;
                }
                else {
                    SchemaRequest = schema.Request;
                    SchemaResponse = schema.Response;
                }
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
            return Name == other.Name && Subject == other.Subject && Equals(Schema, other.Schema);
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
                hashCode = (hashCode * 397) ^ (Schema != null ? Schema.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}