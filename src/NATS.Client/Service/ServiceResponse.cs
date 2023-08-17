using System;
using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// Base class for service responses Info, Ping and Stats
    /// </summary>
    public abstract class ServiceResponse : JsonSerializable
    {
        internal IDictionary<string, string> _metadata;

        /// <value>The type of this response</value>
        public string Type { get; }
        
        /// <value>The unique ID of the service</value>
        public string Id { get; }
        
        /// <value>The name of the service</value>
        public string Name { get; }
        
        /// <value>The version of the service</value>
        public string Version { get; }
        
        /// <value>A copy of the metadata for the service, or null if there is no metadata</value>
        public IDictionary<string, string> Metadata =>
            _metadata == null ? null : new Dictionary<string, string>(_metadata);

        internal ServiceResponse(string type, string id, string name, string version, IDictionary<string, string> metadata)
        {
            Type = type;
            Id = id;
            Name = name;
            Version = version;
            if (metadata == null || metadata.Count == 0)
            {
                _metadata = null;
            }
            else
            {
                _metadata = new Dictionary<string, string>(metadata);
            }
        }

        internal ServiceResponse(string type, ServiceResponse template)
            : this(type, template.Id, template.Name, template.Version, template._metadata) {}
        
        internal ServiceResponse(string type, JSONNode node)
        {
            string inType = node[ApiConstants.Type];
            if (Validator.EmptyAsNull(inType) == null) {
                throw new ArgumentException("Type cannot be null or empty.");
            }
            if (!type.Equals(inType)) {
                throw new ArgumentException("Invalid type for " + GetType().Name + ". Expecting: " + type + ". Received " + inType);
            }
            Type = type;
            Id = Validator.Required(node[ApiConstants.Id], "Id");
            Name = Validator.Required(node[ApiConstants.Name], "Name");
            Version = Validator.ValidateSemVer(node[ApiConstants.Version], "Version", true);
            _metadata = JsonUtils.StringStringDictionary(node, ApiConstants.Metadata);
        }
        
        protected JSONObject BaseJsonObject()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, Id);
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Type, Type);
            JsonUtils.AddField(jso, ApiConstants.Version, Version);
            JsonUtils.AddField(jso, ApiConstants.Metadata, _metadata);
            return jso;
        }

        protected bool Equals(ServiceResponse other)
        {
            return Type == other.Type && Id == other.Id && Name == other.Name && Version == other.Version;
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ServiceResponse)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Type != null ? Type.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Id != null ? Id.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Name != null ? Name.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Version != null ? Version.GetHashCode() : 0);
                return hashCode;
            }
        }

        public override string ToString()
        {
            return JsonUtils.ToKey(GetType()) + ToJsonString();
        }
    }
}