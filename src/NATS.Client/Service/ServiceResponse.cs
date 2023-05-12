using System;
using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public abstract class ServiceResponse : JsonSerializable
    {
        public string Type { get; }
        public string Id { get; }
        public string Name { get; }
        public string Version { get; }
        public Dictionary<string, string> Metadata { get; }

        internal ServiceResponse(string type, string id, string name, string version, Dictionary<string, string> metadata)
        {
            Type = type;
            Id = id;
            Name = name;
            Version = version;
            Metadata = metadata == null || metadata.Count == 0 ? null : metadata;
        }
        
        internal ServiceResponse(string type, ServiceResponse template)
        {
            Type = type;
            Id = template.Id;
            Name = template.Name;
            Version = template.Version;
            Metadata = template.Metadata;
        }
        
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
            Metadata = JsonUtils.StringStringDictionary(node, ApiConstants.Metadata);
        }
        
        protected JSONObject BaseJsonObject()
        {
            JSONObject jso = new JSONObject();
            JsonUtils.AddField(jso, ApiConstants.Id, Id);
            JsonUtils.AddField(jso, ApiConstants.Name, Name);
            JsonUtils.AddField(jso, ApiConstants.Type, Type);
            JsonUtils.AddField(jso, ApiConstants.Version, Version);
            JsonUtils.AddField(jso, ApiConstants.Metadata, Metadata);
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