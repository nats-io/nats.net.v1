// Copyright 2023 The NATS Authors
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

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class ServiceEndpoint
    {
        internal Group Group { get; }
        internal EventHandler<ServiceMsgHandlerEventArgs> Handler { get; }
        internal Func<JSONNode> StatsDataSupplier { get; }
        internal Endpoint Endpoint { get; }
        internal string Name => Endpoint.Name;
        internal string Subject => Group == null ? Endpoint.Subject : $"{Group.Subject}.{Endpoint.Subject}";

        public ServiceEndpoint(ServiceEndpointBuilder b, Endpoint e)
        {
            Group = b.Group;
            Endpoint = e;
            Handler = b.Handler;
            StatsDataSupplier = b.StatsDataSupplier;
        }
        
        internal ServiceEndpoint(Endpoint endpoint, EventHandler<ServiceMsgHandlerEventArgs> handler) {
            Group = null;
            Endpoint = endpoint;
            Handler = handler;
            StatsDataSupplier = null;
        }

        public static ServiceEndpointBuilder Builder() {
            return new ServiceEndpointBuilder();
        }

        public sealed class ServiceEndpointBuilder {
            internal Group Group;
            internal EventHandler<ServiceMsgHandlerEventArgs> Handler;
            internal Func<JSONNode> StatsDataSupplier;
            internal readonly Endpoint.EndpointBuilder EndpointBuilder = Endpoint.Builder();

            public ServiceEndpointBuilder WithGroup(Group group) {
                Group = group;
                return this;
            }

            public ServiceEndpointBuilder WithEndpoint(Endpoint endpoint) {
                EndpointBuilder.WithEndpoint(endpoint);
                return this;
            }

            public ServiceEndpointBuilder WithEndpointName(String name) {
                EndpointBuilder.WithName(name);
                return this;
            }

            public ServiceEndpointBuilder WithEndpointSubject(String subject) {
                EndpointBuilder.WithSubject(subject);
                return this;
            }

            public ServiceEndpointBuilder WithEndpointSchemaRequest(String schemaRequest) {
                EndpointBuilder.WithSchemaRequest(schemaRequest);
                return this;
            }

            public ServiceEndpointBuilder WithEndpointSchemaResponse(String schemaResponse) {
                EndpointBuilder.WithSchemaResponse(schemaResponse);
                return this;
            }

            public ServiceEndpointBuilder WithHandler(EventHandler<ServiceMsgHandlerEventArgs> handler) {
                Handler = handler;
                return this;
            }

            public ServiceEndpointBuilder WithStatsDataSupplier(Func<JSONNode> statsDataSupplier) {
                StatsDataSupplier = statsDataSupplier;
                return this;
            }

            public ServiceEndpoint Build() {
                Endpoint endpoint = EndpointBuilder.Build();
                Validator.Required(Handler, "Message Handler");
                return new ServiceEndpoint(this, endpoint);
            }
        }

        protected bool Equals(ServiceEndpoint other)
        {
            return Equals(Group, other.Group) && Equals(Handler, other.Handler) && Equals(StatsDataSupplier, other.StatsDataSupplier) && Equals(Endpoint, other.Endpoint);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;
            if (obj.GetType() != this.GetType()) return false;
            return Equals((ServiceEndpoint)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = (Group != null ? Group.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Handler != null ? Handler.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (StatsDataSupplier != null ? StatsDataSupplier.GetHashCode() : 0);
                hashCode = (hashCode * 397) ^ (Endpoint != null ? Endpoint.GetHashCode() : 0);
                return hashCode;
            }
        }
    }
}
