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
using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.Service
{
    /// <summary>
    /// The ServiceEndpoint represents the working <see cref="Endpoint"/>
    /// <list type="bullet">
    /// <item>
    /// <description>It allows the endpoint to be grouped.</description>
    /// </item>
    /// <item>
    /// <description>It is where you can define the handler that will respond to incoming requests</description>
    /// </item>
    /// <item>
    /// <description>It allows you to define it's dispatcher if desired giving granularity to threads running subscribers</description>
    /// </item>
    /// <item>
    /// <description>It gives you a hook to provide custom data for the <see cref="EndpointStats"/></description>
    /// </item>
    /// </list>
    /// <para>To create a ServiceEndpoint, use the ServiceEndpoint static method <c>builder()</c> or <c>new ServiceEndpoint.Builder() to get an instance.</c></para>
    /// </summary>
    public class ServiceEndpoint
    {
        internal Group Group { get; }
        internal EventHandler<ServiceMsgHandlerEventArgs> Handler { get; }
        internal Func<JSONNode> StatsDataSupplier { get; }
        internal Endpoint Endpoint { get; }
        
        /// <value>The name of the <see cref="Endpoint"/></value>
        public string Name => Endpoint.Name;

        /// <value>The subject of the ServiceEndpoint which takes into account the group path and the <see cref="Endpoint"/> subject</value>
        public string Subject => Group == null ? Endpoint.Subject : $"{Group.Subject}.{Endpoint.Subject}";

        /// <value>A copy of the metadata of the <see cref="Endpoint"/></value>
        public IDictionary<string, string> Metadata => Endpoint.Metadata == null ? null : new Dictionary<string, string>(Endpoint.Metadata);
        
        /// <value>The <see cref="Group"/> name for this ServiceEndpoint, or null if there is no group</value>
        public string GroupName => Group == null ? null : Group.Name;

        internal ServiceEndpoint(ServiceEndpointBuilder b, Endpoint e)
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

        /// <summary>
        /// Get an instance of a ServiceEndpointBuilder.
        /// </summary>
        /// <returns>the instance</returns>
        public static ServiceEndpointBuilder Builder() {
            return new ServiceEndpointBuilder();
        }
        
        /// <summary>
        /// Build a ServiceEndpoint using a fluent builder.
        /// </summary>
        public sealed class ServiceEndpointBuilder {
            internal Group Group;
            internal EventHandler<ServiceMsgHandlerEventArgs> Handler;
            internal Func<JSONNode> StatsDataSupplier;
            internal readonly Endpoint.EndpointBuilder EndpointBuilder = Endpoint.Builder();

            /// <summary>
            /// Set the <see cref="Group"/> for this ServiceEndpoint
            /// </summary>
            /// <param name="group">The group</param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithGroup(Group group) {
                Group = group;
                return this;
            }

            /// <summary>
            /// Set the <see cref="Endpoint"/> for this ServiceEndpoint replacing all existing endpoint information.
            /// </summary>
            /// <param name="endpoint"></param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithEndpoint(Endpoint endpoint) {
                EndpointBuilder.WithEndpoint(endpoint);
                return this;
            }

            /// <summary>
            /// Set the name for the <see cref="Endpoint"/> for this ServiceEndpoint replacing any name already set.
            /// </summary>
            /// <param name="name"></param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithEndpointName(string name) {
                EndpointBuilder.WithName(name);
                return this;
            }

            /// <summary>
            /// Set the subject for the <see cref="Endpoint"/> for this ServiceEndpoint replacing any subject already set.
            /// </summary>
            /// <param name="subject"></param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithEndpointSubject(string subject) {
                EndpointBuilder.WithSubject(subject);
                return this;
            }

            /// <summary>
            /// Set the metadata for the <see cref="Endpoint"/> for this ServiceEndpoint replacing any metadata already set.
            /// </summary>
            /// <param name="metadata">the metadata</param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithEndpointMetadata(IDictionary<string, string> metadata) {
                EndpointBuilder.WithMetadata(metadata);
                return this;
            }

            /// <summary>
            /// Set the Service Message Handler for this ServiceEndpoint
            /// </summary>
            /// <param name="handler"></param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithHandler(EventHandler<ServiceMsgHandlerEventArgs> handler) {
                Handler = handler;
                return this;
            }

            /// <summary>
            /// Set the <see cref="EndpointStats"/> data supplier for this ServiceEndpoint
            /// </summary>
            /// <param name="statsDataSupplier">the data supplier</param>
            /// <returns>the ServiceEndpointBuilder</returns>
            public ServiceEndpointBuilder WithStatsDataSupplier(Func<JSONNode> statsDataSupplier) {
                StatsDataSupplier = statsDataSupplier;
                return this;
            }

            /// <summary>
            /// Build the ServiceEndpoint instance.
            /// </summary>
            /// <returns>the ServiceEndpoint instance</returns>
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
