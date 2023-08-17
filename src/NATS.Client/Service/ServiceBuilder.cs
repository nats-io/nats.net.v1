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

using System.Collections.Generic;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.Service
{
    /// <summary>
    /// Build a service using a fluent builder. Use Service.Builder() to get an instance or <c>new ServiceBuilder()</c>
    /// </summary>
    public class ServiceBuilder
    {
        public const int DefaultDrainTimeoutMillis = 5000;

        internal IConnection Conn;
        internal string Name;
        internal string Description;
        internal string Version;
        internal readonly IDictionary<string, ServiceEndpoint> ServiceEndpoints = new Dictionary<string, ServiceEndpoint>();
        internal int DrainTimeoutMillis = DefaultDrainTimeoutMillis;
        internal IDictionary<string, string> Metadata;

        /// <summary>
        /// The connection the service runs on
        /// </summary>
        /// <param name="conn">connection</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder WithConnection(IConnection conn) 
        {
            Conn = conn;
            return this;
        }

        /// <summary>
        /// The simple name of the service
        /// </summary>
        /// <param name="name">the name</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder WithName(string name) 
        {
            Name = ValidateIsRestrictedTerm(name, "Service Name", true);
            return this;
        }

        /// <summary>
        /// The simple description of the service
        /// </summary>
        /// <param name="description">the description</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder WithDescription(string description) 
        {
            Description = description;
            return this;
        }

        /// <summary>
        /// The simple version of the service.
        /// </summary>
        /// <param name="version">the version</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder WithVersion(string version) 
        {
            Version = ValidateSemVer(version, "Service Version", true);
            return this;
        }

        /// <summary>
        /// Any meta information about this service
        /// </summary>
        /// <param name="metadata">the meta</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder WithMetadata(IDictionary<string, string> metadata)
        {
            Metadata = metadata;
            return this;
        }

        /// <summary>
        /// Add a service endpoint into the service. There can only be one instance of an endpoint by name
        /// </summary>
        /// <param name="serviceEndpoint">the endpoint</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder AddServiceEndpoint(ServiceEndpoint serviceEndpoint) {
            ServiceEndpoints[serviceEndpoint.Name] = serviceEndpoint;
            return this;
        }

        /// <summary>
        /// The timeout when stopping a service. Defaults to 5000 milliseconds
        /// </summary>
        /// <param name="drainTimeoutMillis">the drain timeout in milliseconds</param>
        /// <returns>the ServiceBuilder</returns>
        public ServiceBuilder WithDrainTimeoutMillis(int drainTimeoutMillis)
        {
            DrainTimeoutMillis = drainTimeoutMillis;
            return this;
        }
            
        /// <summary>
        /// Build the Service instance
        /// </summary>
        /// <returns>the Service instance</returns>
        public Service Build() {
            Required(Conn, "Connection");
            Required(Name, "Name");
            Required(Version, "Version");
            Required(ServiceEndpoints, "Service Endpoints");
            return new Service(this);
        }
    }
}
