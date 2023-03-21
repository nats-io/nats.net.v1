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
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class ServiceBuilder
    {
        public const int DefaultDrainTimeoutMillis = 5000;

        internal IConnection Conn;
        internal string Name;
        internal string Description;
        internal string Version;
        internal string ApiUrl;
        internal readonly Dictionary<string, ServiceEndpoint> ServiceEndpoints = new Dictionary<string, ServiceEndpoint>();
        internal int DrainTimeoutMillis = DefaultDrainTimeoutMillis;
        internal Dictionary<string, string> Metadata;

        public ServiceBuilder WithConnection(IConnection conn) 
        {
            Conn = conn;
            return this;
        }

        public ServiceBuilder WithName(string name) 
        {
            Name = ValidateIsRestrictedTerm(name, "Service Name", true);
            return this;
        }

        public ServiceBuilder WithDescription(string description) 
        {
            Description = description;
            return this;
        }

        public ServiceBuilder WithVersion(string version) 
        {
            Version = ValidateSemVer(version, "Service Version", true);
            return this;
        }

        public ServiceBuilder WithMetadata(Dictionary<string, string> metadata)
        {
            Metadata = metadata;
            return this;
        }

        public ServiceBuilder WithApiUrl(string apiUrl)
        {
            ApiUrl = apiUrl;
            return this;
        }

        public ServiceBuilder AddServiceEndpoint(ServiceEndpoint endpoint) {
            ServiceEndpoints[endpoint.Name] = endpoint;
            return this;
        }

        public ServiceBuilder WithDrainTimeoutMillis(int drainTimeoutMillis)
        {
            DrainTimeoutMillis = drainTimeoutMillis;
            return this;
        }
            
        public Service Build() {
            Required(Conn, "Connection");
            Required(Name, "Name");
            Required(Version, "Version");
            Required(ServiceEndpoints, "Service Endpoints");
            return new Service(this);
        }
    }
}
