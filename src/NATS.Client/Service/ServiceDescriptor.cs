// Copyright 2022 The NATS Authors
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

using static NATS.Client.Internals.Validator;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class ServiceDescriptor
    {
        public string Name { get; }
        public string Description { get; }
        public string Version { get; }
        public string Subject { get; }
        public string SchemaRequest { get; }
        public string SchemaResponse { get; }

        public ServiceDescriptor(string name, string description, string version, string subject, string schemaRequest = null, string schemaResponse = null)
        {
            Name = Required(name, "name");
            Description = description;
            Version = Required(version, "version");
            Subject = ValidateIsRestrictedTerm(subject, "subject", true);
            SchemaRequest = schemaRequest;
            SchemaResponse = schemaResponse;
        }
    }
}
