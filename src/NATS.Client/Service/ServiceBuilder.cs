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

using System;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class ServiceBuilder
    {
        internal IConnection Conn;
        internal string Name;
        internal string Description;
        internal string Version;
        internal string Subject;
        internal string SchemaRequest;
        internal string SchemaResponse;
        internal EventHandler<MsgHandlerEventArgs> ServiceMessageHandler;
        internal StatsDataSupplier StatsDataSupplier;
        internal StatsDataDecoder StatsDataDecoder;
        internal int DrainTimeoutMillis = ServiceUtil.DefaultDrainTimeoutMillis;

        public ServiceBuilder WithConnection(IConnection conn) {
            Conn = conn;
            return this;
        }

        public ServiceBuilder WithName(string name) {
            Name = name;
            return this;
        }

        public ServiceBuilder WithDescription(string description) {
            Description = description;
            return this;
        }

        public ServiceBuilder WithVersion(string version) {
            Version = version;
            return this;
        }

        public ServiceBuilder WithSubject(string subject) {
            Subject = subject;
            return this;
        }

        public ServiceBuilder WithSchemaRequest(string schemaRequest) {
            SchemaRequest = schemaRequest;
            return this;
        }

        public ServiceBuilder WithSchemaResponse(string schemaResponse) {
            SchemaResponse = schemaResponse;
            return this;
        }

        public ServiceBuilder WithServiceMessageHandler(EventHandler<MsgHandlerEventArgs> userMessageHandler) {
            ServiceMessageHandler = userMessageHandler;
            return this;
        }

        public ServiceBuilder WithStatsDataHandlers(StatsDataSupplier statsDataSupplier, StatsDataDecoder statsDataDecoder) {
            StatsDataSupplier = statsDataSupplier;
            StatsDataDecoder = statsDataDecoder;
            return this;
        }

        public ServiceBuilder WithDrainTimeoutMillis(int drainTimeoutMillis)
        {
            DrainTimeoutMillis = drainTimeoutMillis;
            return this;
        }
            
        public Service Build() {
            Required(Conn, "Connection");
            Required(ServiceMessageHandler, "Service Message Handler");
            ValidateIsRestrictedTerm(Name, "Name", true);
            ValidateSemVer(Version, "Version", true);
            if ((StatsDataSupplier != null && StatsDataDecoder == null)
                || (StatsDataSupplier == null && StatsDataDecoder != null)) {
                throw new ArgumentException("You must provide neither or both the stats data supplier and decoder");
            }

            return new Service(this);
        }
    }
}