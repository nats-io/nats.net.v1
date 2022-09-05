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

using NATS.Client.Internals;
using NATS.Client.JetStream;

namespace NATS.Client.ObjectStore
{
    public sealed class ObjectStoreOptions : FeatureOptions
    {
        private ObjectStoreOptions(JetStreamOptions jso) : base(jso) {}
        
        /// <summary>
        /// Gets a ObjectStoreOptionsBuilder builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static ObjectStoreOptionsBuilder Builder()
        {
            return new ObjectStoreOptionsBuilder();
        }
        
        /// <summary>
        /// Gets the ObjectStoreOptions builder based on an existing ObjectStoreOptions object.
        /// </summary>
        /// <param name="oso">an existing ObjectStoreOptions object</param>
        /// <returns>The builder</returns>
        public static ObjectStoreOptionsBuilder Builder(ObjectStoreOptions oso)
        {
            return new ObjectStoreOptionsBuilder(oso);
        }
        
        /// <summary>
        /// Gets the ObjectStoreOptions builder based on an existing JetStreamOptions object.
        /// </summary>
        /// <param name="jso">an existing JetStreamOptions object</param>
        /// <returns>The builder</returns>
        public static ObjectStoreOptionsBuilder Builder(JetStreamOptions jso)
        {
            return new ObjectStoreOptionsBuilder().WithJetStreamOptions(jso);
        }

        public sealed class ObjectStoreOptionsBuilder
        {
            private JetStreamOptions.JetStreamOptionsBuilder _jsoBuilder;

            /// <summary>
            /// Construct a builder
            /// </summary>
            public ObjectStoreOptionsBuilder() : this(null) {}

            /// <summary>
            /// Construct a builder from an existing ObjectStoreOptions object
            /// </summary>
            /// <param name="oso">an existing ObjectStoreOptions object</param>
            public ObjectStoreOptionsBuilder(ObjectStoreOptions oso)
            {
                _jsoBuilder = JetStreamOptions.Builder(oso?.JSOptions);
            }
            
            /// <summary>
            /// Sets the JetStreamOptions.
            /// </summary>
            /// <param name="jso">The JetStreamOptions.</param>
            /// <returns>The ObjectStoreOptionsBuilder</returns>
            public ObjectStoreOptionsBuilder WithJetStreamOptions(JetStreamOptions jso)
            {
                _jsoBuilder = JetStreamOptions.Builder(jso);
                return this;
            }

            /// <summary>
            /// Sets the request timeout for JetStream API calls.
            /// </summary>
            /// <param name="requestTimeout">the duration to wait for responses.</param>
            /// <returns>The ObjectStoreOptionsBuilder</returns>
            public ObjectStoreOptionsBuilder WithRequestTimeout(Duration requestTimeout) {
                _jsoBuilder.WithRequestTimeout(requestTimeout);
                return this;
            }

            /// <summary>
            /// Sets the prefix for JetStream subjects. A prefix can be used in conjunction with
            /// user permissions to restrict access to certain JetStream instances.  This must
            /// match the prefix used in the server.
            /// </summary>
            /// <param name="prefix">The prefix.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public ObjectStoreOptionsBuilder WithJsPrefix(string prefix)
            {
                _jsoBuilder.WithPrefix(prefix);
                return this;
            }
            
            /// <summary>
            /// Sets the domain for JetStream subjects. A domain can be used in conjunction with
            /// user permissions to restrict access to certain JetStream instances.  This must
            /// match the domain used in the server.
            /// </summary>
            /// <param name="domain">The domain.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public ObjectStoreOptionsBuilder WithJsDomain(string domain) 
            {
                _jsoBuilder.WithDomain(domain);
                return this;
            }

            /// <summary>
            /// Builds the ObjectStoreOptions
            /// </summary>
            /// <returns>The ObjectStoreOptions object.</returns>
            public ObjectStoreOptions Build()
            {
                return new ObjectStoreOptions(_jsoBuilder.Build());
            }
        }
    }
}
