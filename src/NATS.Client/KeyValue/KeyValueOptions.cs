// Copyright 2021 The NATS Authors
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

namespace NATS.Client.KeyValue
{
    public sealed class KeyValueOptions : FeatureOptions
    {
        private KeyValueOptions(JetStreamOptions jso) : base(jso) {}
        
        /// <summary>
        /// Gets a KeyValueOptionsBuilder builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static KeyValueOptionsBuilder Builder()
        {
            return new KeyValueOptionsBuilder();
        }
        
        /// <summary>
        /// Gets the KeyValueOptions builder based on an existing KeyValueOptions object.
        /// </summary>
        /// <param name="kvo">an existing KeyValueOptions object</param>
        /// <returns>The builder</returns>
        public static KeyValueOptionsBuilder Builder(KeyValueOptions kvo)
        {
            return new KeyValueOptionsBuilder(kvo);
        }
        
        /// <summary>
        /// Gets the KeyValueOptions builder based on an existing JetStreamOptions object.
        /// </summary>
        /// <param name="jso">an existing JetStreamOptions object</param>
        /// <returns>The builder</returns>
        public static KeyValueOptionsBuilder Builder(JetStreamOptions jso)
        {
            return new KeyValueOptionsBuilder().WithJetStreamOptions(jso);
        }

        public sealed class KeyValueOptionsBuilder
        {
            private JetStreamOptions.JetStreamOptionsBuilder _jsoBuilder;

            /// <summary>
            /// Construct a builder
            /// </summary>
            public KeyValueOptionsBuilder() : this(null) {}

            /// <summary>
            /// Construct a builder from an existing KeyValueOptions object
            /// </summary>
            /// <param name="kvo">an existing KeyValueOptions object</param>
            public KeyValueOptionsBuilder(KeyValueOptions kvo)
            {
                _jsoBuilder = JetStreamOptions.Builder(kvo?.JSOptions);
            }
            
            /// <summary>
            /// Sets the JetStreamOptions.
            /// </summary>
            /// <param name="jso">The JetStreamOptions.</param>
            /// <returns>The KeyValueOptionsBuilder</returns>
            public KeyValueOptionsBuilder WithJetStreamOptions(JetStreamOptions jso)
            {
                _jsoBuilder = JetStreamOptions.Builder(jso);
                return this;
            }

            /// <summary>
            /// Sets the request timeout for JetStream API calls.
            /// </summary>
            /// <param name="requestTimeout">the duration to wait for responses.</param>
            /// <returns>The ObjectStoreOptionsBuilder</returns>
            public KeyValueOptionsBuilder WithRequestTimeout(Duration requestTimeout) {
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
            public KeyValueOptionsBuilder WithJsPrefix(string prefix)
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
            public KeyValueOptionsBuilder WithJsDomain(string domain) 
            {
                _jsoBuilder.WithDomain(domain);
                return this;
            }

            /// <summary>
            /// Builds the KeyValueOptions
            /// </summary>
            /// <returns>The KeyValueOptions object.</returns>
            public KeyValueOptions Build()
            {
                return new KeyValueOptions(_jsoBuilder.Build());
            }
        }
    }
}
