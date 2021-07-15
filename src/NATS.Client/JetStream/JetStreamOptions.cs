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

namespace NATS.Client.JetStream
{
    public sealed class JetStreamOptions
    {
        internal static readonly Duration DefaultTimeout = Duration.OfMillis(Defaults.Timeout);

        private readonly string _prefix;
        private readonly Duration _requestTimeout;
        private readonly bool _publishNoAck;

        private JetStreamOptions(string prefix, Duration requestTimeout, bool publishNoAck)
        {
            _prefix = prefix;
            _requestTimeout = requestTimeout;
            _publishNoAck = publishNoAck;
        }

        /// <summary>
        /// The default JetStream prefix
        /// </summary>
        public static readonly string DefaultPrefix = JetStreamConstants.JsapiPrefix;

        /// <summary>
        /// Gets the prefix.
        /// </summary>
        public string Prefix { get => _prefix; }

        /// <summary>
        /// Gets the request timeout
        /// </summary>
        public Duration RequestTimeout { get => _requestTimeout; }
        
        /// <summary>
        /// Gets is publish should be done in no ack (core) style
        /// </summary>
        public bool IsPublishNoAck { get => _publishNoAck; }
        
        /// <summary>
        /// Gets the JetStreamOptions builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static JetStreamOptionsBuilder Builder()
        {
            return new JetStreamOptionsBuilder();
        }
        
        /// <summary>
        /// Gets the JetStreamOptions builder based on an existing JetStreamOptions object.
        /// </summary>
        /// <param name="jso">an existing JetStreamOptions object</param>
        /// <returns>The builder</returns>
        public static JetStreamOptionsBuilder Builder(JetStreamOptions jso)
        {
            return new JetStreamOptionsBuilder(jso);
        }

        public sealed class JetStreamOptionsBuilder
        {
            private string _prefix = DefaultPrefix;
            private Duration _requestTimeout = DefaultTimeout;
            private bool _publishNoAck;

            /// <summary>
            /// Construct a builder
            /// </summary>
            public JetStreamOptionsBuilder() {}

            /// <summary>
            /// Construct a builder from an existing JetStreamOptions object
            /// </summary>
            /// <param name="jso">an existing JetStreamOptions object</param>
            public JetStreamOptionsBuilder(JetStreamOptions jso) 
            {
                if (jso != null)
                {
                    _prefix = jso.Prefix;
                    _requestTimeout = jso.RequestTimeout;
                    _publishNoAck = jso.IsPublishNoAck;
                }
            }
            
            /// <summary>
            /// Sets the prefix for JetStream subjects. A prefix can be used in conjunction with
            /// user permissions to restrict access to certain JetStream instances.  This must
            /// match the prefix used in the server.
            /// </summary>
            /// <param name="prefix">The prefix.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithPrefix(string prefix) 
            {
                _prefix = prefix;
                return this;
            }

            /// <summary>
            /// Sets the request timeout
            /// </summary>
            /// <param name="requestTimeout">The request timeout as Duration.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithRequestTimeout(Duration requestTimeout)
            {
                _requestTimeout = Validator.EnsureNotNullAndNotLessThanMin(requestTimeout, DefaultTimeout, 0);
                return this;
            }

            /// <summary>
            /// Sets the request timeout
            /// </summary>
            /// <param name="requestTimeoutMillis">The request timeout in millis.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithRequestTimeout(long requestTimeoutMillis) 
            {
                _requestTimeout = Validator.EnsureDurationNotLessThanMin(requestTimeoutMillis, DefaultTimeout, 0);
                return this;
            }

            /// <summary>
            /// Sets the Publish No Ack Flag
            /// </summary>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithPublishNoAck(bool publishNoAck)
            {
                _publishNoAck = publishNoAck;
                return this;
            }

            /// <summary>
            /// Builds the JetStreamOptions
            /// </summary>
            /// <returns>The JetStreamOptions object.</returns>
            public JetStreamOptions Build()
            {
                _prefix = JsPrefixManager.AddPrefix(_prefix);
                // _requestTimeout defaulted in WithRequestTimeout
                return new JetStreamOptions(_prefix, _requestTimeout, _publishNoAck);
            }
        }
    }
}
