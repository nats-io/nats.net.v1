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

using System;
using NATS.Client.Internals;
using static NATS.Client.Internals.JetStreamConstants;
using static NATS.Client.Internals.NatsConstants;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.JetStream
{
    public sealed class JetStreamOptions
    {
        [Obsolete("This property is obsolete. The connection options request timeout is used as the default", false)]
        public static readonly Duration DefaultTimeout = Duration.OfMillis(Defaults.Timeout);
        
        public static readonly JetStreamOptions DefaultJsOptions = Builder().Build();

        private JetStreamOptions(JetStreamOptionsBuilder b)
        {
            if (b._jsPrefix == null) {
                IsDefaultPrefix = true;
                Prefix = DefaultApiPrefix;
            }
            else {
                IsDefaultPrefix = false;
                Prefix = b._jsPrefix;
            }

            RequestTimeout = b._requestTimeout;
            IsPublishNoAck = b._publishNoAck;
            IsOptOut290ConsumerCreate = b._optOut290ConsumerCreate;
        }

        /// <summary>
        /// Gets the prefix.
        /// </summary>
        public string Prefix { get; }

        /// <summary>
        /// Gets the request timeout
        /// </summary>
        public Duration RequestTimeout { get; }
        
        /// <summary>
        /// Gets is publish should be done in no ack (core) style
        /// </summary>
        public bool IsPublishNoAck { get; }
        
        /// <summary>
        /// True if the prefix for this options is the default prefix.
        /// </summary>
        public bool IsDefaultPrefix { get; }
        
        /// <summary>
        /// Gets whether the opt-out of the server v2.9.0 consumer create api is set
        /// </summary>
        public bool IsOptOut290ConsumerCreate { get; }
                    
        internal static string ConvertDomainToPrefix(string domain) {
            string valid = ValidatePrefixOrDomain(domain, "Domain", false);
            return valid == null ? null 
                : PrefixDollarJsDot + EnsureEndsWithDot(valid) + PrefixApi;
        }

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
            internal string _jsPrefix;
            internal Duration _requestTimeout;
            internal bool _publishNoAck;
            internal bool _optOut290ConsumerCreate;

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
                    if (jso.IsDefaultPrefix)
                    {
                        _jsPrefix = null;
                    }
                    else
                    {
                        _jsPrefix = jso.Prefix;
                    }

                    _requestTimeout = jso.RequestTimeout;
                    _publishNoAck = jso.IsPublishNoAck;
                    _optOut290ConsumerCreate = jso.IsOptOut290ConsumerCreate;
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
                _jsPrefix = EnsureEndsWithDot(ValidatePrefixOrDomain(prefix, "Prefix", false));
                return this;
            }
            
            /// <summary>
            /// Sets the domain for JetStream subjects. A domain can be used in conjunction with
            /// user permissions to restrict access to certain JetStream instances.  This must
            /// match the domain used in the server.
            /// </summary>
            /// <param name="domain">The domain.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithDomain(string domain) 
            {
                string prefix = ConvertDomainToPrefix(domain);
                _jsPrefix = prefix == null ? null : prefix + Dot;
                return this;
            }

            /// <summary>
            /// Sets the request timeout
            /// </summary>
            /// <param name="requestTimeout">The request timeout as Duration.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithRequestTimeout(Duration requestTimeout)
            {
                _requestTimeout = requestTimeout;
                return this;
            }

            /// <summary>
            /// Sets the request timeout
            /// </summary>
            /// <param name="requestTimeoutMillis">The request timeout in millis.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithRequestTimeout(long requestTimeoutMillis)
            {
                _requestTimeout = requestTimeoutMillis < 0 ? null : Duration.OfMillis(requestTimeoutMillis);
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
            /// Sets whether to opt-out of the server v2.9.0 consumer create api. Default is false (opt-in)
            /// </summary>
            /// <param name="optOut"></param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithOptOut290ConsumerCreate(bool optOut) {
                this._optOut290ConsumerCreate = optOut;
                return this;
            }

            /// <summary>
            /// Builds the JetStreamOptions
            /// </summary>
            /// <returns>The JetStreamOptions object.</returns>
            public JetStreamOptions Build()
            {
                return new JetStreamOptions(this);
            }
        }
    }
}
