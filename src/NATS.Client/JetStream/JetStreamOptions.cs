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
using static NATS.Client.Internals.JetStreamConstants;

namespace NATS.Client.JetStream
{
    public sealed class JetStreamOptions
    {
        public static readonly Duration DefaultTimeout = Duration.OfMillis(Defaults.Timeout);
        public static readonly JetStreamOptions DefaultJsOptions = Builder().Build();

        private JetStreamOptions(string inJsPrefix, string featurePrefix, Duration requestTimeout, bool publishNoAck)
        {
            if (inJsPrefix == null) {
                IsDefaultPrefix = true;
                Prefix = DefaultApiPrefix;
            }
            else {
                IsDefaultPrefix = false;
                Prefix = inJsPrefix;
            }

            FeaturePrefix = featurePrefix;
            RequestTimeout = requestTimeout;
            IsPublishNoAck = publishNoAck;
        }

        /// <summary>
        /// Gets the prefix.
        /// </summary>
        public string Prefix { get; }

        /// <summary>
        /// Gets the feature [subject] prefix.
        /// </summary>
        public string FeaturePrefix { get; }

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
            private string _jsPrefix;
            private string _featurePrefix;
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
                    if (jso.IsDefaultPrefix)
                    {
                        _jsPrefix = null;
                    }
                    else
                    {
                        _jsPrefix = jso.Prefix;
                    }

                    _featurePrefix = jso.FeaturePrefix;
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
                string temp = Validator.EmptyAsNull(prefix);
                if (temp != null) {
                    _jsPrefix = ValidateJsPrefix(temp);
                }
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
                string temp = Validator.EmptyAsNull(domain);
                if (temp != null) {
                    _jsPrefix = ValidateDomain(temp);
                }
                return this;
            }
            
            /// <summary>
            /// Sets the prefix for subjects in features such as KeyValue.
            /// </summary>
            /// <param name="prefix">The prefix.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithFeaturePrefix(string prefix) 
            {
                _featurePrefix = ValidateFeaturePrefix(Validator.EmptyAsNull(prefix));
                return this;
            }

            /// <summary>
            /// Sets the request timeout
            /// </summary>
            /// <param name="requestTimeout">The request timeout as Duration.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithRequestTimeout(Duration requestTimeout)
            {
                _requestTimeout = Validator.EnsureNotNullAndNotLessThanMin(requestTimeout, Duration.Zero, DefaultTimeout);
                return this;
            }

            /// <summary>
            /// Sets the request timeout
            /// </summary>
            /// <param name="requestTimeoutMillis">The request timeout in millis.</param>
            /// <returns>The JetStreamOptionsBuilder</returns>
            public JetStreamOptionsBuilder WithRequestTimeout(long requestTimeoutMillis) 
            {
                _requestTimeout = Validator.EnsureDurationNotLessThanMin(requestTimeoutMillis, Duration.Zero, DefaultTimeout);
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
                _requestTimeout = _requestTimeout ?? DefaultTimeout;
                return new JetStreamOptions(_jsPrefix, _featurePrefix, _requestTimeout, _publishNoAck);
            }

            private string ValidateJsPrefix(string prefix) {
                string valid = Validator.ValidatePrefixOrDomain(prefix, "Prefix", true);
                return valid.EndsWith(".") ? valid : valid + ".";
            }

            private string ValidateFeaturePrefix(string prefix) {
                string valid = Validator.ValidatePrefixOrDomain(prefix, "Feature Prefix", false);
                return valid == null ? null : valid.EndsWith(".") ? valid : valid + ".";
            }

            private string ValidateDomain(string domain) {
                string valid = Validator.ValidatePrefixOrDomain(domain, "Domain", true);
                if (valid.EndsWith(".")) {
                    return PrefixDollarJsDot + valid + PrefixApiDot;
                }
                return PrefixDollarJsDot + valid + "." + PrefixApiDot;
            }
        }
    }
}
