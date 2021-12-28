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

using NATS.Client.JetStream;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.KeyValue
{
    public sealed class KeyValueOptions
    {
        private KeyValueOptions(string featurePrefix, JetStreamOptions jso)
        {
            FeaturePrefix = featurePrefix;
            JSOptions = jso;
        }

        /// <summary>
        /// Gets the JetStreamOptions
        /// </summary>
        public JetStreamOptions JSOptions { get; }

        /// <summary>
        /// Gets the feature [subject] prefix.
        /// </summary>
        public string FeaturePrefix { get; }
        
        /// <summary>
        /// Gets the JetStreamOptions builder.
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
        /// <param name="jso">an existing KeyValueOptions object</param>
        /// <returns>The builder</returns>
        public static KeyValueOptionsBuilder Builder(KeyValueOptions jso)
        {
            return new KeyValueOptionsBuilder(jso);
        }

        public sealed class KeyValueOptionsBuilder
        {
            private string _featurePrefix;
            private JetStreamOptions _jso;

            /// <summary>
            /// Construct a builder
            /// </summary>
            public KeyValueOptionsBuilder() {}

            /// <summary>
            /// Construct a builder from an existing KeyValueOptions object
            /// </summary>
            /// <param name="kvo">an existing KeyValueOptions object</param>
            public KeyValueOptionsBuilder(KeyValueOptions kvo) 
            {
                if (kvo != null)
                {
                    _featurePrefix = kvo.FeaturePrefix;
                    _jso = kvo.JSOptions;
                }
            }
            
            /// <summary>
            /// Sets the prefix for subjects in features such as KeyValue.
            /// </summary>
            /// <param name="prefix">The prefix.</param>
            /// <returns>The KeyValueOptionsBuilder</returns>
            public KeyValueOptionsBuilder WithFeaturePrefix(string prefix)
            {
                _featurePrefix = EnsureEndsWithDot(ValidatePrefixOrDomain(prefix, "Feature Prefix", false));
                return this;
            }
            
            /// <summary>
            /// Sets the JetStreamOptions.
            /// </summary>
            /// <param name="jso">The JetStreamOptions.</param>
            /// <returns>The KeyValueOptionsBuilder</returns>
            public KeyValueOptionsBuilder WithJetStreamOptions(JetStreamOptions jso)
            {
                _jso = jso;
                return this;
            }

            /// <summary>
            /// Builds the KeyValueOptions
            /// </summary>
            /// <returns>The KeyValueOptions object.</returns>
            public KeyValueOptions Build()
            {
                return new KeyValueOptions(_featurePrefix, _jso);
            }
        }
    }
}
