// Copyright 2024 The NATS Authors
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

namespace NATS.Client.KeyValue
{
    public sealed class KeyValueConsumerConfiguration
    {
        public string Description { get; }
        internal IDictionary<string, string> _metadata;
        public IDictionary<string, string> Metadata => _metadata ?? new Dictionary<string, string>();

        private KeyValueConsumerConfiguration(KeyValueConsumerConfigurationBuilder builder)
        {
            Description = builder._description;
            _metadata = builder._metadata;
        }

        public static KeyValueConsumerConfigurationBuilder Builder()
        {
            return new KeyValueConsumerConfigurationBuilder();
        }

        public sealed class KeyValueConsumerConfigurationBuilder
        {   
            internal string _description;
            internal Dictionary<string, string> _metadata;

            /// <summary>
            /// Sets the description.
            /// </summary>
            /// <param name="description">the description</param>
            /// <returns>The KeyValueConsumerConfigurationBuilder</returns>
            public KeyValueConsumerConfigurationBuilder WithDescription(string description)
            {
                _description = EmptyAsNull(description);
                return this;
            }

            /// <summary>
            /// Sets the metadata for the configuration 
            /// </summary>
            /// <param name="metadata">the metadata dictionary</param>
            /// <returns>The KeyValueConsumerConfigurationBuilder</returns>
            public KeyValueConsumerConfigurationBuilder WithMetadata(IDictionary<string, string> metadata) {
                if (metadata == null)
                {
                    _metadata = null;
                }
                else
                {
                    _metadata = new Dictionary<string, string>();
                    foreach (string key in metadata.Keys)
                    {
                        _metadata[key] = metadata[key];
                    }
                }
                return this;
            }

            /// <summary>
            /// Builds the KeyValueConsumerConfiguration
            /// </summary>
            /// <returns>The KeyValueConsumerConfiguration</returns>
            public KeyValueConsumerConfiguration Build()
            {
                return new KeyValueConsumerConfiguration(this);
            }
        }
    }
}
