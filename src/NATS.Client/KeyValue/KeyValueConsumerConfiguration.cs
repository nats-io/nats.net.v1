// Copyright 2021-2023 The NATS Authors
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
using NATS.Client.Internals.SimpleJSON;
using NATS.Client.JetStream;
using static NATS.Client.Internals.JsonUtils;
using static NATS.Client.Internals.Validator;

namespace NATS.Client.KeyValue
{
    public sealed class KeyValueConsumerConfiguration
    {
        public string Description { get; }
        public string Name { get; }
        internal IDictionary<string, string> _metadata;
        public IDictionary<string, string> Metadata => _metadata ?? new Dictionary<string, string>();

        internal KeyValueConsumerConfiguration(string json) : this(JSON.Parse(json)) {}

        internal KeyValueConsumerConfiguration(JSONNode ccNode)
        {
            Description = ccNode[ApiConstants.Description].Value;
            Name = ccNode[ApiConstants.Name].Value;
            _metadata = StringStringDictionary(ccNode, ApiConstants.Metadata, true);
        }

        private KeyValueConsumerConfiguration(KeyValueConsumerConfigurationBuilder builder)
        {
            Description = builder._description;
            Name = builder._name;
            _metadata = builder._metadata;
        }


        public static KeyValueConsumerConfigurationBuilder Builder()
        {
            return new KeyValueConsumerConfigurationBuilder();
        }
        
        public static KeyValueConsumerConfigurationBuilder Builder(ConsumerConfiguration cc)
        {
            return new KeyValueConsumerConfigurationBuilder(cc);
        }

        public sealed class KeyValueConsumerConfigurationBuilder
        {   
            internal string _description;
            internal string _name;      
            internal Dictionary<string, string> _metadata;
            public KeyValueConsumerConfigurationBuilder() {}

            public KeyValueConsumerConfigurationBuilder(ConsumerConfiguration cc)
            {
                if (cc == null) return;
                _description = cc.Description;
                _name = cc.Name;

                if (cc._metadata != null)
                {
                    _metadata = new Dictionary<string, string>();
                    foreach (string key in cc.Metadata.Keys)
                    {
                        _metadata[key] = cc.Metadata[key];
                    }
                }
            }

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
            /// Sets the name of the consumer.
            /// Null or empty clears the field
            /// </summary>
            /// <param name="name">name of the consumer.</param>
            /// <returns>The KeyValueConsumerConfigurationBuilder</returns>
            public KeyValueConsumerConfigurationBuilder WithName(string name)
            {
                _name = ValidateConsumerName(name, false);
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
