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
    public sealed class PullSubscribeOptions
    {
        private readonly string _stream;
        private readonly ConsumerConfiguration _consumerConfiguration;

        /// <summary>
        /// Gets the stream name
        /// </summary>
        public string Stream { get => _stream; }

        /// <summary>
        /// Gets the ConsumerConfiguration
        /// </summary>
        public ConsumerConfiguration ConsumerConfiguration { get => _consumerConfiguration; }
        
        /// <summary>
        /// Gets the Durable name
        /// </summary>
        public string Durable => ConsumerConfiguration.Durable;

        private PullSubscribeOptions(string stream, ConsumerConfiguration consumerConfiguration)
        {
            _stream = stream;
            _consumerConfiguration = consumerConfiguration;
        }
        
        /// <summary>
        /// Gets the PullSubscribeOptions builder.
        /// </summary>
        /// <returns>The PullSubscribeOptionsBuilder</returns>
        public static PullSubscribeOptionsBuilder Builder() {
            return new PullSubscribeOptionsBuilder();
        }

        public sealed class PullSubscribeOptionsBuilder
        {
            private string _stream;
            private string _durable;
            private ConsumerConfiguration _consumerConfig;

            /// <summary>
            /// Set the stream name
            /// </summary>
            /// <param name="stream">the stream value</param>
            /// <returns>The PullSubscribeOptionsBuilder</returns>
            public PullSubscribeOptionsBuilder WithStream(string stream)
            {
                _stream = stream;
                return this;
            }

            /// <summary>
            /// Set the durable
            /// </summary>
            /// <param name="durable">the durable value</param>
            /// <returns>The PullSubscribeOptionsBuilder</returns>
            public PullSubscribeOptionsBuilder WithDurable(string durable)
            {
                _durable = durable;
                return this;
            }

            /// <summary>
            /// Set the ConsumerConfiguration
            /// </summary>
            /// <param name="consumerConfiguration">the ConsumerConfiguration object</param>
            /// <returns>The PullSubscribeOptionsBuilder</returns>
            public PullSubscribeOptionsBuilder WithConfiguration(ConsumerConfiguration consumerConfiguration)
            {
                _consumerConfig = consumerConfiguration;
                return this;
            }

            /// <summary>
            /// Builds the PullSubscribeOptions
            /// </summary>
            /// <returns>The PullSubscribeOptions object.</returns>
            public PullSubscribeOptions Build() {
                _stream = Validator.ValidateStreamNameOrEmptyAsNull(_stream);

                _durable = Validator.ValidateDurableRequired(_durable, _consumerConfig);

                _consumerConfig = ConsumerConfiguration.Builder(_consumerConfig)
                    .Durable(_durable)
                    .Build();

                return new PullSubscribeOptions(_stream, _consumerConfig);
            }
        }
    }
}
