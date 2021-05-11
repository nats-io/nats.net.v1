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
    public sealed class PushSubscribeOptions
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
        /// Gets the durable name
        /// </summary>
        public string Durable => ConsumerConfiguration.Durable;

        /// <summary>
        /// Gets the deliver subject
        /// </summary>
        public string DeliverSubject => ConsumerConfiguration.DeliverSubject;

        private PushSubscribeOptions(string stream, ConsumerConfiguration consumerConfiguration)
        {
            _stream = stream;
            _consumerConfiguration = consumerConfiguration;
        }

        /// <summary>
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, which could be a stream or a mirror
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PushSubscribeOptions Bind(string stream) {
            return new PushSubscribeOptionsBuilder().WithStream(stream).Build();
        }

        /// <summary>
        /// Gets the PushSubscribeOptions builder.
        /// </summary>
        /// <returns>
        /// The builder
        /// </returns>
        public static PushSubscribeOptionsBuilder Builder() {
            return new PushSubscribeOptionsBuilder();
        }

        public sealed class PushSubscribeOptionsBuilder
        {
            private string _stream;
            private string _durable;
            private ConsumerConfiguration _consumerConfig;
            private string _deliverSubject;

            /// <summary>
            /// Set the stream name
            /// </summary>
            /// <param name="stream">the stream value</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithStream(string stream)
            {
                _stream = stream;
                return this;
            }

            /// <summary>
            /// Set the durable
            /// </summary>
            /// <param name="durable">the durable value</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithDurable(string durable)
            {
                _durable = durable;
                return this;
            }

            /// <summary>
            /// Set the deliver subject 
            /// </summary>
            /// <param name="deliverSubject">the deliver subject value</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithDeliverSubject(string deliverSubject)
            {
                _deliverSubject = deliverSubject;
                return this;
            }

            /// <summary>
            /// Set the ConsumerConfiguration
            /// </summary>
            /// <param name="consumerConfiguration">the ConsumerConfiguration object</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithConfiguration(ConsumerConfiguration consumerConfiguration)
            {
                _consumerConfig = consumerConfiguration;
                return this;
            }

            /// <summary>
            /// Builds the PushSubscribeOptions
            /// </summary>
            /// <returns>The PushSubscribeOptions object.</returns>
            public PushSubscribeOptions Build() {
                _stream = Validator.ValidateStreamName(_stream, false);

                _durable = Validator.ValidateDurable(_durable, false);
                if (_durable == null && _consumerConfig != null) {
                    _durable = Validator.ValidateDurable(_consumerConfig.Durable, false);
                }

                _consumerConfig = ConsumerConfiguration.Builder(_consumerConfig)
                    .WithDurable(_durable)
                    .WithDeliverSubject(Validator.EmptyAsNull(_deliverSubject))
                    .Build();

                return new PushSubscribeOptions(_stream, _consumerConfig);
            }
        }
    }
}
