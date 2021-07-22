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
    public sealed class PushSubscribeOptions : SubscribeOptions
    {
        /// <summary>
        /// Gets the durable name
        /// </summary>
        public string Durable => ConsumerConfiguration.Durable;

        /// <summary>
        /// Gets the deliver subject
        /// </summary>
        public string DeliverSubject => ConsumerConfiguration.DeliverSubject;

        // Validation is done by the builder Build()
        private PushSubscribeOptions(string stream, bool direct, ConsumerConfiguration config) 
            : base(stream, direct, config) {}

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
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, specific durable and are using direct mode
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <param name="durable">the durable name</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PushSubscribeOptions DirectBind(string stream, string durable) {
            return new PushSubscribeOptionsBuilder().WithStream(stream).WithDurable(durable).Direct().Build();
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
            private string _durable;
            private string _deliverSubject;
            private string _stream;
            private bool _direct;
            private ConsumerConfiguration _config;

            /// <summary>
            /// Set the stream name
            /// </summary>
            /// <param name="stream">the stream name</param>
            /// <returns>The builder</returns>
            public PushSubscribeOptionsBuilder WithStream(string stream)
            {
                _stream = stream;
                return this;
            }

            /// <summary>
            /// Set as a direct subscribe
            /// </summary>
            /// <returns>The builder</returns>
            public PushSubscribeOptionsBuilder Direct()
            {
                _direct = true;
                return this;
            }

            /// <summary>
            /// Set the ConsumerConfiguration
            /// </summary>
            /// <param name="consumerConfiguration">the ConsumerConfiguration object</param>
            /// <returns>The builder</returns>
            public PushSubscribeOptionsBuilder WithConfiguration(ConsumerConfiguration configuration)
            {
                _config = configuration;
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
            /// Builds the PushSubscribeOptions
            /// </summary>
            /// <returns>The PushSubscribeOptions object.</returns>
            public PushSubscribeOptions Build()
            {
                _stream = Validator.ValidateStreamName(_stream, false);
                
                _durable = Validator.ValidateDurable(_durable, false);
                if (_durable == null && _config != null)
                {
                    _durable = Validator.ValidateDurable(_config.Durable, false);
                }

                _config = ConsumerConfiguration.Builder(_config)
                    .WithDurable(_durable)
                    .WithDeliverSubject(_deliverSubject)
                    .Build();

                return new PushSubscribeOptions(_stream, _direct, _config);
            }
        }
    }
}
