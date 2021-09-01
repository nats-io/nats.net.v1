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

        /// <summary>
        /// Gets the deliver group
        /// </summary>
        public string DeliverGroup => ConsumerConfiguration.DeliverGroup;

        // Validation is done by base class
        private PushSubscribeOptions(string stream, string durable, bool bind, 
            string deliverSubject, string deliverGroup, ConsumerConfiguration cc) 
            : base(stream, durable, false, bind, deliverSubject, deliverGroup, cc) {}

        /// <summary>
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, which could be a stream or a mirror
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PushSubscribeOptions ForStream(string stream) {
            return new PushSubscribeOptionsBuilder().WithStream(stream).Build();
        }

        /// <summary>
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, specific durable and are using direct mode
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <param name="durable">the durable name</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PushSubscribeOptions BindTo(string stream, string durable) {
            return new PushSubscribeOptionsBuilder().WithStream(stream).WithDurable(durable).Bind(true).Build();
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
            private bool _bind;
            private string _durable;
            private ConsumerConfiguration _config;
            private string _deliverSubject;
            private string _deliverGroup;

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
            public PushSubscribeOptionsBuilder Bind(bool b)
            {
                _bind = b;
                return this;
            }

            /// <summary>
            /// Set the ConsumerConfiguration
            /// </summary>
            /// <param name="configuration">the ConsumerConfiguration object</param>
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
            /// Set the deliver group 
            /// </summary>
            /// <param name="deliverGroup">the deliver group value</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithDeliverGroup(string deliverGroup)
            {
                _deliverGroup = deliverGroup;
                return this;
            }

            /// <summary>
            /// Builds the PushSubscribeOptions
            /// </summary>
            /// <returns>The PushSubscribeOptions object.</returns>
            public PushSubscribeOptions Build()
            {
                return new PushSubscribeOptions(_stream, _durable, _bind, _deliverSubject, _deliverGroup, _config);
            }
        }
    }
}
