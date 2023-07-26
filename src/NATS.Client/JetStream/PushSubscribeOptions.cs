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
        public static readonly PushSubscribeOptions DefaultPushOpts = PushSubscribeOptions.Builder().Build();

        // Validation is done by base class
        private PushSubscribeOptions(ISubscribeOptionsBuilder builder, 
            string deliverSubject, string deliverGroup,
            long pendingMessageLimit, long pendingByteLimit) 
            : base(builder, false, deliverSubject, deliverGroup, pendingMessageLimit, pendingByteLimit) {}

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
        /// a specific stream, specific durable and are using bind to mode
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <param name="durable">the durable name</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PushSubscribeOptions BindTo(string stream, string durable)
        {
            return new PushSubscribeOptionsBuilder().WithStream(stream).WithDurable(durable).WithBind(true).Build();
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
            : SubscribeOptionsBuilder<PushSubscribeOptionsBuilder, PushSubscribeOptions>
        {
            private string _deliverSubject;
            private string _deliverGroup;
            private long _pendingMessageLimit = Defaults.SubPendingMsgsLimit;
            private long _pendingByteLimit = Defaults.SubPendingBytesLimit;

            protected override PushSubscribeOptionsBuilder GetThis()
            {
                return this;
            }

            /// <summary>
            /// Set the ordered consumer flag. FOR FUTURE BEHAVIOR. TODO / NOT YET USED.
            /// </summary>
            /// <param name="ordered">flag indicating whether this subscription should be ordered</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithOrdered(bool ordered)
            {
                _ordered = ordered;
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
            /// Set the maximum number of messages that push subscriptions can hold
            /// in the internal (pending) message queue. Defaults to 512 * 1024  (Nats.SubPendingMsgsLimit)
            /// </summary>
            /// <param name="pendingMessageLimit">the number of messages</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithPendingMessageLimit(long pendingMessageLimit)
            {
                _pendingMessageLimit = pendingMessageLimit;
                return this;
            }
            
            /// <summary>
            /// Set the maximum number of bytes that push subscriptions can hold
            /// in the internal (pending) message queue. Defaults to 64 * 1024 * 1024  (Nats.SubPendingBytesLimit)
            /// </summary>
            /// <param name="pendingByteLimit">the number of bytes</param>
            /// <returns>The PushSubscribeOptionsBuilder</returns>
            public PushSubscribeOptionsBuilder WithPendingByteLimit(long pendingByteLimit)
            {
                _pendingByteLimit = pendingByteLimit;
                return this;
            }

            /// <summary>
            /// Builds the PushSubscribeOptions
            /// </summary>
            /// <returns>The PushSubscribeOptions object.</returns>
            public override PushSubscribeOptions Build()
            {
                return new PushSubscribeOptions(this, _deliverSubject, _deliverGroup,
                    _pendingMessageLimit, _pendingByteLimit);
            }
        }
    }
}
