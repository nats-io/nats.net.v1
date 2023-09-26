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
    public sealed class PullSubscribeOptions : SubscribeOptions
    {
        public static readonly PullSubscribeOptions DefaultPullOpts = PullSubscribeOptions.Builder().Build();

        // Validation is done by base class
        private PullSubscribeOptions(ISubscribeOptionsBuilder builder) : base(builder, true, null, null) {}

        /// <summary>
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, specific durable and are using bind mode
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <param name="name">the consumer name, commonly the durable name</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PullSubscribeOptions BindTo(string stream, string name) {
            return Builder().WithStream(stream).WithDurable(name).WithBind(true).Build();
        }

        /// <summary>
        /// Gets the PullSubscribeOptions builder.
        /// </summary>
        /// <returns>The PullSubscribeOptionsBuilder</returns>
        public static PullSubscribeOptionsBuilder Builder()
        {
            return new PullSubscribeOptionsBuilder();
        }

        public class PullSubscribeOptionsBuilder : PullSubscribeOptionsSubscribeOptionsBuilder { }

        /// <summary>
        /// PullSubscribeOptionsSubscribeOptionsBuilder was a naming type. Please use the simpler PullSubscribeOptionsBuilder
        /// </summary>
        public class PullSubscribeOptionsSubscribeOptionsBuilder 
            : SubscribeOptionsBuilder<PullSubscribeOptionsSubscribeOptionsBuilder, PullSubscribeOptions>
        {
            protected override PullSubscribeOptionsSubscribeOptionsBuilder GetThis()
            {
                return this;
            }

            /// <summary>
            /// Specify binding to an existing consumer via name.
            /// The client does not validate that the provided consumer configuration
            /// is consistent with the server version or that
            /// consumer type (push versus pull) matches the subscription type.
            /// An inconsistent consumer configuration for instance can result in
            /// receiving messages from unexpected subjects.
            /// A consumer type mismatch will result in an error from the server.
            /// </summary>
            /// <param name="isFastBind">whether to fast bind or not</param>
            /// <returns>The builder</returns>
            public PullSubscribeOptionsSubscribeOptionsBuilder WithFastBind(bool isFastBind)
            {
                _fastBind = isFastBind;
                return GetThis();
            }

            /// <summary>
            /// Builds the PullSubscribeOptions
            /// </summary>
            /// <returns>The PullSubscribeOptions object.</returns>
            public override PullSubscribeOptions Build()
            {
                return new PullSubscribeOptions(this);
            }
        }
    }
}
