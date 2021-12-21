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
        // Validation is done by base class
        private PullSubscribeOptions(ISubscribeOptionsBuilder builder) : base(builder, true, false, null, null) {}

        /// <summary>
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, specific durable and are using bind mode
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <param name="durable">the durable name</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PullSubscribeOptions BindTo(string stream, string durable) {
            return new PullSubscribeOptionsSubscribeOptionsBuilder().WithStream(stream).WithDurable(durable).WithBind(true).Build();
        }

        /// <summary>
        /// Gets the PullSubscribeOptions builder.
        /// </summary>
        /// <returns>The PullSubscribeOptionsBuilder</returns>
        public static PullSubscribeOptionsSubscribeOptionsBuilder Builder()
        {
            return new PullSubscribeOptionsSubscribeOptionsBuilder();
        }

        public sealed class PullSubscribeOptionsSubscribeOptionsBuilder 
            : SubscribeOptionsBuilder<PullSubscribeOptionsSubscribeOptionsBuilder, PullSubscribeOptions>
        {
            protected override PullSubscribeOptionsSubscribeOptionsBuilder GetThis()
            {
                return this;
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
