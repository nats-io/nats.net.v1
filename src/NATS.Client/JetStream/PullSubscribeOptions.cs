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
        /// <summary>
        /// Gets the Durable name
        /// </summary>
        public string Durable => ConsumerConfiguration.Durable;

        // Validation is done by base class
        private PullSubscribeOptions(string stream, string durable, bool bind, 
            bool detectGaps, ulong expectedConsumerSeq, long messageAlarmTime,
            ConsumerConfiguration cc) 
            : base(stream, durable, true, bind, null, null, 
                detectGaps, expectedConsumerSeq, messageAlarmTime, cc) {}

        /// <summary>
        /// Create PushSubscribeOptions where you are binding to
        /// a specific stream, specific durable and are using bind mode
        /// </summary>
        /// <param name="stream">the stream name to bind to</param>
        /// <param name="durable">the durable name</param>
        /// <returns>the PushSubscribeOptions</returns>
        public static PullSubscribeOptions BindTo(string stream, string durable) {
            return new PullSubscribeOptionsBuilder().WithStream(stream).WithDurable(durable).Bind(true).Build();
        }

        /// <summary>
        /// Gets the PullSubscribeOptions builder.
        /// </summary>
        /// <returns>The PullSubscribeOptionsBuilder</returns>
        public static PullSubscribeOptionsBuilder Builder()
        {
            return new PullSubscribeOptionsBuilder();
        }

        public sealed class PullSubscribeOptionsBuilder
        {
            private string _stream;
            private bool _bind;
            private string _durable;
            private ConsumerConfiguration _config;
            private bool _detectGaps;
            private ulong _expectedConsumerSeq;
            private long _messageAlarmTime;

            /// <summary>
            /// Set the stream name
            /// </summary>
            /// <param name="stream">the stream name</param>
            /// <returns>The builder</returns>
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
            /// Set as a direct subscribe
            /// </summary>
            /// <returns>The builder</returns>
            public PullSubscribeOptionsBuilder Bind(bool isBind)
            {
                _bind = isBind;
                return this;
            }

            /// <summary>
            /// Set the ConsumerConfiguration
            /// </summary>
            /// <param name="configuration">the ConsumerConfiguration object</param>
            /// <returns>The builder</returns>
            public PullSubscribeOptionsBuilder WithConfiguration(ConsumerConfiguration configuration)
            {
                _config = configuration;
                return this;
            }

            /// <summary>
            /// Sets or clears the auto gap manage flag 
            /// </summary>
            /// <param name="detectGaps">the flag</param>
            /// <returns>The PullSubscribeOptionsBuilder</returns>
            public PullSubscribeOptionsBuilder WithDetectGaps(bool detectGaps)
            {
                _detectGaps = detectGaps;
                return this;
            }

            /// <summary>
            /// Sets the expected consumer sequence to use the first
            /// time on auto gap detect. Set to less than 1 to allow
            /// any first sequence
            /// </summary>
            /// <param name="expectedConsumerSeq"> the time</param>
            /// <returns>The PullSubscribeOptionsBuilder</returns>
            public PullSubscribeOptionsBuilder WithExpectedConsumerSeq(ulong expectedConsumerSeq)
            {
                _expectedConsumerSeq = expectedConsumerSeq;
                return this;
            }

            /// <summary>
            /// Set the total amount of time to not receive any messages or heartbeats
            /// before calling the ErrorListener heartbeatAlarm 
            /// </summary>
            /// <param name="messageAlarmTime"> the time</param>
            /// <returns>The PullSubscribeOptionsBuilder</returns>
            public PullSubscribeOptionsBuilder WithMessageAlarmTime(long messageAlarmTime)
            {
                _messageAlarmTime = messageAlarmTime;
                return this;
            }

            /// <summary>
            /// Builds the PullSubscribeOptions
            /// </summary>
            /// <returns>The PullSubscribeOptions object.</returns>
            public PullSubscribeOptions Build()
            {
                return new PullSubscribeOptions(_stream, _durable, _bind, 
                    _detectGaps, _expectedConsumerSeq, _messageAlarmTime, _config);
            }
        }
    }
}
