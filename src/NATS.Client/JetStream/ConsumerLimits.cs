// Copyright 2023 The NATS Authors
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
using NATS.Client.Internals.SimpleJSON;
using static NATS.Client.Internals.JsonUtils;
using static NATS.Client.JetStream.ConsumerConfiguration;

namespace NATS.Client.JetStream
{
    /// <summary>
    /// ConsumerLimits options for a stream
    /// </summary>
    public sealed class ConsumerLimits : JsonSerializable
    {
        internal int? _maxAckPending;
        public int MaxAckPending => GetOrUnset(_maxAckPending);
        
        public Duration InactiveThreshold { get; }

        internal static ConsumerLimits OptionalInstance(JSONNode consumerLimitsNode)
        {
            return consumerLimitsNode.Count == 0 ? null : new ConsumerLimits(consumerLimitsNode);
        }

        private ConsumerLimits(ConsumerLimitsBuilder b)
        {
            InactiveThreshold = b._inactiveThreshold;
            _maxAckPending = b._maxAckPending;
        }

        private ConsumerLimits(JSONNode clNode)
        {
            InactiveThreshold = AsDuration(clNode, ApiConstants.InactiveThreshold, null);
            _maxAckPending = clNode[ApiConstants.MaxAckPending].AsIntOr(IntUnset);
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();

            AddField(o, ApiConstants.InactiveThreshold, InactiveThreshold);
            AddField(o, ApiConstants.MaxAckPending, MaxAckPending);
            return o;
        }

        /// <summary>
        /// Creates a builder for a ConsumerLimits object. 
        /// </summary>
        /// <returns>The Builder</returns>
        public static ConsumerLimitsBuilder Builder() {
            return new ConsumerLimitsBuilder();
        }

        /// <summary>
        /// ConsumerLimits can be created using a ConsumerLimitsBuilder. 
        /// </summary>
        public sealed class ConsumerLimitsBuilder {
            internal Duration _inactiveThreshold;
            internal int? _maxAckPending;
            
            /// <summary>
            /// Set the amount of time before the ephemeral consumer is deemed inactive.
            /// </summary>
            /// <param name="inactiveThreshold">the max amount of expire as a Duration</param>
            /// <returns>The ConsumerLimitsBuilder</returns>
            public ConsumerLimitsBuilder WithInactiveThreshold(Duration inactiveThreshold) {
                _inactiveThreshold = Normalize(inactiveThreshold);
                return this;
            }

            /// <summary>
            /// Set the amount of time before the ephemeral consumer is deemed inactive.
            /// </summary>
            /// <param name="inactiveThresholdMillis">the max amount of expire as milliseconds</param>
            /// <returns>The ConsumerLimitsBuilder</returns>
            public ConsumerLimitsBuilder WithInactiveThreshold(long inactiveThresholdMillis) {
                _inactiveThreshold = NormalizeDuration(inactiveThresholdMillis);
                return this;
            }

            /// <summary>
            /// Sets the maximum ack pending.
            /// </summary>
            /// <param name="maxAckPending">maximum pending acknowledgements.</param>
            /// <returns>The ConsumerLimitsBuilder</returns>
            public ConsumerLimitsBuilder WithMaxAckPending(long? maxAckPending)
            {
                _maxAckPending = NormalizeToInt(maxAckPending, StandardMin);
                return this;
            }

            /// <summary>
            /// Build a ConsumerLimits object
            /// </summary>
            /// <returns>The ConsumerLimits</returns>
            public ConsumerLimits Build() {
                return new ConsumerLimits(this);
            }
        }
    }
}
