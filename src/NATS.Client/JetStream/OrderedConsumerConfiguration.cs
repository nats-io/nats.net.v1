// Copyright 2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using NATS.Client.Internals;

namespace NATS.Client.JetStream
{
    public sealed class OrderedConsumerConfiguration
    {
        public const string DefaultFilterSubject = ">";
        
        public string FilterSubject { get; private set; }
        public DeliverPolicy DeliverPolicy { get; private set; }
        public ulong StartSequence { get; private set; }
        public DateTime StartTime { get; private set; }
        public ReplayPolicy ReplayPolicy { get; private set; }
        public bool HeadersOnly { get; private set; }

        /// <summary>
        /// OrderedConsumerConfiguration creation works like a builder.
        /// The builder supports chaining and will create a default set of options if
        /// no methods are calls, including setting the filter subject to "&gt;"
        public OrderedConsumerConfiguration()
        {
            StartSequence = ConsumerConfiguration.UlongUnset;
            FilterSubject = DefaultFilterSubject;
        }
        
        /// <summary>
        /// Sets the filter subject of the OrderedConsumerConfiguration.
        /// </summary>
        /// <param name="filterSubject">the filter subject</param>
        /// <returns>Builder</returns>
        public OrderedConsumerConfiguration WithFilterSubject(string filterSubject) {
            FilterSubject = Validator.EmptyOrNullAs(filterSubject, DefaultFilterSubject);
            return this;
        }

        /// <summary>
        /// Sets the delivery policy of the OrderedConsumerConfiguration.
        /// </summary>
        /// <param name="deliverPolicy">the delivery policy.</param>
        /// <returns>Builder</returns>
        public OrderedConsumerConfiguration WithDeliverPolicy(DeliverPolicy deliverPolicy) {
            DeliverPolicy = deliverPolicy;
            return this;
        }
    
        /// <summary>
        /// Sets the start sequence of the OrderedConsumerConfiguration.
        /// </summary>
        /// <param name="startSequence">the start sequence</param>
        /// <returns>Builder</returns>
        public OrderedConsumerConfiguration WithStartSequence(ulong startSequence) {
            StartSequence = startSequence;
            return this;
        }
    
        /// <summary>
        /// Sets the start time of the OrderedConsumerConfiguration.
        /// </summary>
        /// <param name="startTime">the start time</param>
        /// <returns>Builder</returns>
        public OrderedConsumerConfiguration WithStartTime(DateTime startTime) {
            StartTime = startTime;
            return this;
        }
    
        /// <summary>
        /// Sets the replay policy of the OrderedConsumerConfiguration.
        /// </summary>
        /// <param name="replayPolicy">the replay policy.</param>
        /// <returns>Builder</returns>
        public OrderedConsumerConfiguration WithReplayPolicy(ReplayPolicy replayPolicy) {
            ReplayPolicy = replayPolicy;
            return this;
        }
    
        /// <summary>
        /// set the headers only flag saying to deliver only the headers of
        /// messages in the stream and not the bodies
        /// </summary>
        /// <param name="headersOnly">the flag</param>
        /// <returns>Builder</returns>
        public OrderedConsumerConfiguration WithHeadersOnly(bool headersOnly) {
            HeadersOnly = headersOnly;
            return this;
        }
    }
}
