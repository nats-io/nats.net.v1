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

using System;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class ConsumerConfiguration
    {
        public DeliverPolicy DeliverPolicy { get; }
        public AckPolicy AckPolicy { get; }
        public ReplayPolicy ReplayPolicy { get; }
        public string Durable { get; }
        public string DeliverSubject { get; }
        public long StartSeq { get; }
        public DateTime StartTime { get; }
        public Duration AckWait { get; }
        public long MaxDeliver { get; }
        public string FilterSubject { get; }
        public string SampleFrequency { get; }
        public long RateLimit { get; }
        public long MaxAckPending { get; }

        internal ConsumerConfiguration(string json) : this(JSON.Parse(json)) {}

        internal ConsumerConfiguration(JSONNode ccNode)
        {
            DeliverPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConstants.DeliverPolicy].Value, DeliverPolicy.All);
            AckPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConstants.AckPolicy].Value, AckPolicy.Explicit);
            ReplayPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConstants.ReplayPolicy], ReplayPolicy.Instant);
            Durable = ccNode[ApiConstants.DurableName].Value;
            DeliverSubject = ccNode[ApiConstants.DeliverSubject].Value;
            StartSeq = ccNode[ApiConstants.OptStartSeq].AsLong;
            StartTime = JsonUtils.AsDate(ccNode[ApiConstants.OptStartTime]);
            AckWait = Duration.OfNanos(ccNode[ApiConstants.AckWait]);
            MaxDeliver = JsonUtils.AsLongOrMinus1(ccNode, ApiConstants.MaxDeliver);
            FilterSubject = ccNode[ApiConstants.FilterSubject].Value;
            SampleFrequency = ccNode[ApiConstants.SampleFreq].Value;
            RateLimit = ccNode[ApiConstants.RateLimit].AsLong;
            MaxAckPending = ccNode[ApiConstants.MaxAckPending].AsLong;
        }

        internal ConsumerConfiguration(string durable, DeliverPolicy deliverPolicy, long startSeq, DateTime startTime,
            AckPolicy ackPolicy, Duration ackWait, long maxDeliver, string filterSubject, ReplayPolicy replayPolicy,
            string sampleFrequency, long rateLimit, string deliverSubject, long maxAckPending)
        {
            Durable = durable;
            DeliverPolicy = deliverPolicy;
            StartSeq = startSeq;
            StartTime = startTime;
            AckPolicy = ackPolicy;
            AckWait = ackWait;
            MaxDeliver = maxDeliver;
            FilterSubject = filterSubject;
            ReplayPolicy = replayPolicy;
            SampleFrequency = sampleFrequency;
            RateLimit = rateLimit;
            DeliverSubject = deliverSubject;
            MaxAckPending = maxAckPending;
        }

        internal JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.DurableName] = Durable,
                [ApiConstants.DeliverPolicy] = DeliverPolicy.GetString(),
                [ApiConstants.DeliverSubject] = DeliverSubject,
                [ApiConstants.OptStartSeq] = StartSeq,
                [ApiConstants.OptStartTime] = JsonUtils.ToString(StartTime),
                [ApiConstants.AckPolicy] = AckPolicy.GetString(),
                [ApiConstants.AckWait] = AckWait.Nanos,
                [ApiConstants.MaxDeliver] = MaxDeliver,
                [ApiConstants.FilterSubject] = FilterSubject,
                [ApiConstants.ReplayPolicy] = ReplayPolicy.GetString(),
                [ApiConstants.SampleFreq] = SampleFrequency,
                [ApiConstants.RateLimit] = RateLimit,
                [ApiConstants.MaxAckPending] = MaxAckPending
            };
        }

        public static ConsumerConfigurationBuilder Builder()
        {
            return new ConsumerConfigurationBuilder();
        }
        
        public static ConsumerConfigurationBuilder Builder(ConsumerConfiguration cc)
        {
            return new ConsumerConfigurationBuilder(cc);
        }

        public sealed class ConsumerConfigurationBuilder
        {
            private DeliverPolicy _deliverPolicy;
            private AckPolicy _ackPolicy;
            private ReplayPolicy _replayPolicy;
            private string _durable;
            private string _deliverSubject;
            private long _startSeq;
            private DateTime _startTime;
            private Duration _ackWait;
            private long _maxDeliver;
            private string _filterSubject;
            private string _sampleFrequency;
            private long _rateLimit;
            private long _maxAckPending;

            public string Durable() => _durable;
            public string DeliverSubject() => _deliverSubject;
            public long MaxAckPending() => _maxAckPending;
            public AckPolicy AckPolicy() => _ackPolicy;

            public ConsumerConfigurationBuilder() {}

            public ConsumerConfigurationBuilder(ConsumerConfiguration cc)
            {
                if (cc == null) return;
                _durable = cc.Durable;
                _deliverPolicy = cc.DeliverPolicy;
                _startSeq = cc.StartSeq;
                _startTime = cc.StartTime;
                _ackPolicy = cc.AckPolicy;
                _ackWait = cc.AckWait;
                _maxDeliver = cc.MaxDeliver;
                _filterSubject = cc.FilterSubject;
                _replayPolicy = cc.ReplayPolicy;
                _sampleFrequency = cc.SampleFrequency;
                _rateLimit = cc.RateLimit;
                _deliverSubject = cc.DeliverSubject;
                _maxAckPending = cc.MaxAckPending;
            }

            /// <summary>
            /// Sets the name of the durable subscription.
            /// </summary>
            /// <param name="durable">name of the durable subscription.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder Durable(string durable)
            {
                _durable = durable;
                return this;
            }

            /// <summary>
            /// Sets the delivery policy of the ConsumerConfiguration.
            /// </summary>
            /// <param name="policy">the delivery policy.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder DeliverPolicy(DeliverPolicy policy)
            {
                _deliverPolicy = policy;
                return this;
            }

            /// <summary>
            /// Sets the subject to deliver messages to.
            /// </summary>
            /// <param name="subject">the delivery subject.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder DeliverSubject(string subject)
            {
                _deliverSubject = subject;
                return this;
            }

            /// <summary>
            /// Sets the start sequence of the ConsumerConfiguration.
            /// </summary>
            /// <param name="sequence">the start sequence</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder StartSequence(long sequence)
            {
                _startSeq = sequence;
                return this;
            }

            /// <summary>
            /// Sets the start time of the ConsumerConfiguration.
            /// </summary>
            /// <param name="startTime">the start time</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder StartTime(DateTime startTime)
            {
                _startTime = startTime;
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement policy of the ConsumerConfiguration.
            /// </summary>
            /// <param name="policy">the acknowledgement policy.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder AckPolicy(AckPolicy policy)
            {
                _ackPolicy = policy;
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement wait duration of the ConsumerConfiguration.
            /// </summary>
            /// <param name="timeout">the wait timeout</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder AckWait(Duration timeout)
            {
                _ackWait = timeout;
                return this;
            }

            /// <summary>
            /// Sets the maximum delivery amount of the ConsumerConfiguration.
            /// </summary>
            /// <param name="maxDeliver">the maximum delivery amount</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder MaxDeliver(long maxDeliver)
            {
                _maxDeliver = maxDeliver;
                return this;
            }

            /// <summary>
            /// Sets the filter subject of the ConsumerConfiguration.
            /// </summary>
            /// <param name="filterSubject">the filter subject</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder FilterSubject(string filterSubject)
            {
                _filterSubject = filterSubject;
                return this;
            }

            /// <summary>
            /// Sets the replay policy of the ConsumerConfiguration.
            /// </summary>
            /// <param name="policy">the replay policy.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder ReplayPolicy(ReplayPolicy policy)
            {
                _replayPolicy = policy;
                return this;
            }

            /// <summary>
            /// Sets the sample frequency of the ConsumerConfiguration.
            /// </summary>
            /// <param name="frequency">the frequency</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder SampleFrequency(string frequency)
            {
                _sampleFrequency = frequency;
                return this;
            }

            /// <summary>
            /// Set the rate limit of the ConsumerConfiguration.
            /// </summary>
            /// <param name="msgsPerSecond">messages per second to deliver</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder RateLimit(int msgsPerSecond)
            {
                _rateLimit = msgsPerSecond;
                return this;
            }

            /// <summary>
            /// Sets the maximum ack pending.
            /// </summary>
            /// <param name="maxAckPending">maximum pending acknowledgements.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder MaxAckPending(long maxAckPending)
            {
                _maxAckPending = maxAckPending;
                return this;
            }

            /// <summary>
            /// Builds the ConsumerConfiguration
            /// </summary>
            /// <returns>The ConsumerConfiguration</returns>
            public ConsumerConfiguration Build()
            {
                return new ConsumerConfiguration(
                    _durable,
                    _deliverPolicy,
                    _startSeq,
                    _startTime,
                    _ackPolicy,
                    _ackWait,
                    _maxDeliver,
                    _filterSubject,
                    _replayPolicy,
                    _sampleFrequency,
                    _rateLimit,
                    _deliverSubject,
                    _maxAckPending
                );
            }
        }
    }
}
