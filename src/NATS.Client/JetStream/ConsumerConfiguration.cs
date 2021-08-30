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
    public sealed class ConsumerConfiguration : JsonSerializable
    {
        private static readonly Duration MinAckWait = Duration.One;
        private static readonly Duration MinDefaultIdleHeartbeat = Duration.Zero;
        private static readonly Duration DefaultAckWait = Duration.OfSeconds(30);

        public DeliverPolicy DeliverPolicy { get; }
        public AckPolicy AckPolicy { get; }
        public ReplayPolicy ReplayPolicy { get; }
        public string Description { get; }
        public string Durable { get; }
        public string DeliverSubject { get; }
        public string DeliverGroup { get; }
        public ulong StartSeq { get; }
        public DateTime StartTime { get; }
        public Duration AckWait { get; }
        public long MaxDeliver { get; }
        public string FilterSubject { get; }
        public string SampleFrequency { get; }
        public long RateLimit { get; }
        public long MaxAckPending { get; }
        public Duration IdleHeartbeat { get; }
        public bool FlowControl { get; }
        public long MaxPullWaiting { get; }

        internal ConsumerConfiguration(string json) : this(JSON.Parse(json)) {}

        internal ConsumerConfiguration(JSONNode ccNode)
        {
            DeliverPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConstants.DeliverPolicy].Value, DeliverPolicy.All);
            AckPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConstants.AckPolicy].Value, AckPolicy.Explicit);
            ReplayPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConstants.ReplayPolicy], ReplayPolicy.Instant);
            Description = ccNode[ApiConstants.Description].Value;
            Durable = ccNode[ApiConstants.DurableName].Value;
            DeliverSubject = ccNode[ApiConstants.DeliverSubject].Value;
            DeliverGroup = ccNode[ApiConstants.DeliverGroup].Value;
            StartSeq = ccNode[ApiConstants.OptStartSeq].AsUlong;
            StartTime = JsonUtils.AsDate(ccNode[ApiConstants.OptStartTime]);
            AckWait = JsonUtils.AsDuration(ccNode, ApiConstants.AckWait, DefaultAckWait);
            MaxDeliver = JsonUtils.AsLongOrMinus1(ccNode, ApiConstants.MaxDeliver);
            FilterSubject = ccNode[ApiConstants.FilterSubject].Value;
            SampleFrequency = ccNode[ApiConstants.SampleFreq].Value;
            RateLimit = ccNode[ApiConstants.RateLimitBps].AsLong;
            MaxAckPending = ccNode[ApiConstants.MaxAckPending].AsLong;
            IdleHeartbeat = JsonUtils.AsDuration(ccNode, ApiConstants.IdleHeartbeat, MinDefaultIdleHeartbeat);
            FlowControl = ccNode[ApiConstants.FlowControl].AsBool;
            MaxPullWaiting = JsonUtils.AsLongOrMinus1(ccNode, ApiConstants.MaxWaiting);
        }

        internal ConsumerConfiguration(string description, string durable, DeliverPolicy deliverPolicy, ulong startSeq, DateTime startTime,
            AckPolicy ackPolicy, Duration ackWait, long maxDeliver, string filterSubject, ReplayPolicy replayPolicy,
            string sampleFrequency, long rateLimit, string deliverSubject, string deliverGroup, long maxAckPending, 
            Duration idleHeartbeat, bool flowControl, long maxPullWaiting)
        {
            Description = description;
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
            DeliverGroup = deliverGroup;
            MaxAckPending = maxAckPending;
            IdleHeartbeat = idleHeartbeat;
            FlowControl = flowControl;
            MaxPullWaiting = maxPullWaiting;
        }

        internal override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Description] = Description,
                [ApiConstants.DurableName] = Durable,
                [ApiConstants.DeliverPolicy] = DeliverPolicy.GetString(),
                [ApiConstants.DeliverSubject] = DeliverSubject,
                [ApiConstants.DeliverGroup] = DeliverGroup,
                [ApiConstants.OptStartSeq] = StartSeq,
                [ApiConstants.OptStartTime] = JsonUtils.ToString(StartTime),
                [ApiConstants.AckPolicy] = AckPolicy.GetString(),
                [ApiConstants.AckWait] = AckWait.Nanos,
                [ApiConstants.MaxDeliver] = MaxDeliver,
                [ApiConstants.FilterSubject] = FilterSubject,
                [ApiConstants.ReplayPolicy] = ReplayPolicy.GetString(),
                [ApiConstants.SampleFreq] = SampleFrequency,
                [ApiConstants.RateLimitBps] = RateLimit,
                [ApiConstants.MaxAckPending] = MaxAckPending,
                [ApiConstants.IdleHeartbeat] = IdleHeartbeat.Nanos,
                [ApiConstants.FlowControl] = FlowControl,
                [ApiConstants.MaxWaiting] = MaxPullWaiting
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
            private DeliverPolicy _deliverPolicy = DeliverPolicy.All;
            private AckPolicy _ackPolicy = AckPolicy.Explicit;
            private ReplayPolicy _replayPolicy = ReplayPolicy.Instant;
            private string _description;
            private string _durable;
            private string _deliverSubject;
            private string _deliverGroup;
            private ulong _startSeq;
            private DateTime _startTime; 
            private Duration _ackWait = Duration.OfSeconds(30);
            private long _maxDeliver;
            private string _filterSubject;
            private string _sampleFrequency;
            private long _rateLimit;
            private long _maxAckPending;
            private Duration _idleHeartbeat = Duration.Zero;
            private bool _flowControl;
            private long _maxPullWaiting;

            public string Durable => _durable;
            public string DeliverSubject => _deliverSubject;
            public string DeliverGroup => _deliverGroup;
            public string FilterSubject => _filterSubject;
            public long MaxAckPending => _maxAckPending;
            public AckPolicy AcknowledgementPolicy => _ackPolicy;

            public ConsumerConfigurationBuilder() {}

            public ConsumerConfigurationBuilder(ConsumerConfiguration cc)
            {
                if (cc == null) return;
                _description = cc.Description;
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
                _deliverGroup = cc.DeliverGroup;
                _maxAckPending = cc.MaxAckPending;
                _idleHeartbeat = cc.IdleHeartbeat;
                _flowControl = cc.FlowControl;
                _maxPullWaiting = cc.MaxPullWaiting;
            }

            /// <summary>
            /// Sets the description.
            /// </summary>
            /// <param name="description">the description</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithDescription(string description)
            {
                _description = description;
                return this;
            }

            /// <summary>
            /// Sets the name of the durable subscription.
            /// </summary>
            /// <param name="durable">name of the durable subscription.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithDurable(string durable)
            {
                _durable = durable;
                return this;
            }

            /// <summary>
            /// Sets the delivery policy of the ConsumerConfiguration.
            /// </summary>
            /// <param name="policy">the delivery policy.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithDeliverPolicy(DeliverPolicy? policy)
            {
                _deliverPolicy = policy ?? DeliverPolicy.All;
                return this;
            }

            /// <summary>
            /// Sets the subject to deliver messages to.
            /// </summary>
            /// <param name="subject">the delivery subject.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithDeliverSubject(string subject)
            {
                _deliverSubject = subject;
                return this;
            }

            /// <summary>
            /// Sets the group to deliver messages to.
            /// </summary>
            /// <param name="group">the delivery group.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithDeliverGroup(string group)
            {
                _deliverGroup = group;
                return this;
            }

            /// <summary>
            /// Sets the start sequence of the ConsumerConfiguration.
            /// </summary>
            /// <param name="sequence">the start sequence</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithStartSequence(ulong sequence)
            {
                _startSeq = sequence;
                return this;
            }

            /// <summary>
            /// Sets the start time of the ConsumerConfiguration.
            /// </summary>
            /// <param name="startTime">the start time</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithStartTime(DateTime startTime)
            {
                _startTime = startTime;
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement policy of the ConsumerConfiguration.
            /// </summary>
            /// <param name="policy">the acknowledgement policy.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithAckPolicy(AckPolicy? policy)
            {
                _ackPolicy = policy ?? AckPolicy.Explicit;
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement wait duration of the ConsumerConfiguration.
            /// </summary>
            /// <param name="timeout">the wait timeout as a duration</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithAckWait(Duration timeout)
            {
                _ackWait = Validator.EnsureNotNullAndNotLessThanMin(timeout, MinAckWait, DefaultAckWait); 
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement wait duration of the ConsumerConfiguration.
            /// </summary>
            /// <param name="timeoutMillis">the wait timeout as millis</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithAckWait(long timeoutMillis)
            {
                _ackWait = Validator.EnsureDurationNotLessThanMin(timeoutMillis, MinAckWait, DefaultAckWait);
                return this;
            }

            /// <summary>
            /// Sets the maximum delivery amount of the ConsumerConfiguration.
            /// </summary>
            /// <param name="maxDeliver">the maximum delivery amount</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithMaxDeliver(long maxDeliver)
            {
                _maxDeliver = maxDeliver;
                return this;
            }

            /// <summary>
            /// Sets the filter subject of the ConsumerConfiguration.
            /// </summary>
            /// <param name="filterSubject">the filter subject</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithFilterSubject(string filterSubject)
            {
                _filterSubject = filterSubject;
                return this;
            }

            /// <summary>
            /// Sets the replay policy of the ConsumerConfiguration.
            /// </summary>
            /// <param name="policy">the replay policy.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithReplayPolicy(ReplayPolicy? policy)
            {
                _replayPolicy = policy ?? ReplayPolicy.Instant;
                return this;
            }

            /// <summary>
            /// Sets the sample frequency of the ConsumerConfiguration.
            /// </summary>
            /// <param name="frequency">the frequency</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithSampleFrequency(string frequency)
            {
                _sampleFrequency = frequency;
                return this;
            }

            /// <summary>
            /// Set the rate limit of the ConsumerConfiguration.
            /// </summary>
            /// <param name="msgsPerSecond">messages per second to deliver</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithRateLimit(int msgsPerSecond)
            {
                _rateLimit = msgsPerSecond;
                return this;
            }

            /// <summary>
            /// Sets the maximum ack pending.
            /// </summary>
            /// <param name="maxAckPending">maximum pending acknowledgements.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithMaxAckPending(long maxAckPending)
            {
                _maxAckPending = maxAckPending;
                return this;
            }

            /// <summary>
            /// Sets the idle heart beat wait time.
            /// </summary>
            /// <param name="idleHeartbeat">the wait timeout as a Duration</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithIdleHeartbeat(Duration idleHeartbeat)
            {
                _idleHeartbeat = Validator.EnsureNotNullAndNotLessThanMin(idleHeartbeat, MinDefaultIdleHeartbeat, MinDefaultIdleHeartbeat); 
                return this;
            }

            /// <summary>
            /// Sets the idle heart beat wait time.
            /// </summary>
            /// <param name="idleHeartbeatMillis">the wait timeout as a Duration</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithIdleHeartbeat(long idleHeartbeatMillis)
            {
                _idleHeartbeat = Validator.EnsureDurationNotLessThanMin(idleHeartbeatMillis, MinDefaultIdleHeartbeat, MinDefaultIdleHeartbeat); 
                return this;
            }

            /// <summary>
            /// Sets the flow control mode of the ConsumerConfiguration
            /// </summary>
            /// <param name="flowControl">true to enable flow control.</param>
            /// <returns>The ConsumerConfiguration</returns>
            public ConsumerConfigurationBuilder WithFlowControl(bool flowControl) {
                _flowControl = flowControl;
                return this;
            }

            /// <summary>
            /// Sets the maximum pull waiting.
            /// </summary>
            /// <param name="maxPullWaiting">the maximum delivery amount</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithMaxPullWaiting(long maxPullWaiting)
            {
                _maxPullWaiting = maxPullWaiting;
                return this;
            }

            /// <summary>
            /// Builds the ConsumerConfiguration
            /// </summary>
            /// <returns>The ConsumerConfiguration</returns>
            public ConsumerConfiguration Build()
            {
                return new ConsumerConfiguration(
                    _description,
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
                    _deliverGroup,
                    _maxAckPending,
                    _idleHeartbeat,
                    _flowControl,
                    _maxPullWaiting
                );
            }
        }
    }
}
