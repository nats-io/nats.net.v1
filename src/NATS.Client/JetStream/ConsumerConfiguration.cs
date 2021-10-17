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
    internal class CcNumeric
    {
        private string err;
        private long min;
        private long normal;
        private long srvrDflt;

        public CcNumeric(string err, long min, long normal, long srvrDflt)
        {
            this.err = err;
            this.min = min;
            this.normal = normal;
            this.srvrDflt = srvrDflt;
        }

        internal long Normalize(long val)
        {
            return val < min ? -1 : val;
        }

        internal ulong Normalize(ulong val)
        {
            return val <= (ulong)min ? (ulong)normal : val;
        }

        internal long Comparable(long val) {
            return val <= min || val == srvrDflt ? srvrDflt : val;
        }

        internal ulong Comparable(ulong val) {
            return val <= (ulong)min || val == (ulong)srvrDflt ? (ulong)srvrDflt : val;
        }

        internal string GetErr() {
            return err;
        }

        internal static readonly CcNumeric StartSeq = new CcNumeric("Start Sequence", 1, 0, 0);
        internal static readonly CcNumeric MaxDeliver = new CcNumeric("Max Deliver", 1, -1, -1);
        internal static readonly CcNumeric RateLimit = new CcNumeric("Rate Limit", 1, -1, -1);
        internal static readonly CcNumeric MaxAckPending = new CcNumeric("Max Ack Pending", 0, 0, 20000L);
        internal static readonly CcNumeric MaxPullWaiting = new CcNumeric("Max Pull Waiting", 1, 0, 512);
    }

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
        public DateTime StartTime { get; }
        public Duration AckWait { get; }
        public string FilterSubject { get; }
        public string SampleFrequency { get; }
        public Duration IdleHeartbeat { get; }
        public bool FlowControl { get; }
        public bool HeadersOnly { get; }

        public ulong StartSeq { get; }
        public long MaxDeliver { get; }
        public long RateLimit { get; }
        public long MaxAckPending { get; }
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
            StartTime = JsonUtils.AsDate(ccNode[ApiConstants.OptStartTime]);
            AckWait = JsonUtils.AsDuration(ccNode, ApiConstants.AckWait, DefaultAckWait);
            FilterSubject = ccNode[ApiConstants.FilterSubject].Value;
            SampleFrequency = ccNode[ApiConstants.SampleFreq].Value;
            IdleHeartbeat = JsonUtils.AsDuration(ccNode, ApiConstants.IdleHeartbeat, MinDefaultIdleHeartbeat);
            FlowControl = ccNode[ApiConstants.FlowControl].AsBool;
            HeadersOnly = ccNode[ApiConstants.HeadersOnly].AsBool;

            StartSeq = CcNumeric.StartSeq.Normalize(ccNode[ApiConstants.OptStartSeq].AsUlong);
            MaxDeliver = CcNumeric.MaxDeliver.Normalize(JsonUtils.AsLongOrMinus1(ccNode, ApiConstants.MaxDeliver));
            RateLimit = CcNumeric.RateLimit.Normalize(ccNode[ApiConstants.RateLimitBps].AsLong);
            MaxAckPending = CcNumeric.MaxAckPending.Normalize(ccNode[ApiConstants.MaxAckPending].AsLong);
            MaxPullWaiting = CcNumeric.MaxPullWaiting.Normalize(JsonUtils.AsLongOrMinus1(ccNode, ApiConstants.MaxWaiting));
        }

        private ConsumerConfiguration(string description, string durable, DeliverPolicy deliverPolicy, ulong startSeq, DateTime startTime,
            AckPolicy ackPolicy, Duration ackWait, long maxDeliver, string filterSubject, ReplayPolicy replayPolicy,
            string sampleFrequency, long rateLimit, string deliverSubject, string deliverGroup, long maxAckPending, 
            Duration idleHeartbeat, bool flowControl, long maxPullWaiting, bool headersOnly)
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
            HeadersOnly = headersOnly;
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
                [ApiConstants.MaxWaiting] = MaxPullWaiting,
                [ApiConstants.HeadersOnly] = HeadersOnly
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
            private bool _headersOnly;

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
                _headersOnly = cc.HeadersOnly;
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
            /// <param name="deliverSubject">the delivery subject.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithDeliverSubject(string deliverSubject)
            {
                _deliverSubject = deliverSubject;
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
            /// <param name="idleHeartbeatMillis">the wait timeout as milliseconds</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithIdleHeartbeat(long idleHeartbeatMillis)
            {
                _idleHeartbeat = Validator.EnsureDurationNotLessThanMin(idleHeartbeatMillis, MinDefaultIdleHeartbeat, MinDefaultIdleHeartbeat); 
                return this;
            }

            /// <summary>
            /// Set the flow control on and set the idle heartbeat
            /// </summary>
            /// <param name="idleHeartbeat">the idle heart beat as a Duration</param>
            /// <returns>The ConsumerConfiguration</returns>
            public ConsumerConfigurationBuilder WithFlowControl(Duration idleHeartbeat) {
                _flowControl = true;
                return WithIdleHeartbeat(idleHeartbeat);
            }

            /// <summary>
            /// Set the flow control on and set the idle heartbeat
            /// </summary>
            /// <param name="idleHeartbeatMillis">the idle heart beat as milliseconds</param>
            /// <returns>The ConsumerConfiguration</returns>
            public ConsumerConfigurationBuilder WithFlowControl(long idleHeartbeatMillis) {
                _flowControl = true;
                return WithIdleHeartbeat(idleHeartbeatMillis);
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
            /// Sets the headers only flag
            /// </summary>
            /// <param name="headersOnly">true to enable flow control.</param>
            /// <returns>The ConsumerConfiguration</returns>
            public ConsumerConfigurationBuilder WithHeadersOnly(bool headersOnly) {
                _headersOnly = headersOnly;
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
                    _maxPullWaiting,
                    _headersOnly
                );
            }

            /// <summary>
            /// Builds the PushSubscribeOptions with this configuration
            /// </summary>
            /// <returns>The PushSubscribeOptions</returns>
            public PushSubscribeOptions BuildPushSubscribeOptions()
            {
                return PushSubscribeOptions.Builder().WithConfiguration(Build()).Build();
            }

            /// <summary>
            /// Builds the PullSubscribeOptions with this configuration
            /// </summary>
            /// <returns>The PullSubscribeOptions</returns>
            public PullSubscribeOptions BuildPullSubscribeOptions()
            {
                return PullSubscribeOptions.Builder().WithConfiguration(Build()).Build();
            }
        }
    }
}
