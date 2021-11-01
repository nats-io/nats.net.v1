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
using static NATS.Client.Internals.JsonUtils;

namespace NATS.Client.JetStream
{
    public sealed class ConsumerConfiguration : JsonSerializable
    {
        public static readonly Duration MinAckWait = Duration.One;
        public static readonly Duration MinDefaultIdleHeartbeat = Duration.Zero;
        public static readonly Duration DefaultAckWait = Duration.OfSeconds(30);

        internal DeliverPolicy? _DeliverPolicy;
        internal AckPolicy? _AckPolicy;
        internal ReplayPolicy? _ReplayPolicy;
        internal DateTime? _StartTime;
        internal ulong? _StartSeq;
        internal long? _MaxDeliver;
        internal long? _RateLimit;
        internal long? _MaxAckPending;
        internal long? _MaxPullWaiting;

        public DeliverPolicy DeliverPolicy => _DeliverPolicy ?? DeliverPolicy.All;
        public AckPolicy AckPolicy => _AckPolicy ?? AckPolicy.Explicit;
        public ReplayPolicy ReplayPolicy => _ReplayPolicy ?? ReplayPolicy.Instant;
        public string Description { get; }
        public string Durable { get; }
        public string DeliverSubject { get; }
        public string DeliverGroup { get; }
        public string FilterSubject { get; }
        public string SampleFrequency { get; }
        public DateTime StartTime => _StartTime ?? DateTime.MinValue;
        public Duration AckWait { get; }
        public Duration IdleHeartbeat { get; }
        public bool FlowControl { get; }
        public bool HeadersOnly { get; }
        public ulong StartSeq => _StartSeq ?? CcNumeric.StartSeq.InitialUlong();
        public long MaxDeliver => _MaxDeliver ?? CcNumeric.MaxDeliver.Initial();
        public long RateLimit => _RateLimit ?? CcNumeric.RateLimit.Initial();
        public long MaxAckPending => _MaxAckPending ?? CcNumeric.MaxAckPending.Initial();
        public long MaxPullWaiting => _MaxPullWaiting ?? CcNumeric.MaxPullWaiting.Initial();

        internal ConsumerConfiguration(string json) : this(JSON.Parse(json))
        {
        }

        internal ConsumerConfiguration(JSONNode ccNode)
        {
            _DeliverPolicy = ApiEnums.GetDeliverPolicy(ccNode[ApiConstants.DeliverPolicy].Value);
            _AckPolicy = ApiEnums.GetAckPolicy(ccNode[ApiConstants.AckPolicy].Value);
            _ReplayPolicy = ApiEnums.GetReplayPolicy(ccNode[ApiConstants.ReplayPolicy]);

            Description = ccNode[ApiConstants.Description].Value;
            Durable = ccNode[ApiConstants.DurableName].Value;
            DeliverSubject = ccNode[ApiConstants.DeliverSubject].Value;
            DeliverGroup = ccNode[ApiConstants.DeliverGroup].Value;
            FilterSubject = ccNode[ApiConstants.FilterSubject].Value;
            SampleFrequency = ccNode[ApiConstants.SampleFreq].Value;
            
            _StartTime = AsDate(ccNode[ApiConstants.OptStartTime]);
            AckWait = AsDuration(ccNode, ApiConstants.AckWait, DefaultAckWait);
            IdleHeartbeat = AsDuration(ccNode, ApiConstants.IdleHeartbeat, MinDefaultIdleHeartbeat);
            FlowControl = ccNode[ApiConstants.FlowControl].AsBool;
            HeadersOnly = ccNode[ApiConstants.HeadersOnly].AsBool;

            _StartSeq = CcNumeric.StartSeq.InitialUlong(ccNode[ApiConstants.OptStartSeq].AsUlong);
            _MaxDeliver = CcNumeric.MaxDeliver.Initial(ccNode[ApiConstants.MaxDeliver].AsLong);
            _RateLimit = CcNumeric.RateLimit.Initial(ccNode[ApiConstants.RateLimitBps].AsLong);
            _MaxAckPending = CcNumeric.MaxAckPending.Initial(ccNode[ApiConstants.MaxAckPending].AsLong);
            _MaxPullWaiting = CcNumeric.MaxPullWaiting.Initial(ccNode[ApiConstants.MaxWaiting].AsLong);
        }

        private ConsumerConfiguration(ConsumerConfigurationBuilder builder)
        {
            _DeliverPolicy = builder._deliverPolicy;
            _AckPolicy = builder._ackPolicy;
            _ReplayPolicy = builder._replayPolicy;

            Description = builder._description;
            Durable = builder._durable;
            DeliverSubject = builder._deliverSubject;
            DeliverGroup = builder._deliverGroup;
            FilterSubject = builder._filterSubject;
            SampleFrequency = builder._sampleFrequency;

            _StartTime = builder._startTime;
            AckWait = builder._ackWait;
            IdleHeartbeat = builder._idleHeartbeat;
            FlowControl = builder._flowControl;
            HeadersOnly = builder._headersOnly;

            _StartSeq = builder._startSeq;
            _MaxDeliver = builder._maxDeliver;
            _RateLimit = builder._rateLimit;
            _MaxAckPending = builder._maxAckPending;
            _MaxPullWaiting = builder._maxPullWaiting;
        }

        internal override JSONNode ToJsonNode()
        {
            JSONObject o = new JSONObject();

            AddField(o, ApiConstants.Description, Description);
            AddField(o, ApiConstants.DurableName, Durable);
            AddField(o, ApiConstants.DeliverPolicy, DeliverPolicy.GetString());
            AddField(o, ApiConstants.DeliverSubject, DeliverSubject);
            AddField(o, ApiConstants.DeliverGroup, DeliverGroup);
            AddField(o, ApiConstants.OptStartSeq, StartSeq);
            AddField(o, ApiConstants.OptStartTime, JsonUtils.ToString(StartTime));
            AddField(o, ApiConstants.AckPolicy, AckPolicy.GetString());
            AddField(o, ApiConstants.AckWait, AckWait.Nanos);
            AddField(o, ApiConstants.MaxDeliver, MaxDeliver);
            AddField(o, ApiConstants.FilterSubject, FilterSubject);
            AddField(o, ApiConstants.ReplayPolicy, ReplayPolicy.GetString());
            AddField(o, ApiConstants.SampleFreq, SampleFrequency);
            AddField(o, ApiConstants.RateLimitBps, RateLimit);
            AddField(o, ApiConstants.MaxAckPending, MaxAckPending);
            AddField(o, ApiConstants.IdleHeartbeat, IdleHeartbeat.Nanos);
            AddField(o, ApiConstants.FlowControl, FlowControl);
            AddField(o, ApiConstants.MaxWaiting, MaxPullWaiting);
            AddField(o, ApiConstants.HeadersOnly, HeadersOnly);

            return o;
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
            internal DeliverPolicy _deliverPolicy = DeliverPolicy.All;
            internal AckPolicy _ackPolicy = AckPolicy.Explicit;
            internal ReplayPolicy _replayPolicy = ReplayPolicy.Instant;
            internal string _description;
            internal string _durable;
            internal string _deliverSubject;
            internal string _deliverGroup;
            internal DateTime _startTime; 
            internal Duration _ackWait = Duration.OfSeconds(30);
            internal string _filterSubject;
            internal string _sampleFrequency;
            internal Duration _idleHeartbeat = Duration.Zero;
            internal bool _flowControl;
            internal bool _headersOnly;

            internal ulong? _startSeq = null;
            internal long? _maxDeliver = null;
            internal long? _rateLimit = null;
            internal long? _maxAckPending = null;
            internal long? _maxPullWaiting = null;

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
            public ConsumerConfigurationBuilder WithStartSequence(ulong? sequence)
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
            public ConsumerConfigurationBuilder WithMaxDeliver(long? maxDeliver)
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
            public ConsumerConfigurationBuilder WithRateLimit(int? msgsPerSecond)
            {
                _rateLimit = msgsPerSecond;
                return this;
            }

            /// <summary>
            /// Sets the maximum ack pending.
            /// </summary>
            /// <param name="maxAckPending">maximum pending acknowledgements.</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithMaxAckPending(long? maxAckPending)
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
            public ConsumerConfigurationBuilder WithMaxPullWaiting(long? maxPullWaiting)
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
                return new ConsumerConfiguration(this);
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
    
    internal class CcNumeric
    {
        private string err;
        private long min;
        private long initial;
        private long server;

        public CcNumeric(string err, long min, long initial, long server)
        {
            this.err = err;
            this.min = min;
            this.initial = initial;
            this.server = server;
        }

        internal long Initial()                => initial;
        internal long Initial(long val)        => val < min ? initial : val;
        internal long Comparable(long val)     => val < min || val == server ? initial : val;

        internal ulong InitialUlong()          => (ulong)initial;
        internal ulong InitialUlong(ulong val) => val < (ulong)min ? (ulong)initial : val;
        internal ulong Comparable(ulong val)   => val < (ulong)min || val == (ulong)server ? (ulong)initial : val;

        public bool NotEquals(long val1, long val2) {
            return Comparable(val1) != Comparable(val2);
        }

        public bool NotEquals(ulong val1, ulong val2) {
            return Comparable(val1) != Comparable(val2);
        }

        internal static readonly CcNumeric StartSeq = new CcNumeric("Start Sequence", 1, 0, 0);
        internal static readonly CcNumeric MaxDeliver = new CcNumeric("Max Deliver", 1, -1, -1);
        internal static readonly CcNumeric RateLimit = new CcNumeric("Rate Limit", 1, -1, -1);
        internal static readonly CcNumeric MaxAckPending = new CcNumeric("Max Ack Pending", 0, 0, 20000L);
        internal static readonly CcNumeric MaxPullWaiting = new CcNumeric("Max Pull Waiting", 0, 0, 512);
    }
}
