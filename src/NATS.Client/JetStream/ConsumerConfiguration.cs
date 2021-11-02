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
using static NATS.Client.Internals.Validator;

namespace NATS.Client.JetStream
{
    public sealed class ConsumerConfiguration : JsonSerializable
    {
        public static readonly Duration MinAckWait = Duration.One;
        public static readonly Duration MinDefaultIdleHeartbeat = Duration.OfMillis(100);

        private DeliverPolicy? _deliverPolicy;
        private AckPolicy? _ackPolicy;
        private ReplayPolicy? _replayPolicy;
        private ulong? _startSeq;
        private long? _maxDeliver;
        private long? _rateLimit;
        private long? _maxAckPending;
        private long? _maxPullWaiting;
        private bool? _flowControl;
        private bool? _headersOnly;

        public DeliverPolicy DeliverPolicy => _deliverPolicy ?? DeliverPolicy.All;
        public AckPolicy AckPolicy => _ackPolicy ?? AckPolicy.Explicit;
        public ReplayPolicy ReplayPolicy => _replayPolicy ?? ReplayPolicy.Instant;
        public string Description { get; }
        public string Durable { get; }
        public string DeliverSubject { get; }
        public string DeliverGroup { get; }
        public string FilterSubject { get; }
        public string SampleFrequency { get; }
        public DateTime StartTime { get; }
        public Duration AckWait { get; }
        public Duration IdleHeartbeat { get; }
        public ulong StartSeq => _startSeq ?? CcNumeric.StartSeq.InitialUlong();
        public long MaxDeliver => _maxDeliver ?? CcNumeric.MaxDeliver.Initial();
        public long RateLimit => _rateLimit ?? CcNumeric.RateLimit.Initial();
        public long MaxAckPending => _maxAckPending ?? CcNumeric.MaxAckPending.Initial();
        public long MaxPullWaiting => _maxPullWaiting ?? CcNumeric.MaxPullWaiting.Initial();
        public bool FlowControl => _flowControl ?? false;
        public bool HeadersOnly => _headersOnly ?? false;

        internal ConsumerConfiguration(string json) : this(JSON.Parse(json)) {}

        internal ConsumerConfiguration(JSONNode ccNode)
        {
            _deliverPolicy = ApiEnums.GetDeliverPolicy(ccNode[ApiConstants.DeliverPolicy].Value);
            _ackPolicy = ApiEnums.GetAckPolicy(ccNode[ApiConstants.AckPolicy].Value);
            _replayPolicy = ApiEnums.GetReplayPolicy(ccNode[ApiConstants.ReplayPolicy]);

            Description = ccNode[ApiConstants.Description].Value;
            Durable = ccNode[ApiConstants.DurableName].Value;
            DeliverSubject = ccNode[ApiConstants.DeliverSubject].Value;
            DeliverGroup = ccNode[ApiConstants.DeliverGroup].Value;
            FilterSubject = ccNode[ApiConstants.FilterSubject].Value;
            SampleFrequency = ccNode[ApiConstants.SampleFreq].Value;
            
            StartTime = AsDate(ccNode[ApiConstants.OptStartTime]);
            AckWait = AsDuration(ccNode, ApiConstants.AckWait, null);
            IdleHeartbeat = AsDuration(ccNode, ApiConstants.IdleHeartbeat, null);

            _startSeq = CcNumeric.StartSeq.InitialUlong(ccNode[ApiConstants.OptStartSeq].AsUlong);
            _maxDeliver = CcNumeric.MaxDeliver.Initial(ccNode[ApiConstants.MaxDeliver].AsLong);
            _rateLimit = CcNumeric.RateLimit.Initial(ccNode[ApiConstants.RateLimitBps].AsLong);
            _maxAckPending = CcNumeric.MaxAckPending.Initial(ccNode[ApiConstants.MaxAckPending].AsLong);
            _maxPullWaiting = CcNumeric.MaxPullWaiting.Initial(ccNode[ApiConstants.MaxWaiting].AsLong);
            _flowControl = ccNode[ApiConstants.FlowControl].AsBool;
            _headersOnly = ccNode[ApiConstants.HeadersOnly].AsBool;
        }

        private ConsumerConfiguration(ConsumerConfigurationBuilder builder)
        {
            _deliverPolicy = builder._deliverPolicy;
            _ackPolicy = builder._ackPolicy;
            _replayPolicy = builder._replayPolicy;

            Description = builder._description;
            Durable = builder._durable;
            DeliverSubject = builder._deliverSubject;
            DeliverGroup = builder._deliverGroup;
            FilterSubject = builder._filterSubject;
            SampleFrequency = builder._sampleFrequency;

            StartTime = builder._startTime;
            AckWait = builder._ackWait;
            IdleHeartbeat = builder._idleHeartbeat;
            _flowControl = builder._flowControl;
            _headersOnly = builder._headersOnly;

            _startSeq = builder._startSeq;
            _maxDeliver = builder._maxDeliver;
            _rateLimit = builder._rateLimit;
            _maxAckPending = builder._maxAckPending;
            _maxPullWaiting = builder._maxPullWaiting;
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
            AddField(o, ApiConstants.AckWait, AckWait);
            AddField(o, ApiConstants.MaxDeliver, MaxDeliver);
            AddField(o, ApiConstants.FilterSubject, FilterSubject);
            AddField(o, ApiConstants.ReplayPolicy, ReplayPolicy.GetString());
            AddField(o, ApiConstants.SampleFreq, SampleFrequency);
            AddField(o, ApiConstants.RateLimitBps, RateLimit);
            AddField(o, ApiConstants.MaxAckPending, MaxAckPending);
            AddField(o, ApiConstants.IdleHeartbeat, IdleHeartbeat);
            AddField(o, ApiConstants.FlowControl, FlowControl);
            AddField(o, ApiConstants.MaxWaiting, MaxPullWaiting);
            AddField(o, ApiConstants.HeadersOnly, HeadersOnly);

            return o;
        }
        
        internal bool WouldBeChangeTo(ConsumerConfiguration original)
        {
            return (_deliverPolicy.HasValue && _deliverPolicy.Value != original.DeliverPolicy)
                   || (_ackPolicy.HasValue && _ackPolicy.Value != original.AckPolicy)
                   || (_replayPolicy.HasValue && _replayPolicy.Value != original.ReplayPolicy)

                   || _flowControl.HasValue && _flowControl.Value != original.FlowControl
                   || _headersOnly.HasValue && _headersOnly.Value != original.HeadersOnly

                   || CcNumeric.StartSeq.Changed(_startSeq, original.StartSeq)
                   || CcNumeric.MaxDeliver.Changed(_maxDeliver, original.MaxDeliver)
                   || CcNumeric.RateLimit.Changed(_rateLimit, original.RateLimit)
                   || CcNumeric.MaxAckPending.Changed(_maxAckPending, original.MaxAckPending)
                   || CcNumeric.MaxPullWaiting.Changed(_maxPullWaiting, original.MaxPullWaiting)

                   || WouldBeChange(AckWait, original.AckWait)
                   || WouldBeChange(IdleHeartbeat, original.IdleHeartbeat)
                   
                   || WouldBeChange(StartTime, original.StartTime)
                   
                   || WouldBeChange(Description, original.Description)
                   || WouldBeChange(SampleFrequency, original.SampleFrequency)
                   || WouldBeChange(DeliverSubject, original.DeliverSubject)
                   || WouldBeChange(DeliverGroup, original.DeliverGroup)
                   ;

            // do not need to check Durable because the original is retrieved by the durable name
            // do not need to check FilterSubject because it's already validated against the original
        }

        private static bool WouldBeChange(string request, string original)
        {
            string r = EmptyAsNull(request);
            return r != null && !r.Equals(EmptyAsNull(original));
        }

        private static bool WouldBeChange(DateTime request, DateTime original)
        {
            return request != DateTime.MinValue && !request.Equals(original);
        }

        private static bool WouldBeChange(Duration request, Duration original)
        {
            return request != null && !request.Equals(original);
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
            internal DeliverPolicy? _deliverPolicy;
            internal AckPolicy? _ackPolicy;
            internal ReplayPolicy? _replayPolicy;
            
            internal string _description;
            internal string _durable;
            internal string _deliverSubject;
            internal string _deliverGroup;
            internal string _filterSubject;
            internal string _sampleFrequency;
            
            internal DateTime _startTime; 
            internal Duration _ackWait;
            internal Duration _idleHeartbeat;

            internal ulong? _startSeq;
            internal long? _maxDeliver;
            internal long? _rateLimit;
            internal long? _maxAckPending;
            internal long? _maxPullWaiting;
            internal bool? _flowControl;
            internal bool? _headersOnly;

            public ConsumerConfigurationBuilder() {}

            public ConsumerConfigurationBuilder(ConsumerConfiguration cc)
            {
                if (cc == null) return;
                
                _deliverPolicy = cc._deliverPolicy;
                _ackPolicy = cc._ackPolicy;
                _replayPolicy = cc._replayPolicy;

                _description = cc.Description;
                _durable = cc.Durable;
                _deliverSubject = cc.DeliverSubject;
                _deliverGroup = cc.DeliverGroup;
                _filterSubject = cc.FilterSubject;
                _sampleFrequency = cc.SampleFrequency;

                _startTime = cc.StartTime;
                _ackWait = cc.AckWait;
                _idleHeartbeat = cc.IdleHeartbeat;

                _startSeq = cc._startSeq;
                _maxDeliver = cc._maxDeliver;
                _rateLimit = cc._rateLimit;
                _maxAckPending = cc._maxAckPending;
                _maxPullWaiting = cc._maxPullWaiting;
                _flowControl = cc._flowControl;
                _headersOnly = cc._headersOnly;
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
                _ackWait = ValidateDurationNotRequiredNotLessThanMin(timeout, MinAckWait); 
                return this;
            }

            /// <summary>
            /// Sets the acknowledgement wait duration of the ConsumerConfiguration.
            /// </summary>
            /// <param name="timeoutMillis">the wait timeout as millis</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithAckWait(long timeoutMillis)
            {
                _ackWait = ValidateDurationNotRequiredNotLessThanMin(timeoutMillis, MinAckWait);
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
                _idleHeartbeat = ValidateDurationNotRequiredNotLessThanMin(idleHeartbeat, MinDefaultIdleHeartbeat); 
                return this;
            }

            /// <summary>
            /// Sets the idle heart beat wait time.
            /// </summary>
            /// <param name="idleHeartbeatMillis">the wait timeout as milliseconds</param>
            /// <returns>The ConsumerConfigurationBuilder</returns>
            public ConsumerConfigurationBuilder WithIdleHeartbeat(long idleHeartbeatMillis)
            {
                _idleHeartbeat = ValidateDurationNotRequiredNotLessThanMin(idleHeartbeatMillis, MinDefaultIdleHeartbeat); 
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
            public ConsumerConfigurationBuilder WithHeadersOnly(bool? headersOnly) {
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
        private long min;
        private long initial;
        private long server;

        public CcNumeric(long min, long initial, long server)
        {
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

        public bool Changed(long? user, long srvr)
        {
            long u = user.GetValueOrDefault(long.MinValue);
            return u != long.MinValue && Comparable(u) != Comparable(srvr);
        }

        public bool Changed(ulong? user, ulong srvr) {
            ulong u = user.GetValueOrDefault(ulong.MaxValue);
            return u != ulong.MaxValue && Comparable(u) != Comparable(srvr);
        }

        internal static readonly CcNumeric StartSeq = new CcNumeric(1, 0, 0);
        internal static readonly CcNumeric MaxDeliver = new CcNumeric(1, -1, -1);
        internal static readonly CcNumeric RateLimit = new CcNumeric(1, -1, -1);
        internal static readonly CcNumeric MaxAckPending = new CcNumeric(0, 0, 20000L);
        internal static readonly CcNumeric MaxPullWaiting = new CcNumeric(0, 0, 512);
    }
}
