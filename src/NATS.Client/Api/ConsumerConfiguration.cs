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
using System.Text;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.Api
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

        internal ConsumerConfiguration(Msg msg) : this(Encoding.UTF8.GetString(msg.Data)) { }

        internal ConsumerConfiguration(string json) : this(JSON.Parse(json)) {}

        internal ConsumerConfiguration(JSONNode ccNode)
        {
            DeliverPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConsts.DELIVER_POLICY].Value,DeliverPolicy.All);
            AckPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConsts.ACK_POLICY].Value, AckPolicy.Explicit);
            ReplayPolicy = ApiEnums.GetValueOrDefault(ccNode[ApiConsts.REPLAY_POLICY],ReplayPolicy.Instant);
            Durable = ccNode[ApiConsts.DURABLE_NAME].Value;
            DeliverSubject = ccNode[ApiConsts.DELIVER_SUBJECT].Value;
            StartSeq = ccNode[ApiConsts.OPT_START_SEQ].AsLong;
            StartTime = JsonUtils.AsDate(ccNode[ApiConsts.OPT_START_TIME]);
            AckWait = Duration.OfNanos(ccNode[ApiConsts.ACK_WAIT]);
            MaxDeliver = JsonUtils.AsLongOrMinus1(ccNode, ApiConsts.MAX_DELIVER);
            FilterSubject = ccNode[ApiConsts.FILTER_SUBJECT].Value;
            SampleFrequency = ccNode[ApiConsts.SAMPLE_FREQ].Value;
            RateLimit = ccNode[ApiConsts.RATE_LIMIT].AsLong;
            MaxAckPending = ccNode[ApiConsts.MAX_ACK_PENDING].AsLong;
        }
    }
}
