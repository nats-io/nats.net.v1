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
using NATS.Client.Internals.SimpleJSON;
using static NATS.Client.Internals.JsonUtils;

namespace NATS.Client.JetStream
{
    public sealed class ConsumerInfo : ApiResponse
    {
        public string Stream { get; private set; }
        public string Name { get; private set; }
        public ConsumerConfiguration ConsumerConfiguration { get; private set; }
        public DateTime Created { get; private set; }
        public SequenceInfo Delivered { get; private set; }
        public SequenceInfo AckFloor { get; private set; }
        public ulong NumPending { get; private set; }
        public long NumWaiting { get; private set; }
        public long NumAckPending { get; private set; }
        public long NumRedelivered { get; private set; }
        public ClusterInfo ClusterInfo { get; private set; }
        public bool PushBound { get; private set; }
        public ulong CalculatedPending => NumPending + Delivered.ConsumerSeq;
        public DateTime Timestamp { get; private set; }

        internal ConsumerInfo(Msg msg, bool throwOnError) : base(msg, throwOnError)
        {
            Init(JsonNode);
        }

        internal ConsumerInfo(string json, bool throwOnError) : base(json, throwOnError)
        {
            Init(JsonNode);
        }

        internal ConsumerInfo(JSONNode ciNode)
        {
            Init(ciNode);
        }

        private void Init(JSONNode ciNode)
        {
            Stream = ciNode[ApiConstants.StreamName].Value;
            ConsumerConfiguration = new ConsumerConfiguration(ciNode[ApiConstants.Config]);
            Name = ciNode[ApiConstants.Name].Value;
            Created = AsDate(ciNode[ApiConstants.Created]);
            Delivered = new SequenceInfo(ciNode[ApiConstants.Delivered]);
            AckFloor = new SequenceInfo(ciNode[ApiConstants.AckFloor]);
            NumPending = ciNode[ApiConstants.NumPending].AsUlong;
            NumWaiting = ciNode[ApiConstants.NumWaiting].AsLong;
            NumAckPending = ciNode[ApiConstants.NumAckPending].AsLong;
            NumRedelivered = ciNode[ApiConstants.NumRedelivered].AsLong;
            ClusterInfo = ClusterInfo.OptionalInstance(ciNode[ApiConstants.Cluster]);
            PushBound = ciNode[ApiConstants.PushBound].AsBool;
            Timestamp = AsDate(ciNode[ApiConstants.Timestamp]);
        }

        public override string ToString()
        {
            return "ConsumerInfo{" +
                   "Stream='" + Stream + '\'' +
                   ", Name='" + Name + '\'' +
                   ", NumPending=" + NumPending +
                   ", NumWaiting=" + NumWaiting +
                   ", NumAckPending=" + NumAckPending +
                   ", NumRedelivered=" + NumRedelivered +
                   ", PushBound=" + PushBound +
                   ", Created=" + Created +
                   ", Timestamp=" + Timestamp +
                   ", Delivered=" + Delivered +
                   ", AckFloor=" + AckFloor +
                   ", " + ObjectString("ClusterInfo", ClusterInfo) +
                   ", " + "ConsumerConfiguration" + ConsumerConfiguration.ToJsonString() +
                   '}';
        }
    }
}
