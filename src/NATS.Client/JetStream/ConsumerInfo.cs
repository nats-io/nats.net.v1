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

namespace NATS.Client.JetStream
{
    public sealed class ConsumerInfo
    {
        public string Stream { get; }
        public string Name { get; }
        public ConsumerConfiguration Configuration { get; }
        public DateTime Created { get; }
        public SequencePair Delivered { get; }
        public SequencePair AckFloor { get; }
        public long NumPending { get; }
        public long NumWaiting { get; }
        public long NumAckPending { get; }
        public long NumRedelivered { get; }

        internal ConsumerInfo(Msg msg) : this(Encoding.UTF8.GetString(msg.Data)) { }

        internal ConsumerInfo(JSONNode jsonNode)
        {
            Stream = jsonNode[ApiConstants.StreamName].Value;
            Configuration = new ConsumerConfiguration(jsonNode[ApiConstants.Config]);
            Name = jsonNode[ApiConstants.Name].Value;
            Created = JsonUtils.AsDate(jsonNode[ApiConstants.Created]);
            Delivered = new SequencePair(jsonNode[ApiConstants.Delivered]);
            AckFloor = new SequencePair(jsonNode[ApiConstants.AckFloor]);
            NumPending = jsonNode[ApiConstants.NumPending].AsLong;
            NumWaiting = jsonNode[ApiConstants.NumWaiting].AsLong;
            NumAckPending = jsonNode[ApiConstants.NumAckPending].AsLong;
            NumRedelivered = jsonNode[ApiConstants.NumRedelivered].AsLong;
        }

        public ConsumerInfo(string json) : this(JSON.Parse(json)) { }
    }
}
