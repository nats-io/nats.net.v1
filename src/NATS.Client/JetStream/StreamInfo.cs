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
using System.Collections.Generic;
using System.Text;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class StreamInfo
    {
        public DateTime Created { get; }
        public StreamConfiguration Config { get; }
        public StreamState State { get; }
        public ClusterInfo ClusterInfo { get; }
        public MirrorInfo MirrorInfo { get; }
        public List<SourceInfo> SourceInfos { get; }

        public StreamInfo(Msg msg) : this(Encoding.UTF8.GetString(msg.Data)) { }

        internal StreamInfo(JSONNode jsonNode)
        {
            Created = JsonUtils.AsDate(jsonNode[ApiConstants.Created]);
            Config = new StreamConfiguration(jsonNode[ApiConstants.Config]);
            State = StreamState.OptionalInstance(jsonNode[ApiConstants.State]);
            ClusterInfo = ClusterInfo.OptionalInstance(jsonNode[ApiConstants.Cluster]);
            MirrorInfo = MirrorInfo.OptionalInstance(jsonNode[ApiConstants.Mirror]);
            SourceInfos = SourceInfo.OptionalListOf(jsonNode[ApiConstants.Sources]);
        }

        public StreamInfo(string json) : this(JSON.Parse(json)) { }
    }
}
