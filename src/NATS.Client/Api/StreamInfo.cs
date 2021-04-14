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

namespace NATS.Client.Api
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

        public StreamInfo(string json)
        {
            var streamInfoNode = JSON.Parse(json);
            Created = JsonUtils.AsDate(streamInfoNode[ApiConsts.CREATED]);
            Config = new StreamConfiguration(streamInfoNode[ApiConsts.CONFIG]);
            State = StreamState.OptionalInstance(streamInfoNode[ApiConsts.STATE]);
            ClusterInfo = ClusterInfo.OptionalInstance(streamInfoNode[ApiConsts.CLUSTER]);
            MirrorInfo = MirrorInfo.OptionalInstance(streamInfoNode[ApiConsts.MIRROR]);
            SourceInfos = SourceInfo.OptionalListOf(streamInfoNode[ApiConsts.SOURCES]);
        }
    }
}
