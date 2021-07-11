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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class StreamInfo : ApiResponse
    {
        public DateTime Created { get; private set; }
        public StreamConfiguration Config { get; private set; }
        public StreamState State { get; private set; }
        public ClusterInfo ClusterInfo { get; private set; }
        public MirrorInfo MirrorInfo { get; private set; }
        public List<SourceInfo> SourceInfos { get; private set; }

        internal StreamInfo(Msg msg, bool throwOnError) : base(msg, throwOnError)
        {
            Init(JsonNode);
        }

        public StreamInfo(string json, bool throwOnError) : base(json, throwOnError)
        {
            Init(JsonNode);
        }

        internal StreamInfo(JSONNode siNode)
        {
            Init(siNode);
        }

        private void Init(JSONNode siNode)
        {
            Created = JsonUtils.AsDate(siNode[ApiConstants.Created]);
            Config = new StreamConfiguration(siNode[ApiConstants.Config]);
            State = StreamState.OptionalInstance(siNode[ApiConstants.State]);
            ClusterInfo = ClusterInfo.OptionalInstance(siNode[ApiConstants.Cluster]);
            MirrorInfo = MirrorInfo.OptionalInstance(siNode[ApiConstants.Mirror]);
            SourceInfos = SourceInfo.OptionalListOf(siNode[ApiConstants.Sources]);
        }
    }
}
