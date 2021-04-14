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

using System.Collections.Generic;
using System.Text;
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.Api
{
    public sealed class StreamConfiguration
    {
        public string Name { get; }
        public List<string> Subjects { get; }
        public RetentionPolicy RetentionPolicy { get; }
        public long MaxConsumers { get; }
        public long MaxMsgs { get; }
        public long MaxBytes { get; }
        public Duration MaxAge { get; }
        public long MaxMsgSize { get; }
        public StorageType StorageType { get; }
        public int Replicas { get; }
        public bool NoAck { get; }
        public string TemplateOwner { get; }
        public DiscardPolicy DiscardPolicy { get; }
        public Duration DuplicateWindow { get; }
        public Placement Placement { get; }
        public Mirror Mirror { get; }
        public List<Source> Sources { get; }

        internal StreamConfiguration(Msg msg) : this(Encoding.UTF8.GetString(msg.Data)) { }

        internal StreamConfiguration(string json) : this(JSON.Parse(json)) { }
        
        internal StreamConfiguration(JSONNode scNode)
        {
            RetentionPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConsts.DELIVER_POLICY].Value,RetentionPolicy.Limits);
            StorageType = ApiEnums.GetValueOrDefault(scNode[ApiConsts.STORAGE].Value,StorageType.File);
            DiscardPolicy = ApiEnums.GetValueOrDefault(scNode[ApiConsts.DISCARD].Value,DiscardPolicy.Old);
            Name = scNode[ApiConsts.NAME].Value;
            Subjects = JsonUtils.StringList(scNode, ApiConsts.SUBJECTS);
            MaxConsumers = scNode[ApiConsts.MAX_CONSUMERS].AsLong;
            MaxMsgs = scNode[ApiConsts.MAX_MSGS].AsLong;
            MaxBytes = scNode[ApiConsts.MAX_BYTES].AsLong;
            MaxAge = Duration.OfNanos(scNode[ApiConsts.MAX_AGE].AsLong);
            MaxMsgSize = scNode[ApiConsts.MAX_MSG_SIZE].AsLong;
            Replicas = scNode[ApiConsts.NUM_REPLICAS].AsInt;
            NoAck = scNode[ApiConsts.NO_ACK].AsBool;
            TemplateOwner = scNode[ApiConsts.TEMPLATE_OWNER].Value;
            DuplicateWindow = Duration.OfNanos(scNode[ApiConsts.DUPLICATE_WINDOW].AsLong);
            Placement = Placement.OptionalInstance(scNode[ApiConsts.PLACEMENT]);
        }
    }
}
