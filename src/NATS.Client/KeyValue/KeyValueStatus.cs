﻿// Copyright 2021 The NATS Authors
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

using NATS.Client.JetStream;

namespace NATS.Client.KeyValue
{
    public class KeyValueStatus
    {
        public StreamInfo BackingStreamInfo { get; }
        public KeyValueConfiguration Config { get; }

        public KeyValueStatus(StreamInfo si) {
            BackingStreamInfo = si;
            Config = new KeyValueConfiguration(si.Config);
        }

        public string BucketName => Config.BucketName;

        public ulong EntryCount => BackingStreamInfo.State.Messages;

        public long MaxHistoryPerKey => Config.MaxHistoryPerKey;

        public long Ttl => Config.Ttl;

        public string BackingStore => "JetStream";
    }
}