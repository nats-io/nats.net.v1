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

using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public abstract class PeerInfo
    {
        public string Name { get; }
        public bool Current { get; }
        public bool Offline { get; }
        public Duration Active { get; }
        public long Lag { get; }

        internal PeerInfo(JSONNode peerInfoNode) {
            Name = peerInfoNode[ApiConstants.Name].Value;
            Current = peerInfoNode[ApiConstants.Current].AsBool;
            Offline = peerInfoNode[ApiConstants.Offline].AsBool;
            Active = JsonUtils.AsDuration(peerInfoNode, ApiConstants.Active, Duration.Zero);
            Lag = peerInfoNode[ApiConstants.Lag].AsLong;
        }
    }
}
