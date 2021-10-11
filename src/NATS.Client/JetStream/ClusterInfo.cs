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
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class ClusterInfo
    {
        public string Name { get; }
        public string Leader { get; }
        public List<Replica> Replicas { get; }

        internal static ClusterInfo OptionalInstance(JSONNode clusterNode)
        {
            return clusterNode == null || clusterNode.Count == 0 ? null : new ClusterInfo(clusterNode);
        }

        private ClusterInfo(JSONNode clusterNode)
        {
            Name = clusterNode[ApiConstants.Name].Value;
            Leader = clusterNode[ApiConstants.Leader].Value;
            Replicas = Replica.OptionalListOf(clusterNode[ApiConstants.Replicas]);
        }
        
        public override string ToString()
        {
            return "ClusterInfo{" +
                   "Name='" + Name + '\'' +
                   ", Leader='" + Leader + '\'' +
                   ", Replicas=" + Replicas +
                   '}';
        }
    }
}
