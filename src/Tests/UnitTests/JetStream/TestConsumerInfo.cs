// Copyright 2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System.Collections.Generic;
using NATS.Client.Internals;
using NATS.Client.JetStream;
using Xunit;

namespace UnitTests.JetStream
{
    public class TestConsumerInfo : TestBase
    {
        [Fact]
        public void JsonIsReadProperly()
        {
            string json = ReadDataFile("ConsumerInfo.json");
            ConsumerInfo ci = new ConsumerInfo(json, false);
            Assert.Equal("foo-stream", ci.Stream);
            Assert.Equal("foo-consumer", ci.Name);
            Assert.Equal(AsDateTime("2020-11-05T19:33:21.163377Z"), ci.Created);
            Assert.Equal(AsDateTime("2023-08-29T19:33:21.163377Z"), ci.Timestamp);
            
            SequencePair sp = ci.Delivered;
            Assert.Equal(1u, sp.ConsumerSeq);
            Assert.Equal(2u, sp.StreamSeq);

            SequenceInfo sinfo = (SequenceInfo)sp;
            Assert.Equal(1u, sinfo.ConsumerSeq);
            Assert.Equal(2u, sinfo.StreamSeq);
            Assert.Equal(AsDateTime("2022-06-29T19:33:21.163377Z"), sinfo.LastActive);

            sp = ci.AckFloor;
            Assert.Equal(3u, sp.ConsumerSeq);
            Assert.Equal(4u, sp.StreamSeq);

            sinfo = (SequenceInfo)sp;
            Assert.Equal(3u, sinfo.ConsumerSeq);
            Assert.Equal(4u, sinfo.StreamSeq);
            Assert.Equal(AsDateTime("2022-06-29T20:33:21.163377Z"), sinfo.LastActive);

            Assert.Equal(24u, ci.NumPending);
            Assert.Equal(42, ci.NumAckPending);
            Assert.Equal(42, ci.NumRedelivered);

            ConsumerConfiguration c = ci.ConsumerConfiguration;
            Assert.Equal("foo-consumer", c.Durable);
            Assert.Equal("bar", c.DeliverSubject);
            Assert.Equal(DeliverPolicy.All, c.DeliverPolicy);
            Assert.Equal(AckPolicy.All, c.AckPolicy);
            Assert.Equal(Duration.OfSeconds(30), c.AckWait);
            Assert.Equal(10, c.MaxDeliver);
            Assert.Equal(ReplayPolicy.Original, c.ReplayPolicy);

            ClusterInfo clusterInfo = ci.ClusterInfo;
            Assert.NotNull(clusterInfo);
            Assert.Equal("clustername", clusterInfo.Name);
            Assert.Equal("clusterleader", clusterInfo.Leader);
            IList<Replica> reps = clusterInfo.Replicas;
            Assert.NotNull(reps);
            Assert.Equal(2, reps.Count);
        }
    }
}
