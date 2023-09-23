﻿// Copyright 2020 The NATS Authors
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
    public class TestStreamInfo : TestBase
    {
        [Fact]
        public void JsonIsReadProperly()
        {
            string json = ReadDataFile("StreamInfo.json");
            StreamInfo si = new StreamInfo(json, false);
            Assert.Equal(AsDateTime("2021-01-25T20:09:10.6225191Z"), si.Created);
            Assert.Equal(AsDateTime("2023-08-29T19:33:21.163377Z"), si.Timestamp);
            
            // StreamConfiguration Config
            Assert.Equal("streamName", si.Config.Name);
            Assert.Equal(3, si.Config.Subjects.Count);
            Assert.Equal("sub0", si.Config.Subjects[0]);
            Assert.Equal("sub1", si.Config.Subjects[1]);
            Assert.Equal("x.>", si.Config.Subjects[2]);
            Assert.Equal(RetentionPolicy.Limits, si.Config.RetentionPolicy);
            Assert.Equal(1, si.Config.MaxConsumers);
            Assert.Equal(2u, si.Config.MaxMsgs);
            Assert.Equal(3u, si.Config.MaxBytes);
            Assert.Equal(DiscardPolicy.Old, si.Config.DiscardPolicy);
            Assert.Equal(100000000000, si.Config.MaxAge.Nanos);
            Assert.Equal(4, si.Config.MaxMsgSize);
            Assert.Equal(StorageType.Memory, si.Config.StorageType);
            Assert.Equal(5, si.Config.Replicas);
            Assert.Equal(120000000000, si.Config.DuplicateWindow.Nanos);
            Assert.Equal("placementclstr", si.Config.Placement.Cluster);
            Assert.Equal(2, si.Config.Placement.Tags.Count);
            Assert.Equal("ptag1", si.Config.Placement.Tags[0]);
            Assert.Equal("ptag2", si.Config.Placement.Tags[1]);
            
            // StreamState State
            Assert.Equal(11ul, si.State.Messages);
            Assert.Equal(12ul, si.State.Bytes);
            Assert.Equal(13ul, si.State.FirstSeq);
            Assert.Equal(14ul, si.State.LastSeq);
            Assert.Equal(15, si.State.ConsumerCount);
            Assert.Equal(3, si.State.SubjectCount);
            Assert.Equal(3, si.State.Subjects.Count);

            Dictionary<string, Subject> map = new Dictionary<string, Subject>();
            foreach (Subject su in si.State.Subjects) {
                map[su.Name] = su;
            }

            Subject s = map["sub0"];
            Assert.NotNull(s);
            Assert.Equal(1, s.Count);

            s = map["sub1"];
            Assert.NotNull(s);
            Assert.Equal(2, s.Count);

            s = map["x.foo"];
            Assert.NotNull(s);
            Assert.Equal(3, s.Count);

            Assert.Equal(6, si.State.DeletedCount);
            Assert.Equal(6, si.State.Deleted.Count);
            for (ulong x = 91; x <= 96; x++) {
                Assert.True(si.State.Deleted.Contains(x));
            }

            LostStreamData lost = si.State.LostStreamData;
            Assert.NotNull(lost);
            Assert.Equal(3, lost.Messages.Count);
            for (ulong x = 101; x <= 103; x++) {
                Assert.True(lost.Messages.Contains(x));
            }
            Assert.Equal(104U, lost.Bytes);

            Assert.Equal(AsDateTime("0001-01-01T00:00:00Z"), si.State.FirstTime);
            Assert.Equal(AsDateTime("0002-01-01T00:00:00Z"), si.State.LastTime);
            
            // ClusterInfo ClusterInfo
            Assert.Equal("clustername", si.ClusterInfo.Name);
            Assert.Equal("clusterleader", si.ClusterInfo.Leader);
            Assert.Equal(2, si.ClusterInfo.Replicas.Count);
            Assert.Equal("name0", si.ClusterInfo.Replicas[0].Name);
            Assert.True(si.ClusterInfo.Replicas[0].Current);
            Assert.True(si.ClusterInfo.Replicas[0].Offline);
            Assert.Equal(230000000000, si.ClusterInfo.Replicas[0].Active.Nanos);
            Assert.Equal(3, si.ClusterInfo.Replicas[0].Lag);
            Assert.Equal("name1", si.ClusterInfo.Replicas[1].Name);
            Assert.False(si.ClusterInfo.Replicas[1].Current);
            Assert.False(si.ClusterInfo.Replicas[1].Offline);
            Assert.Equal(240000000000, si.ClusterInfo.Replicas[1].Active.Nanos);
            Assert.Equal(4, si.ClusterInfo.Replicas[1].Lag);
            
            // MirrorInfo MirrorInfo
            Assert.Equal("mname", si.MirrorInfo.Name);
            Assert.Equal(16u, si.MirrorInfo.Lag);
            Assert.Equal(160000000000, si.MirrorInfo.Active.Nanos);
            Assert.Null(si.MirrorInfo.Error);
            ValidateExternal(si.MirrorInfo.External, 16);
            
            // List<SourceInfo> SourceInfos
            Assert.Equal(2, si.SourceInfos.Count);
            ValidateSourceInfo(si.SourceInfos[0], 17);
            ValidateSourceInfo(si.SourceInfos[1], 18);
        }
        
        private static void ValidateSourceInfo(SourceInfo sourceInfo, ulong id) {
            Assert.Equal("sname" + id, sourceInfo.Name);
            Assert.Equal(id, sourceInfo.Lag);
            Assert.Equal(Duration.OfNanos((long)id * 10000000000L), sourceInfo.Active);
            ValidateExternal(sourceInfo.External, id);
            TestStreamConfiguration.ValidateSubjectTransforms(sourceInfo.SubjectTransforms, 2, "" + id);
        }

        private static void ValidateExternal(External e, ulong id) {
            Assert.NotNull(e);
            Assert.Equal("api" + id, e.Api);
            Assert.Equal("dlvr" + id, e.Deliver);
        }

    }
}
