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
    public sealed class StreamState
    {
        public ulong Messages { get; }
        public ulong Bytes { get; }
        public ulong FirstSeq { get; }
        public ulong LastSeq { get; }
        public long ConsumerCount { get; }
        public long SubjectCount { get; }
        public long DeletedCount { get; }
        public DateTime FirstTime { get; }
        public DateTime LastTime { get; }
        public IList<Subject> Subjects { get; }
        public IList<ulong> Deleted { get; }
        public LostStreamData LostStreamData { get; }

        internal static StreamState OptionalInstance(JSONNode streamState)
        {
            return streamState == null || streamState.Count == 0 ? null : new StreamState(streamState);
        }

        private StreamState(JSONNode streamState)
        {
            Messages = streamState[ApiConstants.Messages].AsUlong;
            Bytes = streamState[ApiConstants.Bytes].AsUlong;
            FirstSeq = streamState[ApiConstants.FirstSeq].AsUlong;
            LastSeq = streamState[ApiConstants.LastSeq].AsUlong;
            ConsumerCount = streamState[ApiConstants.ConsumerCount].AsLong;
            SubjectCount = streamState[ApiConstants.NumSubjects].AsLong;
            DeletedCount = streamState[ApiConstants.NumDeleted].AsLong;
            FirstTime = JsonUtils.AsDate(streamState[ApiConstants.FirstTs]);
            LastTime = JsonUtils.AsDate(streamState[ApiConstants.LastTs]);
            Subjects = Subject.GetList(streamState[ApiConstants.Subjects]);

            Deleted = new List<ulong>();
            JSONNode.Enumerator e = 
                streamState[ApiConstants.Deleted].AsArray.GetEnumerator();
            while (e.MoveNext())
            {
                Deleted.Add(e.Current.Value.AsUlong);
            }      
            
            LostStreamData = LostStreamData.OptionalInstance(streamState[ApiConstants.Lost]);
        }

        internal void AddAll(IList<Subject> optional)
        {
            if (optional != null && optional.Count > 0)
            {
                foreach (Subject s in optional)
                {
                    Subjects.Add(s);
                }
            }
        }

        public override string ToString()
        {
            return "StreamState{" +
                   "Messages=" + Messages +
                   ", Bytes=" + Bytes +
                   ", FirstSeq=" + FirstSeq +
                   ", LastSeq=" + LastSeq +
                   ", ConsumerCount=" + ConsumerCount +
                   ", SubjectCount=" + SubjectCount +
                   ", DeletedCount=" + DeletedCount +
                   ", FirstTime=" + FirstTime +
                   ", LastTime=" + LastTime +
                   '}';
        }
    }
}
