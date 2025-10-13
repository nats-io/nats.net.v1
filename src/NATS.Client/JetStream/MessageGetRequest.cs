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
using NATS.Client.Internals;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class MessageGetRequest : JsonSerializable
    {
        public ulong Sequence { get; }
        public string LastBySubject { get; }
        public string NextBySubject { get; }
        public DateTime? StartTime { get; }

        public bool IsSequenceOnly => Sequence > 0 && NextBySubject == null;
        public bool IsLastBySubject => LastBySubject != null;
        public bool IsNextBySubject => NextBySubject != null;

        public static MessageGetRequest ForSequence(ulong sequence) {
            return new MessageGetRequest(sequence, null, null, null);
        }

        public static MessageGetRequest LastForSubject(string subject) {
            return new MessageGetRequest(0, subject, null, null);
        }

        public static MessageGetRequest FirstForSubject(string subject) {
            return new MessageGetRequest(0, null, subject, null);
        }
        
        public static MessageGetRequest FirstForStartTime(DateTime startTime) {
            return new MessageGetRequest(0, null, null, startTime);
        }

        public static MessageGetRequest FirstForStartTimeAndSubject(DateTime startTime, String subject) {
            return new MessageGetRequest(0, null, subject, startTime);
        }

        public static MessageGetRequest NextForSubject(ulong sequence, string subject) {
            return new MessageGetRequest(sequence, null, subject, null);
        }

        public MessageGetRequest(ulong sequence, string lastBySubject, string nextBySubject) : 
            this(sequence, lastBySubject, nextBySubject, DateTime.MinValue) {}

        public MessageGetRequest(ulong sequence, string lastBySubject, string nextBySubject, DateTime? startTime)
        {
            Sequence = sequence;
            LastBySubject = lastBySubject;
            NextBySubject = nextBySubject;
            StartTime = startTime;
        }

        public override JSONNode ToJsonNode()
        {
            JSONObject jso = new JSONObject();
            if (Sequence > 0)
            {
                jso[ApiConstants.Seq] = Sequence;
            }

            if (LastBySubject != null)
            {
                jso[ApiConstants.LastBySubject] = LastBySubject;
            }

            if (NextBySubject != null)
            {
                jso[ApiConstants.NextBySubject] = NextBySubject;
            }

            JsonUtils.AddField(jso, ApiConstants.StartTime, StartTime);

            return jso;
        }
    }
}
