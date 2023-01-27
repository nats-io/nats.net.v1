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

using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class MessageGetRequest : JsonSerializable
    {
        public ulong Sequence { get; }
        public string LastBySubject { get; }
        public string NextBySubject { get; }

        public bool IsSequenceOnly => Sequence > 0 && NextBySubject == null;
        public bool IsLastBySubject => LastBySubject != null;
        public bool IsNextBySubject => NextBySubject != null;

        public static MessageGetRequest ForSequence(ulong sequence) {
            return new MessageGetRequest(sequence, null, null);
        }

        public static MessageGetRequest LastForSubject(string subject) {
            return new MessageGetRequest(0, subject, null);
        }

        public static MessageGetRequest FirstForSubject(string subject) {
            return new MessageGetRequest(0, null, subject);
        }

        public static MessageGetRequest NextForSubject(ulong sequence, string subject) {
            return new MessageGetRequest(sequence, null, subject);
        }

        public MessageGetRequest(ulong sequence, string lastBySubject, string nextBySubject)
        {
            Sequence = sequence;
            LastBySubject = lastBySubject;
            NextBySubject = nextBySubject;
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

            return jso;
        }
    }
}
