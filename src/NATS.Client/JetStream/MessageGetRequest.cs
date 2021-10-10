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

using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public sealed class MessageGetRequest : JsonSerializable
    {
        public ulong Sequence { get; }
        public string LastBySubject { get; }

        internal static byte[] SeqBytes(ulong sequence) {
            return new MessageGetRequest(sequence).Serialize();
        }

        internal static byte[] LastBySubjectBytes(string lastBySubject) {
            return new MessageGetRequest(lastBySubject).Serialize();
        }

        internal MessageGetRequest(ulong sequence)
        {
            Sequence = sequence;
        }

        public MessageGetRequest(string lastBySubject)
        {
            LastBySubject = lastBySubject;
        }

        internal override JSONNode ToJsonNode()
        {
            return new JSONObject
            {
                [ApiConstants.Seq] = Sequence,
                [ApiConstants.LastBySubject] = LastBySubject
            };
        }
    }
}