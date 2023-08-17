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
    public sealed class MessageInfo : ApiResponse
    {
        public string Subject { get; private set; }
        public ulong Sequence { get; private set; }
        public byte[] Data { get; private set; }
        public DateTime Time { get; private set; }
        public MsgHeader Headers { get; private set; }
        public string Stream { get; private set; }
        public ulong LastSequence { get; private set; }

        [Obsolete("This property is obsolete. Use Sequence instead.", false)]
        public long Seq => Convert.ToInt64(Sequence);

        internal MessageInfo(Msg msg, string streamName, bool fromDirect, bool throwOnError) : base(msg, throwOnError, fromDirect)
        {
            Init(msg, fromDirect, streamName);
        }

        public MessageInfo(string json) : base(json)
        {
            Init(null, false, null);
        }

        private void Init(Msg msg, bool fromDirect, string streamName)
        {
            if (fromDirect)
            {
                Headers = msg.Header;
                Subject = msg.Header[JetStreamConstants.NatsSubject];
                Data = msg.Data;
                Sequence = ulong.Parse(msg.Header[JetStreamConstants.NatsSequence]);
                Time = JsonUtils.AsDate(msg.Header[JetStreamConstants.NatsTimestamp]);
                Stream = msg.Header[JetStreamConstants.NatsStream];
                string temp = msg.Header[JetStreamConstants.NatsLastSequence];
                if (temp != null)
                {
                    LastSequence = ulong.Parse(temp);
                }
                // these are control headers, not real headers so don't give them to the user.
                msg.Header.Remove(JetStreamConstants.NatsStream);
                msg.Header.Remove(JetStreamConstants.NatsSequence);
                msg.Header.Remove(JetStreamConstants.NatsTimestamp);
                msg.Header.Remove(JetStreamConstants.NatsSubject);
                msg.Header.Remove(JetStreamConstants.NatsLastSequence);
            }
            else if (!HasError)
            {
                JSONNode miNode = JsonNode[ApiConstants.Message];
                Subject = miNode[ApiConstants.Subject].Value;
                Sequence = miNode[ApiConstants.Seq].AsUlong;
                Time = JsonUtils.AsDate(miNode[ApiConstants.Time]);
                Data = JsonUtils.AsByteArrayFromBase64(miNode[ApiConstants.Data]);
                byte[] bytes = JsonUtils.AsByteArrayFromBase64(miNode[ApiConstants.Hdrs]);
                if (bytes != null)
                {
                    Headers = new HeaderStatusReader(bytes, bytes.Length).Header;
                }
                Stream = streamName;
                LastSequence = 0;
            }
        }
    }
}
