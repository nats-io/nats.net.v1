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
        public ulong NumPending { get; private set; }
        public MsgStatus Status { get; private set; }

        public bool IsMessage() => Status == null && !HasError;
        public bool IsStatus() => Status != null;
        public bool IsEobStatus() => Status != null && Status.IsEob();
        public bool IsErrorStatus() => Status != null && !Status.IsEob();
        
        private bool fromDirect;

        public override string ToString()
        {
            if (Status != null)
            {
                return $"MessageInfo: status_code {Status.Code}, status_message {Status.Message}";
            }

            if (HasError)
            {
                return $"MessageInfo: error {Error}";
            }

            String dataStr = Data == null ? "Data: null" : $"Data Length: {Data.Length}"; 
            return $"MessageInfo: Sequence: {Sequence}, LastSequence: {LastSequence}, NumPending: {NumPending}, Stream: {Stream}, Subject: {Subject}, Time: {Time}, {dataStr}, FromDirect: {fromDirect}";
        }

        [Obsolete("This property is obsolete. Use Sequence instead.", false)]
        public long Seq => Convert.ToInt64(Sequence);

        internal MessageInfo(Msg msg, string streamName, bool fromDirect, bool throwOnError) 
            : base(msg, throwOnError, fromDirect)
        {
            Init(msg, null, streamName, fromDirect);
        }

        internal MessageInfo(MsgStatus status, string streamName, bool fromDirect) 
            : base(null, fromDirect, true)
        {
            Init(null, status, streamName, fromDirect);
        }

        public MessageInfo(string json) 
            : base(json)
        {
            Init(null, null, null, false);
        }

        private void Init(Msg msg, MsgStatus status, string streamName, bool fromDirect)
        {
            this.fromDirect = fromDirect;

            if (status != null)
            {
                Status = status;
                Stream = streamName;
            }
            else if (fromDirect)
            {
                Headers = msg.Header;
                Subject = msg.Header.GetLast(JetStreamConstants.NatsSubject);
                Data = msg.Data;
                Sequence = ulong.Parse(msg.Header.GetLast(JetStreamConstants.NatsSequence));
                Time = JsonUtils.AsDate(msg.Header.GetLast(JetStreamConstants.NatsTimestamp));
                Stream = msg.Header.GetLast(JetStreamConstants.NatsStream);
                string temp = msg.Header.GetLast(JetStreamConstants.NatsLastSequence);
                if (temp != null)
                {
                    LastSequence = ulong.Parse(temp);
                }
                temp = msg.Header.GetLast(JetStreamConstants.NatsNumPending);
                if (temp != null)
                {
                    NumPending = ulong.Parse(temp) - 1;
                }
                // these are control headers, not real headers so don't give them to the user. Must be done last
                Headers.Remove(JetStreamConstants.NatsStream);
                Headers.Remove(JetStreamConstants.NatsSequence);
                Headers.Remove(JetStreamConstants.NatsTimestamp);
                Headers.Remove(JetStreamConstants.NatsSubject);
                Headers.Remove(JetStreamConstants.NatsLastSequence);
                Headers.Remove(JetStreamConstants.NatsNumPending);
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
            }
        }
    }
}
