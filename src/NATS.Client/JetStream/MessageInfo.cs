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
        public long Seq { get; private set; }
        public byte[] Data { get; private set; }
        public DateTime Time { get; private set; }
        public MsgHeader Headers { get; private set; }

        internal MessageInfo(Msg msg, bool throwOnError) : base(msg, throwOnError)
        {
            Init();
        }

        public MessageInfo(string json) : base(json)
        {
            Init();
        }

        private void Init()
        {
            JSONNode miNode = JsonNode[ApiConstants.Message];
            Subject = miNode[ApiConstants.Subject].Value;
            Seq = miNode[ApiConstants.Seq].AsLong;
            Time = JsonUtils.AsDate(miNode[ApiConstants.Time]);
            Data = JsonUtils.AsByteArrayFromBase64(miNode[ApiConstants.Data]);
            byte[] bytes = JsonUtils.AsByteArrayFromBase64(miNode[ApiConstants.Hdrs]);
            if (bytes != null)
            {
                Headers = new HeaderStatusReader(bytes, bytes.Length).Header;
            }
        }
    }
}
