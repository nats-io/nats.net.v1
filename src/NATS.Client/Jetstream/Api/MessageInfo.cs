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

namespace NATS.Client.Jetstream.Api
{
    public sealed class MessageInfo : ApiResponse
    {
        public string Subject { get; }
        public long Seq { get; }
        public byte[] Data { get; }
        public DateTime Time { get; }
        public MsgHeader Headers { get; }

        public MessageInfo(string json) : base(json)
        {
            var miNode = JsonNode[ApiConstants.Message];
            Subject = miNode[ApiConstants.Subject].Value;
            Seq = miNode[ApiConstants.Seq].AsLong;
            Time = JsonUtils.AsDate(miNode[ApiConstants.Time]);
            Data = JsonUtils.AsByteArrayFromBase64(miNode[ApiConstants.Data]);
            byte[] bytes = JsonUtils.AsByteArrayFromBase64(miNode[ApiConstants.Hdrs]);
            if (bytes != null)
            {
                Headers = new MsgHeader(bytes, bytes.Length);
            }
        }
    }
}
