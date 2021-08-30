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

using System.Text;
using NATS.Client.Internals.SimpleJSON;

namespace NATS.Client.JetStream
{
    public class ApiResponse
    {
        public const string NoType = "io.nats.jetstream.api.v1.no_type";

        public string Type { get; }
        public Error Error { get; }

        internal JSONNode JsonNode { get; }

        internal ApiResponse() {}

        internal ApiResponse(Msg msg, bool throwOnError = false) : 
            this(Encoding.UTF8.GetString(msg.Data), throwOnError) {}

        internal ApiResponse(string json, bool throwOnError = false)
        {
            JsonNode = JSON.Parse(json);
            Type = JsonNode[ApiConstants.Type].Value;
            if (string.IsNullOrWhiteSpace(Type))
            {
                Type = NoType;
            }
            Error = Error.OptionalInstance(JsonNode[ApiConstants.Error]);

            if (throwOnError)
            {
                ThrowOnHasError();
            }
        }
        
        public void ThrowOnHasError() {
            if (HasError) {
                throw new NATSJetStreamException(this);
            }
        }

        public bool HasError => Error != null;

        public int ErrorCode => Error?.Code ?? -1;

        public int ApiErrorCode => Error?.ApiErrorCode ?? -1;

        public string ErrorDescription => Error?.Desc ?? null;
    }
}
