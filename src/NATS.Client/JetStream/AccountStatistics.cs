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

namespace NATS.Client.JetStream
{
    public sealed class AccountStatistics : ApiResponse
    {
        public long Memory { get; private set; }
        public long Storage { get; private set; }
        public long Streams { get; private set; }
        public long Consumers { get; private set; }

        public AccountStatistics(Msg msg, bool throwOnError) : base(msg, throwOnError)
        {
            Init();
        }

        public AccountStatistics(string json, bool throwOnError) : base(json, throwOnError)
        {
            Init();
        }

        private void Init()
        {
            Memory = JsonNode[ApiConstants.Memory].AsLong;
            Storage = JsonNode[ApiConstants.Storage].AsLong;
            Streams = JsonNode[ApiConstants.Streams].AsLong;
            Consumers = JsonNode[ApiConstants.Consumers].AsLong;
        }
    }
}
