// Copyright 2024 The NATS Authors
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
using static NATS.Client.Internals.JsonUtils;

namespace NATS.Client.JetStream
{
    public sealed class ConsumerPauseResponse : ApiResponse
    {
        public bool Paused { get; private set; }
        public DateTime? PauseUntil { get; private set; }
        public Duration PauseRemaining { get; private set; }
        
        internal ConsumerPauseResponse(Msg msg, bool throwOnError) : base(msg, throwOnError)
        {
            Init();
        }

        internal ConsumerPauseResponse(string json, bool throwOnError) : base(json, throwOnError)
        {
            Init();
        }

        private void Init()
        {
            Paused = JsonNode[ApiConstants.Paused].AsBool;
            if (Paused)
            {
                PauseUntil = AsDate(JsonNode[ApiConstants.PauseUntil]);
                PauseRemaining = JsonUtils.AsDuration(JsonNode, ApiConstants.PauseRemaining, Duration.Zero);
            }
            else
            {
                PauseUntil = null;
                PauseRemaining = null;
            }
        }
    }
}
