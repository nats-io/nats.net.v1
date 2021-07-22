// Copyright 2015-2020 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

namespace NATS.Client
{
    public sealed class MsgStatus
    {
        private const string FlowControlText = "FlowControl Request";
        private const string HeartbeatText = "Idle Heartbeat";
        private const string NoRespondersText = "No Responders Available For Request";
        private const int FlowOrHeartbeatStatusCode = 100;
        
        private readonly int _code;
        private readonly string _message;
        
        public MsgStatus(int code, string message)
        {
            _code = code;
            _message = message ?? MakeMessage(code);
        }

        public MsgStatus(Token codeToken, Token messageToken) : this(ExtractCode(codeToken), ExtractMessage(messageToken)) {}

        public int Code => _code;

        public string Message => _message;

        private static string ExtractMessage(Token messageToken) {
            return messageToken.HasValue() ? messageToken.Value() : null;
        }

        private static int ExtractCode(Token codeToken) {
            try {
                return int.Parse(codeToken.Value());
            }
            catch (Exception) {
                throw new NATSInvalidHeaderException(NatsConstants.InvalidHeaderStatusCode);
            }
        }

        private string MakeMessage(int code)
        {
            switch (code)
            {
                case NatsConstants.NoRespondersCode: return NoRespondersText;
                default: return "Server Status Message: " + code;
            }
        }

        private bool IsStatus(int code, string text) {
            return _code == code && _message.Equals(text);
        }

        public bool IsFlowControl() {
            return IsStatus(FlowOrHeartbeatStatusCode, FlowControlText);
        }

        public bool IsHeartbeat() {
            return IsStatus(FlowOrHeartbeatStatusCode, HeartbeatText);
        }

        public bool IsNoResponders() {
            return IsStatus(NatsConstants.NoRespondersCode, NoRespondersText);
        }
    }
}
