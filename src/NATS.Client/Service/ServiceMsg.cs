// Copyright 2023 The NATS Authors
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
using NATS.Client.JetStream;

namespace NATS.Client.Service
{
    /// <summary>
    /// SERVICE IS AN EXPERIMENTAL API SUBJECT TO CHANGE
    /// </summary>
    public class ServiceMsg : Msg
    {
        public const string NatsServiceError = "Nats-Service-Error";
        public const string NatsServiceErrorCode = "Nats-Service-Error-Code";

        internal ServiceMsg(Msg msg) : base(msg) {}

        public void Respond(IConnection conn, byte[] response) {
            conn.Publish(new Msg(_reply, null, null, response));
        }

        public void Respond(IConnection conn, string response) {
            conn.Publish(new Msg(_reply, null, null, Encoding.UTF8.GetBytes(response)));
        }

        public void Respond(IConnection conn, JsonSerializable response) {
            conn.Publish(new Msg(_reply, null, null, response.Serialize()));
        }

        public void Respond(IConnection conn, JSONNode response) {
            conn.Publish(new Msg(_reply, null, null, Encoding.UTF8.GetBytes(response.ToString())));
        }

        public void Respond(IConnection conn, byte[] response, MsgHeader headers)
        {
            conn.Publish(new Msg(_reply, null, headers, response));
        }

        public void Respond(IConnection conn, string response, MsgHeader headers)
        {
            conn.Publish(new Msg(_reply, null, headers, Encoding.UTF8.GetBytes(response)));
        }

        public void Respond(IConnection conn, JsonSerializable response, MsgHeader headers) {
            conn.Publish(new Msg(_reply, null, headers, response.Serialize()));
        }

        public void Respond(IConnection conn, JSONNode response, MsgHeader headers) {
            conn.Publish(new Msg(_reply, null, headers, Encoding.UTF8.GetBytes(response.ToString())));
        }

        public void RespondStandardError(IConnection conn, string errorMessage, int errorCode)
        {
            conn.Publish(new Msg(_reply, null, 
                new MsgHeader 
                {
                    [NatsServiceError] = errorMessage,
                    [NatsServiceErrorCode] = $"{errorCode}" 
                }, 
                null));
        }    
    }
}
